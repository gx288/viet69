import json
import requests
from bs4 import BeautifulSoup
import threading
import queue
import time
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
import re
import os
from urllib.parse import urljoin
import uuid

# Load config
with open('config.json', 'r') as f:
    config = json.load(f)

DOMAIN = config['DOMAIN']
NUM_THREADS = config['NUM_THREADS']
DETAIL_DELAY = config['DETAIL_DELAY']
DATA_TXT = config['DATA_TXT']
TEMP_CSV = config['TEMP_CSV']
SCOPE = config['SCOPE']
CREDENTIALS_FILE = config['CREDENTIALS_FILE']
SHEET_ID = config['SHEET_ID']

# Headers for requests
headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'}

# Thread-safe queue and lock
page_queue = queue.Queue()
all_video_data = []
data_lock = threading.Lock()
stop_scraping = False

def convert_views(views_str):
    """Convert views string (e.g., '128.67K', '1.5M') to integer."""
    views_str = views_str.lower().replace(',', '')
    if 'k' in views_str:
        return int(float(views_str.replace('k', '')) * 1000)
    elif 'm' in views_str:
        return int(float(views_str.replace('m', '')) * 1000000)
    return int(views_str)

def scrape_page(page_num):
    """Scrape data from a single page."""
    try:
        if page_num == 1:
            url = DOMAIN
        else:
            url = f"{DOMAIN}/page/{page_num}/"
        
        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')
        
        items = soup.find_all('div', class_='item cf')
        if not items:
            global stop_scraping
            with data_lock:
                stop_scraping = True
            return
        
        page_data = []
        for item in items:
            # Extract post ID from class (e.g., post-70968)
            classes = item.get('class', [])
            post_id = next((c.replace('post-', '') for c in classes if c.startswith('post-')), None)
            if not post_id:
                continue
            
            # Extract title
            title_elem = item.find('h2', class_='entry-title')
            title = title_elem.find('a').get('title') if title_elem and title_elem.find('a') else ''
            
            # Extract link
            link_elem = item.find('a', class_='clip-link')
            link = urljoin(DOMAIN, link_elem.get('href')) if link_elem else ''
            
            # Extract thumbnail
            img_elem = item.find('img')
            thumbnail = urljoin(DOMAIN, img_elem.get('src')) if img_elem else ''
            
            # Extract views
            views_elem = item.find('span', class_='views')
            views = convert_views(views_elem.find('i', class_='count').text) if views_elem and views_elem.find('i', class_='count') else 0
            
            # Extract comments
            comments_elem = item.find('span', class_='comments')
            comments = int(comments_elem.find('i', class_='count').text) if comments_elem and comments_elem.find('i', class_='count') else 0
            
            # Extract likes
            likes_elem = item.find('span', class_='dp-post-likes')
            likes = int(likes_elem.find('i', class_='count').text) if likes_elem and likes_elem.find('i', class_='count') else 0
            
            # Extract date
            date_elem = item.find('time', class_='entry-date')
            date = date_elem.get('datetime') if date_elem else ''
            
            # Extract author
            author_elem = item.find('span', class_='author')
            author = author_elem.find('a').text if author_elem and author_elem.find('a') else ''
            
            # Extract summary
            summary_elem = item.find('p', class_='entry-summary')
            summary = summary_elem.text.strip() if summary_elem else ''
            
            video_data = {
                'page': page_num,
                'id': post_id,
                'title': title,
                'link': link,
                'thumbnail': thumbnail,
                'views': views,
                'comments': comments,
                'likes': likes,
                'date': date,
                'author': author,
                'summary': summary
            }
            page_data.append(video_data)
        
        with data_lock:
            all_video_data.extend(page_data)
        
        time.sleep(DETAIL_DELAY)
    
    except Exception as e:
        print(f"Error scraping page {page_num}: {e}")

def worker():
    """Worker thread to process pages from queue."""
    while not stop_scraping:
        try:
            page_num = page_queue.get_nowait()
        except queue.Empty:
            break
        scrape_page(page_num)
        page_queue.task_done()

def load_existing_data():
    """Load existing data from data.txt if it exists."""
    if os.path.exists(DATA_TXT):
        try:
            with open(DATA_TXT, 'r', encoding='utf-8') as f:
                return json.load(f)
        except:
            return []
    return []

def save_data(data):
    """Save data to data.txt and update Google Sheets."""
    # Save to data.txt
    with open(DATA_TXT, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    
    # Convert to DataFrame and sort
    df = pd.DataFrame(data)
    if not df.empty:
        df['id'] = pd.to_numeric(df['id'], errors='coerce')
        df = df.sort_values(by=['page', 'id'], ascending=[True, False])
        
        # Save to temp CSV
        df.to_csv(TEMP_CSV, index=False, encoding='utf-8')
        
        # Update Google Sheets
        try:
            creds = Credentials.from_service_account_file(CREDENTIALS_FILE, scopes=SCOPE)
            client = gspread.authorize(creds)
            sheet = client.open_by_key(SHEET_ID).sheet1
            sheet.clear()
            sheet.update([df.columns.values.tolist()] + df.values.tolist())
        except Exception as e:
            print(f"Error updating Google Sheets: {e}")

def main():
    # Load existing data
    existing_data = load_existing_data()
    existing_ids = {item['id'] for item in existing_data}
    existing_links = {item['link'] for item in existing_data}
    
    # Populate page queue
    page_num = 1
    while True:
        page_queue.put(page_num)
        page_num += 1
        # Arbitrary large number to prevent infinite loop, adjust if needed
        if page_num > 1000 or stop_scraping:
            break
    
    # Start worker threads
    threads = []
    for _ in range(NUM_THREADS):
        t = threading.Thread(target=worker)
        t.start()
        threads.append(t)
    
    # Wait for all threads to complete
    for t in threads:
        t.join()
    
    # Merge new data with existing, keeping unique by id and link
    with data_lock:
        unique_data = existing_data[:]
        new_ids = {item['id'] for item in all_video_data}
        new_links = {item['link'] for item in all_video_data}
        unique_data.extend([item for item in all_video_data if item['id'] not in existing_ids and item['link'] not in existing_links])
    
    # Save data
    save_data(unique_data)

if __name__ == '__main__':
    main()
