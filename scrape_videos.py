import json
import requests
from bs4 import BeautifulSoup
import threading
import queue
import time
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
import os
from urllib.parse import urljoin
import logging

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(message)s',
    handlers=[
        logging.FileHandler('scraper.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Load config
try:
    with open('config.json', 'r') as f:
        config = json.load(f)
except Exception as e:
    logger.error(f"Failed to load config.json: {str(e)}")
    raise

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
total_pages_scraped = 0

def convert_views(views_str):
    """Convert views string (e.g., '128.67K', '1.5M') to integer."""
    views_str = views_str.lower().replace(',', '')
    try:
        if 'k' in views_str:
            return int(float(views_str.replace('k', '')) * 1000)
        elif 'm' in views_str:
            return int(float(views_str.replace('m', '')) * 1000000)
        return int(views_str)
    except:
        return 0

def scrape_page(page_num):
    """Scrape data from a single page."""
    global total_pages_scraped
    try:
        if page_num == 1:
            url = DOMAIN
        else:
            url = f"{DOMAIN}/page/{page_num}/"
        
        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')
        
        items = soup.find_all('div', class_='item-video')
        if not items:
            global stop_scraping
            with data_lock:
                stop_scraping = True
                logger.info(f"Last page: {page_num}, found 0 items")
            return
        
        page_data = []
        for item in items:
            try:
                classes = item.get('class', [])
                post_id = next((c.replace('post-', '') for c in classes if c.startswith('post-')), None)
                if not post_id:
                    continue
                
                title_elem = item.find('h2', class_='entry-title')
                title = title_elem.find('a').text if title_elem and title_elem.find('a') else ''
                title = title.lstrip('Permalink to ').strip()  # Remove "Permalink to " from title
                
                link_elem = item.find('a', class_='clip-link')
                link = urljoin(DOMAIN, link_elem.get('href')) if link_elem else ''
                
                img_elem = item.find('img')
                thumbnail = urljoin(DOMAIN, img_elem.get('src')) if img_elem else ''
                
                views_elem = item.find('span', class_='views')
                views = convert_views(views_elem.find('i', class_='count').text) if views_elem and views_elem.find('i', class_='count') else 0
                
                comments_elem = item.find('span', class_='comments')
                comments = int(comments_elem.find('i', class_='count').text) if comments_elem and comments_elem.find('i', class_='count') else 0
                
                likes_elem = item.find('span', class_='dp-post-likes')
                likes = int(likes_elem.find('i', class_='count').text) if likes_elem and likes_elem.find('i', class_='count') else 0
                
                date_elem = item.find('time', class_='entry-date')
                date = date_elem.get('datetime') if date_elem else ''
                
                author_elem = item.find('span', class_='author')
                author = author_elem.find('a').text if author_elem and author_elem.find('a') else ''
                
                summary_elem = item.find('p', class_='entry-summary')
                summary = summary_elem.text.strip() if summary_elem else ''
                summary = summary.lstrip('Video ').strip()  # Remove "Video " from summary
                
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
            
            except:
                continue
        
        with data_lock:
            all_video_data.extend(page_data)
            total_pages_scraped += 1
            if total_pages_scraped % 100 == 0:
                logger.info(f"Found {len(all_video_data)} items on pages {total_pages_scraped-99} to {total_pages_scraped}")
        
        time.sleep(DETAIL_DELAY)
    
    except Exception as e:
        logger.error(f"Error scraping page {page_num}: {str(e)}")

def worker():
    """Worker thread to process pages from queue."""
    while not stop_scraping:
        try:
            page_num = page_queue.get_nowait()
            scrape_page(page_num)
            page_queue.task_done()
        except queue.Empty:
            break
        except Exception as e:
            logger.error(f"Worker error: {str(e)}")

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
    try:
        with open(DATA_TXT, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        
        df = pd.DataFrame(data)
        if not df.empty:
            df['id'] = pd.to_numeric(df['id'], errors='coerce')
            df = df.sort_values(by=['page', 'id'], ascending=[True, False])
            df.to_csv(TEMP_CSV, index=False, encoding='utf-8')
            
            try:
                creds = Credentials.from_service_account_file(CREDENTIALS_FILE, scopes=SCOPE)
                client = gspread.authorize(creds)
                sheet = client.open_by_key(SHEET_ID).sheet1
                sheet.clear()
                sheet.update([df.columns.values.tolist()] + df.values.tolist())
            except Exception as e:
                logger.error(f"Error updating Google Sheets: {str(e)}")
    
    except Exception as e:
        logger.error(f"Error saving data: {str(e)}")

def main():
    logger.info("Starting scraper")
    existing_data = load_existing_data()
    existing_ids = {item['id'] for item in existing_data}
    existing_links = {item['link'] for item in existing_data}
    
    page_num = 1
    max_pages = 1000
    batch_size = 100
    while page_num <= max_pages and not stop_scraping:
        start_page = page_num
        end_page = min(page_num + batch_size - 1, max_pages)
        
        # Enqueue pages for this batch
        for i in range(start_page, end_page + 1):
            page_queue.put(i)
        
        # Start processing batch
        logger.info(f"Processing pages {start_page} to {end_page}")
        threads = []
        for i in range(NUM_THREADS):
            t = threading.Thread(target=worker, name=f"Worker-{i}")
            t.start()
            threads.append(t)
        
        # Wait for threads to complete this batch
        for t in threads:
            t.join()
        
        page_num += batch_size
    
    with data_lock:
        unique_data = existing_data[:]
        new_ids = {item['id'] for item in all_video_data}
        new_links = {item['link'] for item in all_video_data}
        unique_data.extend([item for item in all_video_data if item['id'] not in existing_ids and item['link'] not in existing_links])
        logger.info(f"Total: scraped {total_pages_scraped} pages, found {len(all_video_data)} new items, {len(unique_data)} total items (including existing)")
    
    save_data(unique_data)

if __name__ == '__main__':
    try:
        main()
    except Exception as e:
        logger.error(f"Main function error: {str(e)}")
