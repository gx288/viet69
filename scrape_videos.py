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
import logging
import traceback

# Set up logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('scraper.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Load config
logger.info("Loading config.json")
try:
    with open('config.json', 'r') as f:
        config = json.load(f)
    logger.debug(f"Config loaded: {config}")
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
logger.debug(f"Headers: {headers}")

# Thread-safe queue and lock
page_queue = queue.Queue()
all_video_data = []
data_lock = threading.Lock()
stop_scraping = False

def convert_views(views_str):
    """Convert views string (e.g., '128.67K', '1.5M') to integer."""
    logger.debug(f"Converting views: {views_str}")
    views_str = views_str.lower().replace(',', '')
    try:
        if 'k' in views_str:
            return int(float(views_str.replace('k', '')) * 1000)
        elif 'm' in views_str:
            return int(float(views_str.replace('m', '')) * 1000000)
        return int(views_str)
    except Exception as e:
        logger.error(f"Error converting views '{views_str}': {str(e)}")
        return 0

def scrape_page(page_num):
    """Scrape data from a single page."""
    logger.info(f"Scraping page {page_num}")
    try:
        if page_num == 1:
            url = DOMAIN
        else:
            url = f"{DOMAIN}/page/{page_num}/"
        logger.debug(f"Requesting URL: {url}")
        
        response = requests.get(url, headers=headers, timeout=10)
        logger.debug(f"Status code for page {page_num}: {response.status_code}")
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')
        
        items = soup.find_all('div', class_='item cf')
        logger.info(f"Found {len(items)} items on page {page_num}")
        if not items:
            global stop_scraping
            with data_lock:
                stop_scraping = True
            logger.info(f"No items found, setting stop_scraping to True")
            return
        
        page_data = []
        for i, item in enumerate(items):
            logger.debug(f"Processing item {i+1} on page {page_num}")
            try:
                # Extract post ID from class (e.g., post-70968)
                classes = item.get('class', [])
                post_id = next((c.replace('post-', '') for c in classes if c.startswith('post-')), None)
                if not post_id:
                    logger.warning(f"No post_id found for item {i+1} on page {page_num}")
                    continue
                
                # Extract title
                title_elem = item.find('h2', class_='entry-title')
                title = title_elem.find('a').get('title') if title_elem and title_elem.find('a') else ''
                logger.debug(f"Title: {title}")
                
                # Extract link
                link_elem = item.find('a', class_='clip-link')
                link = urljoin(DOMAIN, link_elem.get('href')) if link_elem else ''
                logger.debug(f"Link: {link}")
                
                # Extract thumbnail
                img_elem = item.find('img')
                thumbnail = urljoin(DOMAIN, img_elem.get('src')) if img_elem else ''
                logger.debug(f"Thumbnail: {thumbnail}")
                
                # Extract views
                views_elem = item.find('span', class_='views')
                views = convert_views(views_elem.find('i', class_='count').text) if views_elem and views_elem.find('i', class_='count') else 0
                logger.debug(f"Views: {views}")
                
                # Extract comments
                comments_elem = item.find('span', class_='comments')
                comments = int(comments_elem.find('i', class_='count').text) if comments_elem and comments_elem.find('i', class_='count') else 0
                logger.debug(f"Comments: {comments}")
                
                # Extract likes
                likes_elem = item.find('span', class_='dp-post-likes')
                likes = int(likes_elem.find('i', class_='count').text) if likes_elem and likes_elem.find('i', class_='count') else 0
                logger.debug(f"Likes: {likes}")
                
                # Extract date
                date_elem = item.find('time', class_='entry-date')
                date = date_elem.get('datetime') if date_elem else ''
                logger.debug(f"Date: {date}")
                
                # Extract author
                author_elem = item.find('span', class_='author')
                author = author_elem.find('a').text if author_elem and author_elem.find('a') else ''
                logger.debug(f"Author: {author}")
                
                # Extract summary
                summary_elem = item.find('p', class_='entry-summary')
                summary = summary_elem.text.strip() if summary_elem else ''
                logger.debug(f"Summary: {summary[:50]}..." if len(summary) > 50 else f"Summary: {summary}")
                
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
                logger.debug(f"Item {i+1} processed successfully")
            
            except Exception as e:
                logger.error(f"Error processing item {i+1} on page {page_num}: {str(e)}")
                logger.debug(traceback.format_exc())
        
        with data_lock:
            all_video_data.extend(page_data)
            logger.info(f"Added {len(page_data)} items from page {page_num} to all_video_data")
        
        time.sleep(DETAIL_DELAY)
    
    except Exception as e:
        logger.error(f"Error scraping page {page_num}: {str(e)}")
        logger.debug(traceback.format_exc())

def worker():
    """Worker thread to process pages from queue."""
    logger.info(f"Starting worker thread {threading.current_thread().name}")
    while not stop_scraping:
        try:
            page_num = page_queue.get_nowait()
            logger.debug(f"Worker {threading.current_thread().name} processing page {page_num}")
            scrape_page(page_num)
            page_queue.task_done()
        except queue.Empty:
            logger.info(f"Queue empty, worker {threading.current_thread().name} exiting")
            break
        except Exception as e:
            logger.error(f"Worker {threading.current_thread().name} error: {str(e)}")
            logger.debug(traceback.format_exc())

def load_existing_data():
    """Load existing data from data.txt if it exists."""
    logger.info(f"Loading existing data from {DATA_TXT}")
    if os.path.exists(DATA_TXT):
        try:
            with open(DATA_TXT, 'r', encoding='utf-8') as f:
                data = json.load(f)
            logger.info(f"Loaded {len(data)} existing items from {DATA_TXT}")
            return data
        except Exception as e:
            logger.error(f"Error loading {DATA_TXT}: {str(e)}")
            logger.debug(traceback.format_exc())
            return []
    logger.info(f"No existing {DATA_TXT} found")
    return []

def save_data(data):
    """Save data to data.txt and update Google Sheets."""
    logger.info(f"Saving {len(data)} items to {DATA_TXT} and Google Sheets")
    try:
        # Save to data.txt
        with open(DATA_TXT, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        logger.info(f"Saved data to {DATA_TXT}")
        
        # Convert to DataFrame and sort
        df = pd.DataFrame(data)
        if not df.empty:
            df['id'] = pd.to_numeric(df['id'], errors='coerce')
            df = df.sort_values(by=['page', 'id'], ascending=[True, False])
            logger.debug(f"DataFrame created with {len(df)} rows")
            
            # Save to temp CSV
            df.to_csv(TEMP_CSV, index=False, encoding='utf-8')
            logger.info(f"Saved temp CSV to {TEMP_CSV}")
            
            # Update Google Sheets
            try:
                creds = Credentials.from_service_account_file(CREDENTIALS_FILE, scopes=SCOPE)
                logger.debug(f"Authorized Google Sheets with {CREDENTIALS_FILE}")
                client = gspread.authorize(creds)
                sheet = client.open_by_key(SHEET_ID).sheet1
                sheet.clear()
                logger.debug("Cleared Google Sheet")
                sheet.update([df.columns.values.tolist()] + df.values.tolist())
                logger.info(f"Updated Google Sheet {SHEET_ID}")
            except Exception as e:
                logger.error(f"Error updating Google Sheets: {str(e)}")
                logger.debug(traceback.format_exc())
        
    except Exception as e:
        logger.error(f"Error saving data: {str(e)}")
        logger.debug(traceback.format_exc())

def main():
    logger.info("Starting main function")
    # Load existing data
    existing_data = load_existing_data()
    existing_ids = {item['id'] for item in existing_data}
    existing_links = {item['link'] for item in existing_data}
    logger.debug(f"Existing data: {len(existing_data)} items, {len(existing_ids)} unique IDs")
    
    # Populate page queue
    logger.info("Populating page queue")
    page_num = 1
    max_pages = 1000  # Arbitrary limit to prevent infinite loop
    while page_num <= max_pages and not stop_scraping:
        page_queue.put(page_num)
        logger.debug(f"Enqueued page {page_num}")
        page_num += 1
    
    # Start worker threads
    logger.info(f"Starting {NUM_THREADS} worker threads")
    threads = []
    for i in range(NUM_THREADS):
        t = threading.Thread(target=worker, name=f"Worker-{i}")
        t.start()
        threads.append(t)
        logger.debug(f"Started thread Worker-{i}")
    
    # Wait for all threads to complete
    logger.info("Waiting for threads to complete")
    for t in threads:
        t.join()
        logger.debug(f"Thread {t.name} completed")
    
    # Merge new data with existing, keeping unique by id and link
    logger.info("Merging new data with existing")
    with data_lock:
        unique_data = existing_data[:]
        new_ids = {item['id'] for item in all_video_data}
        new_links = {item['link'] for item in all_video_data}
        unique_data.extend([item for item in all_video_data if item['id'] not in existing_ids and item['link'] not in existing_links])
        logger.info(f"Merged data: {len(unique_data)} total items")
    
    # Save data
    save_data(unique_data)
    logger.info("Script completed")

if __name__ == '__main__':
    try:
        main()
    except Exception as e:
        logger.error(f"Main function error: {str(e)}")
        logger.debug(traceback.format_exc())
