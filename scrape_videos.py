import json
from curl_cffi import requests
from bs4 import BeautifulSoup
import threading
import queue
import time
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
import os
from urllib.parse import urljoin, urlparse
import logging
import random

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
NUM_THREADS = config.get('NUM_THREADS', 10)  # Default to 10 threads if not specified
LIMIT_PAGES_NO_NEW = config.get('LIMIT_PAGES_NO_NEW', 10)  # New config for pages to scrape when no new posts, default 10
DETAIL_DELAY = config['DETAIL_DELAY']
DATA_TXT = config['DATA_TXT']
TEMP_CSV = config['TEMP_CSV']
SCOPE = config['SCOPE']
CREDENTIALS_FILE = config['CREDENTIALS_FILE']
SHEET_ID = config['SHEET_ID']

# Headers for requests
headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'}

# Proxies configuration
proxies = None
if config.get('PROXY'):
    proxies = {
        'http': config['PROXY'],
        'https': config['PROXY']
    }

# Load working proxies pool
working_proxies = []
if os.path.exists("working_proxies.txt"):
    try:
        with open("working_proxies.txt", "r", encoding="utf-8") as f:
            working_proxies = [line.strip() for line in f if line.strip()]
        logger.info(f"Loaded {len(working_proxies)} active proxies for pool rotation.")
    except Exception as e:
        logger.error(f"Failed to load working_proxies.txt: {e}")

def requests_get_with_retry(url, max_retries=5):
    global working_proxies
    for attempt in range(max_retries):
        proxy_url = None
        proxies_config = None
        
        if working_proxies:
            proxy_url = random.choice(working_proxies)
            proxies_config = {
                'http': proxy_url,
                'https': proxy_url
            }
            
        try:
            response = requests.get(url, headers=headers, impersonate="chrome120", timeout=10, proxies=proxies_config)
            if response.status_code == 200:
                return response
            elif response.status_code == 403:
                logger.warning(f"Proxy {proxy_url} got 403 Forbidden. Rotating...")
            else:
                logger.warning(f"Proxy {proxy_url} got status {response.status_code}. Rotating...")
        except Exception as e:
            logger.warning(f"Request failed with proxy {proxy_url}: {str(e)}. Rotating...")
            
        # If proxy failed, remove it from pool thread-safely
        if proxy_url and proxy_url in working_proxies:
            with data_lock:
                if proxy_url in working_proxies:
                    working_proxies.remove(proxy_url)
                    logger.info(f"Removed dead proxy: {proxy_url}. Remaining active proxies: {len(working_proxies)}")
                    
        time.sleep(1)
        
    # If we get here, all retries failed. Attempt one last direct request as fallback.
    logger.info("All proxy retries failed. Attempting final request without proxy...")
    return requests.get(url, headers=headers, impersonate="chrome120", timeout=10)

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
        
        response = requests_get_with_retry(url)
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
        # Sort data before saving to txt (same as sheet: page asc, id desc)
        sorted_data = sorted(data, key=lambda x: (x['page'], -int(x['id'])))
        
        with open(DATA_TXT, 'w', encoding='utf-8') as f:
            json.dump(sorted_data, f, ensure_ascii=False, indent=2)
        
        df = pd.DataFrame(sorted_data)
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

def process_batch(start_page, end_page):
    """Process a batch of pages using threads."""
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

def update_domain_everywhere(old_host, new_host):
    logger.info(f"Updating domain on all fronts from {old_host} to {new_host}...")
    if os.path.exists(DATA_TXT):
        try:
            logger.info(f"Updating {DATA_TXT}...")
            with open(DATA_TXT, 'r', encoding='utf-8') as f:
                content = f.read()
            updated_content = content.replace(old_host, new_host)
            with open(DATA_TXT, 'w', encoding='utf-8') as f:
                f.write(updated_content)
            logger.info(f"Successfully updated {DATA_TXT}")
        except Exception as e:
            logger.error(f"Failed to update {DATA_TXT}: {str(e)}")

def check_domain_redirect():
    global DOMAIN
    parsed_current = urlparse(DOMAIN)
    current_base = f"{parsed_current.scheme}://{parsed_current.netloc}"
    
    redirect_targets = []
    logger.info(f"Checking for domain redirects on {DOMAIN}...")
    
    for i in range(3):
        try:
            response = requests_get_with_retry(DOMAIN)
            if response.status_code == 200:
                parsed_final = urlparse(response.url)
                final_base = f"{parsed_final.scheme}://{parsed_final.netloc}"
                redirect_targets.append(final_base)
            else:
                redirect_targets.append(None)
        except Exception as e:
            logger.error(f"Redirect check {i+1} failed: {str(e)}")
            redirect_targets.append(None)
        time.sleep(1)
        
    # If all 3 succeeded and redirected to the same new domain
    if len(redirect_targets) == 3 and all(x is not None for x in redirect_targets):
        unique_targets = set(redirect_targets)
        if len(unique_targets) == 1:
            new_domain = list(unique_targets)[0]
            if new_domain.rstrip('/') != current_base.rstrip('/'):
                parsed_new = urlparse(new_domain)
                old_host = parsed_current.netloc
                new_host = parsed_new.netloc
                logger.info(f"Domain redirect detected consistently: {DOMAIN} -> {new_domain}. Updating config.json.")
                # Update DOMAIN global variable
                DOMAIN = new_domain
                # Write back to config.json
                try:
                    config['DOMAIN'] = new_domain
                    with open('config.json', 'w', encoding='utf-8') as f:
                        json.dump(config, f, ensure_ascii=False, indent=2)
                    logger.info(f"Successfully updated config.json with new domain: {new_domain}")
                except Exception as e:
                    logger.error(f"Failed to update config.json: {str(e)}")
                
                # Update data.txt
                update_domain_everywhere(old_host, new_host)

def main():
    global all_video_data, stop_scraping
    logger.info("Starting scraper")
    check_domain_redirect()
    existing_data = load_existing_data()
    existing_dict = {item['id']: item for item in existing_data}  # Use dict for quick lookup and override

    max_pages = 1000
    batch_size = 10  # Set batch size to 10 pages

    # First, scrape page 1 to check for new posts
    logger.info("Scraping page 1 to check for new posts")
    all_video_data = []  # Reset for page 1
    stop_scraping = False
    scrape_page(1)  # Scrape page 1 synchronously
    page1_data = all_video_data[:]
    all_video_data = []  # Reset for further scraping

    has_new_posts = False
    for item in page1_data:
        if item['id'] not in existing_dict:
            has_new_posts = True
            break

    # Check for FORCE_FULL_SCRAPE environment variable
    force_full = os.environ.get("FORCE_FULL_SCRAPE", "false").lower() == "true"

    if has_new_posts or force_full:
        logger.info("New posts found or force full enabled. Scraping all pages.")
        pages_to_scrape = max_pages
    else:
        logger.info(f"No new posts on page 1. Scraping first {LIMIT_PAGES_NO_NEW} pages for stats update.")
        pages_to_scrape = LIMIT_PAGES_NO_NEW

    # Determine initial list of pages to scrape
    pages_list = list(range(1, pages_to_scrape + 1))
    
    # Gap Detection: Find missing pages in the existing data up to the maximum page recorded
    existing_pages = {int(item['page']) for item in existing_data if 'page' in item}
    if existing_pages:
        max_existing_page = max(existing_pages)
        missing_pages = [p for p in range(1, max_existing_page) if p not in existing_pages]
        if missing_pages:
            logger.info(f"Gap detection: Found {len(missing_pages)} missing pages in database: {missing_pages[:20]}...")
            for p in missing_pages:
                if p not in pages_list:
                    pages_list.append(p)
            pages_list.sort()

    # Now scrape the determined pages in batches
    pages_to_process = [p for p in pages_list if p != 1]
    if 1 in pages_list:
        all_video_data.extend(page1_data)

    for i in range(0, len(pages_to_process), batch_size):
        if stop_scraping:
            break
        batch = pages_to_process[i:i+batch_size]
        
        # Enqueue pages for this batch
        for p in batch:
            page_queue.put(p)
            
        logger.info(f"Processing batch of pages: {batch}")
        threads = []
        for j in range(NUM_THREADS):
            t = threading.Thread(target=worker, name=f"Worker-{j}")
            t.start()
            threads.append(t)
            
        for t in threads:
            t.join()

    # Merge and override data
    with data_lock:
        for item in all_video_data:
            existing_dict[item['id']] = item  # Override if exists, add if new
        
        unique_data = list(existing_dict.values())
        logger.info(f"Total: scraped {total_pages_scraped} pages, updated/added {len(all_video_data)} items, {len(unique_data)} total items")

    # Save sorted data
    save_data(unique_data)

if __name__ == '__main__':
    try:
        main()
    except Exception as e:
        logger.error(f"Main function error: {str(e)}")
