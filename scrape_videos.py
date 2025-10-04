import json
import requests
from bs4 import BeautifulSoup
import csv
import os
import logging
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
import time
from urllib.parse import urljoin

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('scrape_videos.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Load config
try:
    with open('config.json', 'r') as f:
        config = json.load(f)
    DOMAIN = config['DOMAIN'].rstrip('/')
    NUM_THREADS = config.get('NUM_THREADS', 10)
    LIMIT_PAGES_NO_NEW = config.get('LIMIT_PAGES_NO_NEW', 10)
    DETAIL_DELAY = config.get('DETAIL_DELAY', 1.0)
    DATA_TXT = config.get('DATA_TXT', 'data.txt')
    TEMP_CSV = config.get('TEMP_CSV', 'temp_videos.csv')
    FORCE_ALL_PAGES = config.get('FORCE_ALL_PAGES', False)
    HEADERS = config.get('HEADERS', {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.5',
        'Referer': 'https://xnhau.sh/',
        'Connection': 'keep-alive',
        'Upgrade-Insecure-Requests': '1',
        'Accept-Encoding': 'gzip, deflate, br',
        'DNT': '1'
    })
    PROXIES = config.get('PROXIES', {})
except Exception as e:
    logger.error(f"Failed to load config.json: {str(e)}. Using defaults.")
    DOMAIN = "https://xnhau.sh/clip-sex-moi"
    NUM_THREADS = 10
    LIMIT_PAGES_NO_NEW = 10
    DETAIL_DELAY = 1.0
    DATA_TXT = "data.txt"
    TEMP_CSV = "temp_videos.csv"
    FORCE_ALL_PAGES = False
    HEADERS = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.5',
        'Referer': 'https://xnhau.sh/',
        'Connection': 'keep-alive',
        'Upgrade-Insecure-Requests': '1',
        'Accept-Encoding': 'gzip, deflate, br',
        'DNT': '1'
    }
    PROXIES = {
        "http": "http:// user49005:2lsBkRUegO@42.96.12.24:49005",
        "https": "http:// user49005:2lsBkRUegO@42.96.12.24:49005"
    }

def scrape_page(page_num):
    """Scrape video data from a single page."""
    url = DOMAIN if page_num == 1 else f"{DOMAIN}/{page_num}"
    logger.info(f"Scraping page {page_num}: {url}")
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    
    try:
        session = requests.Session()
        response = session.get(url, headers=HEADERS, proxies=PROXIES, timeout=20, allow_redirects=True)
        logger.info(f"Page {page_num} - Status code: {response.status_code}")
        logger.debug(f"Page {page_num} - Redirect history: {[r.url for r in response.history]}")
        
        if response.status_code != 200:
            logger.error(f"Page {page_num} - Failed with status code: {response.status_code}")
            return None, page_num, None
        
        # Parse with BeautifulSoup
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # Save HTML
        html_file = f"response_page_{page_num}_{timestamp}.html"
        with open(html_file, 'w', encoding='utf-8') as f:
            f.write(soup.prettify())
        logger.info(f"Page {page_num} - Saved HTML to {html_file}")
        
        # Check for Cloudflare issues
        if 'cloudflare' in response.text.lower() or 'cf-ray' in response.headers:
            logger.warning(f"Page {page_num} - Detected Cloudflare protection")
            return None, page_num, html_file
        
        # Extract video items
        videos = []
        items = soup.find_all('div', class_='item ')
        logger.info(f"Page {page_num} - Found {len(items)} items with class 'item '")
        
        for item in items:
            video = {}
            title_elem = item.find('a', class_='title')
            video['title'] = title_elem.get_text(strip=True) if title_elem else "No title"
            video['link'] = title_elem['href'] if title_elem and title_elem.get('href') else "No link"
            if video['link'].startswith('/'):
                video['link'] = urljoin(DOMAIN, video['link'])
            img_elem = item.find('img')
            video['thumbnail'] = img_elem['src'] if img_elem and img_elem.get('src') else "No thumbnail"
            if video['thumbnail'].startswith('/'):
                video['thumbnail'] = urljoin(DOMAIN, video['thumbnail'])
            duration_elem = item.find('span', class_='duration')
            video['duration'] = duration_elem.get_text(strip=True) if duration_elem else "No duration"
            videos.append(video)
        
        return videos, page_num, html_file
    
    except Exception as e:
        logger.error(f"Page {page_num} - Error: {str(e)}")
        return None, page_num, None

def save_to_files(videos, data_txt, temp_csv):
    """Save videos to data.txt and temp_videos.csv."""
    if not videos:
        return
    with open(data_txt, 'a', encoding='utf-8') as f:
        for video in videos:
            f.write(f"{video['title']} | {video['link']} | {video['thumbnail']} | {video['duration']}\n")
    with open(temp_csv, 'a', encoding='utf-8', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=['title', 'link', 'thumbnail', 'duration'])
        if os.path.getsize(temp_csv) == 0:
            writer.writeheader()
        writer.writerows(videos)
    logger.info(f"Saved {len(videos)} videos to {data_txt} and {temp_csv}")

def main():
    """Main function to scrape videos across multiple pages."""
    all_videos = []
    no_new_pages = 0
    page_num = 1
    
    # Clear output files
    if os.path.exists(DATA_TXT):
        os.remove(DATA_TXT)
    if os.path.exists(TEMP_CSV):
        os.remove(TEMP_CSV)
    
    with ThreadPoolExecutor(max_workers=min(NUM_THREADS, 10)) as executor:
        while True:
            if no_new_pages >= LIMIT_PAGES_NO_NEW and not FORCE_ALL_PAGES:
                logger.info(f"Stopping after {no_new_pages} pages with no new videos")
                break
            
            future = executor.submit(scrape_page, page_num)
            videos, scraped_page, html_file = future.result()
            
            if videos:
                all_videos.extend(videos)
                save_to_files(videos, DATA_TXT, TEMP_CSV)
                no_new_pages = 0
            else:
                no_new_pages += 1
                logger.warning(f"Page {scraped_page} - No videos found. No new pages: {no_new_pages}")
            
            page_num += 1
            time.sleep(DETAIL_DELAY)
    
    logger.info(f"Total videos scraped: {len(all_videos)}")
    if all_videos:
        logger.info(f"Sample video: {all_videos[0]}")

if __name__ == '__main__':
    main()
