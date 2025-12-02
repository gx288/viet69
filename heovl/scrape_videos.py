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
from urllib.parse import urljoin, urlparse, parse_qs
import logging
import re

# ====================== CONFIG & LOGGING ======================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('scraper.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Load config
with open('config.json', 'r', encoding='utf-8') as f:
    config = json.load(f)

CATEGORIES = config['CATEGORIES']  # List các category cần scrape
NUM_THREADS = config.get('NUM_THREADS', 8)
DETAIL_DELAY = config.get('DETAIL_DELAY', 0.5)
DATA_FOLDER = config.get('DATA_FOLDER', 'data')
TEMP_CSV = config.get('TEMP_CSV', 'temp_videos.csv')
SCOPE = config['SCOPE']
CREDENTIALS_FILE = config['CREDENTIALS_FILE']
SHEET_ID = config['SHEET_ID']

os.makedirs(DATA_FOLDER, exist_ok=True)

headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36'
}

# Thread-safe structures
page_queue = queue.Queue()
data_lock = threading.Lock()
stop_event = threading.Event()

def clean_title(title):
    return re.sub(r'^Vén váy lên rồi.*\.\.\.$', lambda m: m.group(0)[:-3], title).strip()

def extract_video_data(box, base_url, page_num):
    try:
        a_tag = box.find('a', class_='video-box__thumbnail__link')
        if not a_tag:
            return None

        link = urljoin(base_url, a_tag.get('href'))
        title_full = a_tag.get('title') or ''
        title_short = box.find('h3', class_='video-box__heading')
        title = title_full or (title_short.text.strip() if title_short else '')
        title = clean_title(title)

        img = a_tag.find('img')
        thumbnail = urljoin(base_url, img['src']) if img and img.get('src') else ''

        # Extract views
        views_text = '0'
        views_el = box.find('small', string=re.compile(r'\d+'))
        if views_el:
            views_text = views_el.text.strip()
        views = int(re.sub(r'\D', '', views_text) or '0')

        # Comments
        comment_el = box.find_all('small')[-1] if len(box.find_all('small')) > 1 else None
        comments = int(re.sub(r'\D', '', comment_el.text.strip())) if comment_el else 0

        # Extract video ID from URL
        video_id = link.split('/')[-1] if '/' in link else link

        return {
            'page': page_num,
            'id': video_id,
            'title': title,
            'link': link,
            'thumbnail': thumbnail,
            'views': views,
            'comments': comments,
            'scraped_at': time.strftime('%Y-%m-%d %H:%M:%S')
        }
    except Exception as e:
        logger.warning(f"Error parsing item: {e}")
        return None

def scrape_page(url, page_num):
    try:
        response = requests.get(url, headers=headers, timeout=15)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')

        boxes = soup.find_all('div', class_='video-box')
        if not boxes:
            logger.info(f"No videos found on page {page_num} -> end of content")
            return [], True  # is_last_page

        items = []
        for box in boxes:
            data = extract_video_data(box, url, page_num)
            if data:
                items.append(data)

        # Check if next page exists
        next_page = soup.find('a', rel='next')
        is_last = not bool(next_page)
        return items, is_last

    except Exception as e:
        logger.error(f"Error scraping {url}: {e}")
        return [], True

def worker(category_name, base_url):
    existing_file = os.path.join(DATA_FOLDER, f"{category_name}.json")
    existing_data = {}
    if os.path.exists(existing_file):
        try:
            with open(existing_file, 'r', encoding='utf-8') as f:
                existing_data = {item['id']: item for item in json.load(f)}
        except:
            existing_data = {}

    all_new_data = []
    page = 1
    while not stop_event.is_set():
        url = base_url if page == 1 else f"{base_url}&page={page}"
        logger.info(f"[{category_name}] Scraping page {page}")

        items, is_last = scrape_page(url, page)
        if not items or is_last and page > 1:
            logger.info(f"[{category_name}] Reached last page: {page}")
            break

        new_items = 0
        for item in items:
            vid = item['id']
            if vid not in existing_data or existing_data[vid]['views'] != item['views'] or existing_data[vid]['comments'] != item['comments']:
                existing_data[vid] = item
                all_new_data.append(item)
                new_items += 1

        logger.info(f"[{category_name}] Page {page}: {len(items)} items, {new_items} updated/new")
        page += 1
        time.sleep(DETAIL_DELAY)

    # Save per category
    sorted_data = sorted(existing_data.values(), key=lambda x: (x['page'], -x['views']), reverse=False)
    with open(existing_file, 'w', encoding='utf-8') as f:
        json.dump(sorted_data, f, ensure_ascii=False, indent=2)

    with data_lock:
        global_category_data[category_name] = sorted_data

# Global dict to store all category data
global_category_data = {}

def main():
    global global_category_data
    threads = []

    for cat in CATEGORIES:
        name = cat['name']
        url = cat['url']
        logger.info(f"Starting scrape for category: {name} -> {url}")
        t = threading.Thread(target=worker, args=(name, url), daemon=True)
        t.start()
        threads.append(t)

    for t in threads:
        t.join(timeout=600)

    if stop_event.is_set():
        logger.error("Scraping stopped due to timeout")
        return

    # =============== UPDATE GOOGLE SHEETS ===============
    try:
        creds = Credentials.from_service_account_file(CREDENTIALS_FILE, scopes=SCOPE)
        client = gspread.authorize(creds)
        spreadsheet = client.open_by_key(SHEET_ID)

        for cat_name, data in global_category_data.items():
            df = pd.DataFrame(data)
            if df.empty:
                continue

            df = df.sort_values(by=['page', 'views'], ascending=[True, False])
            df['views'] = df['views'].astype(int)
            df['comments'] = df['comments'].astype(int)

            try:
                sheet = spreadsheet.worksheet(cat_name)
                sheet.clear()
            except gspread.exceptions.WorksheetNotFound:
                sheet = spreadsheet.add_worksheet(title=cat_name, rows=1000, cols=10)

            sheet.update([df.columns.values.tolist()] + df.values.tolist())
            logger.info(f"Updated sheet: {cat_name} ({len(df)} rows)")

        logger.info("All done! Google Sheets updated successfully.")

    except Exception as e:
        logger.error(f"Google Sheets update failed: {e}")

if __name__ == '__main__':
    main()
