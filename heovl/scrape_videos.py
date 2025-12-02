# heovl/scrape_videos.py

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
import re
from datetime import datetime

# ====================== SETUP PATH & LOGGING ======================
# Lấy đúng thư mục chứa file .py này (dù chạy từ đâu)
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
os.chdir(SCRIPT_DIR)  # Đảm bảo working directory là heovl/

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('scraper.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# ====================== LOAD CONFIG ======================
try:
    with open('config.json', 'r', encoding='utf-8') as f:
        config = json.load(f)
except Exception as e:
    logger.error("Không tìm thấy hoặc lỗi config.json")
    raise

CATEGORIES = config['CATEGORIES']
NUM_THREADS = config.get('NUM_THREADS', 6)
DETAIL_DELAY = config.get('DETAIL_DELAY', 0.8)
DATA_FOLDER = config.get('DATA_FOLDER', 'data')
SCOPE = config['SCOPE']
CREDENTIALS_FILE = config['CREDENTIALS_FILE']
SHEET_ID = config['SHEET_ID']

# Tạo thư mục data nếu chưa có
os.makedirs(DATA_FOLDER, exist_ok=True)

headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
    'Accept-Language': 'vi-VN,vi;q=0.9,en-US;q=0.8,en;q=0.7',
    'Accept-Encoding': 'gzip, deflate, br',
    'Connection': 'keep-alive',
    'Upgrade-Insecure-Requests': '1',
    'Sec-Fetch-Dest': 'document',
    'Sec-Fetch-Mode': 'navigate',
    'Sec-Fetch-Site': 'none',
    'Cache-Control': 'max-age=0',
}

# ====================== GLOBAL DATA ======================
global_category_data = {}  # {category_name: [list of videos]}
data_lock = threading.Lock()
stop_event = threading.Event()

# ====================== HELPERS ======================
def clean_title(title):
    """Loại bỏ dấu "..." ở cuối tiêu đề ngắn"""
    return re.sub(r'\.\.\.$', '', title).strip()

def parse_number(text):
    """Chuyển '12.5K', '1.2M' → số nguyên"""
    if not text:
        return 0
    text = text.strip().replace(',', '')
    if 'K' in text.upper():
        return int(float(text.upper().replace('K', '')) * 1000)
    if 'M' in text.upper():
        return int(float(text.upper().replace('M', '')) * 1000000)
    return int(re.sub(r'\D', '', text) or 0)

def extract_video_data(box, base_url, page_num):
    try:
        a_tag = box.find('a', class_='video-box__thumbnail__link')
        if not a_tag:
            return None

        link = urljoin(base_url, a_tag.get('href'))
        full_title = a_tag.get('title') or ''
        short_title = box.find('h3', class_='video-box__heading')
        title = full_title or (short_title.get_text(strip=True) if short_title else '')
        title = clean_title(title)

        img = a_tag.find('img')
        thumbnail = urljoin(base_url, img['src']) if img and img.get('src') else ''

        # Views & Comments
        stats = box.find_all('small')
        views = 0
        comments = 0
        if len(stats) >= 2:
            views = parse_number(stats[0].get_text(strip=True))
            comments = parse_number(stats[1].get_text(strip=True))
        elif len(stats) == 1:
            views = parse_number(stats[0].get_text(strip=True))

        video_id = link.strip('/').split('/')[-1]

        return {
            'page': page_num,
            'id': video_id,
            'title': title,
            'link': link,
            'thumbnail': thumbnail,
            'views': views,
            'comments': comments,
            'scraped_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        }
    except Exception as e:
        logger.warning(f"Parse error: {e}")
        return None

def scrape_page(url, page_num):
    try:
        r = requests.get(url, headers=headers, timeout=15)
        r.raise_for_status()
        soup = BeautifulSoup(r.text, 'html.parser')

        boxes = soup.find_all('div', class_='video-box')
        if not boxes:
            return [], True  # end of content

        items = []
        for box in boxes:
            data = extract_video_data(box, url, page_num)
            if data:
                items.append(data)

        # Kiểm tra có trang tiếp không
        next_btn = soup.find('a', rel='next')
        is_last = not bool(next_btn)
        return items, is_last

    except Exception as e:
        logger.error(f"Error scraping {url}: {e}")
        return [], True

# ====================== WORKER PER CATEGORY ======================
def scrape_category(category_name, base_url):
    data_file = os.path.join(DATA_FOLDER, f"{category_name}.json")
    existing = {}

    if os.path.exists(data_file):
        try:
            with open(data_file, 'r', encoding='utf-8') as f:
                existing = {item['id']: item for item in json.load(f)}
            logger.info(f"[{category_name}] Loaded {len(existing)} existing items")
        except:
            existing = {}

    all_items = existing.copy()
    page = 1
    updated_count = 0

    while not stop_event.is_set():
        url = base_url if page == 1 else f"{base_url}&page={page}"
        logger.info(f"[{category_name}] Đang quét trang {page}")

        items, is_last = scrape_page(url, page)
        if not items:
            logger.info(f"[{category_name}] Không còn video → dừng")
            break

        for item in items:
            vid = item['id']
            if vid not in all_items or \
               all_items[vid]['views'] != item['views'] or \
               all_items[vid]['comments'] != item['comments']:
                all_items[vid] = item
                updated_count += 1

        logger.info(f"[{category_name}] Trang {page}: {len(items)} video, +{updated_count} cập nhật mới")
        
        if is_last:
            logger.info(f"[{category_name}] Đã tới trang cuối: {page}")
            break

        page += 1
        time.sleep(DETAIL_DELAY)

    # Sort: trang tăng dần, views giảm dần
    sorted_items = sorted(all_items.values(), key=lambda x: (x['page'], -x['views']))

    # Lưu file JSON riêng cho category
    with open(data_file, 'w', encoding='utf-8') as f:
        json.dump(sorted_items, f, ensure_ascii=False, indent=2)

    with data_lock:
        global_category_data[category_name] = sorted_items

    logger.info(f"[{category_name}] Hoàn tất! Tổng: {len(sorted_items)} video (cập nhật {updated_count})")

# ====================== MAIN ======================
def main():
    logger.info("=== BẮT ĐẦU SCRAPE HEOVL.MOE ===")

    threads = []
    for cat in CATEGORIES:
        name = cat['name']
        url = cat['url']
        t = threading.Thread(target=scrape_category, args=(name, url), daemon=True)
        t.start()
        threads.append(t)

    # Chờ tất cả category hoàn thành (tối đa 15 phút)
    for t in threads:
        t.join(timeout=900)

    if stop_event.is_set():
        logger.error("Timeout! Một số category chưa hoàn thành.")
        return

    # ====================== CẬP NHẬT GOOGLE SHEETS ======================
    try:
        logger.info("Đang kết nối Google Sheets...")
        creds = Credentials.from_service_account_file(CREDENTIALS_FILE, scopes=SCOPE)
        client = gspread.authorize(creds)
        sh = client.open_by_key(SHEET_ID)

        for cat_name, data_list in global_category_data.items():
            if not data_list:
                continue

            df = pd.DataFrame(data_list)
            df = df.sort_values(by=['page', 'views'], ascending=[True, False])
            df['views'] = df['views'].astype(int)
            df['comments'] = df['comments'].astype(int)

            try:
                worksheet = sh.worksheet(cat_name)
                worksheet.clear()
                logger.info(f"Cập nhật sheet hiện có: {cat_name}")
            except gspread.exceptions.WorksheetNotFound:
                worksheet = sh.add_worksheet(title=cat_name, rows=2000, cols=10)
                logger.info(f"Tạo sheet mới: {cat_name}")

            worksheet.update([df.columns.values.tolist()] + df.values.tolist())
            logger.info(f"Đã cập nhật sheet '{cat_name}' → {len(df)} dòng")

        logger.info("HOÀN TẤT! Tất cả dữ liệu đã được cập nhật lên Google Sheets.")

    except Exception as e:
        logger.error(f"Lỗi cập nhật Google Sheets: {e}")

if __name__ == '__main__':
    main()
