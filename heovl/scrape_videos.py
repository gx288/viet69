# heovl/scrape_videos.py
# Phiên bản: HOÀN CHỈNH – CHẠY MƯỢT HEOVL.MOE TRÊN GITHUB ACTIONS

import json
import requests
from bs4 import BeautifulSoup
import threading
import time
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
import os
from urllib.parse import urljoin
import logging
import re
from datetime import datetime
import random

# ====================== PATH & LOGGING ======================
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
os.chdir(SCRIPT_DIR)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('scraper.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# ====================== TỰ ĐỘNG LẤY PROXY MỚI HÀNG NGÀY ======================
PROXY_SOURCES = [
    "https://raw.githubusercontent.com/clarketm/proxy-list/master/proxy-list-raw.txt",
    "https://raw.githubusercontent.com/TheSpeedX/PROXY-List/master/http.txt",
    "https://raw.githubusercontent.com/monosans/proxy-list/main/proxies/http.txt",
    "https://raw.githubusercontent.com/mertguvencli/proxy-list/main/http.txt",
    "https://raw.githubusercontent.com/yokelvin9/proxy-list/main/http.txt",
    "https://api.proxyscrape.com/v2/?request=get&protocol=http&timeout=10000&country=all&ssl=all&anonymity=all",
    "https://www.proxy-list.download/api/v1/get?type=http",
]

def fetch_fresh_proxies():
    proxies = set()
    for url in PROXY_SOURCES:
        try:
            logger.info(f"Đang lấy proxy từ {url.split('/')[-1] if '/' in url else url}...")
            r = requests.get(url, timeout=20)
            if r.status_code == 200:
                for line in r.text.strip().splitlines():
                    line = line.strip()
                    if line and ':' in line and not line.startswith('#'):
                        if not line.startswith(('http://', 'https://')):
                            line = 'http://' + line
                        proxies.add(line)
        except Exception as e:
            logger.warning(f"Lỗi lấy proxy: {e}")
        time.sleep(1)
    proxy_list = list(proxies)
    logger.info(f"Đã tải thành công {len(proxy_list)} proxy mới!")
    return proxy_list if proxy_list else ["http://154.202.119.177:80"]

WORKING_PROXIES = fetch_fresh_proxies()
random.shuffle(WORKING_PROXIES)

def get_next_proxy():
    i = 0
    while True:
        yield {"http": WORKING_PROXIES[i % len(WORKING_PROXIES)], "https": WORKING_PROXIES[i % len(WORKING_PROXIES)]}
        i += 1

proxy_gen = get_next_proxy()

# ====================== RANDOM HEADERS ======================
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/130.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:131.0) Gecko/20100101 Firefox/131.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_6_1) AppleWebKit/605.1.15 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome/129.0 Safari/537.36",
]

def get_headers():
    return {
        "User-Agent": random.choice(USER_AGENTS),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "vi-VN,vi;q=0.9,en;q=0.8",
        "Accept-Encoding": "gzip, deflate, br",
        "Referer": "https://heovl.moe/",
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1",
        "Sec-Fetch-Dest": "document",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-Site": "none",
        "Cache-Control": "max-age=0",
    }

# ====================== LOAD CONFIG ======================
with open('config.json', 'r', encoding='utf-8') as f:
    config = json.load(f)

CATEGORIES = config['CATEGORIES']
DETAIL_DELAY = config.get('DETAIL_DELAY', 1.5)
DATA_FOLDER = config.get('DATA_FOLDER', 'data')
SCOPE = config['SCOPE']
CREDENTIALS_FILE = os.path.join(SCRIPT_DIR, 'credentials.json')
SHEET_ID = config['SHEET_ID']
os.makedirs(DATA_FOLDER, exist_ok=True)

global_category_data = {}
data_lock = threading.Lock()

# ====================== SCRAPE PAGE VỚI PROXY VÒNG ======================
def scrape_page(url, page_num):
    for _ in range(20):  # thử tối đa 20 proxy
        try:
            proxy = next(proxy_gen)
            r = requests.get(url, headers=get_headers(), proxies=proxy, timeout=25, verify=False)
            if r.status_code == 200:
                soup = BeautifulSoup(r.text, 'html.parser')
                boxes = soup.find_all('div', class_='video-box')
                if not boxes:
                    return [], True

                items = []
                for box in boxes:
                    a = box.find('a', class_='video-box__thumbnail__link')
                    if not a: continue

                    link = urljoin(url, a.get('href'))
                    title = (a.get('title') or '').strip()
                    if not title:
                        h3 = box.find('h3', class_='video-box__heading')
                        title = h3.get_text(strip=True) if h3 else "No title"
                    title = re.sub(r'\.\.\.$', '', title).strip()

                    img = a.find('img')
                    thumb = urljoin(url, img['src']) if img and img.get('src') else ''

                    stats = box.find_all('small')
                    views = int(re.sub(r'\D', '', stats[0].text)) if stats else 0
                    comments = int(re.sub(r'\D', '', stats[1].text)) if len(stats) > 1 else 0

                    vid_id = link.strip('/').split('/')[-1]

                    items.append({
                        'page': page_num,
                        'id': vid_id,
                        'title': title,
                        'link': link,
                        'thumbnail': thumb,
                        'views': views,
                        'comments': comments,
                        'scraped_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    })

                next_btn = soup.find('a', rel='next')
                return items, not bool(next_btn)

        except:
            pass
        time.sleep(1)

    logger.error(f"Không thể scrape {url} sau 20 proxy")
    return [], True

# ====================== SCRAPE 1 CATEGORY ======================
def scrape_category(name, base_url):
    file_path = os.path.join(DATA_FOLDER, f"{name}.json")
    existing = {}
    if os.path.exists(file_path):
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                existing = {item['id']: item for item in json.load(f)}
        except: pass

    all_data = existing.copy()
    page = 1
    updated = 0

    while True:
        url = base_url if page == 1 else f"{base_url}&page={page}"
        logger.info(f"[{name}] Đang quét trang {page}...")
        items, is_last = scrape_page(url, page)
        if not items and page > 1:
            break

        for item in items:
            vid = item['id']
            if vid not in all_data or all_data[vid]['views'] != item['views'] or all_data[vid]['comments'] != item['comments']:
                all_data[vid] = item
                updated += 1

        logger.info(f"[{name}] Trang {page} → {len(items)} video | +{updated} cập nhật")
        if is_last or not items:
            break
        page += 1
        time.sleep(DETAIL_DELAY + random.uniform(0.5, 1.5))

    # Lưu lại
    sorted_data = sorted(all_data.values(), key=lambda x: (x['page'], -x['views']))
    with open(file_path, 'w', encoding='utf-8') as f:
        json.dump(sorted_data, f, ensure_ascii=False, indent=2)

    with data_lock:
        global_category_data[name] = sorted_data

    logger.info(f"[{name}] HOÀN TẤT → {len(sorted_data)} video (cập nhật {updated})")

# ====================== MAIN ======================
def main():
    logger.info("=== BẮT ĐẦU SCRAPE HEOVL.MOE VỚI PROXY TỰ ĐỘNG ===")
    threads = []
    for cat in CATEGORIES:
        t = threading.Thread(target=scrape_category, args=(cat['name'], cat['url']), daemon=True)
        t.start()
        threads.append(t)

    for t in threads:
        t.join(timeout=900)

    # CẬP NHẬT GOOGLE SHEETS
    try:
        logger.info("Đang cập nhật Google Sheets...")
        creds = Credentials.from_service_account_file(CREDENTIALS_FILE, scopes=SCOPE)
        client = gspread.authorize(creds)
        sh = client.open_by_key(SHEET_ID)

        for name, data in global_category_data.items():
            if not data: continue
            df = pd.DataFrame(data).sort_values(by=['page', 'views'], ascending=[True, False])
            try:
                ws = sh.worksheet(name)
                ws.clear()
            except gspread.exceptions.WorksheetNotFound:
                ws = sh.add_worksheet(title=name, rows=3000, cols=10)
            ws.update([df.columns.tolist()] + df.values.tolist())
            logger.info(f"Đã cập nhật sheet '{name}' – {len(df)} dòng")

        logger.info("HOÀN TẤT TOÀN BỘ! DỮ LIỆU ĐÃ LÊN GOOGLE SHEETS")

    except Exception as e:
        logger.error(f"Lỗi Google Sheets: {e}")

if __name__ == '__main__':
    main()
