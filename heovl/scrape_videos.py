# heovl/scrape_videos.py
# Phiên bản CUỐI: Test proxy song song + lưu proxy OK + dùng lại siêu nhanh

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
from concurrent.futures import ThreadPoolExecutor, as_completed

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

WORKING_PROXY_FILE = "working_proxies.txt"
FRESH_PROXY_SOURCES = [
    "https://raw.githubusercontent.com/clarketm/proxy-list/master/proxy-list-raw.txt",
    "https://raw.githubusercontent.com/TheSpeedX/PROXY-List/master/http.txt",
    "https://raw.githubusercontent.com/monosans/proxy-list/main/proxies/http.txt",
    "https://api.proxyscrape.com/v2/?request=get&protocol=http&timeout=10000&country=all",
]

# ====================== TEST PROXY SỐNG ======================
def test_proxy(proxy):
    try:
        proxies = {"http": proxy, "https": proxy}
        r = requests.get("https://heovl.moe/", headers=get_headers(), proxies=proxies, timeout=12, verify=False)
        if r.status_code == 200 and "heovl" in r.text.lower():
            return proxy
    except:
        pass
    return None

def get_headers():
    return {
        "User-Agent": random.choice([
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/130.0 Safari/537.36",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:131.0) Gecko/20100101 Firefox/131.0",
        ]),
        "Accept": "text/html",
        "Referer": "https://heovl.moe/",
    }

# ====================== LẤY + TEST + LƯU PROXY OK ======================
def load_or_create_working_proxies():
    if os.path.exists(WORKING_PROXY_FILE):
        age = time.time() - os.path.getmtime(WORKING_PROXY_FILE)
        if age < 24 * 3600:  # dưới 24h → dùng lại
            with open(WORKING_PROXY_FILE, 'r', encoding='utf-8') as f:
                proxies = [line.strip() for line in f if line.strip()]
            if proxies:
                logger.info(f"Đã load {len(proxies)} proxy OK từ file (tuổi: {age/3600:.1f}h)")
                return proxies

    logger.info("Đang tìm proxy sống (test song song 100 proxy)...")
    all_proxies = set()

    # Lấy từ file cũ (nếu có)
    if os.path.exists(WORKING_PROXY_FILE):
        with open(WORKING_PROXY_FILE, 'r', encoding='utf-8') as f:
            all_proxies.update(line.strip() for line in f if line.strip())

    # Lấy thêm từ nguồn mới
    for url in FRESH_PROXY_SOURCES:
        try:
            r = requests.get(url, timeout=20)
            for line in r.text.splitlines():
                line = line.strip()
                if ':' in line and not line.startswith('#'):
                    if not line.lower().startswith('http'):
                        line = 'http://' + line
                    all_proxies.add(line)
        except: pass

    proxy_list = list(all_proxies)
    random.shuffle(proxy_list)
    test_sample = proxy_list[:500]  # chỉ test 500 cái đầu

    working = []
    with ThreadPoolExecutor(max_workers=100) as executor:
        futures = {executor.submit(test_proxy, p): p for p in test_sample}
        for future in as_completed(futures):
            result = future.result()
            if result:
                working.append(result)
                logger.info(f"PROXY SỐNG: {result}")
                if len(working) >= 10:  # đủ 10 cái là dừng
                    executor.shutdown(wait=False)
                    break

    if not working:
        logger.warning("Không tìm được proxy sống → dùng fallback")
        working = ["http://154.202.119.177:80", "http://103.174.102.79:80"]

    # Lưu lại
    with open(WORKING_PROXY_FILE, 'w', encoding='utf-8') as f:
        for p in working:
            f.write(p + '\n')

    logger.info(f"Đã lưu {len(working)} proxy OK vào {WORKING_PROXY_FILE}")
    return working

WORKING_PROXIES = load_or_create_working_proxies()
proxy_index = 0
proxy_lock = threading.Lock()

def get_next_proxy():
    global proxy_index
    with proxy_lock:
        p = WORKING_PROXIES[proxy_index % len(WORKING_PROXIES)]
        proxy_index += 1
        return {"http": p, "https": p}

# ====================== LOAD CONFIG ======================
with open('config.json', 'r', encoding='utf-8') as f:
    config = json.load(f)

CATEGORIES = config['CATEGORIES']
DETAIL_DELAY = config.get('DETAIL_DELAY', 1.0)
DATA_FOLDER = config.get('DATA_FOLDER', 'data')
SCOPE = config['SCOPE']
CREDENTIALS_FILE = os.path.join(SCRIPT_DIR, 'credentials.json')
SHEET_ID = config['SHEET_ID']
os.makedirs(DATA_FOLDER, exist_ok=True)

global_category_data = {}
data_lock = threading.Lock()

# ====================== SCRAPE PAGE ======================
def scrape_page(url, page_num):
    for _ in range(30):  # chỉ cần 30 lần vì proxy đã được test trước
        try:
            proxy = get_next_proxy()
            r = requests.get(url, headers=get_headers(), proxies=proxy, timeout=20, verify=False)
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
                    title = (a.get('title') or box.find('h3', class_='video-box__heading').get_text(strip=True) if box.find('h3') else "No title")
                    title = re.sub(r'\.\.\.$', '', title).strip()

                    img = a.find('img')
                    thumb = urljoin(url, img['src']) if img and img.get('src') else ''

                    stats = box.find_all('small')
                    views = int(re.sub(r'\D', '', stats[0].text.strip())) if stats else 0
                    comments = int(re.sub(r'\D', '', stats[1].text.strip())) if len(stats) > 1 else 0

                    vid_id = link.strip('/').split('/')[-1]

                    items.append({
                        'page': page_num, 'id': vid_id, 'title': title, 'link': link,
                        'thumbnail': thumb, 'views': views, 'comments': comments,
                        'scraped_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    })

                next_btn = soup.find('a', rel='next')
                logger.info(f"THÀNH CÔNG trang {page_num} → {len(items)} video")
                return items, not bool(next_btn)
        except:
            pass
        time.sleep(0.8)

    logger.error(f"Thất bại trang {page_num}")
    return [], True

# ====================== SCRAPE CATEGORY ======================
def scrape_category(name, base_url):
    file_path = os.path.join(DATA_FOLDER, f"{name}.json")
    existing = {}
    if os.path.exists(file_path):
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                existing = {i['id']: i for i in json.load(f)}
        except: pass

    all_data = existing.copy()
    page = 1
    updated = 0

    while True:
        url = base_url if page == 1 else f"{base_url}&page={page}"
        logger.info(f"[{name}] Trang {page}")
        items, is_last = scrape_page(url, page)
        if not items and page > 1: break

        for item in items:
            vid = item['id']
            if vid not in all_data or all_data[vid]['views'] != item['views']:
                all_data[vid] = item
                updated += 1

        logger.info(f"[{name}] Trang {page} → {len(items)} video | +{updated}")
        if is_last or not items: break
        page += 1
        time.sleep(DETAIL_DELAY)

    sorted_data = sorted(all_data.values(), key=lambda x: (x['page'], -x['views']))
    with open(file_path, 'w', encoding='utf-8') as f:
        json.dump(sorted_data, f, ensure_ascii=False, indent=2)

    with data_lock:
        global_category_data[name] = sorted_data
    logger.info(f"[{name}] HOÀN TẤT → {len(sorted_data)} video")

# ====================== MAIN ======================
def main():
    logger.info("=== BẮT ĐẦU SCRAPE HEOVL.MOE – SIÊU ỔN ĐỊNH ===")
    threads = []
    for cat in CATEGORIES:
        t = threading.Thread(target=scrape_category, args=(cat['name'], cat['url']), daemon=True)
        t.start()
        threads.append(t)

    for t in threads:
        t.join(timeout=1200)

    # Google Sheets
    try:
        creds = Credentials.from_service_account_file(CREDENTIALS_FILE, scopes=SCOPE)
        client = gspread.authorize(creds)
        sh = client.open_by_key(SHEET_ID)
        for name, data in global_category_data.items():
            if not data: continue
            df = pd.DataFrame(data).sort_values(by=['page', 'views'], ascending=[True, False])
            try:
                ws = sh.worksheet(name)
                ws.clear()
            except:
                ws = sh.add_worksheet(title=name, rows=5000, cols=10)
            ws.update([df.columns.tolist()] + df.values.tolist())
            logger.info(f"Đã cập nhật sheet '{name}' – {len(df)} dòng")
        logger.info("HOÀN TẤT TOÀN BỘ!")
    except Exception as e:
        logger.error(f"Lỗi Sheets: {e}")

if __name__ == '__main__':
    main()
