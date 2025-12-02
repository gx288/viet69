# heovl/scrape_videos.py
# PHIÊN BẢN CUỐI – CHẠY MƯỢT 100% TRÊN GITHUB ACTIONS (test 2025-12-02)

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

# ====================== SETUP ======================
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
PROXY_SOURCES = [
    "https://raw.githubusercontent.com/clarketm/proxy-list/master/proxy-list-raw.txt",
    "https://raw.githubusercontent.com/TheSpeedX/PROXY-List/master/http.txt",
    "https://raw.githubusercontent.com/monosans/proxy-list/main/proxies/http.txt",
    "https://api.proxyscrape.com/v2/?request=get&protocol=http&timeout=10000&country=all",
    "https://www.proxy-list.download/api/v1/get?type=http",
]

# ====================== TEST PROXY ======================
def test_proxy(proxy):
    try:
        proxies = {"http": proxy, "https": proxy}
        r = requests.get(
            "https://heovl.moe/",
            headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"},
            proxies=proxies,
            timeout=15,
            verify=False
        )
        if r.status_code == 200 and ("heovl" in r.text.lower() or "video-box" in r.text):
            return proxy
    except:
        pass
    return None

# ====================== TÌM PROXY SỐNG BẰNG MỌI GIÁ ======================
def get_working_proxies():
    if os.path.exists(WORKING_PROXY_FILE):
        age = time.time() - os.path.getmtime(WORKING_PROXY_FILE)
        if age < 12 * 3600:  # dưới 12 tiếng → dùng lại
            with open(WORKING_PROXY_FILE, 'r', encoding='utf-8') as f:
                proxies = [line.strip() for line in f if line.strip()]
            if proxies:
                logger.info(f"LOAD SIÊU NHANH: {len(proxies)} proxy sống từ file cũ!")
                return proxies

    logger.info("BẮT ĐẦU SĂN PROXY SỐNG – KHÔNG NGỪNG CHO ĐẾN KHI CÓ!")
    all_proxies = set()

    for url in PROXY_SOURCES:
        try:
            r = requests.get(url, timeout=25)
            if r.status_code == 200:
                for line in r.text.splitlines():
                    line = line.strip()
                    if ':' in line and not line.startswith('#'):
                        if not line.lower().startswith('http'):
                            line = 'http://' + line
                        all_proxies.add(line)
        except:
            pass

    proxy_list = list(all_proxies)
    random.shuffle(proxy_list)
    logger.info(f"Tổng: {len(proxy_list):,} proxy → test song song...")

    working = []
    batch_size = 200
    tested = 0

    while len(working) < 8 and tested < len(proxy_list):
        batch = proxy_list[tested:tested + batch_size]
        tested += batch_size

        with ThreadPoolExecutor(max_workers=120) as executor:
            for proxy in executor.map(test_proxy, batch):
                if proxy:
                    working.append(proxy)
                    logger.info(f"PROXY SỐNG #{len(working)}: {proxy}")
                    if len(working) >= 8:
                        break

        logger.info(f"Đã test {tested:,} proxy → hiện có {len(working)} proxy sống")

    if not working:
        working = ["http://103.174.102.79:80", "http://154.202.119.177:80"]
        logger.warning("Dùng fallback proxy")

    # LƯU LẠI
    with open(WORKING_PROXY_FILE, 'w', encoding='utf-8') as f:
        for p in working:
            f.write(p + '\n')
    logger.info(f"ĐÃ LƯU {len(working)} PROXY SỐNG → lần sau chạy cực nhanh!")
    return working

WORKING_PROXIES = get_working_proxies()
proxy_index = 0
proxy_lock = threading.Lock()

def get_next_proxy():
    global proxy_index
    with proxy_lock:
        p = WORKING_PROXIES[proxy_index % len(WORKING_PROXIES)]
        proxy_index += 1
        return {"http": p, "https": p}

def get_headers():
    return {
        "User-Agent": random.choice([
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/130.0 Safari/537.36",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:131.0) Gecko/20100101 Firefox/131.0",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_6_1) AppleWebKit/605.1.15 Safari/605.1.15",
        ]),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Referer": "https://heovl.moe/",
        "Connection": "keep-alive",
    }

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
    for _ in range(40):
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
                    title = (a.get('title') or '').strip() or box.find('h3', class_='video-box__heading').get_text(strip=True) if box.find('h3') else "No title"
                    title = re.sub(r'\.\.\.$', '', title).strip()

                    img = a.find('img')
                    thumb = urljoin(url, img['src']) if img and img.get('src') else ''

                    stats = box.find_all('small')
                    views = int(re.sub(r'\D', '', stats[0].text.strip())) if stats else 0
                    comments = int(re.sub(r'\D', '', stats[1].text.strip())) if len(stats) > 1 else 0
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
                logger.info(f"THÀNH CÔNG trang {page_num} → {len(items)} video")
                return items, not bool(next_btn)
        except:
            pass
        time.sleep(0.7)
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
            logger.info(f"[{name}] Load {len(existing)} video cũ")
        except: pass

    all_data = existing.copy()
    page = 1
    updated = 0

    while True:
        url = base_url if page == 1 else f"{base_url}&page={page}"
        logger.info(f"[{name}] Đang quét trang {page}")
        items, is_last = scrape_page(url, page)
        if not items and page > 1: break

        for item in items:
            vid = item['id']
            if vid not in all_data or all_data[vid]['views'] != item['views']:
                all_data[vid] = item
                updated += 1

        logger.info(f"[{name}] Trang {page} → {len(items)} video | +{updated} cập nhật")
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
    logger.info("=== BẮT ĐẦU SCRAPE HEOVL.MOE – ỔN ĐỊNH 100% ===")
    threads = []
    for cat in CATEGORIES:
        t = threading.Thread(target=scrape_category, args=(cat['name'], cat['url']), daemon=True)
        t.start()
        threads.append(t)

    for t in threads:
        t.join(timeout=1800)

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
            except:
                ws = sh.add_worksheet(title=name, rows=5000, cols=10)
            ws.update([df.columns.tolist()] + df.values.tolist())
            logger.info(f"Đã cập nhật sheet '{name}' – {len(df)} dòng")

        logger.info("HOÀN TẤT TOÀN BỘ! TẤT CẢ ĐÃ LÊN GOOGLE SHEETS")
    except Exception as e:
        logger.error(f"Lỗi Google Sheets: {e}")

if __name__ == '__main__':
    main()
