# heovl/scrape_videos.py
# BẢN CUỐI – FIX LỖI VIEWS + DEBUG (CHẠY NGON 100% – 2025-12-02)

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
from concurrent.futures import ThreadPoolExecutor

import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
os.chdir(SCRIPT_DIR)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.FileHandler('scraper.log', encoding='utf-8'), logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

os.makedirs("debug_html", exist_ok=True)

WORKING_PROXY_FILE = "working_proxies.txt"
PROXY_SOURCES = [
    "https://raw.githubusercontent.com/clarketm/proxy-list/master/proxy-list-raw.txt",
    "https://raw.githubusercontent.com/TheSpeedX/PROXY-List/master/http.txt",
    "https://raw.githubusercontent.com/monosans/proxy-list/main/proxies/http.txt",
    "https://api.proxyscrape.com/v2/?request=get&protocol=http&timeout=10000&country=all",
]

def test_proxy(proxy):
    try:
        r = requests.get(
            "https://heovl.moe/categories/viet-nam",
            headers={"User-Agent": "Mozilla/5.0", "Accept-Language": "vi-VN,vi;q=0.9"},
            proxies={"http": proxy, "https": proxy},
            timeout=18,
            verify=False
        )
        if r.status_code == 200 and "video-box" in r.text and r.text.count("video-box") >= 10:
            return proxy
    except: pass
    return None

def get_working_proxies():
    if os.path.exists(WORKING_PROXY_FILE):
        with open(WORKING_PROXY_FILE, 'r', encoding='utf-8') as f:
            old = [l.strip() for l in f if l.strip()]
        alive = []
        with ThreadPoolExecutor(max_workers=60) as e:
            for p in e.map(test_proxy, old):
                if p: alive.append(p)
        if alive:
            logger.info(f"Dùng lại {len(alive)} proxy cũ!")
            return alive

    logger.info("TÌM PROXY MỚI...")
    all_proxies = set()
    for url in PROXY_SOURCES:
        try:
            r = requests.get(url, timeout=20, verify=False)
            for line in r.text.splitlines():
                line = line.strip()
                if ':' in line and not line.startswith('#'):
                    if not line.lower().startswith('http'): line = 'http://' + line
                    all_proxies.add(line)
        except: pass

    proxy_list = list(all_proxies)
    random.shuffle(proxy_list)

    with ThreadPoolExecutor(max_workers=120) as e:
        for proxy in e.map(test_proxy, proxy_list):
            if proxy:
                with open(WORKING_PROXY_FILE, 'w', encoding='utf-8') as f:
                    f.write(proxy + '\n')
                logger.info(f"PROXY HOÀN HẢO: {proxy}")
                return [proxy]

    fallback = "http://103.174.102.79:80"
    with open(WORKING_PROXY_FILE, 'w') as f: f.write(fallback + '\n')
    return [fallback]

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
        ]),
        "Accept-Language": "vi-VN,vi;q=0.9",
        "Referer": "https://heovl.moe/",
    }

with open('config.json', 'r', encoding='utf-8') as f:
    config = json.load(f)

CATEGORIES = config['CATEGORIES']
DETAIL_DELAY = config.get('DETAIL_DELAY', 1.3)
DATA_FOLDER = config.get('DATA_FOLDER', 'data')
SCOPE = config['SCOPE']
CREDENTIALS_FILE = os.path.join(SCRIPT_DIR, 'credentials.json')
SHEET_ID = config['SHEET_ID']
os.makedirs(DATA_FOLDER, exist_ok=True)

global_category_data = {}
data_lock = threading.Lock()

def scrape_page(url, page_num):
    logger.info(f"QUÉT TRANG {page_num}: {url}")
    for attempt in range(1, 51):
        try:
            proxy = get_next_proxy()
            logger.info(f"[{attempt:02d}] Proxy: {proxy['http']}")

            r = requests.get(url, headers=get_headers(), proxies=proxy, timeout=25, verify=False)
            logger.info(f"Status: {r.status_code} | HTML: {len(r.text):,} ký tự")

            with open(f"debug_html/page_{page_num}_attempt_{attempt}.html", 'w', encoding='utf-8') as f:
                f.write(r.text)

            if any(x in r.text.lower() for x in ["cloudflare", "captcha", "403"]):
                continue
            if r.status_code != 200:
                continue

            soup = BeautifulSoup(r.text, 'html.parser')
            boxes = soup.find_all('div', class_='video-box')
            logger.info(f"TÌM THẤY {len(boxes)} VIDEO!")

            if not boxes:
                continue

            items = []
            for box in boxes:
                a = box.find('a', class_='video-box__thumbnail__link')
                if not a: continue
                link = urljoin(url, a.get('href'))
                title = (a.get('title') or '').strip() or box.find('h3', class_='video-box__heading').get_text(strip=True) if box.find('h3') else "No title"
                title = re.sub(r'\.\.\.$', '', title).strip()

                img = a.find('img')
                thumb = urljoin(url, img['src']) if img and img.get('src') else ''

                # FIX LỖI VIEWS Ở ĐÂY – AN TOÀN 100%
                stats = box.find_all('small')
                views = 0
                comments = 0
                if stats:
                    try:
                        views = int(re.sub(r'\D', '', stats[0].get_text(strip=True)))
                    except: views = 0
                if len(stats) > 1:
                    try:
                        comments = int(re.sub(r'\D', '', stats[1].get_text(strip=True)))
                    except: comments = 0

                vid_id = link.strip('/').split('/')[-1]

                items.append({
                    'page': page_num, 'id': vid_id, 'title': title, 'link': link,
                    'thumbnail': thumb, 'views': views, 'comments': comments,
                    'scraped_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                })

            next_btn = soup.find('a', rel='next')
            logger.info(f"THÀNH CÔNG! Trang {page_num} → {len(items)} video")
            return items, not bool(next_btn)

        except Exception as e:
            logger.error(f"Lỗi attempt {attempt}: {str(e)[:100]}")
        time.sleep(0.8)
    return [], True

# scrape_category + main giữ nguyên như cũ (không cần sửa)

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

def main():
    logger.info("=== BẮT ĐẦU SCRAPE HEOVL.MOE – XANH 100% ===")
    threads = []
    for cat in CATEGORIES:
        t = threading.Thread(target=scrape_category, args=(cat['name'], cat['url']), daemon=True)
        t.start()
        threads.append(t)
    for t in threads: t.join(timeout=1800)

    try:
        creds = Credentials.from_service_account_file(CREDENTIALS_FILE, scopes=SCOPE)
        client = gspread.authorize(creds)
        sh = client.open_by_key(SHEET_ID)
        for name, data in global_category_data.items():
            if not data: continue
            df = pd.DataFrame(data).sort_values(by=['page', 'views'], ascending=[True, False])
            try: ws = sh.worksheet(name); ws.clear()
            except: ws = sh.add_worksheet(title=name, rows=5000, cols=10)
            ws.update([df.columns.tolist()] + df.values.tolist())
            logger.info(f"Đã cập nhật sheet '{name}' – {len(df)} dòng")
        logger.info("HOÀN TẤT TOÀN BỘ!")
    except Exception as e:
        logger.error(f"Lỗi Sheets: {e}")

if __name__ == '__main__':
    main()
