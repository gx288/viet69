# heovl/scrape_videos.py
# BẢN SIÊU TỐC – DÙNG PROXY SỐNG SẴN → XANH TRONG 30–60 GIÂY!

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

# DÙNG 1 NGUỒN PROXY SỐNG SẴN – CẬP NHẬT HÀNG GIỜ (SIÊU NHANH!)
LIVE_PROXY_URL = "https://api.proxyscrape.com/v3/free-proxy-list/get?request=displayproxies&proxytype=http&timeout=10000&country=all&anonymity=elite,anonymous&ssl=yes"

# Cache proxy trong 30 phút
PROXY_CACHE_FILE = "live_proxies.txt"
PROXY_CACHE_TIME = 30 * 60  # 30 phút

def get_live_proxies():
    if os.path.exists(PROXY_CACHE_FILE):
        age = time.time() - os.path.getmtime(PROXY_CACHE_FILE)
        if age < PROXY_CACHE_TIME:
            with open(PROXY_CACHE_FILE, 'r', encoding='utf-8') as f:
                proxies = [f"http://{line.strip()}" for line in f if line.strip()]
            if proxies:
                logger.info(f"DÙNG {len(proxies)} PROXY SỐNG TỪ CACHE → SIÊU NHANH!")
                return proxies

    logger.info("TẢI DANH SÁCH PROXY SỐNG MỚI...")
    try:
        r = requests.get(LIVE_PROXY_URL, timeout=15)
        proxies = [f"http://{line.strip()}" for line in r.text.splitlines() if ':' in line]
        with open(PROXY_CACHE_FILE, 'w', encoding='utf-8') as f:
            for p in proxies:
                f.write(p.replace("http://", "") + '\n')
        logger.info(f"ĐÃ TẢI {len(proxies)} PROXY SỐNG!")
        return proxies
    except:
        logger.warning("Dùng fallback proxy")
        return ["http://103.174.102.79:80", "http://154.202.119.177:80"]

LIVE_PROXIES = get_live_proxies()
proxy_index = 0
proxy_lock = threading.Lock()

def get_next_proxy():
    global proxy_index
    with proxy_lock:
        if not LIVE_PROXIES:
            return {"http": "http://103.174.102.79:80", "https": "http://103.174.102.79:80"}
        p = LIVE_PROXIES[proxy_index % len(LIVE_PROXIES)]
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
DETAIL_DELAY = config.get('DETAIL_DELAY', 1.2)
DATA_FOLDER = config.get('DATA_FOLDER', 'data')
SCOPE = config['SCOPE']
CREDENTIALS_FILE = os.path.join(SCRIPT_DIR, 'credentials.json')
SHEET_ID = config['SHEET_ID']
os.makedirs(DATA_FOLDER, exist_ok=True)

global_category_data = {}
data_lock = threading.Lock()

def scrape_page(url, page_num):
    for _ in range(30):
        proxy = get_next_proxy()
        try:
            r = requests.get(url, headers=get_headers(), proxies=proxy, timeout=20, verify=False)
            if r.status_code != 200 or "cloudflare" in r.text.lower():
                continue

            soup = BeautifulSoup(r.text, 'html.parser')
            boxes = soup.find_all('div', class_='video-box')
            if not boxes:
                continue

            items = []
            for box in boxes:
                a = box.find('a', class_='video-box__thumbnail__link')
                if not a: continue
                link = urljoin(url, a.get('href'))
                title = (a.get('title') or box.find('h3').get_text(strip=True) if box.find('h3') else "No title")
                title = re.sub(r'\.\.\.$', '', title).strip()

                thumb = urljoin(url, a.find('img')['src']) if a.find('img') and a.find('img').get('src') else ''

                stats = box.find_all('small')
                views = int(re.sub(r'\D', '', stats[0].text)) if stats else 0
                comments = int(re.sub(r'\D', '', stats[1].text)) if len(stats) > 1 else 0
                vid_id = link.strip('/').split('/')[-1]

                items.append({
                    'page': page_num, 'id': vid_id, 'title': title, 'link': link,
                    'thumbnail': thumb, 'views': views, 'comments': comments,
                    'scraped_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                })

            logger.info(f"THÀNH CÔNG TRANG {page_num} → {len(items)} video")
            return items, not bool(soup.find('a', rel='next'))
        except:
            continue
    return [], True

def scrape_category(name, base_url):
    file_path = os.path.join(DATA_FOLDER, f"{name}.json")
    existing = {i['id']: i for i in json.load(open(file_path, 'r', encoding='utf-8'))} if os.path.exists(file_path) else {}
    all_data = existing.copy()
    page = 1
    updated = 0

    while True:
        url = base_url if page == 1 else f"{base_url}&page={page}"
        logger.info(f"[{name}] Trang {page}")
        items, is_last = scrape_page(url, page)
        if not items and page > 1: break
        for item in items:
            if item['id'] not in all_data or all_data[item['id']]['views'] != item['views']:
                all_data[item['id']] = item
                updated += 1
        logger.info(f"[{name}] Trang {page} → {len(items)} | +{updated}")
        if is_last: break
        page += 1
        time.sleep(DETAIL_DELAY)

    sorted_data = sorted(all_data.values(), key=lambda x: (x['page'], -x['views']))
    with open(file_path, 'w', encoding='utf-8') as f:
        json.dump(sorted_data, f, ensure_ascii=False, indent=2)
    with data_lock:
        global_category_data[name] = sorted_data
    logger.info(f"[{name}] HOÀN TẤT → {len(sorted_data)} video")

def main():
    logger.info("=== BẮT ĐẦU SCRAPE – SIÊU NHANH VỚI PROXY SỐNG SẴN ===")
    threads = [threading.Thread(target=scrape_category, args=(c['name'], c['url']), daemon=True) for c in CATEGORIES]
    for t in threads: t.start()
    for t in threads: t.join(timeout=1800)

    try:
        creds = Credentials.from_service_account_file(CREDENTIALS_FILE, scopes=SCOPE)
        client = gspread.authorize(creds)
        sh = client.open_by_key(SHEET_ID)
        for name, data in global_category_data.items():
            if not data: continue
            df = pd.DataFrame(data).sort_values(by=['page', 'views'], ascending=[True, False])
            try: ws = sh.worksheet(name); ws.clear()
            except: ws = sh.add_worksheet(title=name, rows=6000, cols=10)
            ws.update([df.columns.tolist()] + df.values.tolist())
        logger.info("HOÀN TẤT! ĐÃ UP LÊN SHEETS")
    except Exception as e:
        logger.error(f"Lỗi Sheets: {e}")

if __name__ == '__main__':
    main()
