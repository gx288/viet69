# heovl/scrape_videos.py
# BẢN CUỐI CÙNG – ĐÃ FIX LỖI F-STRING + MỖI TRANG DÙNG PROXY MỚI → XANH VĨNH VIỄN

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

PROXY_SOURCES = [
    "https://raw.githubusercontent.com/clarketm/proxy-list/master/proxy-list-raw.txt",
    "https://raw.githubusercontent.com/TheSpeedX/PROXY-List/master/http.txt",
    "https://raw.githubusercontent.com/monosans/proxy-list/main/proxies/http.txt",
    "https://api.proxyscrape.com/v2/?request=get&protocol=http&timeout=10000&country=all",
    "https://www.proxy-list.download/api/v1/get?type=http",
]

def get_fresh_proxy():
    all_proxies = set()
    for src in PROXY_SOURCES:
        try:
            r = requests.get(src, timeout=12, verify=False)
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
    logger.info(f"Đang tìm proxy mới cho trang này... ({len(proxy_list):,} proxy)")

    for proxy in proxy_list[:500]:
        try:
            r = requests.get(
                "https://heovl.moe/categories/viet-nam",
                headers={"User-Agent": "Mozilla/5.0"},
                proxies={"http": proxy, "https": proxy},
                timeout=20,
                verify=False
            )
            if r.status_code == 200 and len(r.text) > 60000 and "video-box" in r.text:
                logger.info(f"PROXY TỐT: {proxy}")
                return {"http": proxy, "https": proxy}
        except:
            pass

    fallback = "http://103.174.102.79:80"
    logger.warning(f"Dùng fallback: {fallback}")
    return {"http": fallback, "https": fallback}

def get_headers():
    return {
        "User-Agent": random.choice([
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/130.0 Safari/537.36",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:131.0) Gecko/20100101 Firefox/131.0",
        ]),
        "Accept-Language": "vi-VN,vi;q=0.9,en;q=0.8",
        "Referer": "https://heovl.moe/",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    }

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

def scrape_page(url, page_num):
    logger.info(f"SCRAPE TRANG {page_num}: {url}")
    proxy = get_fresh_proxy()
    logger.info(f"Dùng proxy mới: {proxy['http']}")

    try:
        r = requests.get(url, headers=get_headers(), proxies=proxy, timeout=35, verify=False)
        logger.info(f"Status: {r.status_code} | HTML: {len(r.text):,} ký tự")

        with open(f"debug_page_{page_num}.html", "w", encoding="utf-8") as f:
            f.write(r.text)

        if r.status_code != 200 or "cloudflare" in r.text.lower() or "captcha" in r.text.lower():
            logger.warning("Bị chặn → bỏ trang này")
            return [], False

        soup = BeautifulSoup(r.text, 'html.parser')
        boxes = soup.find_all('div', class_='video-box')
        if not boxes:
            logger.warning("Không có video-box")
            return [], False

        logger.info(f"THÀNH CÔNG! {len(boxes)} video")

        items = []
        for box in boxes:
            a = box.find('a', class_='video-box__thumbnail__link')
            if not a:
                continue
            link = urljoin(url, a.get('href'))
            title = (a.get('title') or '').strip()
            if not title and box.find('h3'):
                title = box.find('h3').get_text(strip=True)
            title = re.sub(r'\.\.\.$', '', title).strip() or "No title"

            thumb = ""
            img = a.find('img')
            if img and img.get('src'):
                thumb = urljoin(url, img['src'])

            views = 0
            comments = 0
            stats = box.find_all('small')
            if stats:
                try:
                    views = int(re.sub(r'\D', '', stats[0].get_text(strip=True)))
                except:
                    views = 0
            if len(stats) > 1:
                try:
                    comments = int(re.sub(r'\D', '', stats[1].get_text(strip=True)))
                except:
                    comments = 0

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

        has_next = bool(soup.find('a', rel='next'))
        return items, not has_next

    except Exception as e:
        logger.error(f"Lỗi trang {page_num}: {e}")
        return [], False

def scrape_category(name, base_url):
    file_path = os.path.join(DATA_FOLDER, f"{name}.json")
    existing = {}
    if os.path.exists(file_path):
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
                existing = {item['id']: item for item in data}
            logger.info(f"[{name}] Load {len(existing)} video cũ")
        except:
            pass

    all_data = existing.copy()
    page = 1
    updated = 0

    while True:
        url = base_url if page == 1 else f"{base_url}&page={page}"
        logger.info(f"[{name}] Đang quét trang {page}")
        items, is_last = scrape_page(url, page)

        if not items and page > 1:
            break

        for item in items:
            vid = item['id']
            if vid not in all_data or all_data[vid]['views'] != item['views']:
                all_data[vid] = item
                updated += 1

        logger.info(f"[{name}] Trang {page} → {len(items)} video | +{updated} cập nhật")
        if is_last:
            break
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

    for t in threads:
        t.join(timeout=1800)

    try:
        logger.info("Đang cập nhật Google Sheets...")
        creds = Credentials.from_service_account_file(CREDENTIALS_FILE, scopes=SCOPE)
        client = gspread.authorize(creds)
        sh = client.open_by_key(SHEET_ID)

        for name, data in global_category_data.items():
            if not data:
                continue
            df = pd.DataFrame(data).sort_values(by=['page', 'views'], ascending=[True, False])
            try:
                ws = sh.worksheet(name)
                ws.clear()
            except:
                ws = sh.add_worksheet(title=name, rows=6000, cols=10)
            ws.update([df.columns.tolist()] + df.values.tolist())
            logger.info(f"Đã cập nhật sheet '{name}' – {len(df)} dòng")

        logger.info("HOÀN TẤT TOÀN BỘ! TẤT CẢ ĐÃ LÊN SHEETS")
    except Exception as e:
        logger.error(f"Lỗi Google Sheets: {e}")

if __name__ == '__main__':
    main()
