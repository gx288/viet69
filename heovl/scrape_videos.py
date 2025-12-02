# heovl/scrape_videos.py
# BẢN CUỐI + DEBUG SIÊU CHI TIẾT – CHẠY NGON 100% (2025-12-02)

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

# Tắt cảnh báo SSL
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

# Thư mục debug
os.makedirs("debug_html", exist_ok=True)

WORKING_PROXY_FILE = "working_proxies.txt"
PROXY_SOURCES = [
    "https://raw.githubusercontent.com/clarketm/proxy-list/master/proxy-list-raw.txt",
    "https://raw.githubusercontent.com/TheSpeedX/PROXY-List/master/http.txt",
    "https://raw.githubusercontent.com/monosans/proxy-list/main/proxies/http.txt",
    "https://api.proxyscrape.com/v2/?request=get&protocol=http&timeout=10000&country=all",
    "https://www.proxy-list.download/api/v1/get?type=http",
]

# ====================== TEST PROXY SIÊU CHẶT ======================
def test_proxy(proxy):
    try:
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/130.0 Safari/537.36",
            "Accept-Language": "vi-VN,vi;q=0.9",
            "Referer": "https://heovl.moe/",
        }
        r = requests.get(
            "https://heovl.moe/categories/viet-nam",
            headers=headers,
            proxies={"http": proxy, "https": proxy},
            timeout=18,
            verify=False
        )
        if r.status_code != 200:
            return None
        text = r.text.lower()
        if any(x in text for x in ["cloudflare", "checking your browser", "captcha", "403 forbidden"]):
            return None
        if text.count("video-box") >= 10:
            return proxy
    except:
        pass
    return None

# ====================== TÌM PROXY HOÀN HẢO ======================
def get_working_proxies():
    if os.path.exists(WORKING_PROXY_FILE):
        with open(WORKING_PROXY_FILE, 'r', encoding='utf-8') as f:
            old = [l.strip() for l in f if l.strip()]
        logger.info(f"Test lại {len(old)} proxy cũ...")
        alive = []
        with ThreadPoolExecutor(max_workers=60) as e:
            for p in e.map(test_proxy, old):
                if p: alive.append(p)
        if alive:
            logger.info(f"Dùng lại {len(alive)} proxy cũ HOÀN HẢO!")
            return alive

    logger.info("TÌM PROXY MỚI – CHỈ CHẤP NHẬN QUA CLOUDFLARE!")
    all_proxies = set()
    for url in PROXY_SOURCES:
        try:
            r = requests.get(url, timeout=20, verify=False)
            if r.status_code == 200:
                for line in r.text.splitlines():
                    line = line.strip()
                    if ':' in line and not line.startswith('#'):
                        if not line.lower().startswith('http'):
                            line = 'http://' + line
                        all_proxies.add(line)
        except: pass

    proxy_list = list(all_proxies)
    random.shuffle(proxy_list)
    logger.info(f"Tổng {len(proxy_list):,} proxy → test nghiêm ngặt...")

    with ThreadPoolExecutor(max_workers=120) as executor:
        for proxy in executor.map(test_proxy, proxy_list):
            if proxy:
                with open(WORKING_PROXY_FILE, 'w', encoding='utf-8') as f:
                    f.write(proxy + '\n')
                logger.info(f"TÌM THẤY PROXY HOÀN HẢO: {proxy} → CHẠY NGAY!")
                return [proxy]

    fallback = "http://103.174.102.79:80"
    with open(WORKING_PROXY_FILE, 'w', encoding='utf-8') as f:
        f.write(fallback + '\n')
    logger.warning("Dùng fallback proxy")
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
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "vi-VN,vi;q=0.9",
        "Referer": "https://heovl.moe/",
    }

# ====================== LOAD CONFIG ======================
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

# ====================== SCRAPE PAGE + DEBUG SIÊU CHI TIẾT ======================
def scrape_page(url, page_num):
    logger.info(f"=== BẮT ĐẦU QUÉT TRANG {page_num}: {url} ===")
    for attempt in range(1, 51):
        try:
            proxy = get_next_proxy()
            logger.info(f"[{attempt:02d}] Dùng proxy → {proxy['http']}")

            r = requests.get(url, headers=get_headers(), proxies=proxy, timeout=25, verify=False)

            logger.info(f"Status Code: {r.status_code} | Độ dài HTML: {len(r.text):,} ký tự")

            # LƯU HTML ĐỂ DEBUG (rất quan trọng!)
            debug_file = f"debug_html/page_{page_num}_attempt_{attempt}.html"
            with open(debug_file, 'w', encoding='utf-8') as f:
                f.write(r.text)
            logger.info(f"Đã lưu HTML debug → {debug_file}")

            # Kiểm tra Cloudflare chặn
            if any(x in r.text.lower() for x in ["cloudflare", "checking your browser", "captcha", "403 forbidden"]):
                logger.warning("Bị Cloudflare chặn → bỏ proxy này")
                continue

            if r.status_code != 200:
                logger.warning(f"Status {r.status_code} → thử tiếp")
                continue

            soup = BeautifulSoup(r.text, 'html.parser')
            boxes = soup.find_all('div', class_='video-box')
            logger.info(f"TÌM THẤY {len(boxes)} VIDEO-BOX TRÊN TRANG!")

            if len(boxes) == 0:
                logger.warning("Không có video-box → có thể bị chặn nhẹ")
                continue

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
                views = int(re.sub(r'\D', '', stats[0].text.strip())) if stats else 0
                comments = int(re.sub(r'\D', '', stats[1].text.strip())) if len(stats)>1 else 0
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
            logger.error(f"Lỗi ở attempt {attempt}: {str(e)[:100]}")
        time.sleep(0.8)

    logger.error(f"THẤT BẠI HOÀN TOÀN trang {page_num} sau 50 lần thử")
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
    logger.info(f"[{name}] HOÀN TẤT → {len(sorted_data)} video (cập nhật {updated})")

# ====================== MAIN ======================
def main():
    logger.info("=== BẮT ĐẦU SCRAPE HEOVL.MOE – ỔN ĐỊNH VĨNH VIỄN + DEBUG ===")
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
        logger.info("HOÀN TẤT TOÀN BỘ!")
    except Exception as e:
        logger.error(f"Lỗi Sheets: {e}")

if __name__ == '__main__':
    main()
