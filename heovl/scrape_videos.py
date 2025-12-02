# heovl/scrape_videos.py
# BẢN CUỐI CÙNG – CHẠY MƯỢT TỪ LẦN ĐẦU ĐẾN MÃI MÃI (2025-12-02)

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

# TẮT HẾT CẢNH BÁO SSL (bắt buộc khi dùng proxy miễn phí)
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

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

# ====================== TEST 1 PROXY ======================
def test_proxy(proxy):
    try:
        r = requests.get(
            "https://heovl.moe/",
            headers={"User-Agent": "Mozilla/5.0"},
            proxies={"http": proxy, "https": proxy},
            timeout=12,
            verify=False
        )
        if r.status_code == 200 and len(r.text) > 15000:
            return proxy
    except:
        pass
    return None

# ====================== CHỌN PROXY THÔNG MINH ======================
def get_working_proxies():
    # 1. Nếu có file cũ → test lại trước (ưu tiên proxy cũ còn sống)
    if os.path.exists(WORKING_PROXY_FILE):
        with open(WORKING_PROXY_FILE, 'r', encoding='utf-8') as f:
            old_proxies = [line.strip() for line in f if line.strip()]

        logger.info(f"Đang kiểm tra lại {len(old_proxies)} proxy cũ...")
        still_alive = []
        with ThreadPoolExecutor(max_workers=50) as executor:
            for result in executor.map(test_proxy, old_proxies):
                if result:
                    still_alive.append(result)

        if still_alive:
            logger.info(f"→ Còn {len(still_alive)} proxy cũ sống → DÙNG LUÔN!")
            return still_alive

    # 2. Nếu không còn proxy cũ nào sống → tìm mới
    logger.info("Không còn proxy cũ nào sống → TÌM MỚI (có 1 cái là chạy luôn)!")
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
        except:
            pass

    proxy_list = list(all_proxies)
    random.shuffle(proxy_list)
    logger.info(f"Tổng {len(proxy_list):,} proxy mới → test song song...")

    # TÌM 1 CÁI LÀ DỪNG LUÔN
    with ThreadPoolExecutor(max_workers=150) as executor:
        for proxy in executor.map(test_proxy, proxy_list):
            if proxy:
                logger.info(f"→ TÌM THẤY PROXY SỐNG: {proxy} → CHẠY SCRAPE NGAY!")
                with open(WORKING_PROXY_FILE, 'w', encoding='utf-8') as f:
                    f.write(proxy + '\n')
                return [proxy]  # Chỉ cần 1 cái là đủ!

    # 3. Nếu kỳ lạ lắm không có → dùng fallback
    fallback = "http://103.174.102.79:80"
    logger.warning("Dùng proxy fallback")
    with open(WORKING_PROXY_FILE, 'w', encoding='utf-8') as f:
        f.write(fallback + '\n')
    return [fallback]

# ====================== KHỞI TẠO PROXY ======================
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
        "Referer": "https://heovl.moe/",
    }

# ====================== LOAD CONFIG ======================
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

# ====================== SCRAPE PAGE ======================
def scrape_page(url, page_num):
    for _ in range(30):
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
                    title = (a.get('title') or '').strip()
                    if not title:
                        h3 = box.find('h3', class_='video-box__heading')
                        title = h3.get_text(strip=True) if h3 else "No title"
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
        time.sleep(0.8)
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
    logger.info("=== BẮT ĐẦU SCRAPE HEOVL.MOE – ỔN ĐỊNH VĨNH VIỄN ===")
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

        logger.info("HOÀN TẤT TOÀN BỘ! DỮ LIỆU ĐÃ LÊN GOOGLE SHEETS")
    except Exception as e:
        logger.error(f"Lỗi Google Sheets: {e}")

if __name__ == '__main__':
    main()
