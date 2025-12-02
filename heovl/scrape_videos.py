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
            logger.info(f"Đang lấy proxy từ {url.split('/')[-2] if '/' in url else url}...")
            r = requests.get(url, timeout=15)
            if r.status_code == 200:
                lines = r.text.strip().splitlines()
                for line in lines:
                    line = line.strip()
                    if line and ':' in line and not line.startswith('#'):
                        if not line.startswith('http'):
                            line = 'http://' + line
                        proxies.add(line)
        except Exception as e:
            logger.warning(f"Không lấy được proxy từ {url}: {e}")
        time.sleep(1)

    proxy_list = list(proxies)
    logger.info(f"Đã tải thành công {len(proxy_list)} proxy mới!")
    return proxy_list

# Lấy proxy mới mỗi lần chạy
try:
    WORKING_PROXIES = fetch_fresh_proxies()
    if not WORKING_PROXIES:
        raise Exception("Không lấy được proxy nào")
except:
    logger.error("Lấy proxy thất bại → dùng fallback")
    WORKING_PROXIES = [
        "http://154.202.119.177:80",
        "http://103.174.102.79:80",
        "http://43.135.164.2:13000",
        "http://47.74.152.29:8888",
        "http://156.236.74.247:80",
        "http://43.153.207.93:3128",
    ]

random.shuffle(WORKING_PROXIES)

def get_next_proxy():
    """Lấy proxy tiếp theo (vòng lặp)"""
    while True:
        for proxy in WORKING_PROXIES:
            yield {"http": proxy, "https": proxy}
        time.sleep(5)  # nếu hết thì đợi 5s rồi lặp lại

proxy_generator = get_next_proxy()

# ====================== HEADERS ======================
def get_headers():
    agents = [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/130.0 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:131.0) Gecko/20100101 Firefox/131.0",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_6_1) AppleWebKit/605.1.15 Safari/605.1.15",
    ]
    return {
        "User-Agent": random.choice(agents),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "vi-VN,vi;q=0.9,en;q=0.8",
        "Referer": "https://heovl.moe/",
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1",
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

# ====================== SCRAPE PAGE VỚI PROXY VÒNG ======================
def scrape_page(url, page_num):
    for attempt in range(15):  # thử tối đa 15 proxy
        try:
            proxy = next(proxy_generator)
            r = requests.get(
                url,
                headers=get_headers(),
                proxies=proxy,
                timeout=20,
                verify=False  # một số proxy cần tắt SSL verify
            )
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
                    title = a.get('title') or box.find('h3').get_text(strip=True) if box.find('h3') else "No title"
                    title = re.sub(r'\.\.\.$', '', title).strip()

                    img = a.find('img')
                    thumb = urljoin(url, img['src']) if img and img.get('src') else ''

                    stats = box.find_all('small')
                    views = int(re.sub(r'\D', '', stats[0].text)) if stats else 0
                    comments = int(re.sub(r'\D', '', stats[1].text)) if len(stats)>1 else 0

                    vid = link.strip('/').split('/')[-1]

                    items.append({
                        'page': page_num,
                        'id': vid,
                        'title': title,
                        'link': link,
                        'thumbnail': thumb,
                        'views': views,
                        'comments': comments,
                        'scraped_at': datetime.now().strftime('%Y-%m-%d %H:%M')
                    })

                next_page = soup.find('a', rel='next')
                return items, not bool(next_page)

        except Exception as e:
            pass  # im lặng, thử proxy tiếp

        time.sleep(1.5)

    logger.error(f"Không thể scrape {url} sau 15 proxy")
    return [], True

# ====================== RANDOM HEADERS (CHỐNG 403) ======================
ua = UserAgent(browsers=['chrome', 'firefox', 'edge', 'safari'])

def get_headers():
    return {
        'User-Agent': ua.random,
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Language': 'vi-VN,vi;q=0.9,en-US;q=0.8,en;q=0.7',
        'Accept-Encoding': 'gzip, deflate, br',
        'Connection': 'keep-alive',
        'Upgrade-Insecure-Requests': '1',
        'Sec-Fetch-Dest': 'document',
        'Sec-Fetch-Mode': 'navigate',
        'Sec-Fetch-Site': 'none',
        'Sec-Fetch-User': '?1',
        'Cache-Control': 'max-age=0',
        'Referer': 'https://heovl.moe/',
        'DNT': '1',
    }

# ====================== HELPERS ======================
def clean_title(title):
    return re.sub(r'\.\.\.$', '', title).strip()

def parse_number(text):
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
        title = a_tag.get('title') or ''
        if not title:
            h3 = box.find('h3', class_='video-box__heading')
            title = h3.get_text(strip=True) if h3 else ''

        title = clean_title(title)

        img = a_tag.find('img')
        thumbnail = urljoin(base_url, img['src']) if img and img.get('src') else ''

        stats = box.find_all('small')
        views = parse_number(stats[0].get_text(strip=True)) if len(stats) >= 1 else 0
        comments = parse_number(stats[1].get_text(strip=True)) if len(stats) >= 2 else 0

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
        logger.warning(f"Lỗi parse 1 video: {e}")
        return None

def scrape_page(url, page_num):
    try:
        r = requests.get(url, headers=get_headers(), timeout=20)
        if r.status_code == 403:
            logger.warning(f"403 Forbidden tại {url} – thử lại với header mới...")
            time.sleep(3)
            r = requests.get(url, headers=get_headers(), timeout=20)

        r.raise_for_status()
        soup = BeautifulSoup(r.text, 'html.parser')
        boxes = soup.find_all('div', class_='video-box')

        if not boxes:
            return [], True

        items = []
        for box in boxes:
            data = extract_video_data(box, url, page_num)
            if data:
                items.append(data)

        next_btn = soup.find('a', rel='next')
        return items, not bool(next_btn)

    except Exception as e:
        logger.error(f"Lỗi khi scrape {url}: {e}")
        return [], True

# ====================== SCRAPE 1 CATEGORY ======================
def scrape_category(category_name, base_url):
    data_file = os.path.join(DATA_FOLDER, f"{category_name}.json")
    existing = {}

    if os.path.exists(data_file):
        try:
            with open(data_file, 'r', encoding='utf-8') as f:
                existing = {item['id']: item for item in json.load(f)}
            logger.info(f"[{category_name}] Đã load {len(existing)} video cũ")
        except:
            existing = {}

    all_items = existing.copy()
    page = 1
    updated = 0

    while True:
        url = base_url if page == 1 else f"{base_url}&page={page}"
        logger.info(f"[{category_name}] Đang quét trang {page}")

        items, is_last = scrape_page(url, page)
        if not items:
            break

        for item in items:
            vid = item['id']
            if vid not in all_items or all_items[vid]['views'] != item['views'] or all_items[vid]['comments'] != item['comments']:
                all_items[vid] = item
                updated += 1

        logger.info(f"[{category_name}] Trang {page} → {len(items)} video | +{updated} cập nhật")

        if is_last:
            logger.info(f"[{category_name}] Đã tới trang cuối")
            break

        page += 1
        time.sleep(DETAIL_DELAY + random.uniform(0.3, 0.9))

    # Lưu lại
    sorted_items = sorted(all_items.values(), key=lambda x: (x['page'], -x['views']))
    with open(data_file, 'w', encoding='utf-8') as f:
        json.dump(sorted_items, f, ensure_ascii=False, indent=2)

    global_category_data[category_name] = sorted_items
    logger.info(f"[{category_name}] HOÀN TẤT – Tổng {len(sorted_items)} video (cập nhật {updated})")

# ====================== MAIN ======================
global_category_data = {}
lock = threading.Lock()

def main():
    logger.info("=== BẮT ĐẦU SCRAPE HEOVL.MOE ===")

    threads = []
    for cat in CATEGORIES:
        t = threading.Thread(target=scrape_category, args=(cat['name'], cat['url']), daemon=True)
        t.start()
        threads.append(t)

    for t in threads:
        t.join(timeout=900)

    # ====================== CẬP NHẬT GOOGLE SHEETS ======================
    try:
        logger.info("Đang kết nối Google Sheets...")
        creds = Credentials.from_service_account_file(CREDENTIALS_FILE, scopes=SCOPE)
        client = gspread.authorize(creds)
        sh = client.open_by_key(SHEET_ID)

        for name, data in global_category_data.items():
            if not data:
                continue
            df = pd.DataFrame(data)
            df = df.sort_values(by=['page', 'views'], ascending=[True, False])

            try:
                ws = sh.worksheet(name)
                ws.clear()
            except gspread.exceptions.WorksheetNotFound:
                ws = sh.add_worksheet(title=name, rows=2000, cols=10)

            ws.update([df.columns.tolist()] + df.values.tolist())
            logger.info(f"Đã cập nhật sheet '{name}' – {len(df)} dòng")

        logger.info("HOÀN TẤT TOÀN BỘ! Dữ liệu đã lên Google Sheets.")

    except Exception as e:
        logger.error(f"Lỗi Google Sheets: {e}")

if __name__ == '__main__':
    main()
