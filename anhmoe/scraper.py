import os
import json
import time
import sys
import subprocess
import pickle
import base64
import tempfile
from urllib.parse import unquote
from pathlib import Path
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.common.exceptions import NoSuchElementException
from google.oauth2.service_account import Credentials
import gspread

# ────────────────────────────────────────────────
# CONFIG
# ────────────────────────────────────────────────
creds_json = os.getenv('GOOGLE_CREDENTIALS')
if not creds_json:
    raise ValueError("Biến môi trường GOOGLE_CREDENTIALS chưa được thiết lập")

creds_dict = json.loads(creds_json)
scopes = ['https://www.googleapis.com/auth/spreadsheets']
creds = Credentials.from_service_account_info(creds_dict, scopes=scopes)
client = gspread.authorize(creds)

SPREADSHEET_ID = '1RWAd7HrgnzfRK9PpD5Zy7OHwMv6mfQh17jvqNWGHsaU'
SHEET_NAME = 'anhmoe videos'
BASE_URL = 'https://zpic.io/category/video-nsfw'
JSON_PATH = 'anhmoe/videos_data.json'
COOKIE_FILE = 'zpic_cookies.pkl'  # chỉ dùng local, không commit

HEADERS = ['Title', 'Author', 'Duration', 'Thumb URL', 'Video URL', 'Page Number', 'Page Link']

# ────────────────────────────────────────────────
# GOOGLE SHEET FUNCTIONS
# ────────────────────────────────────────────────

def get_or_create_sheet():
    spreadsheet = client.open_by_key(SPREADSHEET_ID)
    try:
        sheet = spreadsheet.worksheet(SHEET_NAME)
    except gspread.exceptions.WorksheetNotFound:
        sheet = spreadsheet.add_worksheet(title=SHEET_NAME, rows=2000, cols=10)
        sheet.append_row(HEADERS)
        print("Tạo sheet mới với header.")
    else:
        current_headers = sheet.row_values(1)
        if current_headers != HEADERS:
            print("Header không khớp → cập nhật.")
            sheet.update('A1:G1', [HEADERS])
    return sheet

def load_sheet_video_urls(sheet):
    print("Đọc toàn bộ Google Sheet để lấy cache URLs...")
    values = sheet.get_all_values()
    if not values or len(values) < 2:
        return set(), []
    existing_urls = {row[4] for row in values[1:] if len(row) > 4 and row[4].strip()}
    existing_rows = values[1:]
    print(f"Sheet cache: {len(existing_urls)} video URLs.")
    return existing_urls, existing_rows

# ────────────────────────────────────────────────
# JSON FUNCTIONS
# ────────────────────────────────────────────────

def load_json_video_data():
    path = Path(JSON_PATH)
    if not path.exists():
        print(f"File JSON {JSON_PATH} chưa tồn tại → khởi tạo rỗng")
        return set(), []
    with open(path, 'r', encoding='utf-8') as f:
        try:
            data = json.load(f)
        except json.JSONDecodeError:
            print(f"JSON lỗi → coi như rỗng")
            data = []
    urls = {item.get('video_url', '') for item in data if item.get('video_url')}
    print(f"JSON cache: {len(urls)} video.")
    return urls, data

def write_json(all_data):
    path = Path(JSON_PATH)
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, 'w', encoding='utf-8') as f:
        json.dump(all_data, f, ensure_ascii=False, indent=2)
    print(f"Đã ghi JSON: {len(all_data)} items")

def append_or_update_json(new_rows):
    existing_urls, existing_data = load_json_video_data()
    added_count = 0
    new_json_rows = []
    for row in new_rows:
        vid_url = row[4]
        if vid_url and vid_url not in existing_urls:
            new_json_rows.append({
                "title": row[0],
                "author": row[1],
                "duration": row[2],
                "thumb_url": row[3],
                "video_url": vid_url,
                "page_number": row[5],
                "page_link": row[6],
                "scraped_at": time.strftime("%Y-%m-%d %H:%M:%S %z")
            })
            added_count += 1
    if new_json_rows:
        all_data = new_json_rows + existing_data
        write_json(all_data)
        print(f"Thêm {added_count} video mới vào JSON")
    else:
        print("Không có video mới để thêm vào JSON")

def sync_sheet_to_json_if_needed(sheet):
    sheet_urls, sheet_rows = load_sheet_video_urls(sheet)
    json_urls, json_data = load_json_video_data()
    missing_in_json = []
    for row in sheet_rows:
        if len(row) < 5:
            continue
        vid_url = row[4]
        if vid_url and vid_url not in json_urls:
            missing_in_json.append({
                "title": row[0],
                "author": row[1],
                "duration": row[2],
                "thumb_url": row[3],
                "video_url": vid_url,
                "page_number": row[5],
                "page_link": row[6],
                "scraped_at": time.strftime("%Y-%m-%d %H:%M:%S %z")
            })
    if missing_in_json:
        print(f"Đồng bộ {len(missing_in_json)} video từ Sheet → JSON")
        all_data = missing_in_json + json_data
        write_json(all_data)

# ────────────────────────────────────────────────
# MAIN SCRAPE FUNCTION
# ────────────────────────────────────────────────

def scrape_pages(max_pages=None):
    sheet = get_or_create_sheet()
    existing_video_urls, _ = load_sheet_video_urls(sheet)

    options = Options()
    options.add_argument('--headless')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument('--disable-gpu')
    options.add_argument('--window-size=1920,1080')
    options.add_argument('--disable-blink-features=AutomationControlled')

    print("Khởi tạo Selenium...")
    try:
        print("ChromeDriver version: " + subprocess.check_output(['chromedriver', '--version']).decode().strip())
    except:
        print("ChromeDriver: từ PATH")

    driver = webdriver.Chrome(options=options)

    # ─── LOAD COOKIE ───
    cookies_loaded = False
    driver.get("https://zpic.io/")
    time.sleep(4)

    if os.getenv('GITHUB_ACTIONS'):
        print("Chạy trên GitHub Actions → dùng secret ZPIC_COOKIES_BASE64")
        encoded = os.getenv('ZPIC_COOKIES_BASE64')
        if encoded:
            try:
                decoded_bytes = base64.b64decode(encoded)
                with tempfile.NamedTemporaryFile(delete=False, suffix='.pkl') as tmp:
                    tmp.write(decoded_bytes)
                    tmp_path = tmp.name

                with open(tmp_path, 'rb') as f:
                    cookies = pickle.load(f)

                for cookie in cookies:
                    if 'expiry' in cookie and (cookie['expiry'] is None or cookie['expiry'] < time.time()):
                        cookie['expiry'] = -1
                    try:
                        driver.add_cookie(cookie)
                    except:
                        pass
                cookies_loaded = True
                print(f"Đã load {len(cookies)} cookies từ GitHub Secret")
                os.unlink(tmp_path)
            except Exception as e:
                print(f"Lỗi decode/load secret: {e}")
        else:
            print("Không tìm thấy secret ZPIC_COOKIES_BASE64 → chạy không login")
    else:
        # Local
        if os.path.exists(COOKIE_FILE):
            try:
                with open(COOKIE_FILE, 'rb') as f:
                    cookies = pickle.load(f)
                for cookie in cookies:
                    if 'expiry' in cookie and (cookie['expiry'] is None or cookie['expiry'] < time.time()):
                        cookie['expiry'] = -1
                    driver.add_cookie(cookie)
                cookies_loaded = True
                print(f"Đã load {len(cookies)} cookies từ file local")
            except Exception as e:
                print(f"Lỗi load cookie local: {e}")

    driver.get(BASE_URL)
    time.sleep(6)

    # Kiểm tra login
    page_lower = driver.page_source.lower()
    if "login" in driver.current_url.lower() or "sign in" in page_lower:
        print("Vẫn bị redirect về login → cookie có thể hết hạn hoặc không hợp lệ")
    else:
        print("Cookie hoạt động → truy cập được nội dung")

    current_url = BASE_URL
    page_number = 1
    consecutive_duplicates = 0
    all_new_rows_this_run = []

    while True:
        print(f"\n=== Trang {page_number} === {current_url}")
        driver.get(current_url)
        time.sleep(6)

        try:
            items = driver.find_elements(By.CSS_SELECTOR,
                'div.list-item.fixed-size.c8.gutter-margin-right-bottom.jsly.position-absolute.--show.ui-selectee')
        except Exception as e:
            print(f"Lỗi lấy items: {e}")
            items = []

        page_new_rows = []
        for item in items:
            data_object_str = item.get_attribute('data-object') or ''
            video_url = ''
            title = ''

            if data_object_str:
                try:
                    decoded_str = unquote(data_object_str)
                    decoded_str = decoded_str.replace('\\"', '"').replace('\\\\', '\\').strip()
                    data_obj = json.loads(decoded_str)
                    video_url = (
                        data_obj.get('image', {}).get('url') or
                        data_obj.get('url') or
                        data_obj.get('path') or ''
                    )
                    title = (
                        data_obj.get('display_title') or
                        data_obj.get('title') or
                        data_obj.get('name') or
                        data_obj.get('display_name') or
                        data_obj.get('image', {}).get('name') or
                        data_obj.get('image', {}).get('filename', '').rsplit('.', 1)[0] or
                        'Unknown'
                    )
                except Exception as e:
                    print(f"Lỗi parse data-object: {e}")

            if not title:
                try:
                    title_elem = item.find_element(By.CSS_SELECTOR, 'a.list-item-desc-title-link')
                    title = title_elem.get_attribute('title') or title_elem.text.strip() or 'Unknown'
                except:
                    title = 'Unknown'

            if not video_url:
                continue

            if video_url in existing_video_urls:
                consecutive_duplicates += 1
                if consecutive_duplicates > 5:
                    print(f">5 trùng liên tiếp → dừng scrape sớm")
                    break
                continue

            consecutive_duplicates = 0

            thumb_url = ''
            try:
                imgs = item.find_elements(By.TAG_NAME, 'img')
                for img in imgs:
                    src = img.get_attribute('src') or ''
                    if 'fr.jpeg' in src:
                        thumb_url = src
                        break
                if not thumb_url and imgs:
                    thumb_url = imgs[0].get_attribute('src') or ''
            except:
                thumb_url = ''

            author = ''
            try:
                author = item.find_element(By.CSS_SELECTOR, 'div.list-item-from').text.strip()
            except:
                author = 'Guest'

            duration = ''
            try:
                duration = item.find_element(By.CSS_SELECTOR, 'div.list-item-duration').text.strip()
            except:
                duration = 'N/A'

            row = [title, author, duration, thumb_url, video_url, str(page_number), current_url]
            page_new_rows.append(row)
            existing_video_urls.add(video_url)
            print(f"Added: {title[:50]}... | Video: {video_url[:60]}...")

        if page_new_rows:
            all_new_rows_this_run.extend(page_new_rows)
            print(f"Thu thập {len(page_new_rows)} item mới từ trang {page_number}")

        if consecutive_duplicates > 5:
            break

        if max_pages is not None and page_number >= max_pages:
            print(f"Đạt giới hạn {max_pages} trang → dừng")
            break

        try:
            next_btn = driver.find_element(By.CSS_SELECTOR, 'li.pagination-next a[data-pagination="next"]')
            current_url = next_btn.get_attribute('href')
            if not current_url:
                print("Không tìm thấy link next")
                break
            page_number += 1
            time.sleep(2)
        except NoSuchElementException:
            print("Hết trang")
            break

    driver.quit()

    if all_new_rows_this_run:
        print(f"\nTổng mới trong lần chạy: {len(all_new_rows_this_run)} video")
        sheet.insert_rows(all_new_rows_this_run, row=2)
        print(f"Chèn {len(all_new_rows_this_run)} dòng mới vào đầu Sheet")
        append_or_update_json(all_new_rows_this_run)
        sync_sheet_to_json_if_needed(sheet)
    else:
        print("Không có dữ liệu mới nào trong lần chạy này.")

    print("Hoàn tất scrape.")

if __name__ == '__main__':
    max_pages = None
    if len(sys.argv) > 1:
        arg = sys.argv[1].strip().lower()
        if arg in ('all', 'tất cả'):
            max_pages = None
        else:
            try:
                max_pages = int(arg)
            except ValueError:
                print(f"Tham số không hợp lệ '{arg}' → chạy all")
                max_pages = None
    scrape_pages(max_pages)
