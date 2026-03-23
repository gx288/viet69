import os
import json
import time
import sys
import subprocess
from urllib.parse import unquote
from pathlib import Path
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import NoSuchElementException, TimeoutException
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

HEADERS = ['Title', 'Author', 'Duration', 'Thumb URL', 'Video URL', 'Page Number', 'Page Link']

# ────────────────────────────────────────────────
# HELPER FUNCTIONS (Sheet & JSON)
# ────────────────────────────────────────────────

def get_or_create_sheet():
    spreadsheet = client.open_by_key(SPREADSHEET_ID)
    try:
        sheet = spreadsheet.worksheet(SHEET_NAME)
    except gspread.exceptions.WorksheetNotFound:
        sheet = spreadsheet.add_worksheet(title=SHEET_NAME, rows=2000, cols=10)
        sheet.append_row(HEADERS)
    return sheet

def load_sheet_video_urls(sheet):
    values = sheet.get_all_values()
    if not values or len(values) < 2: return set(), []
    existing_urls = {row[4] for row in values[1:] if len(row) > 4 and row[4].strip()}
    return existing_urls, values[1:]

def write_json(all_data):
    path = Path(JSON_PATH)
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, 'w', encoding='utf-8') as f:
        json.dump(all_data, f, ensure_ascii=False, indent=2)

def append_or_update_json(new_rows):
    path = Path(JSON_PATH)
    existing_data = []
    existing_urls = set()
    if path.exists():
        with open(path, 'r', encoding='utf-8') as f:
            try: 
                existing_data = json.load(f)
                existing_urls = {item.get('video_url', '') for item in existing_data}
            except: pass

    new_json_items = []
    for row in new_rows:
        if row[4] not in existing_urls:
            new_json_items.append({
                "title": row[0], "author": row[1], "duration": row[2],
                "thumb_url": row[3], "video_url": row[4],
                "page_number": row[5], "page_link": row[6],
                "scraped_at": time.strftime("%Y-%m-%d %H:%M:%S")
            })
    
    if new_json_items:
        write_json(new_json_items + existing_data)

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
    options.add_argument('--window-size=1920,1080')
    # QUAN TRỌNG: Thêm User-Agent để server không block session
    options.add_argument('--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36')
    options.add_argument('--disable-blink-features=AutomationControlled')

    driver = webdriver.Chrome(options=options)
    wait = WebDriverWait(driver, 20)

    # ─── ĐĂNG NHẬP THẲNG BẰNG ACCOUNT ───
    username = os.getenv('ZPIC_USERNAME')
    password = os.getenv('ZPIC_PASSWORD')

    if not username or not password:
        print("❌ Lỗi: Thiếu ZPIC_USERNAME hoặc ZPIC_PASSWORD trong Secret!")
        driver.quit()
        return

    try:
        print(f"🔄 Đang tiến hành đăng nhập cho: {username}")
        driver.get("https://zpic.io/login")
        
        # Đợi các trường input xuất hiện
        user_input = wait.until(EC.presence_of_element_located((By.NAME, "login-subject")))
        pass_input = driver.find_element(By.NAME, "password")

        user_input.send_keys(username)
        pass_input.send_keys(password)
        
        # Click nút submit
        driver.find_element(By.CSS_SELECTOR, "button[type='submit']").click()
        
        # Đợi chuyển hướng sau khi login thành công
        time.sleep(8)
        
        if "login" in driver.current_url.lower():
            print("❌ Đăng nhập thất bại - Vẫn ở trang login")
            driver.quit()
            return
        print("✅ ĐĂNG NHẬP THÀNH CÔNG!")

    except Exception as e:
        print(f"❌ Lỗi trong quá trình đăng nhập: {e}")
        driver.quit()
        return

    # ─── BẮT ĐẦU SCRAPE ───
    current_url = BASE_URL
    page_number = 1
    all_new_rows_this_run = []

    while True:
        print(f"\n🚀 Trang {page_number}: {current_url}")
        driver.get(current_url)
        time.sleep(5)

        # Kiểm tra xem có bị đá ra trang login không
        if "login" in driver.current_url.lower():
            print("⚠️ Bị redirect về login - Session hỏng. Dừng.")
            break

        items = driver.find_elements(By.CSS_SELECTOR, 'div.list-item')
        if not items:
            print("Không tìm thấy item nào. Có thể hết trang hoặc bị block.")
            break

        consecutive_duplicates = 0
        page_new_rows = []

        for item in items:
            try:
                data_object_str = item.get_attribute('data-object') or ''
                if not data_object_str: continue
                
                decoded_str = unquote(data_object_str).replace('\\"', '"').replace('\\\\', '\\').strip()
                data_obj = json.loads(decoded_str)
                
                video_url = data_obj.get('image', {}).get('url') or data_obj.get('url') or ''
                title = data_obj.get('display_title') or data_obj.get('title') or 'Unknown'

                if not video_url: continue

                if video_url in existing_video_urls:
                    consecutive_duplicates += 1
                    if consecutive_duplicates > 7: break
                    continue

                consecutive_duplicates = 0
                
                # Lấy thêm Duration & Author
                duration = "N/A"
                try: duration = item.find_element(By.CSS_SELECTOR, 'div.list-item-duration').text.strip()
                except: pass
                
                author = "Guest"
                try: author = item.find_element(By.CSS_SELECTOR, 'div.list-item-from').text.strip()
                except: pass

                thumb_url = ""
                try: thumb_url = item.find_element(By.TAG_NAME, 'img').get_attribute('src') or ""
                except: pass

                row = [title, author, duration, thumb_url, video_url, str(page_number), current_url]
                page_new_rows.append(row)
                existing_video_urls.add(video_url)
                print(f"  + New: {title[:40]}...")

            except Exception as e:
                continue

        if page_new_rows:
            all_new_rows_this_run.extend(page_new_rows)

        if consecutive_duplicates > 7:
            print("⏩ Phát hiện nhiều video cũ liên tiếp. Dừng sớm.")
            break

        if max_pages and page_number >= max_pages: break

        # Tìm nút Next
        try:
            next_btn = driver.find_element(By.CSS_SELECTOR, 'li.pagination-next a')
            current_url = next_btn.get_attribute('href')
            page_number += 1
        except:
            print("🏁 Đã đến trang cuối.")
            break

    # ─── LƯU DỮ LIỆU ───
    if all_new_rows_this_run:
        sheet.insert_rows(all_new_rows_this_run, row=2)
        append_or_update_json(all_new_rows_this_run)
        print(f"✅ Hoàn tất! Đã lưu {len(all_new_rows_this_run)} video mới.")
    else:
        print("ℹ️ Không có dữ liệu mới.")

    driver.quit()

if __name__ == '__main__':
    max_p = None
    if len(sys.argv) > 1:
        val = sys.argv[1].lower()
        if val not in ('all', 'tất cả'):
            try: max_p = int(val)
            except: pass
    scrape_pages(max_p)
