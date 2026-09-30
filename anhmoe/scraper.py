import os
import json
import time
import sys
import subprocess
import re
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

def extract_video_id(url):
    """Trích xuất ID/tên file video duy nhất, không phụ thuộc vào subdomain CDN."""
    if not url: return ""
    m = re.search(r'/([^/?#]+\.(?:mp4|webm|m4v))', url, re.IGNORECASE)
    if m:
        return m.group(1).lower()
    clean = url.split('?')[0].rstrip('/')
    return clean.split('/')[-1].lower()

def normalize_url(url):
    """Tự động chuyển nguồn các domain cũ zpi.cx/zzpi.cc sang amvideo.cfd đang sống."""
    if not url: return ""
    return re.sub(r'https?://(?:zpi\.cx|zzpi\.cc)/s?(\d+)/', r'https://s\1.amvideo.cfd/', url)

def load_sheet_video_urls(sheet):
    values = sheet.get_all_values()
    existing_ids = set()
    if values and len(values) >= 2:
        for row in values[1:]:
            if len(row) > 4 and row[4].strip():
                vid = extract_video_id(row[4])
                if vid: existing_ids.add(vid)
    
    # Đồng thời nạp toàn bộ ID từ file JSON để chống trùng lặp tuyệt đối
    path = Path(JSON_PATH)
    if path.exists():
        try:
            with open(path, 'r', encoding='utf-8') as f:
                for item in json.load(f):
                    vid = extract_video_id(item.get('video_url', ''))
                    if vid: existing_ids.add(vid)
        except Exception as e:
            print(f"Lỗi nạp ID từ {JSON_PATH}: {e}")
            
    print(f"📦 Đã nạp {len(existing_ids):,} ID video hiện có trong kho để check trùng lặp.")
    return existing_ids, values[1:] if values else []

def write_json(all_data):
    path = Path(JSON_PATH)
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, 'w', encoding='utf-8') as f:
        json.dump(all_data, f, ensure_ascii=False, indent=2)

def append_or_update_json(new_rows):
    path = Path(JSON_PATH)
    existing_data = []
    existing_ids = set()
    if path.exists():
        with open(path, 'r', encoding='utf-8') as f:
            try: 
                existing_data = json.load(f)
                existing_ids = {extract_video_id(item.get('video_url', '')) for item in existing_data}
            except Exception as e:
                print(f"Lỗi đọc JSON khi cập nhật: {e}")

    new_json_items = []
    for row in new_rows:
        vid = extract_video_id(row[4])
        if vid and vid not in existing_ids:
            existing_ids.add(vid)
            new_json_items.append({
                "title": row[0],
                "author": row[1],
                "duration": row[2],
                "thumb_url": normalize_url(row[3]),
                "video_url": normalize_url(row[4]),
                "page_number": row[5],
                "page_link": row[6],
                "scraped_at": time.strftime("%Y-%m-%d %H:%M:%S")
            })
    
    if new_json_items:
        # Ghi video mới lên đầu danh sách
        write_json(new_json_items + existing_data)
        print(f"💾 Đã ghi thêm {len(new_json_items)} video mới vào {JSON_PATH}")
    else:
        print("ℹ️ Không có video mới hợp lệ để ghi vào JSON.")

# ────────────────────────────────────────────────
# MAIN SCRAPE FUNCTION
# ────────────────────────────────────────────────

def scrape_pages(max_pages=None):
    sheet = get_or_create_sheet()
    existing_video_ids, _ = load_sheet_video_urls(sheet)

    options = Options()
    options.add_argument('--headless')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument('--window-size=1920,1080')
    options.add_argument('--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36')
    options.add_argument('--disable-blink-features=AutomationControlled')

    driver = webdriver.Chrome(options=options)
    wait = WebDriverWait(driver, 20)

    # ─── ĐỌC CẤU HÌNH CONFIG ───
    CONFIG_PATH = 'anhmoe/config.json'
    config_data = {
        "base_url": "https://zpic.io/category/video-nsfw",
        "pending_redirect_url": "",
        "redirect_count": 0
    }
    if os.path.exists(CONFIG_PATH):
        try:
            with open(CONFIG_PATH, 'r', encoding='utf-8') as f:
                config_data.update(json.load(f))
        except Exception as e:
            print(f"Lỗi đọc config.json: {e}")

    current_url = config_data.get("base_url")
    page_number = 1
    all_new_rows_this_run = []

    while True:
        print(f"\n🚀 Trang {page_number}: {current_url}")
        driver.get(current_url)
        time.sleep(5)

        # ─── KIỂM TRA REDIRECT Ở TRANG ĐẦU TIÊN ───
        if page_number == 1:
            actual_url = driver.current_url
            clean_actual = actual_url.rstrip('/')
            clean_target = current_url.rstrip('/')
            config_changed = False
            
            if clean_actual != clean_target and "login" not in clean_actual.lower():
                print(f"⚠️ Phát hiện redirect! (Từ {clean_target} -> {clean_actual})")
                pending_url = config_data.get("pending_redirect_url", "")
                
                if clean_actual == pending_url.rstrip('/'):
                    config_data["redirect_count"] = config_data.get("redirect_count", 0) + 1
                    print(f"  -> Lần redirect thứ {config_data['redirect_count']}/5")
                else:
                    config_data["pending_redirect_url"] = actual_url
                    config_data["redirect_count"] = 1
                    print("  -> Bắt đầu theo dõi URL mới này.")
                    
                if config_data["redirect_count"] >= 5:
                    print(f"🔄 Đã đạt 5 lần redirect liên tiếp. Cập nhật BASE_URL chính thức thành {actual_url}")
                    config_data["base_url"] = actual_url
                    config_data["pending_redirect_url"] = ""
                    config_data["redirect_count"] = 0
                    
                config_changed = True
                current_url = actual_url  # Chạy tiếp với link thực tế
            else:
                # Nếu không bị redirect thì reset theo dõi
                if config_data.get("redirect_count", 0) > 0:
                    config_data["pending_redirect_url"] = ""
                    config_data["redirect_count"] = 0
                    config_changed = True

            if config_changed:
                try:
                    with open(CONFIG_PATH, 'w', encoding='utf-8') as f:
                        json.dump(config_data, f, indent=2)
                except Exception as e:
                    print(f"Lỗi ghi config.json: {e}")

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

                vid_id = extract_video_id(video_url)
                if not vid_id: continue

                if vid_id in existing_video_ids:
                    consecutive_duplicates += 1
                    if consecutive_duplicates > 7:
                        print("⏩ Gặp 7 video cũ liên tiếp trên trang. Dừng cào sớm.")
                        break
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

                norm_video_url = normalize_url(video_url)
                norm_thumb_url = normalize_url(thumb_url)

                row = [title, author, duration, norm_thumb_url, norm_video_url, str(page_number), current_url]
                page_new_rows.append(row)
                existing_video_ids.add(vid_id)
                print(f"  + New ({vid_id}): {title[:40]}...")

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
