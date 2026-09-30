import os
import json
import time
import sys
import re
from urllib.parse import unquote
from pathlib import Path
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from google.oauth2.service_account import Credentials
import gspread

# ────────────────────────────────────────────────
# CẤU HÌNH CRAWLER THEO LƯỢT LIKE (LIKES_DESC)
# ────────────────────────────────────────────────
BASE_URL = 'https://zpic.org/category/video-nsfw/?list=images&sort=likes_desc&page=1'
JSON_PATH = 'anhmoe/videos_likes.json'
SPREADSHEET_ID = '1RWAd7HrgnzfRK9PpD5Zy7OHwMv6mfQh17jvqNWGHsaU'
SHEET_NAME = 'anhmoe top likes'
HEADERS = ['Title', 'Author', 'Duration', 'Thumb URL', 'Video URL', 'Page Number', 'Page Link']

def get_sheet():
    creds_json = os.getenv('GOOGLE_CREDENTIALS')
    if not creds_json:
        return None
    try:
        creds_dict = json.loads(creds_json)
        scopes = ['https://www.googleapis.com/auth/spreadsheets']
        creds = Credentials.from_service_account_info(creds_dict, scopes=scopes)
        client = gspread.authorize(creds)
        spreadsheet = client.open_by_key(SPREADSHEET_ID)
        try:
            return spreadsheet.worksheet(SHEET_NAME)
        except gspread.exceptions.WorksheetNotFound:
            sheet = spreadsheet.add_worksheet(title=SHEET_NAME, rows=2000, cols=10)
            sheet.append_row(HEADERS)
            return sheet
    except Exception as e:
        print(f"Lưu ý: Không kết nối được Google Sheet ({e}). Bỏ qua cập nhật Sheet.")
        return None

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

def write_json(all_data):
    path = Path(JSON_PATH)
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, 'w', encoding='utf-8') as f:
        json.dump(all_data, f, ensure_ascii=False, indent=2)
    sz = path.stat().st_size / 1024 / 1024
    print(f"💾 Đã lưu thành công {len(all_data):,} video vào {JSON_PATH} ({sz:.2f} MB)")

def scrape_likes(max_pages=None):
    sheet = get_sheet()

    options = Options()
    options.add_argument('--headless')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument('--window-size=1920,1080')
    options.add_argument('--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36')
    options.add_argument('--disable-blink-features=AutomationControlled')

    driver = webdriver.Chrome(options=options)
    wait = WebDriverWait(driver, 20)

    # Đọc dữ liệu hiện có trong JSON (nếu có)
    existing_items = []
    seen_ids = set()
    if os.path.exists(JSON_PATH):
        try:
            with open(JSON_PATH, 'r', encoding='utf-8') as f:
                existing_items = json.load(f)
        except Exception as e:
            print(f"Lỗi đọc JSON cũ: {e}")

    current_url = BASE_URL
    page_number = 1
    scraped_rows = []
    scraped_json_items = []

    print(f"🚀 Bắt đầu cào video theo nhiều LIKE nhất (likes_desc)")
    print(f"   Target URL: {BASE_URL}")
    if max_pages:
        print(f"   Số trang dự kiến cào: {max_pages} trang")
    else:
        print("   Chế độ: Cào TOÀN BỘ các trang (All pages)")

    while True:
        print(f"\n📄 Đang cào Trang {page_number}: {current_url}")
        try:
            driver.get(current_url)
            time.sleep(5)
        except Exception as e:
            print(f"Lỗi tải trang: {e}")
            break

        items = driver.find_elements(By.CSS_SELECTOR, 'div.list-item')
        if not items:
            print("Không tìm thấy item nào trên trang. Đã đến trang cuối hoặc bị chặn.")
            break

        page_count = 0
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
                if not vid_id or vid_id in seen_ids:
                    continue

                seen_ids.add(vid_id)

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
                scraped_rows.append(row)
                scraped_json_items.append({
                    "title": title,
                    "author": author,
                    "duration": duration,
                    "thumb_url": norm_thumb_url,
                    "video_url": norm_video_url,
                    "page_number": str(page_number),
                    "page_link": current_url,
                    "scraped_at": time.strftime("%Y-%m-%d %H:%M:%S")
                })
                page_count += 1
            except Exception as e:
                continue

        print(f"   + Nhặt được {page_count} video trên trang {page_number}")

        if max_pages and page_number >= max_pages:
            print(f"🏁 Đã cào đủ {max_pages} trang yêu cầu.")
            break

        # Tìm nút Next
        next_found = False
        try:
            next_btn = driver.find_element(By.CSS_SELECTOR, 'li.pagination-next a')
            current_url = next_btn.get_attribute('href')
            page_number += 1
            next_found = True
        except:
            pass

        if not next_found:
            # Fallback tạo URL trang tiếp theo theo mẫu
            page_number += 1
            current_url = f"https://zpic.org/category/video-nsfw/?list=images&sort=likes_desc&page={page_number}"

    driver.quit()

    if scraped_json_items:
        # Giữ lại các video cũ đã có trong JSON nhưng chưa xuất hiện ở các trang đầu
        for old_it in existing_items:
            vid = extract_video_id(old_it.get('video_url', ''))
            if vid and vid not in seen_ids:
                seen_ids.add(vid)
                scraped_json_items.append(old_it)

        write_json(scraped_json_items)

        if sheet and scraped_rows:
            try:
                sheet.insert_rows(scraped_rows[:1000], row=2)
                print(f"✅ Đã đồng bộ {min(len(scraped_rows), 1000)} dòng lên Google Sheet ({SHEET_NAME}).")
            except Exception as e:
                print(f"Lỗi ghi Sheet: {e}")
    else:
        print("ℹ️ Không có dữ liệu mới.")

if __name__ == '__main__':
    max_p = None
    if len(sys.argv) > 1:
        val = sys.argv[1].strip().lower()
        if val not in ('all', 'tất cả'):
            try: max_p = int(val)
            except: pass
    scrape_likes(max_p)
