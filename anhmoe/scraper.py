# anhmoe/scraper.py
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
from selenium.common.exceptions import NoSuchElementException
from google.oauth2.service_account import Credentials
import gspread

# Load Google credentials
creds_json = os.getenv('GOOGLE_CREDENTIALS')
if not creds_json:
    raise ValueError("Biến môi trường GOOGLE_CREDENTIALS chưa được thiết lập")
creds_dict = json.loads(creds_json)
scopes = ['https://www.googleapis.com/auth/spreadsheets']
creds = Credentials.from_service_account_info(creds_dict, scopes=scopes)
client = gspread.authorize(creds)

SPREADSHEET_ID = '1RWAd7HrgnzfRK9PpD5Zy7OHwMv6mfQh17jvqNWGHsaU'
SHEET_NAME = 'anhmoe videos'
BASE_URL = 'https://anh.moe/category/video-nsfw'

def get_or_create_sheet():
    spreadsheet = client.open_by_key(SPREADSHEET_ID)
    try:
        sheet = spreadsheet.worksheet(SHEET_NAME)
    except gspread.exceptions.WorksheetNotFound:
        sheet = spreadsheet.add_worksheet(title=SHEET_NAME, rows=2000, cols=10)
        headers = ['Title', 'Author', 'Duration', 'Thumb URL', 'Video URL', 'Page Number', 'Page Link']
        sheet.append_row(headers)
        print("Tạo sheet mới với header.")
    else:
        headers = sheet.row_values(1)
        expected = ['Title', 'Author', 'Duration', 'Thumb URL', 'Video URL', 'Page Number', 'Page Link']
        if headers != expected:
            print("Header không khớp → cập nhật header mới.")
            sheet.update(range_name='A1:G1', values=[expected])
    return sheet

def load_existing_video_urls(sheet):
    print("Load cache video URLs...")
    values = sheet.get_all_values()
    if not values:
        return set()
    existing_urls = {row[4] for row in values[1:] if len(row) > 4 and row[4].strip()}
    print(f"Cache {len(existing_urls)} video tồn tại.")
    return existing_urls

def append_to_json_file(new_rows, file_path='anhmoe/videos_data.json'):
    """Chèn dữ liệu mới vào đầu file JSON (array of objects)"""
    path = Path(file_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    existing_data = []
    if path.exists():
        try:
            with open(path, 'r', encoding='utf-8') as f:
                existing_data = json.load(f)
        except (json.JSONDecodeError, FileNotFoundError):
            print(f"File {file_path} lỗi hoặc rỗng → khởi tạo mới")
            existing_data = []
    # Chuyển new_rows thành list dict
    new_json_rows = []
    for row in new_rows:
        new_json_rows.append({
            "title": row[0],
            "author": row[1],
            "duration": row[2],
            "thumb_url": row[3],
            "video_url": row[4],
            "page_number": row[5],
            "page_link": row[6],
            "scraped_at": time.strftime("%Y-%m-%d %H:%M:%S %z")
        })
    # Đưa mới lên đầu
    all_data = new_json_rows + existing_data
    with open(path, 'w', encoding='utf-8') as f:
        json.dump(all_data, f, ensure_ascii=False, indent=2)
    print(f"Chèn {len(new_json_rows)} item mới lên ĐẦU {file_path} (tổng: {len(all_data)})")

def scrape_pages(max_pages=None):
    sheet = get_or_create_sheet()
    existing_video_urls = load_existing_video_urls(sheet)
    options = Options()
    options.add_argument('--headless')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument('--disable-gpu')
    options.add_argument('--window-size=1920,1080')
    print("Khởi tạo Selenium...")
    try:
        print("ChromeDriver: " + subprocess.check_output(['chromedriver', '--version']).decode().strip())
    except:
        print("ChromeDriver: từ PATH")
    driver = webdriver.Chrome(options=options)
    current_url = BASE_URL
    page_number = 1
    consecutive_duplicates = 0
    while True:
        print(f"\n=== Trang {page_number} === {current_url}")
        driver.get(current_url)
        time.sleep(6)
        items = driver.find_elements(By.CSS_SELECTOR,
                                     'div.list-item.fixed-size.c8.gutter-margin-right-bottom.jsly.position-absolute.--show.ui-selectee')
        new_rows = []
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
                    print(f"Parse OK - Title: {title} | Video: {video_url}")
                except Exception as e:
                    print(f"Lỗi parse JSON: {e} | Raw: {data_object_str[:150]}...")
            else:
                print("No data-object")
            if not title:
                try:
                    title_elem = item.find_element(By.CSS_SELECTOR, 'a.list-item-desc-title-link')
                    title = title_elem.get_attribute('title') or title_elem.text.strip() or 'Unknown'
                except:
                    title = 'Unknown'
            if not video_url:
                print("Bỏ item - no video URL")
                continue
            if video_url in existing_video_urls:
                consecutive_duplicates += 1
                if consecutive_duplicates > 5:
                    print(f">5 trùng → dừng")
                    driver.quit()
                    return
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
                thumb_url = item.get_attribute('data-thumb') or ''
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
            new_rows.append(row)
            existing_video_urls.add(video_url)
            print(f"Added: {title[:50]}... | Video: {video_url[:60]}...")
        if new_rows:
            # Chèn vào ngay sau header (dòng 2)
            sheet.insert_rows(new_rows, row=2)
            print(f"Chèn {len(new_rows)} item mới vào ĐẦU sheet (sau header)")
            # Append vào file JSON trong repo (với mới ở đầu)
            append_to_json_file(new_rows)
        if max_pages is not None and page_number >= max_pages:
            print(f"Đạt {max_pages} trang → dừng")
            break
        try:
            next_btn = driver.find_element(By.CSS_SELECTOR, 'li.pagination-next a[data-pagination="next"]')
            current_url = next_btn.get_attribute('href')
            if not current_url:
                print("Không tìm thấy link next")
                break
            page_number += 1
            print("Chờ 1 giây...")
            time.sleep(1)
        except NoSuchElementException:
            print("Hết trang")
            break
    driver.quit()
    print("Hoàn tất scrape.")

if __name__ == '__main__':
    if len(sys.argv) > 1:
        arg = sys.argv[1].strip().lower()
        if arg in ('all', 'tất cả'):
            max_pages = None
        else:
            try:
                max_pages = int(arg)
            except ValueError:
                print(f"Tham số '{arg}' không hợp lệ → chạy tất cả")
                max_pages = None
    else:
        max_pages = None
    scrape_pages(max_pages)
