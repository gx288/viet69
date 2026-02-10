# anhmoe/scraper.py
import os
import json
import time
import sys
import subprocess
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
        headers = [
            'Title', 'Author', 'Duration', 'Thumb URL', 'Video URL', 'Page Number', 'Page Link'
        ]
        sheet.append_row(headers)
        print("Tạo sheet mới với header chuẩn.")
    else:
        # Kiểm tra header nếu sheet tồn tại
        headers = sheet.row_values(1)
        expected = ['Title', 'Author', 'Duration', 'Thumb URL', 'Video URL', 'Page Number', 'Page Link']
        if headers != expected:
            print("Header không khớp, cập nhật header mới (có thể mất dữ liệu cũ nếu cột thay đổi).")
            sheet.update('A1:G1', [expected])
    return sheet

def load_existing_video_urls(sheet):
    """Cache video URL để check duplicate (vì title có thể trùng)"""
    print("Load cache video URLs từ sheet...")
    values = sheet.get_all_values()
    if not values:
        return set()
    # Cột Video URL là cột E (index 4, bắt đầu từ 0)
    existing_urls = {row[4] for row in values[1:] if len(row) > 4 and row[4].strip()}
    print(f"Đã cache {len(existing_urls)} video tồn tại.")
    return existing_urls

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
        time.sleep(5)  # Tăng chờ để img load

        items = driver.find_elements(By.CSS_SELECTOR,
                                     'div.list-item.fixed-size.c8.gutter-margin-right-bottom.jsly.position-absolute.--show.ui-selectee')

        new_rows = []
        for item in items:
            # Lấy data-object trước để extract video url & title
            data_object_str = item.get_attribute('data-object') or ''
            video_url = ''
            title = ''

            if data_object_str:
                try:
                    # Fix escape trong JSON string
                    data_obj = json.loads(data_object_str.replace('\\', ''))
                    # Video URL: thường ở image.url hoặc url
                    video_url = data_obj.get('image', {}).get('url') or data_obj.get('url') or ''
                    # Title: display_title hoặc title hoặc name
                    title = (data_obj.get('display_title') or 
                             data_obj.get('title') or 
                             data_obj.get('name') or 
                             data_obj.get('display_name') or '')
                    print(f"Data-object title: {title} | Video: {video_url}")
                except json.JSONDecodeError:
                    print("Lỗi parse data-object JSON")

            # Fallback title từ link text nếu data-object không có
            if not title:
                try:
                    title_elem = item.find_element(By.CSS_SELECTOR, 'a.list-item-desc-title-link')
                    title = title_elem.get_attribute('title') or title_elem.text.strip()
                except:
                    title = 'Unknown Title'

            if not video_url:
                continue  # Bỏ item không có video link

            if video_url in existing_video_urls:
                consecutive_duplicates += 1
                if consecutive_duplicates > 5:
                    print(f">5 video trùng → dừng tại trang {page_number}")
                    driver.quit()
                    return
                continue
            else:
                consecutive_duplicates = 0

            # Thumb URL: ưu tiên fr.jpeg từ img
            thumb_url = ''
            try:
                img_elements = item.find_elements(By.TAG_NAME, 'img')
                for img in img_elements:
                    src = img.get_attribute('src') or ''
                    if src and 'fr.jpeg' in src:
                        thumb_url = src
                        print(f"Thumb cao cấp: {thumb_url}")
                        break
                if not thumb_url and img_elements:
                    thumb_url = img_elements[0].get_attribute('src') or ''
            except:
                thumb_url = item.get_attribute('data-thumb') or ''

            # Author
            author = ''
            try:
                author = item.find_element(By.CSS_SELECTOR, 'div.list-item-from').text.strip()
            except:
                author = 'Unknown'

            # Duration
            duration = ''
            try:
                duration = item.find_element(By.CSS_SELECTOR, 'div.list-item-duration').text.strip()
            except:
                duration = 'Unknown'

            row = [
                title,
                author,
                duration,
                thumb_url,
                video_url,
                str(page_number),
                current_url
            ]

            new_rows.append(row)
            existing_video_urls.add(video_url)  # Cache

            print(f"Item added: {title} | Video: {video_url} | Thumb: {thumb_url}")

        if new_rows:
            sheet.append_rows(new_rows)
            print(f"→ Thêm {len(new_rows)} item mới")

        if max_pages is not None and page_number >= max_pages:
            print(f"Đạt {max_pages} trang → dừng")
            break

        try:
            next_btn = driver.find_element(By.CSS_SELECTOR, 'li.pagination-next a[data-pagination="next"]')
            current_url = next_btn.get_attribute('href')
            if not current_url:
                break
            page_number += 1
            print("Chờ 10s trước trang mới...")
            time.sleep(10)
        except NoSuchElementException:
            print("Hết trang")
            break

    driver.quit()
    print("Hoàn tất scrape.")

if __name__ == '__main__':
    if len(sys.argv) > 1:
        arg = sys.argv[1].strip().lower()
        max_pages = None if arg in ('all', 'tất cả') else int(arg) if arg.isdigit() else None
    else:
        max_pages = None
    scrape_pages(max_pages)
