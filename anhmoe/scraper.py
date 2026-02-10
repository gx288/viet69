# anhmoe/scraper.py
import os
import json
import time
import sys
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.common.exceptions import NoSuchElementException
from google.oauth2.service_account import Credentials
import gspread

# Load Google credentials từ biến môi trường
creds_json = os.getenv('GOOGLE_CREDENTIALS')
if not creds_json:
    raise ValueError("Biến môi trường GOOGLE_CREDENTIALS chưa được thiết lập")
creds_dict = json.loads(creds_json)

# Thiết lập Google Sheets
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
        sheet = spreadsheet.add_worksheet(title=SHEET_NAME, rows=2000, cols=25)
        headers = [
            'data-id', 'data-category-id', 'data-flag', 'data-type', 'data-media',
            'data-size', 'data-liked', 'data-description', 'data-privacy',
            'data-url-short', 'data-thumb', 'data-object', 'title', 'uploaded_by',
            'duration', 'page_number', 'page_link'
        ]
        sheet.append_row(headers)
    return sheet

def item_exists(sheet, data_id):
    if not data_id:
        return False
    values = sheet.get_all_values()
    for row in values[1:]:  # Bỏ header
        if row and row[0] == data_id:
            return True
    return False

def scrape_pages(max_pages=None):
    sheet = get_or_create_sheet()

    options = Options()
    options.add_argument('--headless')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument('--disable-gpu')
    options.add_argument('--window-size=1920,1080')

    driver = webdriver.Chrome(options=options)

    current_url = BASE_URL
    page_number = 1
    consecutive_duplicates = 0

    while True:
        print(f"Đang xử lý trang {page_number} → {current_url}")
        driver.get(current_url)
        time.sleep(4)  # Chờ load trang

        items = driver.find_elements(By.CSS_SELECTOR, 'div.list-item.fixed-size.c8.gutter-margin-right-bottom.jsly.position-absolute.--show.ui-selectee')

        new_rows = []
        for item in items:
            data_id = item.get_attribute('data-id') or ''
            if not data_id:
                continue

            if item_exists(sheet, data_id):
                consecutive_duplicates += 1
                if consecutive_duplicates > 5:
                    print(f"Phát hiện >5 item trùng liên tiếp → dừng tại trang {page_number}")
                    driver.quit()
                    return
                continue
            else:
                consecutive_duplicates = 0

            # Lấy các thuộc tính
            row = [
                data_id,
                item.get_attribute('data-category-id') or '',
                item.get_attribute('data-flag') or '',
                item.get_attribute('data-type') or '',
                item.get_attribute('data-media') or '',
                item.get_attribute('data-size') or '',
                item.get_attribute('data-liked') or '',
                item.get_attribute('data-description') or '',
                item.get_attribute('data-privacy') or '',
                item.get_attribute('data-url-short') or '',
                item.get_attribute('data-thumb') or '',
                item.get_attribute('data-object') or '',
            ]

            # Lấy thông tin từ nội dung
            try:
                title_elem = item.find_element(By.CSS_SELECTOR, 'a.list-item-desc-title-link')
                row.append(title_elem.get_attribute('title') or title_elem.text.strip())
            except:
                row.append('')

            try:
                row.append(item.find_element(By.CSS_SELECTOR, 'div.list-item-from').text.strip())
            except:
                row.append('')

            try:
                row.append(item.find_element(By.CSS_SELECTOR, 'div.list-item-duration').text.strip())
            except:
                row.append('')

            # Thêm page info
            row.append(str(page_number))
            row.append(current_url)

            new_rows.append(row)

        if new_rows:
            sheet.append_rows(new_rows)
            print(f"Đã thêm {len(new_rows)} item mới vào sheet")

        # Kiểm tra giới hạn page
        if max_pages is not None and page_number >= max_pages:
            print(f"Đạt giới hạn {max_pages} trang → kết thúc")
            break

        # Tìm nút next page
        try:
            next_btn = driver.find_element(By.CSS_SELECTOR, 'li.pagination-next a[data-pagination="next"]')
            current_url = next_btn.get_attribute('href')
            if not current_url:
                break
            page_number += 1
            time.sleep(2)
        except NoSuchElementException:
            print("Không còn trang tiếp theo")
            break

    driver.quit()

if __name__ == '__main__':
    if len(sys.argv) > 1:
        arg = sys.argv[1].strip().lower()
        if arg in ('all', 'tất cả'):
            max_pages = None
        else:
            try:
                max_pages = int(arg)
            except ValueError:
                print(f"Giá trị không hợp lệ: {arg} → chạy mặc định tất cả trang")
                max_pages = None
    else:
        max_pages = None

    scrape_pages(max_pages)
