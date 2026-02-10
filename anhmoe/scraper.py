# anhmoe/scraper.py
import os
import json
import time
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.common.exceptions import NoSuchElementException
from google.oauth2.service_account import Credentials
import gspread

# Load Google credentials from environment variable
creds_json = os.getenv('GOOGLE_CREDENTIALS')
if not creds_json:
    raise ValueError("GOOGLE_CREDENTIALS environment variable not set")
creds_dict = json.loads(creds_json)

# Setup Google Sheets client
scopes = ['https://www.googleapis.com/auth/spreadsheets']
creds = Credentials.from_service_account_info(creds_dict, scopes=scopes)
client = gspread.authorize(creds)

# Spreadsheet ID and sheet name
spreadsheet_id = '1RWAd7HrgnzfRK9PpD5Zy7OHwMv6mfQh17jvqNWGHsaU'
sheet_name = 'anhmoe videos'

# Base URL
base_url = 'https://anh.moe/category/video-nsfw'

# Function to get or create sheet
def get_or_create_sheet():
    spreadsheet = client.open_by_key(spreadsheet_id)
    try:
        sheet = spreadsheet.worksheet(sheet_name)
    except gspread.exceptions.WorksheetNotFound:
        sheet = spreadsheet.add_worksheet(title=sheet_name, rows=1000, cols=20)
        # Set headers (adjust based on data attributes you want to save)
        headers = [
            'data-id', 'data-category-id', 'data-flag', 'data-type', 'data-media', 'data-size',
            'data-liked', 'data-description', 'data-privacy', 'data-url-short', 'data-thumb',
            'data-object', 'title', 'uploaded_by', 'duration', 'page_number', 'page_link'
        ]
        sheet.append_row(headers)
    return sheet

# Function to check if item exists in sheet by data-id
def item_exists(sheet, data_id):
    data = sheet.get_all_values()
    for row in data[1:]:  # Skip header
        if row[0] == data_id:  # Assuming data-id is first column
            return True
    return False

# Main scraping function
def scrape_pages(max_pages=None):
    sheet = get_or_create_sheet()

    # Setup Selenium
    options = Options()
    options.add_argument('--headless')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    driver = webdriver.Chrome(options=options)

    current_url = base_url
    page_number = 1
    while True:
        driver.get(current_url)
        time.sleep(3)  # Wait for page load

        # Find all items
        items = driver.find_elements(By.CSS_SELECTOR, 'div.list-item.fixed-size.c8.gutter-margin-right-bottom.jsly.position-absolute.--show.ui-selectee')
        duplicate_count = 0
        new_rows = []

        for item in items:
            # Extract data attributes
            data_id = item.get_attribute('data-id') or ''
            data_category_id = item.get_attribute('data-category-id') or ''
            data_flag = item.get_attribute('data-flag') or ''
            data_type = item.get_attribute('data-type') or ''
            data_media = item.get_attribute('data-media') or ''
            data_size = item.get_attribute('data-size') or ''
            data_liked = item.get_attribute('data-liked') or ''
            data_description = item.get_attribute('data-description') or ''
            data_privacy = item.get_attribute('data-privacy') or ''
            data_url_short = item.get_attribute('data-url-short') or ''
            data_thumb = item.get_attribute('data-thumb') or ''
            data_object = item.get_attribute('data-object') or ''

            # Extract inner elements
            try:
                title = item.find_element(By.CSS_SELECTOR, 'a.list-item-desc-title-link').get_attribute('title') or ''
            except NoSuchElementException:
                title = ''
            try:
                uploaded_by = item.find_element(By.CSS_SELECTOR, 'div.list-item-from').text or ''
            except NoSuchElementException:
                uploaded_by = ''
            try:
                duration = item.find_element(By.CSS_SELECTOR, 'div.list-item-duration').text or ''
            except NoSuchElementException:
                duration = ''

            # Check duplicate
            if item_exists(sheet, data_id):
                duplicate_count += 1
                if duplicate_count > 5:
                    print(f"More than 5 duplicates on page {page_number}, stopping.")
                    driver.quit()
                    return
            else:
                # Prepare row (order matches headers)
                row = [
                    data_id, data_category_id, data_flag, data_type, data_media, data_size,
                    data_liked, data_description, data_privacy, data_url_short, data_thumb,
                    data_object, title, uploaded_by, duration, str(page_number), current_url
                ]
                new_rows.append(row)

        # Append new rows to sheet (at the end, but since we process pages sequentially, earlier pages first)
        if new_rows:
            sheet.append_rows(new_rows)

        # Check if we reached max pages
        if max_pages and page_number >= max_pages:
            print(f"Reached max pages: {max_pages}")
            driver.quit()
            return

        # Find next page button
        try:
            next_button = driver.find_element(By.CSS_SELECTOR, 'li.pagination-next a[data-pagination="next"]')
            current_url = next_button.get_attribute('href')
            page_number += 1
        except NoSuchElementException:
            print("No more pages.")
            driver.quit()
            return

# For manual run: prompt for number of pages
if __name__ == '__main__':
    input_str = input("Enter number of pages to scrape (or 'all' for all pages): ").strip().lower()
    if input_str == 'all':
        max_pages = None
    else:
        try:
            max_pages = int(input_str)
        except ValueError:
            print("Invalid input, running for 1 page.")
            max_pages = 1
    scrape_pages(max_pages)
