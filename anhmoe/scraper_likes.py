import os
import json
import time
import sys
import re
from urllib.parse import unquote
import urllib.request
from pathlib import Path
try:
    from google.oauth2.service_account import Credentials
    import gspread
except ImportError:
    Credentials = None
    gspread = None

# ────────────────────────────────────────────────
# CẤU HÌNH CRAWLER THEO LƯỢT LIKE (LIKES_DESC)
# ────────────────────────────────────────────────
BASE_URL = 'https://zpic.org/category/video-nsfw/?list=images&sort=likes_desc&page=1'
JSON_PATH = 'anhmoe/videos_likes.json'
SPREADSHEET_ID = '1RWAd7HrgnzfRK9PpD5Zy7OHwMv6mfQh17jvqNWGHsaU'
SHEET_NAME = 'anhmoe top likes'
HEADERS = ['Title', 'Author', 'Duration', 'Thumb URL', 'Video URL', 'Page Number', 'Page Link']

REQ_HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
    'Accept-Language': 'en-US,en;q=0.5',
    'Referer': 'https://zpic.org/'
}

def get_sheet():
    if not gspread or not Credentials:
        return None
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

def is_video_file(url):
    """Kiểm tra URL có phải là định dạng video hợp lệ hay không (loại bỏ ảnh tĩnh jpeg/png/webp)."""
    if not url: return False
    return bool(re.search(r'\.(?:mp4|webm|m4v|mov|mkv)(?:$|[?#])', url, re.IGNORECASE))

def extract_video_id(url):
    """Trích xuất ID/tên file video duy nhất, không phụ thuộc vào subdomain CDN."""
    if not url or not is_video_file(url): return ""
    m = re.search(r'/([^/?#]+)\.(?:mp4|webm|m4v|mov|mkv)', url, re.IGNORECASE)
    if m:
        return m.group(1).lower()
    clean = url.split('?')[0].rstrip('/')
    base = clean.split('/')[-1].lower()
    if is_video_file(base):
        return re.sub(r'\.(?:mp4|webm|m4v|mov|mkv)$', '', base, flags=re.IGNORECASE)
    return ""

def normalize_url(url):
    """Tự động chuyển nguồn các domain cũ zpi.cx/zzpi.cc sang amvideo.cfd đang sống."""
    if not url: return ""
    return re.sub(r'https?://(?:[a-zA-Z0-9_-]+\.)?(?:zpi\.cx|zzpi\.cc)/s?(\d+)/', r'https://s\1.amvideo.cfd/', url)

def write_json(all_data):
    path = Path(JSON_PATH)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_suffix('.tmp')
    with open(tmp_path, 'w', encoding='utf-8') as f:
        json.dump(all_data, f, ensure_ascii=False, indent=2)
    tmp_path.replace(path)
    sz = path.stat().st_size / 1024 / 1024
    print(f"💾 Đã lưu thành công {len(all_data):,} video vào {JSON_PATH} ({sz:.2f} MB)")

def fetch_html_direct(url, retries=3):
    """Tải HTML trực tiếp qua HTTP request với retry."""
    for attempt in range(1, retries + 1):
        try:
            req = urllib.request.Request(url, headers=REQ_HEADERS)
            with urllib.request.urlopen(req, timeout=20) as resp:
                if resp.status == 200:
                    return resp.read().decode('utf-8', errors='ignore')
        except Exception as e:
            if attempt < retries:
                time.sleep(attempt * 2)
            else:
                print(f"   ⚠️ Lỗi tải trang {url} sau {retries} lần thử: {e}")
    return None

def parse_page_html(html, current_url, page_number):
    """Trích xuất danh sách video và liên kết trang tiếp theo theo đúng thứ tự DOM."""
    items = []
    next_url = None

    if not html:
        return items, next_url

    # Khớp từng block list-item (dùng lookahead tránh ăn vào list-item-desc / list-item-image)
    raw_blocks = re.findall(
        r'<div[^>]*class=["\'][^"\']*\blist-item\b(?!\-)[^"\']*["\'][^>]*data-object=["\']([^"\']+)["\'][^>]*>(.*?)(?=<div[^>]*class=["\'][^"\']*\blist-item\b(?!\-)|<ul class="content-listing-pagination|<div class="footer")',
        html,
        re.DOTALL
    )

    for data_raw, inner in raw_blocks:
        try:
            decoded_str = unquote(data_raw).replace('\\"', '"').replace('\\\\', '\\').strip()
            data_obj = json.loads(decoded_str)

            video_url = data_obj.get('image', {}).get('url') or data_obj.get('url') or ''
            if not video_url or not is_video_file(video_url):
                continue

            title = data_obj.get('display_title') or data_obj.get('title') or 'Unknown'

            # Thời lượng
            dur_m = re.search(r'class=["\'][^"\']*list-item-duration[^"\']*["\'][^>]*>(.*?)</div>', inner, re.DOTALL)
            duration = dur_m.group(1).strip() if dur_m else 'N/A'

            # Tác giả
            author = 'Guest'
            author_m = re.search(r'class=["\'][^"\']*list-item-from[^"\']*["\'][^>]*>(.*?)</div>', inner, re.DOTALL)
            if author_m:
                author_text = re.sub(r'<[^>]+>', '', author_m.group(1)).strip()
                author = re.sub(r'^(?:Uploaded\s+)?by\s*', '', author_text, flags=re.I).strip()
            elif data_obj.get('user', {}).get('username'):
                author = data_obj['user']['username']

            # Ảnh thumbnail
            thumb_url = data_obj.get('display_url') or data_obj.get('url_frame') or data_obj.get('medium', {}).get('url') or data_obj.get('thumb', {}).get('url') or ''

            norm_video_url = normalize_url(video_url)
            norm_thumb_url = normalize_url(thumb_url)

            items.append({
                "title": title,
                "author": author,
                "duration": duration,
                "thumb_url": norm_thumb_url,
                "video_url": norm_video_url,
                "page_number": str(page_number),
                "page_link": current_url,
                "scraped_at": time.strftime("%Y-%m-%d %H:%M:%S")
            })
        except Exception:
            continue

    # Tìm liên kết trang kế tiếp
    next_m = re.search(
        r'<li[^>]*class=["\'][^"\']*pagination-next(?![^"\']*pagination-disabled)[^"\']*["\'][^>]*>\s*<a[^>]*href=["\']([^"\']+)["\']',
        html,
        re.DOTALL
    )
    if next_m:
        href = next_m.group(1).strip()
        if href.startswith('/'):
            next_url = 'https://zpic.org' + href
        elif href.startswith('http'):
            next_url = href

    return items, next_url

def scrape_likes(max_pages=None):
    sheet = get_sheet()

    current_url = BASE_URL
    page_number = 1
    seen_ids = set()
    scraped_rows = []
    scraped_json_items = []

    print(f"🚀 Bắt đầu cào video theo nhiều LIKE nhất (likes_desc)")
    print(f"   Target URL: {BASE_URL}")
    if max_pages:
        print(f"   Số trang yêu cầu cào: {max_pages} trang")
    else:
        print("   Chế độ: Cào TOÀN BỘ các trang (All pages) đến trang cuối cùng")
    print("   Thứ tự lưu: Bảo toàn chính xác thứ tự xuất hiện trên trang chính (Top like giảm dần)")

    start_time = time.time()

    while current_url:
        html = fetch_html_direct(current_url)
        if not html:
            print(f"⚠️ Không nhận được phản hồi từ {current_url}. Dừng cào.")
            break

        items, next_url = parse_page_html(html, current_url, page_number)
        if not items:
            print(f"ℹ️ Trang {page_number} không có video nào. Đã đến trang cuối.")
            break

        added_on_page = 0
        for it in items:
            vid_id = extract_video_id(it['video_url'])
            if not vid_id or vid_id in seen_ids:
                # Bỏ qua nếu đã xuất hiện trước đó (giữ lại vị trí rank cao nhất ở trang trước)
                continue

            seen_ids.add(vid_id)
            scraped_json_items.append(it)
            scraped_rows.append([
                it['title'],
                it['author'],
                it['duration'],
                it['thumb_url'],
                it['video_url'],
                it['page_number'],
                it['page_link']
            ])
            added_on_page += 1

        elapsed = time.time() - start_time
        print(f"📄 Trang {page_number:3d}: +{added_on_page:2d} video (Tổng: {len(scraped_json_items):,} video) | {elapsed:.1f}s", flush=True)

        if max_pages and page_number >= max_pages:
            print(f"\n🏁 Đã đạt giới hạn {max_pages} trang theo yêu cầu.")
            break

        if not next_url:
            print(f"\n🏁 Đã cào tới trang cuối cùng (Trang {page_number}). Hoàn tất duyệt toàn bộ trang!")
            break

        current_url = next_url
        page_number += 1
        time.sleep(0.15)  # Nghỉ nhẹ tránh spam server

    total_time = time.time() - start_time
    print(f"\n🎉 Quá trình cào hoàn tất trong {total_time:.1f} giây!")
    print(f"   Tổng số video độc nhất thu thập được: {len(scraped_json_items):,} video")

    if scraped_json_items:
        # Ghi trực tiếp mảng scraped_json_items để bảo toàn tuyệt đối thứ tự từ trang 1 đến cuối
        write_json(scraped_json_items)

        # Cập nhật Google Sheet
        if sheet and scraped_rows:
            try:
                print(f"📊 Đang đồng bộ top 2,000 video nhiều like nhất lên Google Sheet ({SHEET_NAME})...")
                sheet.clear()
                sheet.append_row(HEADERS)
                # Ghi tối đa 2000 dòng theo đúng thứ tự top likes
                rows_to_insert = scraped_rows[:2000]
                sheet.append_rows(rows_to_insert)
                print(f"✅ Đã đồng bộ thành công {len(rows_to_insert):,} dòng lên Google Sheet.")
            except Exception as e:
                print(f"Lỗi ghi Sheet: {e}")
    else:
        print("ℹ️ Không có dữ liệu video.")

if __name__ == '__main__':
    max_p = None
    if len(sys.argv) > 1:
        val = sys.argv[1].strip().lower()
        if val not in ('all', 'tatca', 'tất cả'):
            try:
                max_p = int(val)
            except ValueError:
                pass
    scrape_likes(max_p)
