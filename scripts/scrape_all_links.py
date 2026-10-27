import os
import re
import json
import time
import sys
from urllib.parse import urljoin, urlparse, parse_qs, urlunparse
from curl_cffi import requests
from bs4 import BeautifulSoup
import base64
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed

# Force UTF-8 encoding for Windows terminals to prevent crashes on Vietnamese characters
if sys.stdout.encoding != 'utf-8':
    try:
        sys.stdout.reconfigure(encoding='utf-8')
    except Exception:
        pass

# Configuration
INPUT_FILE = "data/data.txt"
OUTPUT_FILE = "data/viet69_database.json"
MAX_WORKERS = 8  # Number of concurrent threads for high-speed scraping
DELAY_BETWEEN_REQUESTS = 0.2  # Tiny delay per thread to be polite but fast

# Active Domain for viet69 (We normalize all old domains to this active one)
ACTIVE_DOMAIN = "viet69.be"

# Thread safety lock for writing to files
file_lock = threading.Lock()

# Known Server Types for Classification
KNOWN_TYPES = ["10", "12", "13", "19", "20", "25"]

# Complete Chrome simulation headers to bypass WAF / anti-bot filters
headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Referer': f'https://{ACTIVE_DOMAIN}/',
    'X-Requested-With': 'XMLHttpRequest',
    'Origin': f'https://{ACTIVE_DOMAIN}',
    'Sec-Fetch-Mode': 'cors',
    'Sec-Fetch-Site': 'same-origin',
    'Sec-Fetch-Dest': 'empty',
    'Accept': '*/*',
    'Accept-Language': 'vi-VN,vi;q=0.9,en-US;q=0.8,en;q=0.7'
}

def normalize_url(url):
    """Normalize old domains in viet69 URLs to the currently active domain."""
    if not url:
        return url
    try:
        parsed = urlparse(url)
        if parsed.netloc.startswith("viet69."):
            new_netloc = ACTIVE_DOMAIN
            if ":" in parsed.netloc:
                port = parsed.netloc.split(":")[-1]
                new_netloc = f"{ACTIVE_DOMAIN}:{port}"
            
            new_parsed = parsed._replace(netloc=new_netloc)
            return urlunparse(new_parsed)
    except Exception:
        pass
    return url

def base64_decode(data):
    """Safely decode base64 string with padding correction."""
    try:
        missing_padding = len(data) % 4
        if missing_padding:
            data += '=' * (4 - missing_padding)
        decoded = base64.b64decode(data).decode('utf-8')
        return decoded
    except Exception:
        return None

def check_link_status(url, referer, session):
    """Perform a rapid HTTP HEAD request to verify if a link is alive."""
    head_headers = {
        'User-Agent': headers['User-Agent'],
        'Referer': referer
    }
    try:
        res = session.head(url, headers=head_headers, impersonate="chrome120", timeout=8)
        if res.status_code in [200, 206, 302]:
            return "live"
        return f"die (status {res.status_code})"
    except Exception as e:
        return f"die (error: {str(e)[:50]})"

def get_link_durability(url):
    """Analyze URL to determine if it is permanent or temporary."""
    if not url or not url.startswith("http"):
        return "unknown"
    try:
        parsed = urlparse(url)
        query_params = parse_qs(parsed.query)
        if any(k in query_params for k in ["expires", "sign", "token", "ip", "ttl"]):
            return "temporary"
        if "cdn.cd-vs.com" in parsed.netloc:
            return "permanent"
        return "stable"
    except Exception:
        return "unknown"

def load_existing_progress():
    """Load already scraped items to support checkpoints."""
    if os.path.exists(OUTPUT_FILE):
        try:
            with open(OUTPUT_FILE, 'r', encoding='utf-8') as f:
                data = json.load(f)
                return {item['source_url']: item for item in data}
        except Exception as e:
            print(f"[*] Warning: Could not parse existing output file ({e}). Starting fresh.")
    return {}

def save_progress(database):
    """Thread-safe save of the database as both JSON and JS file (to bypass CORS)."""
    items_list = list(database.values())
    with file_lock:
        try:
            # Save JSON
            with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
                json.dump(items_list, f, ensure_ascii=False, indent=2)
                
            # Save JS for local HTML dashboard loading (bypassing CORS)
            js_file = OUTPUT_FILE.replace('.json', '.js')
            with open(js_file, 'w', encoding='utf-8') as f:
                f.write("window.viet69_data = ")
                json.dump(items_list, f, ensure_ascii=False, indent=2)
                f.write(";")
        except Exception as e:
            print(f"[!] Error saving database to file: {e}")

def scrape_video_page(page_url, session):
    """Scrape a single detail page and resolve all its server links."""
    result = {
        "servers": {}
    }
    
    target_url = normalize_url(page_url)
    
    page_headers = headers.copy()
    page_headers['Referer'] = f"https://{ACTIVE_DOMAIN}/"
    
    r = session.get(target_url, headers=page_headers, impersonate="chrome120", timeout=12)
    if r.status_code != 200:
        return None
        
    soup = BeautifulSoup(r.text, 'html.parser')
    
    # Find all server buttons
    buttons = soup.find_all('button', class_='video2-btn')
    if not buttons:
        return None
        
    for btn in buttons:
        srv_name = btn.get_text().strip()
        movie_id = btn.get('data-video')
        video_type = btn.get('data-type')
        
        if not movie_id or not video_type:
            continue
            
        # Detect and Alert on NEW Server Types
        if video_type not in KNOWN_TYPES:
            print(f"\n[⚠️ WARNING] DETECTED NEW SERVER TYPE! data-type={video_type} on page: {target_url}")
            print(f"    - Server Name: {srv_name}")
            print(f"    - Raw data-video: {movie_id[:60]}...")
            
        # Case 1: Base64 Decodable static links (old videos, e.g., type 19, 20)
        if video_type in ["19", "20"]:
            decoded_path = base64_decode(movie_id)
            if decoded_path:
                direct_url = f"https://cdn.cd-vs.com/{decoded_path}"
                status = check_link_status(direct_url, target_url, session)
                
                result["servers"][srv_name] = {
                    "type": "hls" if ".m3u8" in decoded_path.lower() else "mp4",
                    "url": direct_url,
                    "status": status,
                    "durability": get_link_durability(direct_url),
                    "data_type": video_type
                }
                
        # Case 2: AJAX Endpoint flow (new videos, e.g., type 25, 10, 11, 12, 13, 15, 18, 22)
        elif video_type in ["25", "10", "11", "12", "13", "15", "18", "22"]:
            try:
                ajax_url = f"https://{ACTIVE_DOMAIN}/get.video.php"
                if video_type == "10":
                    ajax_url = f"https://{ACTIVE_DOMAIN}/get.xvideo.php"
                    
                ajax_headers = headers.copy()
                ajax_headers['Content-Type'] = 'application/x-www-form-urlencoded; charset=UTF-8'
                ajax_headers['Referer'] = target_url
                
                payload = f"movie_id={movie_id}&type={video_type}&index=1"
                ajax_res = session.post(ajax_url, data=payload, headers=ajax_headers, impersonate="chrome120", timeout=10)
                
                if ajax_res.status_code == 200 and "Movie id is not valid" not in ajax_res.text:
                    # Check for JWPlayer error fallbacks (backend file deletion)
                    if "jwplayer" in ajax_res.text and ("error:" in ajax_res.text or "Can not get video" in ajax_res.text or "Response from server" in ajax_res.text):
                        result["servers"][srv_name] = {
                            "type": "unknown",
                            "url": f"raw_id:{movie_id}",
                            "status": "die (backend file deleted)",
                            "durability": "unknown",
                            "data_type": video_type
                        }
                    else:
                                "type": "mp4",
                                "url": embed_url,
                                "status": status,
                                "durability": "stable",
                                "embed_url": embed_url,
                                "data_type": video_type
                            }
                            
                        # Special flow for Type 22 (Player V6 / BunnyCDN)
                        elif video_type == "22":
                            try:
                                parsed_embed = urlparse(embed_url)
                                query_params = parse_qs(parsed_embed.query)
                                if 'video' in query_params:
                                    encoded_video_url = query_params['video'][0]
                                    direct_url = base64_decode(encoded_video_url)
                                    if direct_url:
                                        status = check_link_status(direct_url, embed_url, session)
                                        result["servers"][srv_name] = {
                                            "type": "hls" if ".m3u8" in direct_url.lower() else "mp4",
                                            "url": direct_url,
                                            "status": status,
                                            "durability": get_link_durability(direct_url),
                                            "embed_url": embed_url,
                                            "data_type": video_type
                                        }
                            except Exception as e:
                                print(f"    [!] Error decoding Type 22: {e}")
                                
                        # Standard flow for Type 25 & 10 (HLS CDN via API call)
                        else:
                            parsed_url = urlparse(embed_url)
                            path_parts = parsed_url.path.strip('/').split('/')
                            v_id = path_parts[-1] if path_parts else None
                            
                            if v_id and len(v_id) > 10:
                                api_url = f"https://{parsed_url.netloc}/api/get-video?id={v_id}&counter=0&tried_ids="
                                api_headers = {
                                    'User-Agent': headers['User-Agent'],
                                    'Referer': embed_url,
                                    'Accept': 'application/json, text/plain, */*'
                                }
                                
                                api_res = session.get(api_url, headers=api_headers, impersonate="chrome120", timeout=10)
                                if api_res.status_code == 200:
                                    api_data = api_res.json()
                                    direct_url = api_data.get('url')
                                    if not direct_url and api_data.get('urls'):
                                        urls = api_data.get('urls')
                                        if isinstance(urls, list) and len(urls) > 0:
                                            direct_url = urls[0]
                                        elif isinstance(urls, str):
                                            direct_url = urls
                                    
                                    if direct_url:
                                        status = check_link_status(direct_url, embed_url, session)
                                        result["servers"][srv_name] = {
                                            "type": "hls" if ".m3u8" in direct_url.lower() else "mp4",
                                            "url": direct_url,
                                            "status": status,
                                            "durability": get_link_durability(direct_url),
                                            "embed_url": embed_url,
                                            "data_type": video_type
                                        }
            except Exception:
                pass
                
        # Case 3: Other/New types (e.g. 12, 13, etc.) - We collect metadata for future analysis
        else:
            result["servers"][srv_name] = {
                "type": "unknown",
                "url": f"raw_id:{movie_id}",
                "status": "unsupported (needs analysis)",
                "data_type": video_type
            }
                
    return result if result["servers"] else None

def process_single_video(item, scraped_db):
    """Worker function to process one video page."""
    original_url = item.get('link')
    if not original_url:
        return None
        
    normalized_page_url = normalize_url(original_url)
    if normalized_page_url in scraped_db:
        return None
        
    thread_session = requests.Session()
    
    try:
        scraped_data = scrape_video_page(normalized_page_url, thread_session)
        if scraped_data:
            video_record = {
                "id": item.get('id'),
                "title": item.get('title'),
                "source_url": normalized_page_url,
                "thumbnail": item.get('thumbnail'),
                "views": item.get('views'),
                "likes": item.get('likes'),
                "date": item.get('date'),  # Original publish date from data.txt
                "extracted_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                "servers": scraped_data["servers"]
            }
            return normalized_page_url, video_record
    except Exception as e:
        print(f"    [!] Error on {item.get('title')[:30]}: {e}")
        
    return None

def main():
    print("=== VIET69 OFFLINE HIGH-SPEED MULTI-THREADED SCRAPER ===")
    print(f"[*] Configuration: Thread Workers = {MAX_WORKERS}")
    print(f"[*] Normalizing all domains to: {ACTIVE_DOMAIN}")
    
    if not os.path.exists(INPUT_FILE):
        print(f"[!] Error: Input file '{INPUT_FILE}' not found. Please run the script in the correct directory.")
        return
        
    print(f"[*] Loading {INPUT_FILE}...")
    try:
        with open(INPUT_FILE, 'r', encoding='utf-8') as f:
            raw_videos = json.load(f)
    except Exception as e:
        print(f"[!] Failed to parse {INPUT_FILE}: {e}")
        return
        
    total_videos = len(raw_videos)
    print(f"[+] Loaded {total_videos} videos from {INPUT_FILE}.")
    
    scraped_db = load_existing_progress()
    scraped_count = len(scraped_db)
    print(f"[*] Checkpoint loaded: {scraped_count} / {total_videos} videos already scraped.")
    
    if scraped_count > 0:
        save_progress(scraped_db)
        print("[*] Synchronized local database files (.json & .js) successfully.")
        
    # Filter out already scraped videos (using normalized URLs)
    videos_to_scrape = []
    for item in raw_videos:
        norm_url = normalize_url(item.get('link'))
        if norm_url and norm_url not in scraped_db:
            videos_to_scrape.append(item)
            
    to_scrape_count = len(videos_to_scrape)
    print(f"[*] Remaining videos to scrape: {to_scrape_count}")
    
    if to_scrape_count == 0:
        print("[+] All videos are already scraped. Database is up to date!")
        return
        
    newly_scraped = 0
    save_interval = 10
    
    print("\n[*] Initializing thread pool. Starting parallel extraction...")
    
    try:
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = {executor.submit(process_single_video, item, scraped_db): item for item in videos_to_scrape}
            
            for future in as_completed(futures):
                res = future.result()
                if res:
                    page_url, video_record = res
                    scraped_db[page_url] = video_record
                    newly_scraped += 1
                    
                    completed_pct = ((scraped_count + newly_scraped) / total_videos) * 100
                    print(f"[{scraped_count + newly_scraped}/{total_videos}] ({completed_pct:.2f}%) Extracted: {video_record['title'][:45]}...")
                    for srv, details in video_record["servers"].items():
                        print(f"    - {srv} [type {details['data_type']}]: {details['status']} -> {details['url'][:60]}...")
                    
                    if newly_scraped % save_interval == 0:
                        save_progress(scraped_db)
                        
                time.sleep(DELAY_BETWEEN_REQUESTS)
                
    except KeyboardInterrupt:
        print("\n[!] Scrape process interrupted by user. Saving progress before exiting...")
        save_progress(scraped_db)
        print(f"[+] Saved {len(scraped_db)} total items. Exiting.")
        sys.exit(0)
        
    # Final Save
    save_progress(scraped_db)
    print(f"\n=== SCRAPE PROCESS COMPLETED ===")
    print(f"[+] Total items in database: {len(scraped_db)}")
    print(f"[+] Newly scraped this session: {newly_scraped}")

if __name__ == '__main__':
    main()
