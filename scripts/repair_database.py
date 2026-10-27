import os
import re
import json
import time
import sys
import base64
import threading
from urllib.parse import urljoin, urlparse, parse_qs, urlunparse
from concurrent.futures import ThreadPoolExecutor, as_completed
from curl_cffi import requests
from bs4 import BeautifulSoup

# Force UTF-8 encoding
if sys.stdout.encoding != 'utf-8':
    try:
        sys.stdout.reconfigure(encoding='utf-8')
    except Exception:
        pass

# Configuration
DATABASE_JSON = "data/viet69_database.json"
DATABASE_JS = "data/viet69_database.js"
ACTIVE_DOMAIN = "viet69.be"
MAX_WORKERS = 16  # High thread count because we only do fast AJAX POST calls (no page loads!)

file_lock = threading.Lock()
print_lock = threading.Lock()

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

def base64_decode(data):
    try:
        missing_padding = len(data) % 4
        if missing_padding:
            data += '=' * (4 - missing_padding)
        return base64.b64decode(data).decode('utf-8')
    except Exception:
        return None

def check_link_status(url, referer, session):
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
        return f"die (error: {str(e)[:40]})"

def get_link_durability(url):
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

def save_database(data_list):
    with file_lock:
        try:
            # Save JSON
            with open(DATABASE_JSON, 'w', encoding='utf-8') as f:
                json.dump(data_list, f, ensure_ascii=False, indent=2)
            # Save JS
            with open(DATABASE_JS, 'w', encoding='utf-8') as f:
                f.write("window.viet69_data = ")
                json.dump(data_list, f, ensure_ascii=False, indent=2)
                f.write(";")
        except Exception as e:
            print(f"[!] Error saving database: {e}")

def resolve_unsupported_server(page_url, srv_name, movie_id, video_type, session):
    """Attempt to decode and resolve a single unsupported server."""
    try:
        ajax_url = f"https://{ACTIVE_DOMAIN}/get.video.php"
        if video_type == "10":
            ajax_url = f"https://{ACTIVE_DOMAIN}/get.xvideo.php"
            
        ajax_headers = headers.copy()
        ajax_headers['Content-Type'] = 'application/x-www-form-urlencoded; charset=UTF-8'
        ajax_headers['Referer'] = page_url
        
        payload = f"movie_id={movie_id}&type={video_type}&index=1"
        ajax_res = session.post(ajax_url, data=payload, headers=ajax_headers, impersonate="chrome120", timeout=10)
        
        if ajax_res.status_code == 200 and "Movie id is not valid" not in ajax_res.text:
            # Case A: JWPlayer fallback (backend file deleted)
            if "jwplayer" in ajax_res.text and ("error:" in ajax_res.text or "Can not get video" in ajax_res.text or "Response from server" in ajax_res.text):
                return {
                    "type": "unknown",
                    "url": f"raw_id:{movie_id}",
                    "status": "die (backend file deleted)",
                    "durability": "unknown",
                    "data_type": video_type
                }
                
            # Case B: Iframe embedded players
            iframe_soup = BeautifulSoup(ajax_res.text, 'html.parser')
            iframe_tag = iframe_soup.find('iframe')
            embed_url = None
            if iframe_tag:
                embed_url = iframe_tag.get('src')
            else:
                iframe_match = re.search(r'src=["\'](https?://[^"\']+)["\']', ajax_res.text)
                if iframe_match:
                    embed_url = iframe_match.group(1)
                    
            if embed_url:
                embed_url = urljoin(page_url, embed_url)
                
                # Blogger (type 12) or Seaporn (type 13)
                if video_type in ["12", "13"]:
                    return {
                        "type": "mp4",
                        "url": embed_url,
                        "status": "live",
                        "durability": "stable",
                        "embed_url": embed_url,
                        "data_type": video_type
                    }
                    
                # BunnyCDN (type 22)
                elif video_type == "22":
                    parsed_embed = urlparse(embed_url)
                    query_params = parse_qs(parsed_embed.query)
                    if 'video' in query_params:
                        encoded_video_url = query_params['video'][0]
                        direct_url = base64_decode(encoded_video_url)
                        if direct_url:
                            status = check_link_status(direct_url, embed_url, session)
                            return {
                                "type": "hls" if ".m3u8" in direct_url.lower() else "mp4",
                                "url": direct_url,
                                "status": status,
                                "durability": get_link_durability(direct_url),
                                "embed_url": embed_url,
                                "data_type": video_type
                            }
                            
                # HLS CDN via API (type 25, 10, or other API-driven embeds)
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
                                return {
                                    "type": "hls" if ".m3u8" in direct_url.lower() else "mp4",
                                    "url": direct_url,
                                    "status": status,
                                    "durability": get_link_durability(direct_url),
                                    "embed_url": embed_url,
                                    "data_type": video_type
                                }
    except Exception:
        pass
    return None

def repair_task(item):
    """Process a single video's uncompleted servers."""
    session = requests.Session()
    page_url = item["source_url"]
    repaired_any = False
    
    for srv_name, srv in list(item["servers"].items()):
        url = srv.get("url", "")
        if url.startswith("raw_id:"):
            movie_id = url.replace("raw_id:", "")
            video_type = srv.get("data_type", "")
            
            # Resolve it!
            resolved = resolve_unsupported_server(page_url, srv_name, movie_id, video_type, session)
            if resolved:
                item["servers"][srv_name] = resolved
                repaired_any = True
                
    return item if repaired_any else None

def main():
    print("=== VIET69 DATABASE REPAIR DOCTOR ===")
    print(f"[*] Loading database: {DATABASE_JSON}...")
    
    if not os.path.exists(DATABASE_JSON):
        print(f"[!] Error: Database file '{DATABASE_JSON}' not found!")
        return
        
    try:
        with open(DATABASE_JSON, 'r', encoding='utf-8') as f:
            database = json.load(f)
    except Exception as e:
        print(f"[!] Failed to parse database: {e}")
        return
        
    print(f"[+] Loaded {len(database)} videos from database.")
    
    # Identify items needing repair
    repair_queue = []
    unresolved_count = 0
    for item in database:
        needs_repair = False
        for srv in item.get("servers", {}).values():
            if srv.get("url", "").startswith("raw_id:") and srv.get("status") == "unsupported (needs analysis)":
                needs_repair = True
                unresolved_count += 1
        if needs_repair:
            repair_queue.append(item)
            
    print(f"[*] Found {len(repair_queue)} videos containing {unresolved_count} un-decoded servers.")
    
    if not repair_queue:
        print("[🎉 SUCCESS] Your database is already 100% decoded! Nothing to repair.")
        return
        
    print(f"[*] Starting {MAX_WORKERS} thread workers for rapid background repair...")
    
    completed_count = 0
    repaired_count = 0
    start_time = time.time()
    
    # Map from page url to item index in the main database list
    db_map = {item["source_url"]: i for i, item in enumerate(database)}
    
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(repair_task, item): item for item in repair_queue}
        
        for future in as_completed(futures):
            completed_count += 1
            res = future.result()
            
            if res:
                repaired_count += 1
                # Update main database memory list
                idx = db_map[res["source_url"]]
                database[idx] = res
                
            # Log progress
            if completed_count % 50 == 0 or completed_count == len(repair_queue):
                pct = (completed_count / len(repair_queue)) * 100
                elapsed = time.time() - start_time
                speed = completed_count / elapsed if elapsed > 0 else 0
                eta = (len(repair_queue) - completed_count) / speed if speed > 0 else 0
                
                with print_lock:
                    print(f"    - Repaired: {completed_count}/{len(repair_queue)} videos ({pct:.2f}%) | Speed: {speed:.1f} vid/sec | ETA: {eta:.0f}s")
                    
            # Auto-save checkpoint every 50 repaired items to prevent data loss
            if repaired_count > 0 and repaired_count % 50 == 0:
                save_database(database)
                
    # Final save
    save_database(database)
    print("\n=== REPAIR DOCTOR COMPLETED ===")
    print(f"[🎉 SUCCESS] Repaired and decoded: {repaired_count} videos.")
    print(f"[+] Total database items: {len(database)} (Updated in both JSON and JS).")
    print("[*] Open dashboard.html and press F5 to see the fully resolved database!")

if __name__ == "__main__":
    main()
