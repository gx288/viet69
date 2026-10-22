#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Viet69 Monthly Top Videos -> Zpic.org -> Xamvn Forum & XamVid Automation
========================================================================
Tự động cào video xem nhiều nhất theo từng tháng lùi dần về quá khứ:
- Cắt 2s đen màn hình đầu video (-ss 2.0).
- Cắt thumbnail về đúng size video (crop ở giữa) và gắn vào đầu video 1s để tạo preview.
- Đóng dấu bản quyền xamvid.netlify.app.
- Điền đầy đủ tiêu đề và nội dung mô tả video vào comment diễn đàn Xamvn.
- Ưu tiên tháng gần nhất, tự động kiểm tra mỗi lần chạy.
- Tối đa 20 video mỗi tháng, tháng nào đã đủ 20 video sẽ tự động bỏ qua.
- Tự động cập nhật file monthly_videos.json cho web xamvid.netlify.app.

Cách chạy:
    python scripts/monthly_top_to_xamvn.py --post --limit-per-month 20 --past-months 12
"""

import os
import re
import sys
import time
import json
import argparse
import subprocess
from datetime import datetime
from typing import List, Dict, Any, Optional, Tuple, Set

# Force UTF-8 and line buffering on Windows for live logs
try:
    sys.stdout.reconfigure(encoding='utf-8', line_buffering=True)
except Exception:
    pass

CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_DIR = os.path.abspath(os.path.join(CURRENT_DIR, ".."))
XAMVN_DIR = os.path.abspath(os.path.join(PROJECT_DIR, "..", "xamvn"))
XAMVID_SITE_DIR = os.path.abspath(os.path.join(PROJECT_DIR, "..", "xamvid-site"))

if CURRENT_DIR not in sys.path:
    sys.path.insert(0, CURRENT_DIR)
if PROJECT_DIR not in sys.path:
    sys.path.insert(0, PROJECT_DIR)
if XAMVN_DIR not in sys.path:
    sys.path.insert(0, XAMVN_DIR)

from upload_to_zpic import (
    upload_mp4_to_zpic,
    download_stream_to_mp4,
    get_font_file,
    DATABASE_FILE,
    OUTPUT_JSON,
    OUTPUT_BBCODE,
    TEMP_DIR,
    USER_AGENT
)

POSTED_HISTORY_FILE = os.path.join(PROJECT_DIR, "data", "xamvn_posted_monthly.json")
MONTHLY_VIDEOS_JSON = os.path.join(PROJECT_DIR, "data", "monthly_videos.json")
ACTIVE_DOMAIN = "viet69.be"
PRIORITY_SHEET_ID = os.getenv("PRIORITY_SHEET_ID", "1ZQTydO0y5CGfCaFCcw72F0LXzR5b63p_6yzttTUHTyY")
PRIORITY_SHEET_TAB = os.getenv("PRIORITY_SHEET_TAB", "Queue")


def get_cf_session():
    try:
        from curl_cffi import requests as cf_requests
        return cf_requests.Session()
    except ImportError:
        import requests
        return requests.Session()


def clean_slug(s: str) -> str:
    return re.sub(r'[^a-zA-Z0-9]+', '_', s).strip('_')


def load_posted_history() -> Dict[str, Any]:
    if os.path.exists(POSTED_HISTORY_FILE):
        try:
            with open(POSTED_HISTORY_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            pass
    return {"posted": []}


def save_posted_history(history: Dict[str, Any]):
    os.makedirs(os.path.dirname(POSTED_HISTORY_FILE), exist_ok=True)
    with open(POSTED_HISTORY_FILE, "w", encoding="utf-8") as f:
        json.dump(history, f, indent=2, ensure_ascii=False)


def sync_to_xamvid_web(history: Dict[str, Any]):
    """Đồng bộ danh sách video đã đăng vào file monthly_videos.json cho web xamvid.netlify.app."""
    posted_data = history.get("posted", [])
    web_items = []
    seen_ids = set()

    for p in posted_data:
        mp4 = p.get("zpic_mp4")
        if not mp4:
            continue
        vid_id = mp4.split("/")[-1].split(".")[0]
        if vid_id in seen_ids:
            continue
        seen_ids.add(vid_id)

        thumb = p.get("frame_url") or f"https://f.zzpi.cc/{vid_id}.fr.jpeg"
        post_id = p.get("post_id")
        post_url = f"https://xamvn.yachts/posts/{post_id}/" if post_id else "https://xamvn.yachts/threads/311300/"

        web_items.append({
            "id": vid_id,
            "title": p.get("title", "Video"),
            "month": p.get("month", ""),
            "views": p.get("views", ""),
            "duration": p.get("duration", 0),
            "video_url": mp4,
            "thumb_url": thumb,
            "viewer_url": p.get("zpic_viewer", f"https://zpic.org/view/{vid_id}"),
            "author": f"Xamvn - Tháng {p.get('month', '')}",
            "xamvn_post": post_url,
            "posted_at": p.get("posted_at", "")
        })

    # Ghi vào cả thư mục viet69 và xamvid-site
    target_dirs = [
        os.path.join(PROJECT_DIR, "data"),
        os.path.join(XAMVID_SITE_DIR, "data"),
        XAMVID_SITE_DIR
    ]
    for d in target_dirs:
        if os.path.exists(os.path.dirname(d)):
            os.makedirs(d, exist_ok=True)
            out_file = os.path.join(d, "monthly_videos.json") if not d.endswith(".json") else d
            try:
                with open(out_file, "w", encoding="utf-8") as f:
                    json.dump(web_items, f, indent=2, ensure_ascii=False)
            except Exception as e:
                print(f"[!] Lỗi ghi file {out_file}: {e}")


def fetch_thread_posted_hashes(thread_url: str, max_pages: int = 5) -> Set[str]:
    """Quét các trang cuối của thread Xamvn để lấy các hash/id video đã đăng tránh trùng."""
    posted_hashes = set()
    session = get_cf_session()
    try:
        from bs4 import BeautifulSoup
        r = session.get(f"{thread_url.rstrip('/')}/page-999", impersonate="chrome120", timeout=12)
        m = re.search(r'/page-(\d+)', r.url)
        last_page = int(m.group(1)) if m else 51

        start_p = max(1, last_page - max_pages + 1)
        print(f"[*] Đang đồng bộ danh sách đã đăng từ thread Xamvn (trang {start_p} -> {last_page})...")
        for p in range(start_p, last_page + 1):
            p_url = f"{thread_url.rstrip('/')}/page-{p}"
            resp = session.get(p_url, impersonate="chrome120", timeout=10)
            if resp.status_code != 200:
                continue
            found = re.findall(r'(?:f\.zzpi\.cc|zpic\.org/view)/([a-zA-Z0-9_-]+)', resp.text)
            for h in found:
                clean_h = h.split('.')[0]
                posted_hashes.add(clean_h)
        print(f"[+] Tìm thấy {len(posted_hashes)} video đã đăng trên thread Xamvn.")
    except Exception as e:
        print(f"[!] Cảnh báo quét thread: {e}")
    return posted_hashes


def extract_post_details(post_url: str) -> Tuple[Optional[str], str, str, Optional[str]]:
    """
    Bóc tách stream URL, tiêu đề, nội dung mô tả bài viết và link ảnh thumbnail từ Viet69.
    Hỗ trợ tự động fallback qua nhiều server (Server #2 -> Server #1 -> các server khác).
    Trả về: (stream_url_hoặc_None, title, description, thumb_url)
    """
    from urllib.parse import urlparse
    from bs4 import BeautifulSoup
    import base64

    headers = {
        "User-Agent": USER_AGENT,
        "Referer": f"https://{ACTIVE_DOMAIN}/",
        "X-Requested-With": "XMLHttpRequest",
        "Origin": f"https://{ACTIVE_DOMAIN}"
    }
    session = get_cf_session()

    try:
        r = session.get(post_url, headers=headers, impersonate="chrome120" if hasattr(session, "impersonate") else None, timeout=15)
        if r.status_code != 200:
            print(f"  [-] Không mở được bài viết ({r.status_code}): {post_url}")
            return None, "Video", "", None
    except Exception as e:
        print(f"  [-] Lỗi kết nối bài viết ({e}): {post_url}")
        return None, "Video", "", None

    soup = BeautifulSoup(r.text, "html.parser")
    title_el = soup.find("h1") or soup.find("h2")
    if title_el:
        title = title_el.get_text(strip=True)
    elif soup.title:
        title = soup.title.string.split(" - ")[0].strip()
    else:
        title = "Video"

    # Lấy ảnh thumbnail từ meta og:image hoặc twitter:image
    thumb_meta = soup.find("meta", property="og:image") or soup.find("meta", attrs={"name": "twitter:image"})
    thumb_url = thumb_meta.get("content") if thumb_meta else None

    # Lấy tất cả nút server trên trang
    buttons = soup.find_all("button", class_="video2-btn")

    # Lấy nội dung mô tả video
    desc_el = soup.select_one(".entry-content, .video-description, .post-entry, #entry-content, .desc")
    description = ""
    if desc_el:
        desc_text = desc_el.get_text(" ", strip=True)
        for b in buttons:
            desc_text = desc_text.replace(b.get_text(strip=True), "")
        desc_text = re.sub(r"^Loading\.\.\.\s*\d+[\.\d]*[KM]?\s*", "", desc_text).strip()
        description = re.sub(r"\s+", " ", desc_text).strip()

    # Sắp xếp ưu tiên: Server #2 trước, sau đó tới Server #1, rồi các server khác
    def server_sort_key(btn):
        t = btn.get_text(strip=True)
        if "2" in t:
            return 0
        if "1" in t:
            return 1
        return 2

    sorted_buttons = sorted(buttons, key=server_sort_key)

    for btn in sorted_buttons:
        btn_text = btn.get_text(strip=True)
        movie_id = btn.get("data-video")
        video_type = btn.get("data-type")
        if not movie_id:
            continue

        # Trường hợp 1: Base64 direct link (type 19, 20, 22)
        if video_type in ["19", "20", "22"]:
            try:
                pad = len(movie_id) % 4
                b64_m = movie_id + ('=' * (4 - pad) if pad else '')
                decoded = base64.b64decode(b64_m).decode('utf-8', errors='ignore').strip()
                if decoded:
                    cdn_host = "cdn2.cd-vs.com" if video_type == "22" or decoded.startswith("videos/") else "cdn.cd-vs.com"
                    direct_url = decoded if decoded.startswith("http") else f"https://{cdn_host}/{decoded.lstrip('/')}"
                    print(f"  [+] Bóc tách thành công từ {btn_text} (Base64 type {video_type}): {direct_url}")
                    return direct_url, title, description, thumb_url
            except Exception:
                pass

        # Trường hợp 2: Base64 UUID của emb.cd-vs.com (type 25) -> gọi thẳng api/get-video không cần qua get.video.php (tránh bị Cloudflare chặn POST trên GitHub Actions)
        try:
            pad = len(movie_id) % 4
            b64_str = movie_id + ('=' * (4 - pad) if pad else '')
            decoded_id = base64.b64decode(b64_str).decode('utf-8', errors='ignore').strip()
            if re.match(r'^[0-9a-fA-F-]{32,36}$', decoded_id):
                embed_url = f"https://emb.cd-vs.com/embed/{decoded_id}"
                api_url = f"https://emb.cd-vs.com/api/get-video?id={decoded_id}&counter=0&tried_ids="
                api_headers = {
                    "User-Agent": USER_AGENT,
                    "Referer": embed_url,
                    "X-Requested-With": "XMLHttpRequest",
                    "Origin": "https://emb.cd-vs.com"
                }
                api_res = session.get(api_url, headers=api_headers, impersonate="chrome120" if hasattr(session, "impersonate") else None, timeout=12)
                if api_res.status_code == 200:
                    vdata = api_res.json()
                    urls = vdata.get("urls") if isinstance(vdata.get("urls"), list) else ([vdata["url"]] if vdata.get("url") else [])
                    if urls and urls[0]:
                        print(f"  [+] Bóc tách thành công luồng từ {btn_text} (Direct UUID): {urls[0]}")
                        return urls[0], title, description, thumb_url
        except Exception:
            pass

        # Trường hợp 3: AJAX Endpoint (type 25, 10, v.v.)
        ajax_url = f"https://{ACTIVE_DOMAIN}/get.xvideo.php" if video_type == "10" else f"https://{ACTIVE_DOMAIN}/get.video.php"
        payload = f"movie_id={movie_id}&type={video_type}&index=1"
        ajax_headers = {
            "User-Agent": USER_AGENT,
            "Referer": post_url,
            "X-Requested-With": "XMLHttpRequest",
            "Origin": f"https://{ACTIVE_DOMAIN}",
            "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8"
        }

        for attempt in range(2):
            try:
                res = session.post(ajax_url, data=payload, headers=ajax_headers, impersonate="chrome120" if hasattr(session, "impersonate") else None, timeout=12)
                if res.status_code == 200:
                    m = re.search(r'src=[\'"]([^\'"]+)[\'"]', res.text)
                    if m:
                        embed_url = m.group(1)
                        parsed = urlparse(embed_url)
                        vid = parsed.path.strip("/").split("/")[-1]
                        api_url = f"https://{parsed.netloc}/api/get-video?id={vid}&counter=0&tried_ids="
                        api_headers = {
                            "User-Agent": USER_AGENT,
                            "Referer": embed_url,
                            "X-Requested-With": "XMLHttpRequest",
                            "Origin": f"https://{parsed.netloc}"
                        }
                        api_res = session.get(api_url, headers=api_headers, impersonate="chrome120" if hasattr(session, "impersonate") else None, timeout=12)
                        if api_res.status_code == 200:
                            vdata = api_res.json()
                            urls = []
                            if isinstance(vdata.get("urls"), list):
                                urls = vdata["urls"]
                            elif vdata.get("url"):
                                urls = [vdata["url"]]
                            if urls:
                                print(f"  [+] Bóc tách thành công luồng từ {btn_text}: {urls[0]}")
                                return urls[0], title, description, thumb_url
            except Exception as e:
                import time
                time.sleep(1)

    print(f"  [-] Không bóc tách được stream URL nào từ bài viết: {post_url}")
    return None, title, description, thumb_url


def prepend_thumbnail_preview(video_path: str, thumb_url_or_path: Optional[str], out_final_path: str, watermark_text: Optional[str] = None) -> bool:
    """
    Cắt thumbnail về đúng kích thước video (crop ở giữa: lấy ở giữa là được)
    và gắn vào đầu video trong 1 giây để làm ảnh preview/poster tự nhiên cho player.
    """
    # 1. Đọc kích thước video và thông số audio/timebase
    probe_cmd = [
        "ffprobe", "-v", "error",
        "-show_entries", "stream=codec_type,width,height,r_frame_rate,time_base,duration,sample_rate,channels",
        "-of", "json", video_path
    ]
    res_probe = subprocess.run(probe_cmd, capture_output=True, text=True)
    if res_probe.returncode != 0:
        return False

    info = json.loads(res_probe.stdout)
    v_stream = next((s for s in info.get("streams", []) if s.get("codec_type") == "video"), info["streams"][0])
    a_stream = next((s for s in info.get("streams", []) if s.get("codec_type") == "audio"), None)

    w = v_stream["width"]
    h = v_stream["height"]
    fps_raw = v_stream.get("r_frame_rate", "30/1")
    try:
        fps = eval(fps_raw) if "/" in fps_raw else float(fps_raw)
    except Exception:
        fps = 30.0

    tb_raw = v_stream.get("time_base", "1/15360")
    try:
        timescale = int(tb_raw.split("/")[1]) if "/" in tb_raw else 15360
    except Exception:
        timescale = 15360

    a_sr = int(a_stream.get("sample_rate", 44100)) if a_stream else 44100
    a_ch = int(a_stream.get("channels", 2)) if a_stream else 2

    temp_dir = os.path.dirname(out_final_path) or TEMP_DIR
    thumb_file = os.path.join(temp_dir, f"thumb_src_{int(time.time()*1000)}.jpg")

    # 2. Lấy file ảnh thumbnail (hỗ trợ cả URL http và file nội bộ)
    got_thumb = False
    if thumb_url_or_path and os.path.exists(thumb_url_or_path):
        import shutil
        shutil.copy2(thumb_url_or_path, thumb_file)
        got_thumb = True
    elif thumb_url_or_path and thumb_url_or_path.startswith("http"):
        try:
            session = get_cf_session()
            r = session.get(thumb_url_or_path, impersonate="chrome120" if hasattr(session, "impersonate") else None, timeout=10)
            if r.status_code == 200 and len(r.content) > 1000:
                with open(thumb_file, "wb") as f:
                    f.write(r.content)
                got_thumb = True
        except Exception:
            pass

    # Nếu không có hoặc tải ảnh lỗi: trích xuất 1 khung hình ở giữa video (lấy ở giữa là được)
    if not got_thumb:
        try:
            dur = float(v_stream.get("duration") or 10.0)
            mid_time = max(0.5, dur / 2.0)
            extract_cmd = ["ffmpeg", "-ss", str(mid_time), "-i", video_path, "-vframes", "1", "-y", thumb_file]
            subprocess.run(extract_cmd, capture_output=True)
            if os.path.exists(thumb_file) and os.path.getsize(thumb_file) > 1000:
                got_thumb = True
        except Exception:
            pass

    if not got_thumb or not os.path.exists(thumb_file):
        # Nếu hoàn toàn không tạo được thì giữ nguyên video gốc
        return False

    # 3. Tạo đoạn intro 1s từ thumbnail, cắt crop lấy phần giữa theo đúng W x H của video
    vf = f"scale={w}:{h}:force_original_aspect_ratio=increase,crop={w}:{h}:(iw-{w})/2:(ih-{h})/2,format=yuv420p"
    if watermark_text:
        font_path = get_font_file()
        safe_text = watermark_text.replace(":", "\\:").replace("'", "\\'")
        font_opt = f"fontfile='{font_path}':" if font_path else ""
        vf += (
            f",drawtext={font_opt}text='{safe_text}':"
            f"fontsize=min(w\\,h)/16:fontcolor=white@0.35:"
            f"borderw=1:bordercolor=black@0.25:"
            f"x=(w-text_w)/2:y=(h-text_h)/2"
        )

    intro_mp4 = os.path.join(temp_dir, f"intro_{int(time.time()*1000)}.mp4")
    cl_str = "mono" if a_ch == 1 else "stereo"
    cmd_thumb = [
        "ffmpeg", "-loop", "1", "-t", "1.0", "-i", thumb_file,
        "-f", "lavfi", "-t", "1.0", "-i", f"anullsrc=r={a_sr}:cl={cl_str}",
        "-vf", vf,
        "-c:v", "libx264", "-preset", "superfast", "-crf", "25",
        "-profile:v", "high", "-level", "4.0", "-x264-params", "stitchable=1",
        "-r", str(fps), "-video_track_timescale", str(timescale),
        "-c:a", "aac", "-ar", str(a_sr), "-ac", str(a_ch), "-b:a", "128k",
        "-y", intro_mp4
    ]
    subprocess.run(cmd_thumb, capture_output=True)

    if not os.path.exists(intro_mp4) or os.path.getsize(intro_mp4) < 1000:
        if os.path.exists(thumb_file):
            try: os.remove(thumb_file)
            except Exception: pass
        return False

    # 4. Ghép intro vào đầu video bằng concat (đồng bộ chuẩn codec/audio tránh đơ hình ở giây thứ 1)
    list_file = os.path.join(temp_dir, f"concat_list_{int(time.time()*1000)}.txt")
    with open(list_file, "w", encoding="utf-8") as f:
        f.write(f"file '{os.path.abspath(intro_mp4).replace(chr(92), '/')}'\n")
        f.write(f"file '{os.path.abspath(video_path).replace(chr(92), '/')}'\n")

    if a_sr != 44100 or a_ch != 2:
        cmd_concat = [
            "ffmpeg", "-f", "concat", "-safe", "0", "-i", list_file,
            "-c:v", "copy", "-c:a", "aac", "-ar", "44100", "-ac", "2", "-b:a", "128k",
            "-movflags", "+faststart",
            "-y", out_final_path
        ]
    else:
        cmd_concat = [
            "ffmpeg", "-f", "concat", "-safe", "0", "-i", list_file,
            "-c", "copy", "-movflags", "+faststart",
            "-y", out_final_path
        ]
    res_concat = subprocess.run(cmd_concat, capture_output=True)

    # Dọn dẹp file tạm
    for p in [thumb_file, intro_mp4, list_file]:
        if os.path.exists(p):
            try: os.remove(p)
            except Exception: pass

    return res_concat.returncode == 0 and os.path.exists(out_final_path) and os.path.getsize(out_final_path) > 1024


def fetch_monthly_top_videos(month_str: str, limit: int = 20) -> List[Dict[str, Any]]:
    """Truy vấn tối đa limit video xem nhiều nhất của tháng chỉ định."""
    clean_month = month_str.replace("-", "/").strip("/")
    session = get_cf_session()
    headers = {
        "User-Agent": USER_AGENT,
        "Referer": f"https://{ACTIVE_DOMAIN}/"
    }

    results = []
    seen_urls = set()
    page = 1
    max_pages = (limit // 25) + 2

    while len(results) < limit and page <= max_pages:
        url = f"https://{ACTIVE_DOMAIN}/{clean_month}/?v_sortby=views" if page == 1 else f"https://{ACTIVE_DOMAIN}/{clean_month}/page/{page}/?v_sortby=views"
        print(f"[*] Đang quét trang {page} tháng {clean_month} ({url})...")
        try:
            from bs4 import BeautifulSoup
            r = session.get(url, headers=headers, impersonate="chrome120" if hasattr(session, "impersonate") else None, timeout=15)
            if r.status_code != 200:
                break

            soup = BeautifulSoup(r.text, "html.parser")
            items = soup.select("article, .item, .post-item, div.post")
            if not items:
                break

            found_in_page = 0
            for item in items:
                a = item.select_one("a[href*='viet69.be']")
                if not a:
                    continue
                href = a["href"].rstrip("/") + "/"
                if href in seen_urls:
                    continue
                seen_urls.add(href)

                title_el = item.select_one(".title, .entry-title, h2, h3")
                title = title_el.get_text(strip=True) if title_el else (a.get("title", "") or "video")
                views_el = item.select_one(".views, .view-count, span.views")
                views = views_el.get_text(strip=True) if views_el else "N/A"

                m = re.search(r'post-(\d+)', item.get("class", [""])[0] if isinstance(item.get("class"), list) else "")
                vid = m.group(1) if m else None

                results.append({
                    "month": clean_month,
                    "url": href,
                    "title": title,
                    "views": views,
                    "id": vid
                })
                found_in_page += 1
                if len(results) >= limit:
                    break

            if found_in_page == 0:
                break
            page += 1
        except Exception as e:
            print(f"[-] Lỗi khi quét trang {page} tháng {clean_month}: {e}")
            break

    # Fallback từ data.txt nếu cào trực tiếp chưa đủ video
    data_txt_path = os.path.join(PROJECT_DIR, "data.txt")
    if len(results) < limit and os.path.exists(data_txt_path):
        try:
            with open(data_txt_path, "r", encoding="utf-8") as f:
                all_data = json.load(f)
            month_prefix = clean_month.replace("/", "-")
            matched = [x for x in all_data if x.get("date", "").startswith(month_prefix)]
            matched.sort(key=lambda x: int(x.get("views", 0) or 0), reverse=True)
            for item in matched:
                href = item.get("link", "").rstrip("/") + "/"
                if href and href not in seen_urls:
                    seen_urls.add(href)
                    results.append({
                        "month": clean_month,
                        "url": href,
                        "title": item.get("title", "Video"),
                        "views": f"{item.get('views', 0):,} Views",
                        "id": str(item.get("id", ""))
                    })
                    if len(results) >= limit:
                        break
            print(f"[+] Đã bổ sung từ database data.txt: tổng cộng {len(results)}/{limit} video cho tháng {clean_month}.")
        except Exception as e:
            print(f"[!] Không thể đọc từ data.txt: {e}")

    print(f"[+] Tháng {clean_month}: Đã lấy được {len(results)}/{limit} video xem nhiều nhất.")
    return results


def ensure_video_on_zpic(
    video_item: Dict[str, Any],
    watermark: Optional[str] = "xamvid.netlify.app",
    trim_start: float = 2.0
) -> Optional[Dict[str, Any]]:
    """
    Tải luồng video, cắt 2s đen, gắn thumbnail preview 1s ở đầu và upload lên Zpic.
    Hỗ trợ cả link bài viết Viet69 (Cột 1) và link stream trực tiếp .m3u8/.mp4 hoặc HTML (Cột 2).
    """
    uploaded_list = []
    if os.path.exists(OUTPUT_JSON):
        try:
            with open(OUTPUT_JSON, "r", encoding="utf-8") as f:
                uploaded_list = json.load(f)
        except Exception:
            pass

    target_url = (video_item.get("url") or "").rstrip("/")
    preset_stream = (video_item.get("stream_url") or "").strip()
    preset_thumb = (video_item.get("thumb_url") or "").strip()

    for up in uploaded_list:
        up_post = (up.get("source_post") or "").rstrip("/")
        up_stream = (up.get("source_stream") or "").strip()
        if (target_url and up_post == target_url) or (preset_stream and up_stream == preset_stream):
            print(f"[+] Video đã có sẵn trên Zpic: {up['zpic']['direct_mp4']}")
            video_item["description"] = up.get("description", "")
            return up

    os.makedirs(TEMP_DIR, exist_ok=True)
    stream_url = preset_stream or None
    real_title = None
    description = ""
    thumb_url = preset_thumb or None

    is_viet69_post = bool(target_url and ("viet69." in target_url.lower() and not target_url.lower().endswith((".m3u8", ".mp4"))))
    if is_viet69_post:
        print(f"[*] Bóc tách thông tin & luồng HLS từ Viet69: {video_item['title']} ({video_item['url']})...")
        try:
            v69_stream, v69_title, v69_desc, v69_thumb = extract_post_details(video_item["url"])
            if not stream_url:
                stream_url = v69_stream
            if v69_title and v69_title != "Video":
                real_title = v69_title
            description = v69_desc or ""
            if not thumb_url:
                thumb_url = v69_thumb
        except Exception as e:
            print(f"    [!] Cảnh báo khi bóc tách trang Viet69 ({e}).")
            if not stream_url:
                return None

    if not stream_url:
        print(f"    [-] Không tìm thấy luồng stream hợp lệ cho bài viết này. Bỏ qua để thử video tiếp theo...")
        return None

    # Nếu link stream từ Twitter/nguồn ngoài (không phải cd-vs.com của Viet69), không cắt 2s đầu
    effective_trim = 0.0 if ("twimg.com" in stream_url.lower() or "cd-vs.com" not in stream_url.lower()) else trim_start

    if real_title:
        video_item["title"] = real_title
    video_item["description"] = description
    video_item["thumb_url"] = thumb_url

    timestamp = int(time.time())
    raw_mp4 = os.path.join(TEMP_DIR, f"raw_{clean_slug(video_item['month'])}_{timestamp}.mp4")
    final_mp4 = os.path.join(TEMP_DIR, f"final_{clean_slug(video_item['month'])}_{timestamp}.mp4")

    try:
        # Bước 1: Tải và cắt màn hình đen (nếu có) + watermark
        download_stream_to_mp4(stream_url, raw_mp4, watermark_text=watermark, trim_start=effective_trim)

        # Bước 2: Cắt thumbnail về đúng kích thước video (ở giữa) và gắn vào đầu video 1s để làm preview
        print(f"  [*] Đang xử lý thumbnail preview từ '{thumb_url or 'frame giữa video'}' gắn vào đầu clip...")
        attached = prepend_thumbnail_preview(raw_mp4, thumb_url, final_mp4, watermark_text=watermark)
        upload_target = final_mp4 if attached else raw_mp4

        # Bước 3: Upload Zpic
        zpic_data = upload_mp4_to_zpic(upload_target, video_item["title"], TEMP_DIR)

        now_utc = datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ")
        record = {
            "month": video_item["month"],
            "title": video_item["title"],
            "description": description,
            "views": video_item.get("views"),
            "watermark": watermark,
            "trim_start": effective_trim,
            "source_post": video_item["url"] or stream_url,
            "source_stream": stream_url,
            "uploaded_at": now_utc,
            "zpic": zpic_data
        }

        uploaded_list.insert(0, record)
        with open(OUTPUT_JSON, "w", encoding="utf-8") as f:
            json.dump(uploaded_list, f, indent=2, ensure_ascii=False)

        with open(OUTPUT_BBCODE, "a", encoding="utf-8") as f:
            f.write(f"\n=== [{video_item['month']}] {video_item['title']} ({record['uploaded_at']}) ===\n")
            f.write(f"Description   : {description}\n")
            f.write(f"BBCode Full   : {zpic_data['bbcode_full']}\n")
            f.write(f"Direct MP4    : {zpic_data['direct_mp4']}\n")
            f.write(f"Viewer Link   : {zpic_data['viewer_url']}\n")

        return record
    except Exception as e:
        print(f"    [-] Lỗi khi tải hoặc upload video lên Zpic: {e}")
        return None
    finally:
        for p in [raw_mp4, final_mp4]:
            if os.path.exists(p):
                try: os.remove(p)
                except Exception: pass


def get_priority_sheet_worksheet(sheet_id: str = PRIORITY_SHEET_ID, tab_name: str = PRIORITY_SHEET_TAB):
    """Kết nối tới Google Sheet hàng đợi ưu tiên (hỗ trợ cả Service Account trên GitHub Actions và OAuth local)."""
    try:
        import gspread
        creds = None
        creds_json = os.getenv("GOOGLE_CREDENTIALS")
        scopes = [
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive"
        ]
        if creds_json:
            creds_dict = json.loads(creds_json)
            if creds_dict.get("type") == "service_account":
                from google.oauth2.service_account import Credentials as SACredentials
                creds = SACredentials.from_service_account_info(creds_dict, scopes=scopes)
            else:
                from google.oauth2.credentials import Credentials as UserCredentials
                creds = UserCredentials.from_authorized_user_info(creds_dict)
        else:
            local_token = r"D:\AT\GoogleDrive_OAuth\token.json"
            if os.path.exists(local_token):
                from google.oauth2.credentials import Credentials as UserCredentials
                creds = UserCredentials.from_authorized_user_file(local_token)

        if not creds:
            print("[!] Không tìm thấy GOOGLE_CREDENTIALS để kết nối Google Sheet ưu tiên.")
            return None

        gc = gspread.authorize(creds)
        sh = gc.open_by_key(sheet_id)
        try:
            return sh.worksheet(tab_name)
        except Exception:
            return sh.get_worksheet(0)
    except Exception as e:
        print(f"[!] Không thể kết nối Google Sheet ưu tiên ({e}).")
        return None


def translate_to_vietnamese(text: str) -> str:
    """
    Tự động dịch tiêu đề tiếng Trung / Anh / Nhật từ bad.news sang tiếng Việt tự nhiên.
    """
    if not text or not text.strip():
        return "Video Hot Cập Nhật"
    clean_text = re.sub(r"\s+", " ", text).strip()
    # Nếu chuỗi chỉ chứa ký tự Latin/tiếng Việt bình thường (không có chữ Hán/Nhật/Hàn), kiểm tra nhanh
    has_cjk = bool(re.search(r"[\u4e00-\u9fff\u3040-\u30ff\uac00-\ud7af]", clean_text))
    try:
        from urllib.parse import quote
        import requests as std_requests
        url = f"https://translate.googleapis.com/translate_a/single?client=gtx&sl=auto&tl=vi&dt=t&q={quote(clean_text)}"
        r = std_requests.get(url, timeout=10)
        if r.status_code == 200:
            data = r.json()
            translated = "".join(seg[0] for seg in data[0] if seg and seg[0]).strip()
            if translated:
                print(f"  [+] Đã dịch tiêu đề sang tiếng Việt: {translated}")
                return translated
    except Exception as e:
        print(f"  [!] Cảnh báo dịch tiêu đề ({e})")
    return clean_text if not has_cjk else "Video Hot Cập Nhật"


def extract_badnews_details(badnews_url: str) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    """
    Tự động bóc tách link stream (.m3u8 / .mp4), ảnh poster thumbnail và tiêu đề (đã dịch sang tiếng Việt) từ trang https://bad.news/t/...
    Trả về: (stream_url, thumb_url, translated_vi_title)
    """
    from bs4 import BeautifulSoup
    if not badnews_url.lower().startswith("http"):
        badnews_url = "https://" + badnews_url.lstrip("/")

    session = get_cf_session()
    headers = {
        "User-Agent": USER_AGENT,
        "Referer": "https://bad.news/"
    }
    try:
        r = session.get(badnews_url, headers=headers, impersonate="chrome120" if hasattr(session, "impersonate") else None, timeout=15)
        if r.status_code != 200:
            print(f"  [-] Không mở được link bad.news ({r.status_code}): {badnews_url}")
            return None, None, None
    except Exception as e:
        print(f"  [-] Lỗi kết nối bad.news ({e}): {badnews_url}")
        return None, None, None

    soup = BeautifulSoup(r.text, "html.parser")
    m_id = re.search(r"/t/(\d+)", badnews_url)
    post_id = m_id.group(1) if m_id else None

    video_el = None
    if post_id:
        video_el = soup.find("video", attrs={"data-id": post_id})
    if not video_el:
        video_el = soup.find("video", class_="my-videos") or soup.find("video")

    stream_url = None
    thumb_url = None

    if video_el:
        stream_url = video_el.get("data-source") or video_el.get("src")
        if not stream_url:
            src_tag = video_el.find("source")
            if src_tag:
                stream_url = src_tag.get("src") or src_tag.get("data-src")
        thumb_url = video_el.get("poster")

    if not stream_url:
        m_m3u8 = re.search(r"(https?://[^\s\"'<>]+?\.m3u8(?:\?[^\s\"'<>]*)?)", r.text, re.IGNORECASE)
        if m_m3u8:
            stream_url = m_m3u8.group(1)
        else:
            m_mp4 = re.search(r"(https?://[^\s\"'<>]+?\.mp4(?:\?[^\s\"'<>]*)?)", r.text, re.IGNORECASE)
            if m_mp4:
                stream_url = m_mp4.group(1)

    if not thumb_url:
        og_img = soup.find("meta", property="og:image") or soup.find("meta", attrs={"name": "twitter:image:src"})
        if og_img:
            thumb_url = og_img.get("content")

    og_title = soup.find("meta", property="og:title")
    raw_title = og_title.get("content", "").strip() if og_title else (soup.title.get_text(strip=True) if soup.title else "")
    vi_title = translate_to_vietnamese(raw_title) if raw_title else "Video Hot Cập Nhật"

    if stream_url:
        print(f"  [+] Bóc tách thành công từ bad.news ({badnews_url}):")
        print(f"      -> Tiêu đề gốc : {raw_title}")
        print(f"      -> Dịch TV     : {vi_title}")
        print(f"      -> Stream      : {stream_url}")
        if thumb_url:
            print(f"      -> Poster      : {thumb_url}")
    return stream_url, thumb_url, vi_title


def parse_priority_sheet_row(col_a: str, col_b: str) -> Dict[str, str]:
    """
    Phân loại link và chọn nhánh xử lý phù hợp cho từng dòng trên Google Sheet:
    - Loại 'viet69' : Link bài viết Viet69 (lấy tiêu đề tiếng Việt từ Viet69, cắt 2s đen đầu video).
    - Loại 'badnews': Link bài viết https://bad.news/t/... (bóc tách m3u8/mp4 + poster, tự động dịch tiêu đề sang tiếng Việt, không cắt 2s đầu).
    - Loại 'stream' : Link stream trực tiếp (.m3u8 / .mp4 / HTML).
    """
    import html as html_lib
    col_a = (col_a or "").strip()
    col_b_raw = html_lib.unescape((col_b or "").strip())

    viet69_url = ""
    badnews_url = ""
    custom_title = ""
    link_type = "stream"

    if col_a:
        bn_in_a = re.search(r"(https?://(?:www\.)?bad\.news/[^\s\"'<>]+|(?:www\.)?bad\.news/[^\s\"'<>]+)", col_a, re.IGNORECASE)
        if bn_in_a:
            badnews_url = bn_in_a.group(1)
            if not badnews_url.lower().startswith("http"):
                badnews_url = "https://" + badnews_url.lstrip("/")
            link_type = "badnews"
        else:
            url_match = re.search(r"(https?://[^\s\"'<>]+|(?:www\.)?viet69\.[a-z]+/[^\s\"'<>]+)", col_a, re.IGNORECASE)
            if url_match and "viet69." in url_match.group(1).lower():
                raw_u = url_match.group(1)
                if not raw_u.lower().startswith("http"):
                    raw_u = "https://" + raw_u.lstrip("/")
                if not raw_u.endswith("/"):
                    raw_u += "/"
                viet69_url = raw_u
                link_type = "viet69"
            elif url_match:
                raw_u = url_match.group(1)
                if not raw_u.lower().startswith("http"):
                    raw_u = "https://" + raw_u.lstrip("/")
                viet69_url = raw_u
            else:
                custom_title = translate_to_vietnamese(col_a)

    stream_url = ""
    thumb_url = ""

    if col_b_raw:
        bn_match = re.search(r"(https?://(?:www\.)?bad\.news/[^\s\"'<>]+|(?:www\.)?bad\.news/[^\s\"'<>]+)", col_b_raw, re.IGNORECASE)
        if bn_match and not viet69_url:
            badnews_url = bn_match.group(1)
            if not badnews_url.lower().startswith("http"):
                badnews_url = "https://" + badnews_url.lstrip("/")
            link_type = "badnews"
        else:
            m_stream = re.search(r"(https?://[^\s\"'<>]+?\.(?:m3u8|mp4)(?:\?[^\s\"'<>]*)?)", col_b_raw, re.IGNORECASE)
            if m_stream:
                stream_url = m_stream.group(1)
            elif col_b_raw.lower().startswith("http") and not re.search(r"\.(?:jpg|jpeg|png|webp)(?:\?|$)", col_b_raw, re.IGNORECASE):
                stream_url = col_b_raw.split()[0]

            m_thumb = re.search(r"(https?://[^\s\"'<>]+?\.(?:jpg|jpeg|png|webp)(?:\?[^\s\"'<>]*)?)", col_b_raw, re.IGNORECASE)
            if m_thumb:
                thumb_url = m_thumb.group(1)

    if link_type == "badnews" and badnews_url:
        bn_stream, bn_thumb, bn_title = extract_badnews_details(badnews_url)
        if bn_stream and not stream_url:
            stream_url = bn_stream
        if bn_thumb and not thumb_url:
            thumb_url = bn_thumb
        if bn_title and not custom_title:
            custom_title = bn_title

    fallback_title = custom_title
    if not fallback_title and viet69_url:
        slug_part = viet69_url.rstrip("/").split("/")[-1]
        fallback_title = slug_part.replace("-", " ").capitalize() or "Video Hot Cập Nhật"
    if not fallback_title:
        fallback_title = "Video Hot Cập Nhật"

    return {
        "link_type": link_type,
        "viet69_url": viet69_url,
        "badnews_url": badnews_url,
        "stream_url": stream_url,
        "thumb_url": thumb_url,
        "title": fallback_title
    }


def process_priority_sheet_queue(
    args,
    bot_class,
    wm: Optional[str],
    history: Dict[str, Any],
    posted_urls: Set[str],
    posted_titles: Set[str],
    posted_hashes: Set[str],
    month_counts: Dict[str, int]
) -> Tuple[int, int]:
    """
    Quét Google Sheet ưu tiên trước tiên.
    Lấy các dòng chưa có trạng thái 'Đã lấy', xử lý đăng lên Xamvn và đánh dấu 'Đã lấy' ngay lập tức.
    Trả về: (uploaded_count, posted_count)
    """
    print("\n" + "=" * 70)
    print(f">>> BƯỚC 1: KIỂM TRA GOOGLE SHEET ƯU TIÊN ({PRIORITY_SHEET_ID}) <<<")
    print("=" * 70)

    ws = get_priority_sheet_worksheet(PRIORITY_SHEET_ID, PRIORITY_SHEET_TAB)
    if not ws:
        print("[i] Bỏ qua bước kiểm tra Google Sheet ưu tiên (không kết nối được Sheet).")
        return 0, 0

    try:
        rows = ws.get_all_values()
    except Exception as e:
        print(f"[-] Lỗi khi đọc dữ liệu từ Google Sheet: {e}")
        return 0, 0

    if len(rows) <= 1:
        print("[i] Google Sheet ưu tiên hiện chưa có dữ liệu hàng đợi nào.")
        return 0, 0

    uploaded_count = 0
    posted_count = 0
    now_month = datetime.now().strftime("%Y/%m")

    for row_idx in range(2, len(rows) + 1):
        if args.max_posts_per_run and posted_count >= args.max_posts_per_run:
            print(f"[+] Đã xử lý đủ {posted_count}/{args.max_posts_per_run} video từ Google Sheet ưu tiên cho lần chạy này.")
            break

        row = rows[row_idx - 1]
        col_a = (row[0] if len(row) > 0 else "").strip()
        col_b = (row[1] if len(row) > 1 else "").strip()
        col_c = (row[2] if len(row) > 2 else "").strip()

        if not col_a and not col_b:
            continue

        status_lower = col_c.lower()
        if any(k in status_lower for k in ["đã lấy", "da lay", "done", "ok", "skip", "bỏ qua", "lỗi"]):
            continue

        parsed = parse_priority_sheet_row(col_a, col_b)
        v_url = parsed.get("badnews_url") or parsed["viet69_url"] or parsed["stream_url"]
        if not v_url:
            continue

        print(f"\n--- [Google Sheet Dòng #{row_idx}] Phân loại: [{parsed.get('link_type', 'stream').upper()}] ---")
        print(f"    Link nguồn    : {v_url}")
        print(f"    Tiêu đề       : {parsed['title']}")
        print(f"    Stream M3U8   : {parsed['stream_url'] or '(Tự bóc tách khi tải)'}")
        if parsed["thumb_url"]:
            print(f"    Ảnh Poster    : {parsed['thumb_url']}")

        clean_v_url = v_url.rstrip("/")
        clean_stream = parsed["stream_url"].rstrip("/") if parsed["stream_url"] else ""

        if (clean_v_url and clean_v_url in posted_urls) or (clean_stream and clean_stream in posted_urls):
            print(f"    [i] Video ở dòng #{row_idx} đã được đăng trước đó. Cập nhật trạng thái 'Đã lấy' trên Sheet...")
            try:
                ws.update(range_name=f"C{row_idx}", values=[["Đã lấy"]])
            except Exception as e:
                print(f"    [!] Lỗi cập nhật Sheet dòng #{row_idx}: {e}")
            continue

        v_item = {
            "month": now_month,
            "url": parsed["viet69_url"] or parsed["stream_url"],
            "stream_url": parsed["stream_url"],
            "thumb_url": parsed["thumb_url"],
            "title": parsed["title"],
            "views": "Hot Queue"
        }

        record = ensure_video_on_zpic(v_item, watermark=wm, trim_start=args.trim_start)
        if not record:
            print(f"    [-] Không thể tải/upload video dòng #{row_idx}. Đánh dấu lỗi để không kẹt hàng đợi.")
            try:
                ws.update(range_name=f"C{row_idx}", values=[["Lỗi stream"]])
            except Exception:
                pass
            continue

        zpic_info = record["zpic"]
        direct_mp4 = zpic_info["direct_mp4"]
        vid_hash = zpic_info["name"]
        bbcode = zpic_info["bbcode_full"]
        video_desc = record.get("description", "") or v_item.get("description", "")
        final_title = v_item["title"]

        uploaded_count += 1
        print(f"    [+] BBCode chuẩn bị đăng:\n{bbcode}")

        if args.post and bot_class:
            thumb_link = v_item.get("thumb_url") or zpic_info.get("frame_url", "")
            ok, post_id = post_to_xamvn(
                bot_class=bot_class,
                username=args.username,
                password=args.password,
                thread_target=args.thread,
                title=final_title,
                bbcode=bbcode,
                thumb_url=thumb_link,
                base_url=args.base_url
            )
            if ok:
                posted_count += 1
                month_counts[now_month] = month_counts.get(now_month, 0) + 1
                post_link = f"https://xamvn.yachts/posts/{post_id}/" if post_id else args.thread
                now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

                # Đánh dấu Đã lấy ngay lập tức trên Google Sheet
                try:
                    final_stream_col = record.get("source_stream") or col_b
                    ws.update(
                        range_name=f"B{row_idx}:F{row_idx}",
                        values=[[final_stream_col, "Đã lấy", post_link, direct_mp4, now_str]]
                    )
                    print(f"    [+] Đã đánh dấu 'Đã lấy' vào dòng #{row_idx} trên Google Sheet!")
                except Exception as e:
                    print(f"    [!] Cảnh báo không ghi được trạng thái vào Google Sheet dòng #{row_idx}: {e}")

                history_item = {
                    "month": now_month,
                    "title": final_title,
                    "description": video_desc,
                    "views": v_item["views"],
                    "duration": zpic_info.get("duration", 0),
                    "source_post": v_item["url"],
                    "source_stream": parsed["stream_url"] or record.get("source_stream", ""),
                    "zpic_mp4": direct_mp4,
                    "frame_url": zpic_info.get("frame_url", ""),
                    "zpic_viewer": zpic_info.get("viewer_url", ""),
                    "post_id": post_id,
                    "posted_at": datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ")
                }
                history["posted"].append(history_item)

                if clean_v_url:
                    posted_urls.add(clean_v_url)
                if clean_stream:
                    posted_urls.add(clean_stream)
                posted_titles.add(final_title.strip().lower())
                posted_hashes.add(vid_hash)

                save_posted_history(history)
                sync_to_xamvid_web(history)
                print(f"    [SUCCESS] Đăng bài từ Google Sheet thành công (Post ID: {post_id})!")

                if args.max_posts_per_run and posted_count >= args.max_posts_per_run:
                    break
                print(f"    ⏳ Tạm nghỉ {args.delay_between_posts}s chống flood forum...")
                time.sleep(args.delay_between_posts)
            else:
                print(f"    [-] Đăng bài lên Xamvn thất bại cho dòng #{row_idx}!")
        else:
            # Chế độ chỉ upload không post Xamvn
            now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            try:
                ws.update(
                    range_name=f"C{row_idx}:F{row_idx}",
                    values=[["Đã lấy (Zpic)", "", direct_mp4, now_str]]
                )
            except Exception:
                pass

    if posted_count == 0:
        print("[i] Không còn link nào chờ xử lý trong Google Sheet ưu tiên. Chuyển sang quét Top Viet69 hàng tháng...")

    return uploaded_count, posted_count


def post_to_xamvn(
    bot_class,
    username: str,
    password: str,
    thread_target: str,
    title: str,
    bbcode: str,
    thumb_url: str = "",
    description: str = "",
    base_url: str = "https://xamvn.yachts"
) -> Tuple[bool, Optional[str]]:
    """
    Đăng bài bình luận video lên Xamvn theo thứ tự:
    1. Thumbnail [IMG]{thumb_url}[/IMG]
    2. Video embed [video]...[/video]
    3. Text tiêu đề in đậm [B][SIZE=4]{title}[/SIZE][/B]
    """
    bot = bot_class(base_url=base_url, use_cache=True, verbose=True)
    if not bot.login(username, password):
        print(f"[-] Đăng nhập tài khoản [{username}] thất bại!")
        return False, None

    parts = []
    if bbcode:
        parts.append(bbcode)
    if title:
        parts.append(f"[B][SIZE=4]{title}[/SIZE][/B]")

    full_message = "\n\n".join(parts)

    print(f"[*] Đang gửi bài vào thread: {thread_target}...")
    success, post_id = bot.post_reply(thread_target, full_message)
    return success, post_id


def generate_backwards_months(count: int = 12) -> List[str]:
    """
    Tạo danh sách các tháng lùi dần từ tháng hiện tại về quá khứ.
    Bao gồm cả tháng hiện tại (ví dụ: 2026/10) và lùi dần về 2026/09, 2026/08...
    """
    now = datetime.now()
    year = now.year
    month = now.month

    months = []
    for _ in range(count):
        months.append(f"{year:04d}/{month:02d}")
        month -= 1
        if month <= 0:
            month = 12
            year -= 1
    return months


def main():
    parser = argparse.ArgumentParser(description="Viet69 Monthly Top Backwards -> Zpic -> Xamvn & XamVid Web")
    parser.add_argument("--months", help="Danh sách các tháng cần xử lý (ví dụ: 2026/09,2026/08)")
    parser.add_argument("--past-months", type=int, default=12, help="Số tháng lùi về quá khứ cần quét (mặc định: 12)")
    parser.add_argument("--limit-per-month", type=int, default=20, help="Số video tối đa mỗi tháng (mặc định: 20)")
    parser.add_argument("--max-posts-per-run", type=int, default=1, help="Số video tối đa đăng trong một lần chạy (mặc định: 1)")
    parser.add_argument("--trim-start", type=float, default=2.0, help="Số giây đầu cắt bỏ để tránh đen màn hình (mặc định: 2.0s)")
    parser.add_argument("--watermark", default="xamvid.netlify.app", help="Chữ đóng dấu bản quyền")
    parser.add_argument("--no-watermark", action="store_true", help="Không đóng dấu")
    parser.add_argument("--post", action="store_true", help="Tự động post lên Xamvn sau khi upload Zpic")
    parser.add_argument("-u", "--username", default=os.getenv("XAMVN_USERNAME", "cudai2m"), help="Tài khoản Xamvn (mặc định: cudai2m)")
    parser.add_argument("-p", "--password", default=os.getenv("XAMVN_PASSWORD", "lehunghup"), help="Mật khẩu Xamvn")
    default_thread = os.getenv("XAMVN_THREAD")
    if not default_thread or "307074" in default_thread:
        default_thread = "https://xamvn.yachts/threads/311300/"
    parser.add_argument("-t", "--thread", default=default_thread, help="Thread Xamvn")
    parser.add_argument("--base-url", default="https://xamvn.yachts", help="Base URL Xamvn")
    parser.add_argument("--delay-between-posts", type=int, default=25, help="Giây chờ giữa các lần post tránh flood (mặc định: 25)")
    args = parser.parse_args()

    # Nạp lịch sử đăng bài
    history = load_posted_history()
    posted_urls: Set[str] = set()
    posted_titles: Set[str] = set()
    posted_hashes: Set[str] = set()
    month_counts: Dict[str, int] = {}

    for item in history.get("posted", []):
        m = item.get("month", "")
        if m:
            month_counts[m] = month_counts.get(m, 0) + 1
        if item.get("source_post"):
            clean_u = item["source_post"].rstrip("/")
            posted_urls.add(clean_u)
            posted_urls.add(clean_u.split("/")[-1])
        if item.get("source_stream"):
            posted_urls.add(item["source_stream"].rstrip("/"))
        if item.get("title"):
            posted_titles.add(item["title"].strip().lower())
        if item.get("zpic_mp4"):
            match = re.search(r'/([^/.]+)\.mp4', item["zpic_mp4"])
            if match:
                posted_hashes.add(match.group(1))

    # Quét trực tiếp các video đã có trên thread forum để chống trùng tuyệt đối
    if args.post:
        thread_hashes = fetch_thread_posted_hashes(args.thread, max_pages=6)
        posted_hashes.update(thread_hashes)

    # Xác định danh sách tháng theo thứ tự ưu tiên: tháng trước gần nhất, lùi dần về quá khứ
    all_months = [m.strip() for m in args.months.split(",") if m.strip()] if args.months else generate_backwards_months(args.past_months)

    print("=" * 70)
    print("🎬 VIET69 MONTHLY TOP BACKWARDS TO ZPIC, XAMVN & XAMVID WEB")
    print(f"Sheet Ưu Tiên   : https://docs.google.com/spreadsheets/d/{PRIORITY_SHEET_ID}/edit")
    print(f"Danh sách tháng : {all_months}")
    print(f"Số video/tháng  : tối đa {args.limit_per_month}")
    print(f"Số video/lần chạy: tối đa {args.max_posts_per_run}")
    print(f"Cắt màn hình đen: {args.trim_start}s đầu")
    print(f"Thumbnail Prevw : Cắt chuẩn size video (lấy ở giữa), gắn 1s vào đầu video")
    print(f"Nội dung comment: Bao gồm tiêu đề [B][SIZE=4] + mã nhúng BBCode (đã bỏ mô tả rác)")
    print(f"Đăng bài Xamvn  : {args.post} (Nick: {args.username}, Thread: {args.thread})")
    print("=" * 70)

    wm = None if args.no_watermark else args.watermark

    bot_class = None
    if args.post:
        try:
            from xamvn_bot import XamvnBot
            bot_class = XamvnBot
        except ImportError:
            try:
                from scripts.xamvn_bot import XamvnBot
                bot_class = XamvnBot
            except ImportError:
                print("[-] Không tìm thấy xamvn_bot.py! Không thể post.")
                args.post = False

    total_uploaded = 0
    total_posted = 0

    # BƯỚC 1: Ưu tiên xử lý danh sách link từ Google Sheet trước tiên
    sheet_up, sheet_post = process_priority_sheet_queue(
        args=args,
        bot_class=bot_class,
        wm=wm,
        history=history,
        posted_urls=posted_urls,
        posted_titles=posted_titles,
        posted_hashes=posted_hashes,
        month_counts=month_counts
    )
    total_uploaded += sheet_up
    total_posted += sheet_post

    # BƯỚC 2: Chỉ khi Google Sheet ưu tiên đã hết link (hoặc chưa đủ max_posts_per_run) mới quét Top Viet69 theo tháng
    for m in all_months:
        if args.max_posts_per_run and total_posted >= args.max_posts_per_run:
            print(f"\n[+] Đã đăng đủ giới hạn {args.max_posts_per_run} video cho lần chạy hôm nay. Dừng quy trình.")
            break

        current_count = month_counts.get(m, 0)
        print(f"\n=======================================================")
        print(f">>> KIỂM TRA THÁNG: {m} (Hiện có: {current_count}/{args.limit_per_month} video) <<<")
        print(f"=======================================================")

        if current_count >= args.limit_per_month:
            print(f"[i] Tháng {m} đã lấy đủ {current_count}/{args.limit_per_month} video. BỎ QUA chuyển sang tháng cũ hơn.")
            continue

        needed = args.limit_per_month - current_count
        print(f"[*] Tháng {m} cần lấy thêm tối đa {needed} video.")
        top_videos = fetch_monthly_top_videos(m, limit=args.limit_per_month)
        if not top_videos:
            print(f"[-] Không tìm thấy video nào cho tháng {m}. Chuyển sang tháng tiếp theo...")
            continue

        for idx, v in enumerate(top_videos, 1):
            if args.max_posts_per_run and total_posted >= args.max_posts_per_run:
                print(f"[+] Đã đạt giới hạn {args.max_posts_per_run} video cho lần chạy này. Dừng tháng này.")
                break
            if current_count >= args.limit_per_month:
                print(f"[+] Tháng {m} đã đạt mốc tối đa {args.limit_per_month} video! Dừng tháng này.")
                break

            clean_v_url = v["url"].rstrip("/")
            slug = clean_v_url.split("/")[-1]
            title_lower = v["title"].strip().lower()

            print(f"\n--- [Tháng {m}] Video #{idx}/{len(top_videos)}: {v['title']} ({v['views']}) ---")
            print(f"    Link gốc: {v['url']}")

            # Kiểm tra trùng lặp theo link hoặc tiêu đề
            if clean_v_url in posted_urls or slug in posted_urls or title_lower in posted_titles:
                print(f"    [i] BỎ QUA: Video này đã đăng lên Xamvn trước đó.")
                continue

            # Tải, cắt 2s đen, gắn thumbnail preview và upload Zpic
            record = ensure_video_on_zpic(v, watermark=wm, trim_start=args.trim_start)
            if not record:
                print(f"    [-] Lỗi không upload được video này lên Zpic.")
                continue

            zpic_info = record["zpic"]
            direct_mp4 = zpic_info["direct_mp4"]
            vid_hash = zpic_info["name"]
            bbcode = zpic_info["bbcode_full"]
            video_desc = record.get("description", "") or v.get("description", "")

            # Kiểm tra xem mã hash Zpic này đã có trên forum chưa
            if vid_hash in posted_hashes:
                print(f"    [i] BỎ QUA: Mã video Zpic ({vid_hash}) đã tồn tại trên thread Xamvn.")
                posted_urls.add(clean_v_url)
                posted_urls.add(slug)
                continue

            total_uploaded += 1
            print(f"    [+] BBCode chuẩn bị đăng:\n{bbcode}")

            # Đăng lên Xamvn theo thứ tự: Thumbnail -> Video -> Text
            thumb_link = v.get("thumb_url") or zpic_info.get("frame_url", "")
            if args.post and bot_class:
                ok, post_id = post_to_xamvn(
                    bot_class=bot_class,
                    username=args.username,
                    password=args.password,
                    thread_target=args.thread,
                    title=v["title"],
                    bbcode=bbcode,
                    thumb_url=thumb_link,
                    base_url=args.base_url
                )
                if ok:
                    total_posted += 1
                    current_count += 1
                    month_counts[m] = current_count

                    history_item = {
                        "month": m,
                        "title": v["title"],
                        "description": video_desc,
                        "views": v["views"],
                        "duration": zpic_info.get("duration", 0),
                        "source_post": v["url"],
                        "zpic_mp4": direct_mp4,
                        "frame_url": zpic_info.get("frame_url", ""),
                        "zpic_viewer": zpic_info.get("viewer_url", ""),
                        "post_id": post_id,
                        "posted_at": datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ")
                    }
                    history["posted"].append(history_item)

                    posted_urls.add(clean_v_url)
                    posted_urls.add(slug)
                    posted_titles.add(title_lower)
                    posted_hashes.add(vid_hash)

                    save_posted_history(history)
                    sync_to_xamvid_web(history)
                    print(f"    [SUCCESS] Đăng bài thành công (Post ID: {post_id}). Tiến độ tháng {m}: {current_count}/{args.limit_per_month} video.")

                    # Tạm dừng để tránh anti-flood forum
                    if args.max_posts_per_run and total_posted >= args.max_posts_per_run:
                        print(f"    [+] Đã hoàn thành đủ {total_posted}/{args.max_posts_per_run} video của lần chạy hôm nay. Kết thúc.")
                        break

                    print(f"    ⏳ Tạm nghỉ {args.delay_between_posts}s chống flood forum...")
                    time.sleep(args.delay_between_posts)
                else:
                    print(f"    [-] Đăng bài lên Xamvn thất bại!")

    # Đồng bộ cuối cùng
    sync_to_xamvid_web(history)

    print("\n" + "=" * 70)
    print(f"HOÀN TẤT TOÀN BỘ QUY TRÌNH!")
    print(f"Tổng upload mới: {total_uploaded} | Tổng đăng bài mới: {total_posted}")
    print("=" * 70)


if __name__ == "__main__":
    main()
