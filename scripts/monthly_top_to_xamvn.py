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
        post_url = f"https://xamvn.yachts/posts/{post_id}/" if post_id else "https://xamvn.yachts/threads/307074/"

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

        # Trường hợp 1: Base64 direct link (type 19, 20)
        if video_type in ["19", "20"]:
            try:
                pad = len(movie_id) % 4
                if pad:
                    movie_id += '=' * (4 - pad)
                decoded = base64.b64decode(movie_id).decode('utf-8', errors='ignore')
                if decoded:
                    direct_url = f"https://cdn.cd-vs.com/{decoded}"
                    print(f"  [+] Bóc tách thành công từ {btn_text} (Base64): {direct_url}")
                    return direct_url, title, description, thumb_url
            except Exception:
                pass

        # Trường hợp 2: AJAX Endpoint (type 25, 10, v.v.)
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
    """
    uploaded_list = []
    if os.path.exists(OUTPUT_JSON):
        try:
            with open(OUTPUT_JSON, "r", encoding="utf-8") as f:
                uploaded_list = json.load(f)
        except Exception:
            pass

    target_url = video_item["url"].rstrip("/")
    for up in uploaded_list:
        if up.get("source_post", "").rstrip("/") == target_url:
            print(f"[+] Video đã có sẵn trên Zpic: {up['zpic']['direct_mp4']}")
            video_item["description"] = up.get("description", "")
            return up

    os.makedirs(TEMP_DIR, exist_ok=True)
    print(f"[*] Bóc tách luồng HLS & nội dung: {video_item['title']} ({video_item['url']})...")
    try:
        stream_url, real_title, description, thumb_url = extract_post_details(video_item["url"])
    except Exception as e:
        print(f"    [-] Lỗi khi bóc tách luồng ({e}). Bỏ qua video này để thử video tiếp theo...")
        return None

    if not stream_url:
        print(f"    [-] Không tìm thấy luồng stream hợp lệ cho bài viết này. Bỏ qua để thử video tiếp theo...")
        return None

    video_item["title"] = real_title or video_item["title"]
    video_item["description"] = description
    video_item["thumb_url"] = thumb_url

    timestamp = int(time.time())
    raw_mp4 = os.path.join(TEMP_DIR, f"raw_{clean_slug(video_item['month'])}_{timestamp}.mp4")
    final_mp4 = os.path.join(TEMP_DIR, f"final_{clean_slug(video_item['month'])}_{timestamp}.mp4")

    try:
        # Bước 1: Tải và cắt 2s đầu đen + watermark
        download_stream_to_mp4(stream_url, raw_mp4, watermark_text=watermark, trim_start=trim_start)

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
            "trim_start": trim_start,
            "source_post": video_item["url"],
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
