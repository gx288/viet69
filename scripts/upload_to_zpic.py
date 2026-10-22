import os
import re
import sys
import time
import json
import argparse
import subprocess
from urllib.parse import urlparse

# Force UTF-8 output on Windows
if sys.stdout.encoding != 'utf-8':
    try:
        sys.stdout.reconfigure(encoding='utf-8')
    except Exception:
        pass

DATABASE_FILE = "data/viet69_database.json"
QUEUE_FILE = "data/zpic_queue.txt"
OUTPUT_JSON = "data/zpic_uploaded.json"
OUTPUT_BBCODE = "data/zpic_bbcode.txt"
TEMP_DIR = "temp_zpic"

# Fallback IP for zpic.org in case local ISP blocks DNS (GitHub Actions resolves normally)
ZPIC_HOST = "zpic.org"
ZPIC_FALLBACK_IP = "45.145.154.29"
USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"


def resolve_zpic_args():
    """Return curl arguments to bypass ISP DNS block if needed."""
    try:
        import socket
        ip = socket.gethostbyname(ZPIC_HOST)
        if ip and ip not in ("127.0.0.1", "0.0.0.0"):
            return ["-k"]
    except Exception:
        pass
    return ["-k", "--resolve", f"{ZPIC_HOST}:443:{ZPIC_FALLBACK_IP}"]


def get_zpic_session(temp_dir):
    """Fetch zpic.org homepage to get session cookies and Chevereto auth_token."""
    cookies_path = os.path.join(temp_dir, "zpic_cookies.txt")
    html_path = os.path.join(temp_dir, "zpic_home.html")
    extra_args = resolve_zpic_args()

    for p in [html_path, cookies_path]:
        if os.path.exists(p):
            try: os.remove(p)
            except Exception: pass

    cmd = [
        "curl", "-sS", "-L",
        *extra_args,
        "-c", cookies_path,
        "-A", USER_AGENT,
        f"https://{ZPIC_HOST}/",
        "-o", html_path
    ]
    # On Windows use curl.exe explicitly
    if os.name == "nt":
        cmd[0] = "curl.exe"

    subprocess.run(cmd, capture_output=True)
    if not os.path.exists(html_path):
        raise RuntimeError("Không thể kết nối tới zpic.org để lấy auth_token.")

    html = open(html_path, "r", encoding="utf-8", errors="ignore").read()
    m = re.search(r'auth_token\s*=\s*"([^"]+)"', html)
    if not m:
        raise RuntimeError("Không tìm thấy auth_token trong trang chủ zpic.org.")
    return m.group(1), cookies_path, extra_args


def upload_mp4_to_zpic(mp4_path, title, temp_dir):
    """Upload MP4 file directly to zpic.org/json API without Selenium."""
    auth_token, cookies_path, extra_args = get_zpic_session(temp_dir)
    ts = str(int(time.time() * 1000))

    curl_bin = "curl.exe" if os.name == "nt" else "curl"
    cmd = [
        curl_bin, "-sS",
        "--retry", "3",
        "--retry-delay", "2",
        "--max-time", "600",
        *extra_args,
        "-b", cookies_path,
        "-A", USER_AGENT,
        "-H", f"Referer: https://{ZPIC_HOST}/",
        "-H", f"Origin: https://{ZPIC_HOST}",
        "-H", "Accept: application/json",
        "-F", f"source=@{mp4_path};type=video/mp4",
        "-F", "type=file",
        "-F", "action=upload",
        "-F", f"timestamp={ts}",
        "-F", f"auth_token={auth_token}",
        "-F", "nsfw=1",
        f"https://{ZPIC_HOST}/json"
    ]

    print(f"  [*] Đang upload lên zpic.org ({os.path.getsize(mp4_path) / (1024*1024):.2f} MB)...")
    res = subprocess.run(cmd, capture_output=True, text=True, encoding="utf-8", errors="replace")
    if res.returncode != 0 or not res.stdout.strip():
        raise RuntimeError(f"Lỗi curl khi upload: {res.stderr}")

    try:
        data = json.loads(res.stdout)
    except Exception:
        raise RuntimeError(f"Phản hồi không phải JSON hợp lệ: {res.stdout[:500]}")

    if data.get("status_code") != 200 or "image" not in data:
        raise RuntimeError(f"Upload thất bại: {data}")

    img = data["image"]
    name = img.get("name")
    id_encoded = img.get("id_encoded")
    # f.zzpi.cc is the standard embed domain used by zpic's BBCode generator
    direct_mp4_f = f"https://f.zzpi.cc/{name}.mp4"
    direct_mp4_z = img.get("url") or f"https://z.zzpi.cc/{name}.mp4"
    viewer_url = img.get("url_short") or f"https://zpic.org/view/{id_encoded}"
    frame_url = f"https://f.zzpi.cc/{name}.fr.jpeg"
    thumb_url = f"https://f.zzpi.cc/{name}.th.jpeg"

    return {
        "name": name,
        "id_encoded": id_encoded,
        "size_formatted": img.get("size_formatted"),
        "duration": img.get("duration"),
        "width": img.get("width"),
        "height": img.get("height"),
        "direct_mp4": direct_mp4_f,
        "direct_mp4_alt": direct_mp4_z,
        "viewer_url": viewer_url,
        "frame_url": frame_url,
        "thumb_url": thumb_url,
        "delete_url": img.get("delete_url"),
        "bbcode_full": f"[video]{direct_mp4_f}[/video][hr][/hr]{viewer_url}#",
        "bbcode_clean": f"[video]{direct_mp4_f}[/video]",
        "bbcode_thumb": f"[url={viewer_url}][img]{frame_url}[/img][/url]",
        "html_embed": f'<video src="{direct_mp4_f}" controls poster="{frame_url}"></video>'
    }


def extract_live_stream_from_post(post_url):
    """Extract live stream URL directly from a viet69 post URL via AJAX & Player API."""
    from curl_cffi import requests as cf_requests
    from bs4 import BeautifulSoup

    headers = {
        "User-Agent": USER_AGENT,
        "Referer": "https://viet69.be/",
        "X-Requested-With": "XMLHttpRequest"
    }
    session = cf_requests.Session()
    r = session.get(post_url, headers=headers, impersonate="chrome120", timeout=15)
    if r.status_code != 200:
        raise RuntimeError(f"Không mở được trang bài viết ({r.status_code}): {post_url}")
    soup = BeautifulSoup(r.text, "html.parser")
    title_el = soup.find("h1") or soup.find("h2")
    if title_el:
        title = title_el.get_text(strip=True)
    elif soup.title:
        title = soup.title.string.split(" - ")[0].strip()
    else:
        title = "video"

    buttons = soup.find_all("button", class_="video2-btn")
    for btn in buttons:
        btn_text = btn.get_text(strip=True)
        movie_id = btn.get("data-video")
        video_type = btn.get("data-type")
        if not movie_id:
            continue

        # Prefer Server #2 (HLS CDN)
        if "Server #2" in btn_text or len(buttons) == 1:
            ajax_url = "https://viet69.be/get.xvideo.php" if video_type == "10" else "https://viet69.be/get.video.php"
            payload = f"movie_id={movie_id}&type={video_type}&index=1"
            ajax_headers = headers.copy()
            ajax_headers["Content-Type"] = "application/x-www-form-urlencoded; charset=UTF-8"
            ajax_headers["Referer"] = post_url
            res = session.post(ajax_url, data=payload, headers=ajax_headers, impersonate="chrome120", timeout=15)
            if res.status_code == 200:
                m = re.search(r'src=["\'](https?://[^"\']+)["\']', res.text)
                if m:
                    embed_url = m.group(1)
                    parsed = urlparse(embed_url)
                    vid = parsed.path.strip("/").split("/")[-1]
                    api_url = f"https://{parsed.netloc}/api/get-video?id={vid}&counter=0&tried_ids="
                    api_res = session.get(api_url, headers={"User-Agent": USER_AGENT, "Referer": embed_url}, impersonate="chrome120", timeout=15)
                    if api_res.status_code == 200:
                        vdata = api_res.json()
                        urls = vdata.get("urls") or ([vdata.get("url")] if vdata.get("url") else [])
                        if urls:
                            return urls[0], title

    raise RuntimeError(f"Không bóc tách được stream URL từ bài viết: {post_url}")


def get_font_file():
    """Return a clean bold sans-serif font path for Windows or Linux (GitHub Actions)."""
    candidates = [
        "C:/Windows/Fonts/arialbd.ttf",
        "C:/Windows/Fonts/arial.ttf",
        "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf",
        "/usr/share/fonts/truetype/liberation/LiberationSans-Bold.ttf",
    ]
    for c in candidates:
        if os.path.exists(c):
            # Escape colon for FFmpeg filter syntax on Windows
            return c.replace(":", "\\:")
    return None


def get_hls_demuxer_args():
    """Detect and return supported HLS demuxer options for the current FFmpeg version."""
    args = []
    try:
        res = subprocess.run(["ffmpeg", "-h", "demuxer=hls"], capture_output=True, text=True, errors="ignore")
        help_text = res.stdout or ""
        if "allowed_extensions" in help_text:
            args.extend(["-allowed_extensions", "ALL"])
        if "allowed_segment_extensions" in help_text:
            args.extend(["-allowed_segment_extensions", "ALL"])
        if "extension_picky" in help_text:
            args.extend(["-extension_picky", "0"])
    except Exception:
        args.extend(["-allowed_extensions", "ALL"])
    return args


def get_stream_duration(stream_url, referer="https://emb.cd-vs.com/"):
    """Lấy chính xác thời lượng (giây) của luồng .m3u8 / .mp4 hoặc file local."""
    # 1. Thử ffprobe có kèm headers
    try:
        cmd = ["ffprobe", "-v", "error"]
        if str(stream_url).startswith("http"):
            cmd.extend([
                "-headers", f"Referer: {referer}\r\n",
                "-user_agent", USER_AGENT
            ])
        cmd.extend([
            "-show_entries", "format=duration",
            "-of", "default=noprint_wrappers=1:nokey=1",
            stream_url
        ])
        pr = subprocess.run(cmd, capture_output=True, text=True, timeout=12)
        if pr.returncode == 0 and pr.stdout.strip():
            for line in pr.stdout.strip().splitlines():
                try:
                    val = float(line.strip())
                    if val > 0:
                        return val
                except Exception:
                    pass
    except Exception:
        pass

    # 2. Nếu là link .m3u8, thử đọc tổng #EXTINF từ playlist
    if ".m3u8" in str(stream_url).lower() and str(stream_url).startswith("http"):
        try:
            from curl_cffi import requests as cf_requests
            from urllib.parse import urljoin
            s = cf_requests.Session()
            r = s.get(stream_url, headers={"Referer": referer, "User-Agent": USER_AGENT}, impersonate="chrome120", timeout=10)
            if r.status_code == 200:
                text = r.text
                # Nếu là master playlist, lấy sub-playlist đầu tiên
                if "#EXT-X-STREAM-INF" in text:
                    for line in text.splitlines():
                        line = line.strip()
                        if line and not line.startswith("#"):
                            sub_url = urljoin(stream_url, line)
                            r2 = s.get(sub_url, headers={"Referer": referer, "User-Agent": USER_AGENT}, impersonate="chrome120", timeout=10)
                            if r2.status_code == 200:
                                text = r2.text
                            break
                extinfs = re.findall(r"#EXTINF:([0-9.]+)", text)
                if extinfs:
                    total_dur = sum(float(x) for x in extinfs)
                    if total_dur > 0:
                        return total_dur
        except Exception:
            pass

    return 0.0


def download_stream_to_mp4(stream_url, out_mp4, watermark_text=None, trim_start=2, max_target_mb=190):
    """
    Download .m3u8 or .mp4 stream using FFmpeg (or yt-dlp for Twitter/multi-track HLS)
    with optional center watermark, skip black intro, and strict file size control (< max_target_mb MB for Zpic.org).
    """
    raw_source = stream_url
    temp_ytdlp = None
    referer = "https://x.com/" if "twimg.com" in stream_url.lower() else "https://emb.cd-vs.com/"
    stream_duration = get_stream_duration(stream_url, referer=referer)

    is_long_video = stream_duration > 1000  # > ~16.5 minutes

    # Twitter amplify_video / multi-rendition HLS works best via yt-dlp to merge video + audio tracks
    if "twimg.com" in stream_url.lower():
        temp_ytdlp = out_mp4 + ".ytdlp_raw.mp4"
        if os.path.exists(temp_ytdlp):
            try: os.remove(temp_ytdlp)
            except Exception: pass
        # Nếu video dài > 16 phút, chọn bản <= 360p/480p ngay từ đầu để tải siêu nhanh và nhẹ
        fmt_sel = "bestvideo[height<=360]+bestaudio/best[height<=360]/best" if is_long_video else "bestvideo[height<=720]+bestaudio/best[height<=720]/best"
        print(f"  [*] Phát hiện link Twitter/X (thời lượng: {stream_duration/60:.1f} phút), đang tải qua yt-dlp (-N 16, format={fmt_sel})...")
        yt_cmd = [sys.executable, "-m", "yt_dlp", "--no-check-certificate", "-N", "16", "-f", fmt_sel, "-o", temp_ytdlp, stream_url]
        res_yt = subprocess.run(yt_cmd, capture_output=True, text=True, encoding="utf-8", errors="replace")
        if res_yt.returncode == 0 and os.path.exists(temp_ytdlp) and os.path.getsize(temp_ytdlp) > 1024:
            raw_source = temp_ytdlp
            trim_start = 0
            # Đo lại chính xác thời lượng từ file local vừa tải
            local_dur = get_stream_duration(temp_ytdlp)
            if local_dur > 0:
                stream_duration = local_dur
                is_long_video = stream_duration > 1000

    hls_opts = get_hls_demuxer_args() if raw_source.startswith("http") else []
    cmd = ["ffmpeg"]
    if raw_source.startswith("http"):
        cmd.extend([
            "-headers", f"Referer: {referer}\r\n",
            "-user_agent", USER_AGENT,
            *hls_opts
        ])
    if trim_start and trim_start > 0:
        cmd.extend(["-ss", str(trim_start)])
    cmd.extend(["-i", raw_source])

    # Tính toán giới hạn bitrate tự động để file đầu ra KHÔNG BAO GIỜ vượt quá max_target_mb (mặc định 190 MB, an toàn tuyệt đối dưới mức 300 MB của Zpic)
    effective_dur = max(60.0, stream_duration - (trim_start or 0)) if stream_duration > 0 else 900.0
    audio_kbps = 96 if is_long_video else 128
    # Công thức: (MB * 8192 kilobits) / seconds
    calc_max_v_kbps = int((max_target_mb * 8192) / effective_dur) - audio_kbps
    # Giới hạn trần bitrate hợp lý (tối thiểu 200k cho clip siêu dài >1 tiếng, tối đa 1400k cho clip ngắn)
    max_v_kbps = max(200, min(1400, calc_max_v_kbps))
    buf_kbps = max_v_kbps * 2

    if watermark_text:
        font_path = get_font_file()
        safe_text = watermark_text.replace(":", "\\:").replace("'", "\\'")
        font_opt = f"fontfile='{font_path}':" if font_path else ""
        drawtext = (
            f"drawtext={font_opt}text='{safe_text}':"
            f"fontsize=min(w\\,h)/16:"
            f"fontcolor=white@0.35:"
            f"borderw=1:bordercolor=black@0.25:"
            f"x=(w-text_w)/2:y=(h-text_h)/2"
        )
        # Nếu video dài (>16.5 phút hoặc max_v_kbps < 550k), tự động hạ độ phân giải xuống tối đa 480p để hình vẫn nét ở bitrate thấp và encode nhanh gấp 3 lần
        scale_prefix = "scale=-2:'min(480,ih)'," if (is_long_video or max_v_kbps < 550) else ""
        vf_chain = f"{scale_prefix}{drawtext},format=yuv420p"
        preset_val = "ultrafast" if is_long_video else "superfast"
        crf_val = "28" if is_long_video else "25"

        cmd.extend([
            "-vf", vf_chain,
            "-c:v", "libx264",
            "-preset", preset_val,
            "-crf", crf_val,
            "-maxrate", f"{max_v_kbps}k",
            "-bufsize", f"{buf_kbps}k",
            "-profile:v", "high",
            "-level", "4.0",
            "-x264-params", "stitchable=1",
            "-r", "30",
            "-video_track_timescale", "15360",
            "-c:a", "aac",
            "-ar", "44100",
            "-ac", "2",
            "-b:a", f"{audio_kbps}k",
            "-fs", "250M",
            "-movflags", "+faststart",
            "-y", out_mp4
        ])
        print(f"  [*] Đang tải & đóng dấu watermark '{watermark_text}' ({preset_val}, thời lượng={stream_duration/60:.1f}p, maxrate={max_v_kbps}k, giới hạn <={max_target_mb}MB): {stream_url[:65]}...")
    else:
        cmd.extend([
            "-c", "copy",
            "-bsf:a", "aac_adtstoasc",
            "-fs", "250M",
            "-movflags", "+faststart",
            "-y", out_mp4
        ])
        print(f"  [*] Đang tải luồng video gốc (copy): {stream_url[:80]}...")

    t0 = time.time()
    try:
        res = subprocess.run(cmd, capture_output=True, text=True, encoding="utf-8", errors="replace")
        elapsed = time.time() - t0
        if res.returncode != 0 or not os.path.exists(out_mp4) or os.path.getsize(out_mp4) < 1024:
            raise RuntimeError(f"FFmpeg tải video thất bại:\n{res.stderr[-600:]}")
        size_mb = os.path.getsize(out_mp4) / (1024 * 1024)
        print(f"  [+] Hoàn tất xử lý FFmpeg trong {elapsed:.1f} giây ({size_mb:.2f} MB).")
        return out_mp4
    finally:
        if temp_ytdlp and os.path.exists(temp_ytdlp):
            try: os.remove(temp_ytdlp)
            except Exception: pass


def resolve_target(target, db_by_id, db_by_url):
    """
    Resolve a target entry (Video ID, Post URL, or direct .m3u8/.mp4 URL)
    into (video_id, title, stream_url, post_url).
    """
    target = target.strip()
    # Case 1: Video ID in local database
    if target in db_by_id:
        item = db_by_id[target]
        servers = item.get("servers", {})
        # Prefer Server #2 (HLS) or Download (MP4)
        stream_url = None
        if "Server #2" in servers and servers["Server #2"].get("url"):
            stream_url = servers["Server #2"]["url"]
        elif "Download" in servers and servers["Download"].get("url"):
            stream_url = servers["Download"]["url"]
        else:
            for s in servers.values():
                if s.get("url") and (".m3u8" in s["url"] or ".mp4" in s["url"]):
                    stream_url = s["url"]
                    break
        return item["id"], item.get("title", f"video_{target}"), stream_url, item.get("source_url", "")

    # Case 2: Direct .m3u8 or .mp4 link
    if ".m3u8" in target.lower() or target.lower().endswith(".mp4"):
        return f"custom_{int(time.time())}", "Custom Stream Video", target, ""

    # Case 3: Post URL in database or live web
    norm_target = target.rstrip("/")
    if norm_target in db_by_url:
        item = db_by_url[norm_target]
        return resolve_target(item["id"], db_by_id, db_by_url)

    if target.startswith("http"):
        stream_url, title = extract_live_stream_from_post(target)
        return f"web_{int(time.time())}", title, stream_url, target

    raise RuntimeError(f"Không nhận diện được mục tiêu: {target}")


def main():
    parser = argparse.ArgumentParser(description="Tải video từ Viet69 và tự động upload lên Zpic.org lấy mã nhúng Forum")
    parser.add_argument("--targets", type=str, default="", help="Danh sách ID hoặc Link cách nhau bởi dấu phẩy hoặc khoảng trắng")
    parser.add_argument("--queue-file", type=str, default=QUEUE_FILE, help="Đường dẫn file hàng đợi (mỗi dòng 1 ID hoặc Link)")
    parser.add_argument("--watermark", type=str, default="xamvid.netlify.app", help="Chữ đóng dấu giữa video (mặc định: xamvid.netlify.app)")
    parser.add_argument("--no-watermark", action="store_true", help="Tắt đóng dấu watermark (tải luồng gốc -c copy)")
    parser.add_argument("--trim-start", type=float, default=2.0, help="Cắt bỏ số giây đầu (mặc định: 2.0s để bỏ đoạn đen màn hình)")
    args = parser.parse_args()

    watermark_text = None if args.no_watermark else (args.watermark.strip() or None)

    os.makedirs(TEMP_DIR, exist_ok=True)
    os.makedirs("data", exist_ok=True)

    # Load database if available
    db_by_id = {}
    db_by_url = {}
    if os.path.exists(DATABASE_FILE):
        try:
            db_data = json.load(open(DATABASE_FILE, "r", encoding="utf-8"))
            for item in db_data:
                vid = str(item.get("id", "")).strip()
                if vid:
                    db_by_id[vid] = item
                surl = (item.get("source_url") or "").strip().rstrip("/")
                if surl:
                    db_by_url[surl] = item
        except Exception as e:
            print(f"[!] Cảnh báo: Không đọc được {DATABASE_FILE}: {e}")

    # Collect targets
    raw_targets = []
    if args.targets.strip():
        for part in re.split(r"[,\n\r]+", args.targets):
            if part.strip():
                raw_targets.append(part.strip())
    elif os.path.exists(args.queue_file):
        for line in open(args.queue_file, "r", encoding="utf-8"):
            line = line.strip()
            if line and not line.startswith("#"):
                raw_targets.append(line)

    if not raw_targets:
        print("[!] Không có link hoặc ID nào trong hàng đợi để xử lý.")
        return

    # Load existing uploaded results
    uploaded_history = []
    if os.path.exists(OUTPUT_JSON):
        try:
            uploaded_history = json.load(open(OUTPUT_JSON, "r", encoding="utf-8"))
        except Exception:
            uploaded_history = []

    print(f"=== BẮT ĐẦU XỬ LÝ {len(raw_targets)} VIDEO LÊN ZPIC.ORG (Watermark: {watermark_text or 'TẮT'}) ===")

    new_results = []
    for idx, t in enumerate(raw_targets, 1):
        print(f"\n[{idx}/{len(raw_targets)}] Đang xử lý: {t}")
        mp4_temp = os.path.join(TEMP_DIR, f"video_{idx}_{int(time.time())}.mp4")
        try:
            vid_id, title, stream_url, post_url = resolve_target(t, db_by_id, db_by_url)
            if not stream_url:
                raise RuntimeError(f"Không tìm thấy link stream cho ID {vid_id}")

            print(f"  [+] Tiêu đề: {title}")
            print(f"  [+] Stream URL: {stream_url}")

            # Step 1: Download via FFmpeg (with optional watermark and trim)
            download_stream_to_mp4(stream_url, mp4_temp, watermark_text=watermark_text, trim_start=args.trim_start)

            # Check file size limit (300 MB max on zpic.org)
            size_mb = os.path.getsize(mp4_temp) / (1024 * 1024)
            if size_mb > 298:
                raise RuntimeError(f"Dung lượng file ({size_mb:.1f} MB) vượt quá giới hạn 300MB của zpic.org")

            # Step 2: Upload to zpic.org
            zpic_info = upload_mp4_to_zpic(mp4_temp, title, TEMP_DIR)

            record = {
                "video_id": vid_id,
                "title": title,
                "watermark": watermark_text,
                "source_post": post_url,
                "source_stream": stream_url,
                "uploaded_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                "zpic": zpic_info
            }
            new_results.append(record)
            uploaded_history.insert(0, record)

            print(f"  [SUCCESS] Upload thành công!")
            print(f"    -> Direct MP4 : {zpic_info['direct_mp4']}")
            print(f"    -> Viewer Link: {zpic_info['viewer_url']}")
            print(f"    -> BBCode     : {zpic_info['bbcode_full']}")

        except Exception as e:
            print(f"  [ERROR] Thất bại khi xử lý '{t}': {e}")
        finally:
            if os.path.exists(mp4_temp):
                try:
                    os.remove(mp4_temp)
                except Exception:
                    pass

    # Save updated JSON and BBCode text file
    if new_results:
        with open(OUTPUT_JSON, "w", encoding="utf-8") as f:
            json.dump(uploaded_history, f, ensure_ascii=False, indent=2)

        with open(OUTPUT_BBCODE, "a", encoding="utf-8") as f:
            for r in new_results:
                z = r["zpic"]
                f.write(f"=== [{r['video_id']}] {r['title']} ({r['uploaded_at']}) ===\n")
                f.write(f"BBCode Full   : {z['bbcode_full']}\n")
                f.write(f"BBCode Clean  : {z['bbcode_clean']}\n")
                f.write(f"Direct MP4    : {z['direct_mp4']}\n")
                f.write(f"Frame Poster  : {z['frame_url']}\n")
                f.write(f"Viewer Link   : {z['viewer_url']}\n")
                f.write(f"HTML Embed    : {z['html_embed']}\n\n")

        print(f"\n=== HOÀN TẤT: Đã upload thành công {len(new_results)}/{len(raw_targets)} video ===")
        print(f"  - File JSON  : {OUTPUT_JSON}")
        print(f"  - File BBCode: {OUTPUT_BBCODE}")
    else:
        print("\n[!] Không có video nào được upload thành công.")


if __name__ == "__main__":
    main()
