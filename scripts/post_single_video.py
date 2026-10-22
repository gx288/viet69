#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Post Single Video to Zpic & Xamvn Thread
=========================================
Xử lý 1 video chỉ định từ Viet69:
- Cắt 2.0s đen màn hình đầu video (-ss 2.0)
- Đóng dấu watermark xamvid.netlify.app
- Cắt thumbnail preview 1s ở giữa video
- Upload lên Zpic.org
- Đăng trực tiếp vào thread Xamvn chỉ định (Thứ tự: Thumbnail -> Video -> Tiêu đề text)
"""

import os
import sys
import argparse
from typing import Dict, Any

# Force UTF-8 output
try:
    sys.stdout.reconfigure(encoding='utf-8', line_buffering=True)
except Exception:
    pass

CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_DIR = os.path.abspath(os.path.join(CURRENT_DIR, ".."))
if CURRENT_DIR not in sys.path:
    sys.path.insert(0, CURRENT_DIR)
if PROJECT_DIR not in sys.path:
    sys.path.insert(0, PROJECT_DIR)

from monthly_top_to_xamvn import (
    ensure_video_on_zpic,
    post_to_xamvn,
    load_posted_history,
    save_posted_history,
    sync_to_xamvid_web
)
from xamvn_bot import XamvnBot


def main():
    parser = argparse.ArgumentParser(description="Đăng 1 video cụ thể từ Viet69 lên Zpic và post vào thread Xamvn")
    parser.add_argument("--url", required=True, help="Link bài viết Viet69 cần xử lý")
    parser.add_argument("--thread", default="https://xamvn.yachts/threads/311300/", help="Link thread Xamvn cần đăng")
    parser.add_argument("--username", default=os.getenv("XAMVN_USERNAME", "cudai2m"), help="Nick Xamvn")
    parser.add_argument("--password", default=os.getenv("XAMVN_PASSWORD", "lehunghup"), help="Pass Xamvn")
    parser.add_argument("--watermark", default="xamvid.netlify.app", help="Watermark video")
    parser.add_argument("--no-watermark", action="store_true", help="Tắt watermark")
    parser.add_argument("--trim-start", type=float, default=2.0, help="Số giây cắt bỏ đầu video")
    parser.add_argument("--base-url", default="https://xamvn.yachts", help="Base URL forum")
    args = parser.parse_args()

    wm = None if args.no_watermark else args.watermark

    print("=" * 65)
    print("🚀 XỬ LÝ VIDEO ĐƠN LÊN ZPIC & XAMVN")
    print(f"Target URL   : {args.url}")
    print(f"Target Thread: {args.thread}")
    print(f"Watermark    : {wm or 'None'}")
    print(f"Trim Start   : {args.trim_start}s")
    print("Thứ tự đăng  : 1. Thumbnail -> 2. Video Embed -> 3. Text Tiêu đề")
    print("=" * 65)

    video_item = {
        "url": args.url.strip(),
        "title": "Viet69 Video",
        "month": "custom",
        "views": "N/A"
    }

    # 1. Tải luồng, cắt 2s đen, watermark, preview thumbnail và upload Zpic
    print("\n[*] Bước 1: Xử lý video và upload lên Zpic.org...")
    record = ensure_video_on_zpic(video_item, watermark=wm, trim_start=args.trim_start)
    if not record or "zpic" not in record:
        print("[-] Thất bại: Không thể xử lý hoặc upload video lên Zpic.")
        sys.exit(1)

    zpic_info = record["zpic"]
    title = record.get("title", video_item["title"])
    bbcode = zpic_info.get("bbcode_full", "")
    direct_mp4 = zpic_info.get("direct_mp4", "")
    viewer_url = zpic_info.get("viewer_url", "")
    thumb_link = record.get("thumb_url") or zpic_info.get("frame_url", "")

    print(f"[+] Tiêu đề video : {title}")
    print(f"[+] Thumbnail URL : {thumb_link}")
    print(f"[+] Zpic Direct   : {direct_mp4}")
    print(f"[+] Zpic Viewer   : {viewer_url}")
    print(f"[+] BBCode Embed  :\n{bbcode}\n")

    # 2. Đăng bài lên thread Xamvn theo thứ tự: Thumbnail -> Video -> Text
    print(f"[*] Bước 2: Đang đăng bài vào thread: {args.thread}...")
    success, post_id = post_to_xamvn(
        bot_class=XamvnBot,
        username=args.username,
        password=args.password,
        thread_target=args.thread,
        title=title,
        bbcode=bbcode,
        thumb_url=thumb_link,
        base_url=args.base_url
    )

    if not success:
        print("[-] Đăng bài lên Xamvn thất bại!")
        sys.exit(1)

    print(f"\n[SUCCESS] Đăng bài thành công vào thread! Post ID: {post_id}")
    post_link = f"{args.base_url.rstrip('/')}/posts/{post_id}/" if post_id else args.thread
    print(f"Link bài đăng: {post_link}")

    # 3. Cập nhật lịch sử & đồng bộ web
    try:
        from datetime import datetime
        history = load_posted_history()
        history_item = {
            "month": "custom",
            "title": title,
            "description": record.get("description", ""),
            "views": record.get("views", "N/A"),
            "duration": zpic_info.get("duration", 0),
            "source_post": args.url,
            "zpic_mp4": direct_mp4,
            "frame_url": zpic_info.get("frame_url", ""),
            "thumb_url": thumb_link,
            "zpic_viewer": viewer_url,
            "post_id": post_id,
            "posted_at": datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ")
        }
        history["posted"].append(history_item)
        save_posted_history(history)
        sync_to_xamvid_web(history)
        print("[+] Đã đồng bộ video vào lịch sử và dữ liệu web.")
    except Exception as e:
        print(f"[!] Cảnh báo: Lỗi khi đồng bộ dữ liệu web: {e}")

    print("\n" + "=" * 65)
    print("🎉 HOÀN THÀNH!")
    print("=" * 65)


if __name__ == "__main__":
    main()
