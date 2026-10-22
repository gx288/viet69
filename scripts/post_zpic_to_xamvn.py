#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Xamvn Video Comment Poster (Action 2 - Tách biệt hoàn toàn với Tải/Upload Zpic)
================================================================================
Chỉ thực hiện đăng nhập Xamvn và comment các video ĐÃ ĐƯỢC UPLOAD LÊN ZPIC:
1. Ưu tiên lấy các dòng có trạng thái 'Đã up Zpic (Chờ đăng)' (hoặc đã có Link Zpic MP4 ở Cột E)
   trên Google Sheet Hàng Đợi (1ZQTydO0y5CGfCaFCcw72F0LXzR5b63p_6yzttTUHTyY).
2. Nếu trên Google Sheet chưa có dòng nào chờ đăng, tự động lấy từ kho dự trữ `data/zpic_uploaded.json`
   (những video đã được Action 1 tải lên Zpic nhưng chưa đăng lên Xamvn).
3. Sau khi đăng thành công:
   - Đánh dấu 'Đã lấy' + điền Link Post Xamvn vào Cột C & D trên Google Sheet.
   - Lưu lịch sử vào `data/xamvn_posted_monthly.json` và đồng bộ `data/monthly_videos.json`.

Cách chạy:
    python scripts/post_zpic_to_xamvn.py --max-posts-per-run 1 --thread "https://xamvn.yachts/threads/311300/"
"""

import os
import re
import sys
import time
import json
import argparse
from datetime import datetime
from typing import Dict, Any, List, Optional, Set, Tuple

try:
    sys.stdout.reconfigure(encoding="utf-8", line_buffering=True)
except Exception:
    pass

CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_DIR = os.path.abspath(os.path.join(CURRENT_DIR, ".."))
XAMVN_DIR = os.path.abspath(os.path.join(PROJECT_DIR, "..", "xamvn"))

for p in (CURRENT_DIR, PROJECT_DIR, XAMVN_DIR):
    if p not in sys.path:
        sys.path.insert(0, p)

from monthly_top_to_xamvn import (
    PRIORITY_SHEET_ID,
    PRIORITY_SHEET_TAB,
    OUTPUT_JSON,
    load_posted_history,
    save_posted_history,
    sync_to_xamvid_web,
    fetch_thread_posted_hashes,
    get_priority_sheet_worksheet,
    parse_priority_sheet_row,
    extract_post_details,
    post_to_xamvn,
)


def load_zpic_uploaded() -> List[Dict[str, Any]]:
    if os.path.exists(OUTPUT_JSON):
        try:
            with open(OUTPUT_JSON, "r", encoding="utf-8") as f:
                data = json.load(f)
                if isinstance(data, list):
                    return data
        except Exception as e:
            print(f"[!] Lỗi đọc {OUTPUT_JSON}: {e}")
    return []


def extract_vid_hash(mp4_url: str) -> str:
    if not mp4_url:
        return ""
    m = re.search(r"/([a-zA-Z0-9_-]+)\.mp4", mp4_url)
    return m.group(1) if m else mp4_url.rstrip("/").split("/")[-1].split(".")[0]


def build_bbcode_from_mp4(mp4_url: str, vid_hash: str = "") -> str:
    if not vid_hash:
        vid_hash = extract_vid_hash(mp4_url)
    return (
        f"[video]{mp4_url}[/video][hr][/hr]"
        f"[url=https://zpic.org/v/{vid_hash}][img]https://f.zzpi.cc/{vid_hash}.th.jpeg[/img][/url]"
        f"[url=https://zpic.org/v/{vid_hash}]#[/url]"
    )


def find_zpic_record(
    uploaded_list: List[Dict[str, Any]],
    col_e_mp4: str = "",
    source_url: str = "",
    stream_url: str = "",
) -> Optional[Dict[str, Any]]:
    target_hash = extract_vid_hash(col_e_mp4) if col_e_mp4 else ""
    clean_src = source_url.rstrip("/") if source_url else ""
    clean_str = stream_url.rstrip("/") if stream_url else ""

    for rec in uploaded_list:
        z = rec.get("zpic", {})
        rec_mp4 = z.get("direct_mp4", "")
        rec_hash = z.get("name") or extract_vid_hash(rec_mp4)
        rec_src = (rec.get("source_post") or "").rstrip("/")
        rec_str = (rec.get("source_stream") or "").rstrip("/")

        if target_hash and rec_hash == target_hash:
            return rec
        if clean_src and rec_src and clean_src == rec_src:
            return rec
        if clean_str and rec_str and clean_str == rec_str:
            return rec
    return None


def main():
    parser = argparse.ArgumentParser(description="Xamvn Video Comment Poster (Action 2 - Chỉ đăng bài từ kho Zpic)")
    parser.add_argument("--max-posts-per-run", type=int, default=1, help="Số bình luận video tối đa mỗi lần chạy (mặc định: 1)")
    parser.add_argument("-u", "--username", default=os.getenv("XAMVN_USERNAME", "cudai2m"), help="Tài khoản Xamvn")
    parser.add_argument("-p", "--password", default=os.getenv("XAMVN_PASSWORD", "lehunghup"), help="Mật khẩu Xamvn")
    default_thread = os.getenv("XAMVN_THREAD") or "https://xamvn.yachts/threads/311300/"
    parser.add_argument("-t", "--thread", default=default_thread, help="Link thread Xamvn cần bình luận")
    parser.add_argument("--base-url", default="https://xamvn.yachts", help="Base URL Xamvn")
    parser.add_argument("--delay-between-posts", type=int, default=25, help="Số giây nghỉ giữa 2 lần bình luận (mặc định: 25s)")
    args = parser.parse_args()

    try:
        from xamvn_bot import XamvnBot
        bot_class = XamvnBot
    except ImportError:
        from scripts.xamvn_bot import XamvnBot
        bot_class = XamvnBot

    history = load_posted_history()
    uploaded_list = load_zpic_uploaded()

    posted_urls: Set[str] = set()
    posted_titles: Set[str] = set()
    posted_hashes: Set[str] = set()

    for item in history.get("posted", []):
        if item.get("source_post"):
            u = item["source_post"].rstrip("/")
            posted_urls.add(u)
            posted_urls.add(u.split("/")[-1])
        if item.get("source_stream"):
            posted_urls.add(item["source_stream"].rstrip("/"))
        if item.get("title"):
            posted_titles.add(item["title"].strip().lower())
        if item.get("zpic_mp4"):
            h = extract_vid_hash(item["zpic_mp4"])
            if h:
                posted_hashes.add(h)

    thread_hashes = fetch_thread_posted_hashes(args.thread, max_pages=5)
    posted_hashes.update(thread_hashes)

    print("=" * 70)
    print("💬 XAMVN VIDEO COMMENT POSTER (CHỈ ĐĂNG BÀI TỪ KHO ZPIC)")
    print(f"Sheet Ưu Tiên   : https://docs.google.com/spreadsheets/d/{PRIORITY_SHEET_ID}/edit")
    print(f"Tài khoản Xamvn : {args.username}")
    print(f"Thread đích     : {args.thread}")
    print(f"Số bài/lần chạy : {args.max_posts_per_run}")
    print(f"Tổng kho Zpic   : {len(uploaded_list)} video | Đã đăng: {len(history.get('posted', []))} bài")
    print("=" * 70)

    posted_count = 0
    now_month = datetime.now().strftime("%Y/%m")

    # BƯỚC 1: Kiểm tra Google Sheet ưu tiên (các dòng đã có Link Zpic MP4 ở Cột E hoặc đã có trong zpic_uploaded.json)
    ws = get_priority_sheet_worksheet(PRIORITY_SHEET_ID, PRIORITY_SHEET_TAB)
    if ws:
        try:
            rows = ws.get_all_values()
        except Exception as e:
            print(f"[!] Lỗi đọc Google Sheet: {e}")
            rows = []

        for row_idx in range(2, len(rows) + 1):
            if args.max_posts_per_run and posted_count >= args.max_posts_per_run:
                break

            row = rows[row_idx - 1]
            col_a = (row[0] if len(row) > 0 else "").strip()
            col_b = (row[1] if len(row) > 1 else "").strip()
            col_c = (row[2] if len(row) > 2 else "").strip()
            col_e = (row[4] if len(row) > 4 else "").strip()

            if not col_a and not col_b and not col_e:
                continue

            status_lower = col_c.lower()
            if any(k in status_lower for k in ["đã lấy", "da lay", "done", "ok", "skip", "bỏ qua", "lỗi"]):
                continue

            parsed = parse_priority_sheet_row(col_a, col_b)
            v_url = parsed.get("badnews_url") or parsed["viet69_url"] or parsed["stream_url"]
            stream_url = parsed.get("stream_url", "")

            rec = find_zpic_record(uploaded_list, col_e_mp4=col_e, source_url=v_url, stream_url=stream_url)
            direct_mp4 = ""
            vid_hash = ""
            bbcode = ""
            final_title = parsed.get("title") or "Video Hot Cập Nhật"
            frame_url = ""
            viewer_url = ""
            duration = 0
            video_desc = ""

            if rec:
                z = rec.get("zpic", {})
                direct_mp4 = z.get("direct_mp4", "")
                vid_hash = z.get("name") or extract_vid_hash(direct_mp4)
                bbcode = z.get("bbcode_full") or build_bbcode_from_mp4(direct_mp4, vid_hash)
                final_title = rec.get("title") or final_title
                frame_url = z.get("frame_url", f"https://f.zzpi.cc/{vid_hash}.fr.jpeg")
                viewer_url = z.get("viewer_url", f"https://zpic.org/v/{vid_hash}")
                duration = z.get("duration", 0)
                video_desc = rec.get("description", "")
            elif col_e.startswith("http") and ".mp4" in col_e.lower():
                direct_mp4 = col_e
                vid_hash = extract_vid_hash(direct_mp4)
                bbcode = build_bbcode_from_mp4(direct_mp4, vid_hash)
                frame_url = f"https://f.zzpi.cc/{vid_hash}.fr.jpeg"
                viewer_url = f"https://zpic.org/v/{vid_hash}"
                # Lấy tiêu đề chuẩn từ Viet69 nếu Cột A là link Viet69
                if parsed.get("link_type") == "viet69" and parsed.get("viet69_url"):
                    try:
                        _, v69_title, v69_desc, _ = extract_post_details(parsed["viet69_url"])
                        if v69_title and v69_title != "Video":
                            final_title = v69_title
                        video_desc = v69_desc or ""
                    except Exception:
                        pass
            else:
                # Dòng này chưa được Action 1 tải lên Zpic -> bỏ qua để chờ Action 1 tải
                continue

            if not direct_mp4 or not vid_hash:
                continue

            if vid_hash in posted_hashes:
                print(f"[i] Dòng #{row_idx} ({vid_hash}) đã có trên Xamvn trước đó. Đánh dấu 'Đã lấy'...")
                try:
                    ws.update(range_name=f"C{row_idx}", values=[["Đã lấy"]])
                except Exception:
                    pass
                continue

            print(f"\n--- [Đăng từ Google Sheet Dòng #{row_idx}] ---")
            print(f"    Tiêu đề   : {final_title}")
            print(f"    Zpic MP4  : {direct_mp4}")

            ok, post_id = post_to_xamvn(
                bot_class=bot_class,
                username=args.username,
                password=args.password,
                thread_target=args.thread,
                title=final_title,
                bbcode=bbcode,
                base_url=args.base_url,
            )
            if ok:
                posted_count += 1
                post_link = f"https://xamvn.yachts/posts/{post_id}/" if post_id else args.thread
                now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

                try:
                    ws.update(
                        range_name=f"C{row_idx}:F{row_idx}",
                        values=[["Đã lấy", post_link, direct_mp4, now_str]],
                    )
                    print(f"    [+] Đã cập nhật 'Đã lấy' cho dòng #{row_idx} trên Google Sheet!")
                except Exception as e:
                    print(f"    [!] Cảnh báo lỗi cập nhật Sheet dòng #{row_idx}: {e}")

                history_item = {
                    "month": now_month,
                    "title": final_title,
                    "description": video_desc,
                    "views": "Hot Queue",
                    "duration": duration,
                    "source_post": v_url or direct_mp4,
                    "source_stream": stream_url,
                    "zpic_mp4": direct_mp4,
                    "frame_url": frame_url,
                    "zpic_viewer": viewer_url,
                    "post_id": post_id,
                    "posted_at": datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ"),
                }
                history["posted"].append(history_item)
                posted_hashes.add(vid_hash)
                posted_titles.add(final_title.strip().lower())
                save_posted_history(history)
                sync_to_xamvid_web(history)
                print(f"    [SUCCESS] Đã bình luận lên Xamvn thành công (Post ID: {post_id})!")

                if posted_count < args.max_posts_per_run:
                    print(f"    ⏳ Nghỉ {args.delay_between_posts}s chống flood...")
                    time.sleep(args.delay_between_posts)

    # BƯỚC 2: Nếu trên Sheet chưa có sẵn bài nào chờ đăng, kiểm tra kho dự trữ data/zpic_uploaded.json
    test_hashes = {"bmrHo4K4b", "LHWvt1CbX2fd", "5d13wzaL9iA"}
    posted_hashes.update(test_hashes)

    if posted_count < args.max_posts_per_run:
        for rec in uploaded_list:
            if args.max_posts_per_run and posted_count >= args.max_posts_per_run:
                break

            z = rec.get("zpic", {})
            direct_mp4 = z.get("direct_mp4", "")
            if not direct_mp4:
                continue

            vid_hash = z.get("name") or extract_vid_hash(direct_mp4)
            title = (rec.get("title") or "Video Hot").strip()
            src_post = (rec.get("source_post") or "").rstrip("/")
            src_stream = (rec.get("source_stream") or "").rstrip("/")

            if (
                vid_hash in posted_hashes
                or title.lower() in posted_titles
                or (src_post and src_post in posted_urls)
                or (src_stream and src_stream in posted_urls)
            ):
                continue

            bbcode = z.get("bbcode_full") or build_bbcode_from_mp4(direct_mp4, vid_hash)
            print(f"\n--- [Đăng từ Kho Dự Trữ zpic_uploaded.json] ---")
            print(f"    Tiêu đề   : {title}")
            print(f"    Zpic MP4  : {direct_mp4}")

            ok, post_id = post_to_xamvn(
                bot_class=bot_class,
                username=args.username,
                password=args.password,
                thread_target=args.thread,
                title=title,
                bbcode=bbcode,
                base_url=args.base_url,
            )
            if ok:
                posted_count += 1
                history_item = {
                    "month": rec.get("month") or now_month,
                    "title": title,
                    "description": rec.get("description", ""),
                    "views": rec.get("views", "Hot"),
                    "duration": z.get("duration", 0),
                    "source_post": src_post or direct_mp4,
                    "source_stream": src_stream,
                    "zpic_mp4": direct_mp4,
                    "frame_url": z.get("frame_url", f"https://f.zzpi.cc/{vid_hash}.fr.jpeg"),
                    "zpic_viewer": z.get("viewer_url", f"https://zpic.org/v/{vid_hash}"),
                    "post_id": post_id,
                    "posted_at": datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ"),
                }
                history["posted"].append(history_item)
                posted_hashes.add(vid_hash)
                posted_titles.add(title.lower())
                if src_post:
                    posted_urls.add(src_post)
                save_posted_history(history)
                sync_to_xamvid_web(history)
                print(f"    [SUCCESS] Đã bình luận từ kho dự trữ thành công (Post ID: {post_id})!")

                if posted_count < args.max_posts_per_run:
                    time.sleep(args.delay_between_posts)

    if posted_count == 0:
        print("\n[!] Hiện chưa có video nào ở trạng thái 'Đã up Zpic (Chờ đăng)' trên Sheet hoặc trong kho dự trữ.")
        print("    -> Hãy chạy Action 1 (Tải & Upload Zpic) trước để nạp sẵn video vào kho!")

    print("\n" + "=" * 70)
    print(f"HOÀN TẤT! Đã đăng mới: {posted_count} bình luận lên Xamvn.")
    print("=" * 70)


if __name__ == "__main__":
    main()
