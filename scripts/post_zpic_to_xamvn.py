#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Xamvn Hourly Smart Poster (Action 2 - Tự động phân chia Giờ Gửi Video vs Giờ Up Bài)
====================================================================================
Chạy mỗi 1 tiếng 1 lần qua cron-job.org (`xamvn-comment-video.yml`):
- Tự động kiểm tra giờ Việt Nam hiện tại (`UTC+7`):
  + Nếu đúng khung giờ gửi video (`VIDEO_POST_HOURS_VN`, mặc định: 11h, 15h, 20h, 23h giờ VN):
    -> Lấy 1 video từ kho dự trữ `Đã up Zpic (Chờ đăng)` trên Google Sheet đăng vào thớt Xamvn
       và đánh dấu `Đã lấy`.
  + Nếu là các giờ còn lại trong ngày (hoặc khi kho dự trữ đang trống):
    -> Tự động bình luận `"up lên"` để đẩy thớt lên trang 1 (cứ đủ 10 lần `"up lên"` thì chèn
       bình luận nhắc xem web trên avatar).
"""

import os
import re
import sys
import time
import json
import argparse
from datetime import datetime, timezone, timedelta
from typing import Dict, Any, List, Optional, Set

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

# =========================================================================
# CẤU HÌNH KHUNG GIỜ GỬI VIDEO MỚI (GIỜ VIỆT NAM GMT+7)
# - Vào các giờ trong danh sách này: Bot sẽ lấy 1 video mới từ kho dự trữ đăng lên thớt.
# - Vào tất cả các giờ còn lại: Bot sẽ comment "up lên" để đẩy bài.
# =========================================================================
VIDEO_POST_HOURS_VN = [9, 12, 16, 20, 22, 0]

BUMP_MESSAGE = "up lên"
BUMP_LIMIT = 10
MILESTONE_MESSAGE = (
    'thôi dừng nhé. t để web trên ava nhé. tự vào mà xem nha mấy tml\n'
    '[IMG size="430x960"]https://xamvn.yachts/attachments/863527/[/IMG]'
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


def post_bump_comment(bot_class, args, history: Dict[str, Any]) -> bool:
    bump_count = int(history.get("bump_count", 0))
    if bump_count >= BUMP_LIMIT:
        msg = MILESTONE_MESSAGE
        next_bump_count = 0
        label = f"MILESTONE (sau {BUMP_LIMIT} lần up)"
    else:
        msg = BUMP_MESSAGE
        next_bump_count = bump_count + 1
        label = f"UP BÀI ({next_bump_count}/{BUMP_LIMIT})"

    print(f"\n[*] Chế độ [{label}]: Đang đăng bình luận đẩy thớt vào {args.thread}...")
    bot = bot_class(base_url=args.base_url, use_cache=True, verbose=True)
    if not bot.login(args.username, args.password):
        print(f"[-] Đăng nhập tài khoản [{args.username}] thất bại!")
        return False

    ok, post_id = bot.post_reply(args.thread, msg)
    if ok:
        history["bump_count"] = next_bump_count
        history["last_bump_at"] = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        save_posted_history(history)
        print(f"[SUCCESS] Đã bình luận đẩy bài thành công (Post ID: {post_id})!")
        return True
    print("[-] Bình luận đẩy bài thất bại!")
    return False


def main():
    parser = argparse.ArgumentParser(description="Xamvn Smart Hourly Poster (Gửi Video vs Up Bài)")
    parser.add_argument(
        "--mode",
        choices=["auto", "video", "bump"],
        default="auto",
        help="Chế độ chạy: 'auto' (tự chọn theo giờ VN), 'video' (ép gửi video mới), 'bump' (ép comment up bài)",
    )
    parser.add_argument(
        "--video-hours",
        default=",".join(str(h) for h in VIDEO_POST_HOURS_VN),
        help="Các giờ VN (0-23) sẽ gửi video mới (mặc định: 9,12,16,20,22,0)",
    )
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

    vn_tz = timezone(timedelta(hours=7))
    now_vn = datetime.now(vn_tz)
    current_vn_hour = now_vn.hour

    video_hours_set = set()
    for part in args.video_hours.split(","):
        part = part.strip()
        if part.isdigit():
            video_hours_set.add(int(part) % 24)

    history = load_posted_history()
    uploaded_list = load_zpic_uploaded()

    # Kiểm tra xem trong giờ hiện tại đã đăng video mới chưa (tránh đăng 2 video trong cùng 1 khung giờ)
    already_posted_this_hour = False
    now_hour_key = now_vn.strftime("%Y-%m-%d %H")
    if history.get("last_video_hour_vn") == now_hour_key:
        already_posted_this_hour = True

    if args.mode == "video":
        should_post_video = True
    elif args.mode == "bump":
        should_post_video = False
    else:
        should_post_video = (current_vn_hour in video_hours_set) and not already_posted_this_hour

    print("=" * 70)
    print("💬 XAMVN SMART HOURLY POSTER (GỬI VIDEO MỚI & UP BÀI THEO GIỜ)")
    print(f"Giờ Việt Nam    : {now_vn.strftime('%Y-%m-%d %H:%M:%S')} (Giờ hiện tại: {current_vn_hour}h)")
    print(f"Khung giờ Video : {sorted(video_hours_set)}h | Các giờ khác: Comment '{BUMP_MESSAGE}'")
    print(f"Quyết định chạy : {'🎬 GỬI VIDEO MỚI' if should_post_video else '⬆️ COMMENT UP BÀI'}")
    print(f"Tài khoản Xamvn : {args.username} | Thread: {args.thread}")
    print("=" * 70)

    if not should_post_video:
        post_bump_comment(bot_class, args, history)
        return

    # --- CHẾ ĐỘ GỬI VIDEO MỚI TỪ KHO DỰ TRỮ ZPIC ---
    posted_urls: Set[str] = set()
    posted_titles: Set[str] = set()
    posted_hashes: Set[str] = {"bmrHo4K4b", "LHWvt1CbX2fd", "5d13wzaL9iA"}

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

    posted_count = 0
    now_month = now_vn.strftime("%Y/%m")

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
                if parsed.get("link_type") == "viet69" and parsed.get("viet69_url"):
                    try:
                        _, v69_title, v69_desc, _ = extract_post_details(parsed["viet69_url"])
                        if v69_title and v69_title != "Video":
                            final_title = v69_title
                        video_desc = v69_desc or ""
                    except Exception:
                        pass
            else:
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

            print(f"\n--- [Đăng Video Mới từ Google Sheet Dòng #{row_idx}] ---")
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
                now_str = now_vn.strftime("%Y-%m-%d %H:%M:%S")

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
                    "posted_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
                }
                history["posted"].append(history_item)
                history["last_video_hour_vn"] = now_hour_key
                history["bump_count"] = 0
                posted_hashes.add(vid_hash)
                posted_titles.add(final_title.strip().lower())
                save_posted_history(history)
                sync_to_xamvid_web(history)
                print(f"    [SUCCESS] Đã đăng video lên Xamvn thành công (Post ID: {post_id})!")

                if posted_count < args.max_posts_per_run:
                    time.sleep(args.delay_between_posts)

    # Nếu đến giờ gửi video mà trong kho chưa có sẵn video chờ đăng -> tự động chuyển sang comment "up lên"
    if posted_count == 0:
        print("\n[i] Kho dự trữ hiện chưa có sẵn video chờ đăng -> Tự động chuyển sang comment 'up lên' đẩy bài!")
        post_bump_comment(bot_class, args, history)


if __name__ == "__main__":
    main()
