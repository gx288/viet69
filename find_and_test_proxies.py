import json
import os
import random
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

try:
    from curl_cffi import requests
except ImportError:
    print("[!] curl_cffi is required. Please install it first.")
    sys.exit(1)

# List of free proxy sources
PROXY_SOURCES = [
    # SOCKS5 sources (usually higher quality for bypass)
    "https://raw.githubusercontent.com/TheSpeedX/PROXY-List/master/socks5.txt",
    "https://raw.githubusercontent.com/monosans/proxy-list/main/proxies/socks5.txt",
    "https://raw.githubusercontent.com/ShiftyTR/Proxy-List/master/socks5.txt",
    "https://proxyspace.pro/socks5.txt",
    # HTTP sources
    "https://raw.githubusercontent.com/TheSpeedX/PROXY-List/master/http.txt",
    "https://raw.githubusercontent.com/monosans/proxy-list/main/proxies/http.txt",
    "https://raw.githubusercontent.com/ShiftyTR/Proxy-List/master/http.txt",
    "https://proxyspace.pro/http.txt"
]

headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
    'Accept-Language': 'en-US,en;q=0.9,vi;q=0.8',
}

def fetch_all_proxies():
    print("[*] Fetching proxy lists from sources...")
    all_proxies = set()
    for url in PROXY_SOURCES:
        try:
            proto = "socks5" if "socks5" in url else "http"
            r = requests.get(url, timeout=10)
            if r.status_code == 200:
                lines = r.text.split("\n")
                count = 0
                for line in lines:
                    line = line.strip()
                    if line and ":" in line and not line.startswith("#"):
                        parts = line.split(":")
                        if len(parts) == 2:
                            all_proxies.add(f"{proto}://{line}")
                            count += 1
                print(f"    Fetched {count} proxies from {url.split('/')[-1]}")
        except Exception as e:
            print(f"    Failed to fetch from {url}: {e}")
    print(f"[*] Total unique proxies collected: {len(all_proxies)}")
    return list(all_proxies)

def test_proxy(proxy_url, target_url):
    proxies = {
        "http": proxy_url,
        "https": proxy_url
    }
    start_time = time.time()
    try:
        # Use curl_cffi to bypass Cloudflare TLS fingerprinting
        r = requests.get(
            target_url, 
            headers=headers, 
            impersonate="chrome120", 
            proxies=proxies, 
            timeout=8
        )
        if r.status_code == 200 and "viet69" in r.text.lower():
            elapsed = time.time() - start_time
            return proxy_url, elapsed
    except Exception:
        pass
    return None

def main():
    config_file = "config.json"
    if not os.path.exists(config_file):
        print(f"[!] {config_file} not found!")
        return

    with open(config_file, "r", encoding="utf-8") as f:
        config = json.load(f)
    
    target_url = config.get("DOMAIN", "https://viet69.be")
    print(f"[*] Testing proxies against: {target_url}")

    candidates = fetch_all_proxies()
    if not candidates:
        print("[!] No proxies found.")
        return

    random.shuffle(candidates)
    
    # We only test a subset (e.g. 500) to save time, and stop once we find 3 working ones
    test_limit = min(500, len(candidates))
    to_test = candidates[:test_limit]
    print(f"[*] Testing up to {test_limit} random candidates...")

    working_proxies = []
    max_workers = 30  # Keep workers moderate to prevent system overload
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(test_proxy, p, target_url): p for p in to_test}
        
        for future in as_completed(futures):
            res = future.result()
            if res:
                working_proxies.append(res)
                print(f"    [+] Working: {res[0]} (Time: {res[1]:.2f}s)")
                # Stop early if we find 3 working proxies
                if len(working_proxies) >= 3:
                    print("[*] Found 3 working proxies. Stopping test.")
                    break

    if working_proxies:
        # Sort by response time
        working_proxies.sort(key=lambda x: x[1])
        best_proxy = working_proxies[0][0]
        print(f"\n[*] Best working proxy found: {best_proxy}")
        
        # Update config.json
        config["PROXY"] = best_proxy
        with open(config_file, "w", encoding="utf-8") as f:
            json.dump(config, f, ensure_ascii=False, indent=2)
        print("[*] Successfully updated config.json with the new proxy.")
    else:
        print("\n[!] No working proxies found in this batch. Please try again.")

if __name__ == "__main__":
    main()
