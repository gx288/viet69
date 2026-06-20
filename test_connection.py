import sys
import os
import json
import time

try:
    import requests
except ImportError:
    print("Please install requests first: pip install requests")
    sys.exit(1)

try:
    from curl_cffi import requests as curl_requests
    HAS_CURL_CFFI = True
except ImportError:
    HAS_CURL_CFFI = False
    print("curl_cffi is not installed. To test it, run: pip install curl_cffi")

headers_advanced = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
    'Accept-Language': 'en-US,en;q=0.9,vi;q=0.8',
    'Accept-Encoding': 'gzip, deflate, br',
    'Connection': 'keep-alive',
    'Upgrade-Insecure-Requests': '1',
    'Sec-Fetch-Dest': 'document',
    'Sec-Fetch-Mode': 'navigate',
    'Sec-Fetch-Site': 'none',
    'Sec-Fetch-User': '?1',
    'Cache-Control': 'max-age=0'
}

proxies = None
if os.path.exists('config.json'):
    try:
        with open('config.json', 'r') as f:
            config = json.load(f)
            if config.get('PROXY'):
                proxies = {
                    'http': config['PROXY'],
                    'https': config['PROXY']
                }
                print(f"Loaded proxy from config: {config['PROXY']}")
    except:
        pass

def print_result(response, step_name):
    print(f"\n--- {step_name} ---")
    print(f"Status Code: {response.status_code}")
    print(f"Response URL: {response.url}")
    if response.status_code == 200:
        print(f"Success! Content length: {len(response.text)}")
        snippet = response.text[:300].replace('\n', ' ')
        try:
            print(f"Content snippet: {snippet}...")
        except UnicodeEncodeError:
            print(f"Content snippet: {snippet.encode('ascii', errors='ignore').decode('ascii')}...")
    else:
        print(f"Failed with status: {response.status_code}")
        if "cloudflare" in response.text.lower() or "cf-ray" in response.headers.get('Server', '').lower() or 'cf-ray' in response.headers:
            print("Detected Cloudflare protection/cookie challenge in headers/body!")

def main():
    print("==================================================")
    print("Session-based Connection Test for Viet69")
    print("==================================================")
    
    if not HAS_CURL_CFFI:
        print("curl_cffi not installed. Cannot run optimal session test.")
        return

    # Use a single session to persist cookies, mimicking a real browser session
    session = curl_requests.Session()
    session.proxies = proxies
    
    # Step 1: Visit homepage first to get Cloudflare session cookies
    print("\n[Step 1] Visiting homepage to obtain session cookies...")
    try:
        res1 = session.get("https://viet69.be", headers=headers_advanced, impersonate="chrome120", timeout=10)
        print_result(res1, "Homepage Request")
        print(f"Cookies in Session: {session.cookies.get_dict()}")
    except Exception as e:
        print(f"Homepage request failed: {str(e)}")
        return

    # Wait 3 seconds to look like natural human delay
    print("\nWaiting 3 seconds before requesting page 2...")
    time.sleep(3)

    # Step 2: Request Page 2 with Referer pointing to the homepage
    print("\n[Step 2] Requesting Page 2 with Referer and Cookies...")
    page2_headers = headers_advanced.copy()
    page2_headers['Referer'] = 'https://viet69.be/'
    page2_headers['Sec-Fetch-Site'] = 'same-origin' # Since it's from the same origin now
    
    try:
        res2 = session.get("https://viet69.be/page/2/", headers=page2_headers, impersonate="chrome120", timeout=10)
        print_result(res2, "Page 2 Request")
    except Exception as e:
        print(f"Page 2 request failed: {str(e)}")

if __name__ == '__main__':
    main()
