import sys
import os
import json

# Check dependencies
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

DOMAINS = ["https://viet69.be", "https://viet69.be/page/2/"]

headers_simple = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
}

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

# Try to load proxy from config.json
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

def test_url(url, method_name, use_curl=False, headers=None):
    print(f"\n--- Testing {url} using {method_name} ---")
    try:
        if use_curl:
            if not HAS_CURL_CFFI:
                print("Skipped: curl_cffi is not installed")
                return False
            response = curl_requests.get(url, headers=headers, impersonate="chrome120", timeout=10, proxies=proxies)
        else:
            response = requests.get(url, headers=headers, timeout=10, proxies=proxies)
            
        print(f"Status Code: {response.status_code}")
        print(f"Response URL: {response.url}")
        # Print a snippet of the page if successful
        if response.status_code == 200:
            print(f"Success! Content length: {len(response.text)}")
            snippet = response.text[:300].replace('\n', ' ')
            # Use sys.stdout.buffer or encode to avoid Windows charmap encoding errors
            try:
                print(f"Content snippet: {snippet}...")
            except UnicodeEncodeError:
                print(f"Content snippet: {snippet.encode('ascii', errors='ignore').decode('ascii')}...")
            return True
        else:
            print(f"Failed with status: {response.status_code}")
            if "cloudflare" in response.text.lower() or "cf-ray" in response.headers.get('Server', '').lower() or 'cf-ray' in response.headers:
                print("Detected Cloudflare protection/cookie challenge in headers/body!")
            return False
    except Exception as e:
        print(f"Error occurred: {str(e)}")
        return False

def main():
    print("==================================================")
    print("Connection Test Script for Viet69 Scraper")
    print("==================================================")
    
    for domain in DOMAINS:
        print(f"\n==================================================")
        print(f"Testing Domain: {domain}")
        print(f"==================================================")
        
        # Test 1: Simple requests (current implementation)
        test_url(domain, "requests (Simple Headers)", use_curl=False, headers=headers_simple)
        
        # Test 2: Advanced headers requests
        test_url(domain, "requests (Advanced Headers)", use_curl=False, headers=headers_advanced)
        
        # Test 3: curl_cffi (mimic Chrome TLS)
        test_url(domain, "curl_cffi (Impersonating Chrome)", use_curl=True, headers=headers_advanced)

if __name__ == '__main__':
    main()
