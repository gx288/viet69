import json
import requests
import logging
import os
from urllib.parse import urljoin

# Set up logging (detailed for debug)
logging.basicConfig(
    level=logging.DEBUG,  # DEBUG to capture everything
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('debug.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Config (minimal, you can hardcode or load from config.json)
DOMAIN = 'https://xnhau.sh/clip-sex-moi/'  # Or load from config.json if needed
URL = DOMAIN  # Test page 1

# Headers (enhanced for realism)
headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
    'Accept-Language': 'en-US,en;q=0.5',
    'Accept-Encoding': 'gzip, deflate',
    'DNT': '1',
    'Connection': 'keep-alive',
    'Upgrade-Insecure-Requests': '1',
    'Referer': 'https://xnhau.sh/'
}

# Optional proxies (uncomment if needed)
# PROXIES = {
#     'http': 'http://your-proxy:port',
#     'https': 'http://your-proxy:port'
# }
PROXIES = None  # No proxy for now

def debug_request(url, headers, proxies=None):
    """Send request and log everything."""
    logger.info(f"Starting debug request to: {url}")
    logger.info(f"Headers sent: {headers}")
    if proxies:
        logger.info(f"Proxies used: {proxies}")
    
    response = None
    try:
        # Send request
        response = requests.get(
            url,
            headers=headers,
            proxies=proxies,
            timeout=30,  # Longer timeout for debug
            allow_redirects=True,
            verify=True  # Verify SSL
        )
        
        # Success
        logger.info(f"Request successful! Status code: {response.status_code}")
        logger.info(f"Response headers: {dict(response.headers)}")
        logger.info(f"Content length: {len(response.content)} bytes")
        logger.info(f"Content type: {response.headers.get('content-type', 'Unknown')}")
        
        # Log snippet of content (first 1000 chars)
        content_snippet = response.text[:1000].replace('\n', ' ').replace('\r', ' ')
        logger.info(f"Response content snippet: {content_snippet}")
        
        # Check for common issues
        if 'cloudflare' in response.text.lower():
            logger.warning("Detected Cloudflare protection in response.")
        if 'captcha' in response.text.lower():
            logger.warning("Detected CAPTCHA in response.")
        if 'forbidden' in response.text.lower() or 'access denied' in response.text.lower():
            logger.warning("Detected access denied message in response.")
        
        # Save full response to file for inspection
        with open('debug_response.html', 'w', encoding='utf-8') as f:
            f.write(response.text)
        logger.info("Full response saved to debug_response.html")
        
        return response
    
    except requests.exceptions.ConnectTimeout:
        logger.error("Connection timeout: Could not connect to the server.")
    except requests.exceptions.ReadTimeout:
        logger.error("Read timeout: Server took too long to respond.")
    except requests.exceptions.ConnectionError as conn_err:
        logger.error(f"Connection error: {str(conn_err)}. Possible DNS or network issue.")
    except requests.exceptions.ProxyError as proxy_err:
        logger.error(f"Proxy error: {str(proxy_err)}. Proxy may be invalid or blocked.")
    except requests.exceptions.SSLError as ssl_err:
        logger.error(f"SSL error: {str(ssl_err)}. Certificate or verification issue.")
    except requests.exceptions.MissingSchema:
        logger.error("Missing schema: URL is invalid (e.g., not http/https).")
    except requests.exceptions.InvalidURL:
        logger.error("Invalid URL: Check the URL format.")
    except requests.exceptions.HTTPError as http_err:
        logger.error(f"HTTP error ({response.status_code if response else 'No response'}): {str(http_err)}")
        if response:
            logger.error(f"Response headers on error: {dict(response.headers)}")
            error_snippet = response.text[:500].replace('\n', ' ').replace('\r', ' ')
            logger.error(f"Error response snippet: {error_snippet}")
    except requests.exceptions.RequestException as req_err:
        logger.error(f"General request exception: {str(req_err)}")
    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}")
    
    return None

def main():
    """Main debug function."""
    logger.info("Debug scraper starting...")
    logger.info(f"Python version: {os.sys.version}")
    logger.info(f"Requests version: {requests.__version__}")
    logger.info(f"Target URL: {URL}")
    
    # Perform the request
    response = debug_request(URL, headers, PROXIES)
    
    if response:
        logger.info("Debug complete: Request succeeded.")
    else:
        logger.error("Debug complete: Request failed.")
    
    logger.info("Check debug.log and debug_response.html for details.")

if __name__ == '__main__':
    main()
