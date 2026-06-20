import undetected_chromedriver as uc
import time
import sys

def main():
    options = uc.ChromeOptions()
    options.add_argument('--headless')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    
    print("Starting undetected-chromedriver...")
    driver = None
    try:
        driver = uc.Chrome(options=options, version_main=120)  # Or let it auto-detect
        print("Driver started. Loading homepage...")
        driver.get("https://viet69.be")
        
        # Wait for potential Cloudflare challenge to pass
        print("Waiting 10 seconds for page to load and challenge to settle...")
        time.sleep(10)
        
        print(f"Final URL: {driver.current_url}")
        print(f"Page Title: {driver.title}")
        
        source = driver.page_source
        print(f"Page Source length: {len(source)}")
        
        if "cloudflare" in source.lower() or "just a moment" in driver.title.lower() or "attention required" in driver.title.lower():
            print("Failed: Still stuck on Cloudflare challenge screen!")
            # Print a snippet of the page source to see the error
            print(source[:500])
        else:
            print("Success! Bypassed Cloudflare!")
            print(source[:300])
            
    except Exception as e:
        print(f"Error occurred: {str(e)}")
    finally:
        if driver:
            driver.quit()

if __name__ == '__main__':
    main()
