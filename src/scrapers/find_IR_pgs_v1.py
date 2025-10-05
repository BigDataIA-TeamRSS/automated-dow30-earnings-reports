import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
import pandas as pd
import time
from datetime import datetime
import re
import warnings
import os
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import TimeoutException, WebDriverException
import logging

# Suppress SSL warnings and selenium logs
warnings.filterwarnings('ignore', message='Unverified HTTPS request')
logging.getLogger('selenium').setLevel(logging.WARNING)
requests.packages.urllib3.disable_warnings()

def setup_driver():
    """
    Setup Chrome driver with optimized options
    """
    chrome_options = Options()
    chrome_options.add_argument('--headless')  # Run in background
    chrome_options.add_argument('--no-sandbox')
    chrome_options.add_argument('--disable-dev-shm-usage')
    chrome_options.add_argument('--disable-gpu')
    chrome_options.add_argument('--window-size=1920,1080')
    chrome_options.add_argument('--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36')
    chrome_options.add_argument('--disable-blink-features=AutomationControlled')
    chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
    chrome_options.add_experimental_option('useAutomationExtension', False)
    chrome_options.add_argument('--ignore-certificate-errors')
    chrome_options.add_argument('--allow-insecure-localhost')
    chrome_options.add_argument('--log-level=3')  # Suppress logs
    
    try:
        driver = webdriver.Chrome(options=chrome_options)
        return driver
    except Exception as e:
        print(f"Warning: Could not initialize Chrome driver: {e}")
        return None

def check_url_for_ir_content(url, headers, min_indicators=2, company_name=None, check_subpaths=True):
    """
    Check if a URL contains IR content with better validation
    Now checks subpaths on investor subdomains for pages like /investor-home/default.aspx
    """
    try:
        try:
            response = requests.get(url, headers=headers, timeout=10, allow_redirects=True, verify=True)
        except requests.exceptions.SSLError:
            response = requests.get(url, headers=headers, timeout=10, allow_redirects=True, verify=False)
        
        if response.status_code != 200:
            return None
        
        # Check final URL for authentication redirects or errors
        final_url = response.url.lower()
        
        # Skip login pages, authentication, and error pages
        skip_patterns = [
            'login.microsoftonline.com',
            'login.',
            'signin.',
            'auth.',
            'oauth',
            'saml',
            '404',
            'not-found',
            'error',
            'sharepoint.com/_forms',
            'authentication'
        ]
        
        if any(pattern in final_url for pattern in skip_patterns):
            return None
            
        content_type = response.headers.get('Content-Type', '').lower()
        if 'text/html' not in content_type and 'application/xhtml' not in content_type:
            return None
        
        text_lower = response.text.lower()
        
        # If we're redirected to a completely different domain (not investor subdomain)
        if company_name:
            parsed_final = urlparse(response.url)
            final_domain = parsed_final.netloc.lower()
            # Skip if redirected to unrelated domain (unless it's an investor subdomain)
            if company_name not in final_domain and not any(prefix in final_domain for prefix in ['investor', 'ir', 'pginvestor']):
                return None
        
        # IR content indicators
        ir_indicators = [
            'investor','investor-home', 'shareholder', 'financial', 'earnings',
            'annual report', 'quarterly', 'sec filing', 'stock',
            'dividend', 'proxy', 'corporate governance', '10-k', '10-q'
        ]
        
        indicator_count = sum(1 for indicator in ir_indicators if indicator in text_lower)
        
        # Check title for extra confidence
        soup = BeautifulSoup(response.content, 'html.parser')
        title = soup.find('title')
        if title and title.text:
            title_lower = title.text.lower()
            if any(keyword in title_lower for keyword in ['investor', 'shareholder', 'ir','investor-home']):
                indicator_count += 3
        
        # NEW: Check subpaths on investor subdomains AND when original URL was an investor subdomain
        # This handles cases where the subdomain redirects elsewhere
        original_parsed = urlparse(url)
        response_parsed = urlparse(response.url)
        
        # Check if we should look for subpaths (either low indicators OR we started with an investor subdomain)
        should_check_subpaths = (
            check_subpaths and 
            (indicator_count < min_indicators or 
             any(prefix in original_parsed.netloc for prefix in ['investor', 'ir']))
        )
        
        if should_check_subpaths:
            # Determine which base URL to use for checking subpaths
            bases_to_check = []
            
            # If original URL was an investor subdomain, check paths on it
            if any(prefix in original_parsed.netloc for prefix in ['investor', 'ir']):
                bases_to_check.append(f"{original_parsed.scheme}://{original_parsed.netloc}")
            
            # If response URL is an investor subdomain, check paths on it too
            if any(prefix in response_parsed.netloc for prefix in ['investor', 'ir']):
                bases_to_check.append(f"{response_parsed.scheme}://{response_parsed.netloc}")
            
            for base in bases_to_check:
                ir_paths = [
                    '/investor-home/default.aspx',  # Sherwin-Williams specific path
                    '/investor-home/',
                    '/overview/default.aspx',
                    '/home/default.aspx',
                    '/investor-relations/default.aspx',
                    '/investors/overview/default.aspx',
                    '/default.aspx',
                    '/',
                ]
                print(f"    Checking IR subpaths on {base}...")
                for path in ir_paths:
                    test_url = base + path
                    if test_url == response.url:  # Skip if we already checked this
                        continue
                    try:
                        test_response = requests.get(test_url, headers=headers, timeout=5, allow_redirects=True, verify=False)
                        if test_response.status_code == 200:
                            test_text = test_response.text.lower()
                            test_indicators = sum(1 for ind in ir_indicators if ind in test_text)
                            # Check title too
                            test_soup = BeautifulSoup(test_response.content, 'html.parser')
                            test_title = test_soup.find('title')
                            if test_title and 'investor' in test_title.text.lower():
                                test_indicators += 2
                            if test_indicators >= min_indicators:
                                print(f"      ✓ Found IR content at: {test_response.url}")
                                return test_response.url
                    except:
                        continue
        
        if indicator_count >= min_indicators:
            return response.url
            
    except Exception as e:
        print(f"    Error checking URL: {str(e)[:50]}")
        pass
    
    return None

def check_with_selenium(url, company_name):
    """
    Use Selenium to check URLs that might have JavaScript redirects
    """
    driver = None
    try:
        driver = setup_driver()
        if not driver:
            return None
            
        driver.get(url)
        
        # Wait for page to load
        WebDriverWait(driver, 10).until(
            lambda d: d.execute_script('return document.readyState') == 'complete'
        )
        
        # Wait for potential JavaScript redirects
        time.sleep(2)
        
        # Get final URL after all redirects
        final_url = driver.current_url
        
        # Get page source
        page_source = driver.page_source
        soup = BeautifulSoup(page_source, 'html.parser')
        
        # Check if it's an IR page
        text_lower = soup.get_text().lower()
        ir_indicators = [
            'investor','investor-home', 'shareholder', 'financial', 'earnings',
            'annual report', 'quarterly', 'sec filing', 'stock'
        ]
        
        indicator_count = sum(1 for indicator in ir_indicators if indicator in text_lower)
        
        # Check page title
        title = soup.find('title')
        if title and title.text:
            title_lower = title.text.lower()
            if any(keyword in title_lower for keyword in ['investor','investor-home','shareholder', 'ir']):
                indicator_count += 3
        
        if indicator_count >= 3:
            return final_url
            
        # Look for investor links on the page
        links = driver.find_elements(By.TAG_NAME, 'a')
        for link in links:
            try:
                href = link.get_attribute('href')
                link_text = link.text.lower() if link.text else ''
                
                if href and ('investor' in href.lower() or 'investor' in link_text):
                    parsed = urlparse(href)
                    if any(prefix in parsed.netloc for prefix in ['investor','investor-home','ir', 'stock']):
                        if company_name in parsed.netloc:
                            return href
            except:
                continue
                
        return None
        
    except Exception:
        return None
    finally:
        if driver:
            driver.quit()

def find_ir_page(company_url):
    """
    Comprehensive IR page finder with enhanced path checking
    """
    
    # Ensure URL has https://
    if not company_url.startswith(('http://', 'https://')):
        company_url = 'https://' + company_url
    
    # Parse domain
    parsed = urlparse(company_url)
    base_domain = f"{parsed.scheme}://{parsed.netloc}"
    
    # Extract the core domain name (without www)
    domain_parts = parsed.netloc.split('.')
    if domain_parts[0] == 'www':
        domain_without_www = '.'.join(domain_parts[1:])
    else:
        domain_without_www = parsed.netloc
    
    # Get the company name part
    company_name = domain_without_www.split('.')[0]
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.5',
        'Connection': 'keep-alive',
    }
    
    print(f"Checking {company_url}...")
    print(f"  Core domain: {domain_without_www}")
    print(f"  Company name: {company_name}")
    
    # Method 1: Try subdomain-based IR pages
    subdomain_prefixes = [
        'investors',    # Most common (including Sherwin-Williams)
        'investor',     # Alternative
        'ir',          # Short form
        'stock',       # Walmart uses this
        'stocks',      # Alternative
    ]
    
    # Some companies use completely different domains for IR
    # Map company names to their known IR domains
    alternative_ir_domains = {
        'pg': ['pginvestor.com', 'www.pginvestor.com'],  # P&G uses pginvestor.com
    }
    
    print("  Checking investor subdomains...")
    
    # First check if this company has an alternative IR domain
    if company_name in alternative_ir_domains:
        print(f"  Checking known alternative IR domains for {company_name}...")
        for alt_domain in alternative_ir_domains[company_name]:
            for protocol in ['https://', 'http://']:
                alt_url = protocol + alt_domain
                print(f"    Trying alternative domain: {alt_url}")
                result = check_url_for_ir_content(alt_url, headers, company_name=company_name, check_subpaths=False)
                if result:
                    # Verify it's not a login page
                    if 'login.microsoftonline.com' not in result:
                        print(f"  ✓ Found IR page at alternative domain: {result}")
                        return result
    
    for prefix in subdomain_prefixes:
        # Build subdomain URLs - try BOTH http and https
        subdomain_urls = [
            f'https://{prefix}.{domain_without_www}',
            f'http://{prefix}.{domain_without_www}',
        ]
        
        for subdomain_url in subdomain_urls:
            print(f"    Trying: {subdomain_url}")
            
            # Always proactively check common IR paths on investor subdomains
            # This catches cases where the subdomain root doesn't have IR content
            if prefix in ['investors', 'investor', 'ir']:
                ir_subpaths = [
                    '/investor-home/default.aspx',
                    '/investor-home/',
                    '/investors/overview/default.aspx',
                    '/overview/default.aspx',
                    '/home/default.aspx',
                    '/investor-relations/default.aspx',
                    '/default.aspx',
                    '',  # Root path
                ]
                
                for subpath in ir_subpaths:
                    full_url = subdomain_url + subpath
                    print(f"      Checking: {full_url}")
                    try:
                        test_response = requests.get(full_url, headers=headers, timeout=5, allow_redirects=True, verify=False)
                        if test_response.status_code == 200:
                            # Verify it's HTML
                            content_type = test_response.headers.get('Content-Type', '').lower()
                            if 'text/html' not in content_type and 'application/xhtml' not in content_type:
                                continue
                            
                            # Check that we haven't been redirected to the main non-investor site
                            final_url = test_response.url.lower()
                            parsed_final = urlparse(final_url)
                            
                            # Continue if we're still on an investor subdomain or have investor in path
                            if not any(x in parsed_final.netloc for x in ['investor', 'ir', 'stock']) and 'investor' not in parsed_final.path:
                                # We've been redirected away from investor content
                                if final_url.rstrip('/') == company_url.lower().rstrip('/'):
                                    continue
                            
                            # Check for IR content
                            text_lower = test_response.text.lower()
                            ir_indicators = [
                                'investor', 'shareholder', 'financial', 'earnings',
                                'annual report', 'quarterly', 'sec filing', 'stock',
                                'dividend', 'proxy', '10-k', '10-q'
                            ]
                            indicator_count = sum(1 for ind in ir_indicators if ind in text_lower)
                            
                            # Check title for IR keywords
                            soup = BeautifulSoup(test_response.content, 'html.parser')
                            title = soup.find('title')
                            if title and title.text:
                                title_lower = title.text.lower()
                                if any(keyword in title_lower for keyword in ['investor', 'shareholder', 'ir']):
                                    indicator_count += 3
                            
                            # Accept if we have enough IR indicators
                            if indicator_count >= 2:
                                print(f"  ✓ Found IR page: {test_response.url}")
                                return test_response.url
                    except Exception as e:
                        continue
            
            # Also try the base subdomain URL
            result = check_url_for_ir_content(subdomain_url, headers, company_name=company_name, check_subpaths=False)
            if result:
                # Skip if it just returns the main homepage
                if result.rstrip('/').lower() == company_url.rstrip('/').lower():
                    print(f"    Skipping - returned to main homepage")
                    continue
                # Additional check for companies like JPMorgan
                if company_name == 'jpmorganchase' and result.rstrip('/') == base_domain.lower().rstrip('/'):
                    continue
                print(f"  ✓ Found IR subdomain: {result}")
                return result
    
    # Method 2: Check COMPREHENSIVE list of IR paths on main domain
    print("  Checking common IR paths on main domain...")
    
    # Enhanced path list to catch UnitedHealthGroup's /investors.html
    common_paths = [
        # Standard paths
        '/investors',
        '/investor',
        '/investor-relations',
        '/investor_relations',
        '/investorrelations',
        '/ir',
        '/IR',
        '/investor-center',
        '/investor_center',
        '/investorcenter',
        '/shareholder',
        '/shareholders',
        # HTML variants (CRITICAL for UnitedHealthGroup)
        '/investors.html',
        '/investor.html',
        '/investors.htm',
        '/investor.htm',
        '/investor-relations.html',
        '/investor_relations.html',
        '/ir.html',
        # ASPX variants
        '/investors.aspx',
        '/investor.aspx',
        '/investor-home/default.aspx',
        # Nested paths
        '/about/investors',
        '/about-us/investors',
        '/company/investors',
        '/corporate/investors',
        '/en/investors',
        '/en-us/investors',
        # With trailing slash
        '/investors/',
        '/investor/',
        '/ir/',
    ]
    
    base_urls = [base_domain]
    if 'www.' not in base_domain:
        base_urls.append(f'https://www.{domain_without_www}')
    
    for base in base_urls:
        for path in common_paths:
            test_url = base + path
            
            result = check_url_for_ir_content(test_url, headers, company_name=company_name, check_subpaths=False)
            if result:
                # Check if it redirected to an investor subdomain
                final_parsed = urlparse(result)
                if any(prefix in final_parsed.netloc for prefix in ['investor','ir', 'stock', 'pginvestor']):
                    print(f"  ✓ Found via redirect to IR subdomain: {result}")
                    return result
                
                # If it's a valid IR page on the main domain
                print(f"  ✓ Found via direct path: {result}")
                return result
    
    # Method 3: Search homepage for IR links
    print("  Searching homepage for IR links...")
    
    try:
        try:
            response = requests.get(company_url, headers=headers, timeout=10, verify=True)
        except:
            response = requests.get(company_url, headers=headers, timeout=10, verify=False)
            
        soup = BeautifulSoup(response.content, 'html.parser')
        
        # Find all links
        all_links = soup.find_all('a', href=True)
        
        # Check each link
        for link in all_links:
            href = link.get('href', '')
            link_text = link.get_text().strip().lower()
            link_title = link.get('title', '').lower()
            
            # Check if text indicates IR
            if any(term in link_text for term in ['investors','investor relations', 'for investors', 'investor information']):
                full_url = urljoin(company_url, href)
                result = check_url_for_ir_content(full_url, headers, company_name=company_name)
                if result:
                    print(f"  ✓ Found via homepage link: {result}")
                    return result
            
            # Check if href points to an investor subdomain
            if href.startswith(('http://', 'https://')):
                parsed_href = urlparse(href)
                if any(prefix in parsed_href.netloc for prefix in ['investor','ir', 'stock', 'pginvestor']):
                    if company_name in parsed_href.netloc or domain_without_www in parsed_href.netloc or 'pginvestor' in parsed_href.netloc:
                        result = check_url_for_ir_content(href, headers, company_name=company_name)
                        if result:
                            print(f"  ✓ Found IR subdomain link on homepage: {result}")
                            return result
                    
    except Exception as e:
        print(f"  Error searching homepage: {str(e)[:50]}")
    
    # Method 4: Use Selenium as fallback for JavaScript-rendered pages
    if setup_driver():
        print("  Trying Selenium for JavaScript-rendered pages...")
        
        # Try main domain with Selenium
        selenium_result = check_with_selenium(company_url, company_name)
        if selenium_result:
            print(f"  ✓ Selenium found IR page: {selenium_result}")
            return selenium_result
        
        # Try common subdomains with Selenium
        for prefix in ['investors', 'investor', 'ir']:
            for protocol in ['https://', 'http://']:
                test_url = f'{protocol}{prefix}.{domain_without_www}'
                selenium_result = check_with_selenium(test_url, company_name)
                if selenium_result:
                    print(f"  ✓ Selenium found IR page: {selenium_result}")
                    return selenium_result
    
    print(f"  ✗ Could not find IR page automatically")
    return None


def process_companies(companies_df):
    """
    Process multiple companies and find their IR pages
    """
    results = []
    total = len(companies_df)
    
    for idx, row in companies_df.iterrows():
        ticker = row['Ticker']
        website = row['Website']
        
        print(f"\n[{idx+1}/{total}] Processing {ticker} - {row['Company'][:30]}...")
        
        # Find IR page
        ir_url = find_ir_page(website)
        
        results.append({
            'Ticker': ticker,
            'Company': row.get('Company', ''),
            'Website': website,
            'IR_URL': ir_url,
            'Status': 'Found' if ir_url else 'Not Found'
        })
        
        # Be polite to servers
        time.sleep(1)
    
    return pd.DataFrame(results)


def get_dow30_companies():
    """
    Returns all current Dow 30 companies with their official websites
    """
    dow30 = pd.DataFrame({
        'Ticker': [
            'AAPL', 'AMGN', 'AMZN', 'AXP', 'BA', 
            'CAT', 'CRM', 'CSCO', 'CVX', 'DIS', 
            'GS', 'HD', 'HON', 'IBM', 'JNJ', 
            'JPM', 'KO', 'MCD', 'MMM', 'MRK', 
            'MSFT', 'NKE', 'NVDA', 'PG', 'SHW', 
            'TRV', 'UNH', 'V', 'VZ', 'WMT'
        ],
        'Company': [
            'Apple Inc.', 'Amgen Inc.', 'Amazon.com Inc.', 'American Express', 'Boeing Co.',
            'Caterpillar Inc.', 'Salesforce Inc.', 'Cisco Systems', 'Chevron Corp.', 'Walt Disney Co.',
            'Goldman Sachs', 'Home Depot', 'Honeywell', 'IBM', 'Johnson & Johnson',
            'JPMorgan Chase', 'Coca-Cola Co.', 'McDonald\'s Corp.', '3M Co.', 'Merck & Co.',
            'Microsoft Corp.', 'Nike Inc.', 'NVIDIA Corp.', 'Procter & Gamble', 'Sherwin-Williams',
            'Travelers Companies', 'UnitedHealth Group', 'Visa Inc.', 'Verizon', 'Walmart Inc.'
        ],
        'Website': [
            'https://www.apple.com',
            'https://www.amgen.com',
            'https://www.amazon.com',
            'https://www.americanexpress.com',
            'https://www.boeing.com',
            'https://www.caterpillar.com',
            'https://www.salesforce.com',
            'https://www.cisco.com',
            'https://www.chevron.com',
            'https://www.disney.com',
            'https://www.goldmansachs.com',
            'https://www.homedepot.com',
            'https://www.honeywell.com',
            'https://www.ibm.com',
            'https://www.jnj.com',
            'https://www.jpmorganchase.com',
            'https://www.coca-colacompany.com',
            'https://www.mcdonalds.com',
            'https://www.3m.com',
            'https://www.merck.com',
            'https://www.microsoft.com',
            'https://www.nike.com',
            'https://www.nvidia.com',
            'https://www.pg.com',
            'https://www.sherwin-williams.com',
            'https://www.travelers.com',
            'https://www.unitedhealthgroup.com',
            'https://www.visa.com',
            'https://www.verizon.com',
            'https://www.walmart.com'
        ]
    })
    
    return dow30


def safe_save_csv(results, filename):
    """
    Safely save CSV with error handling
    """
    try:
        results.to_csv(filename, index=False)
        return True
    except PermissionError:
        alt_filename = filename.replace('.csv', '_new.csv')
        try:
            results.to_csv(alt_filename, index=False)
            print(f"  Note: Saved as {alt_filename} (original file was locked)")
            return True
        except Exception as e:
            print(f"  Warning: Could not save {filename}: {e}")
            return False
    except Exception as e:
        print(f"  Warning: Could not save {filename}: {e}")
        return False


def main():
    print("=" * 80)
    print(" DOW 30 INVESTOR RELATIONS PAGE FINDER ")
    print(" ENHANCED VERSION - CHECKS SUBPATHS ON INVESTOR SUBDOMAINS ")
    print("=" * 80)
    print(f"\nDate: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("Now detects pages like /investor-home/default.aspx on investor subdomains")
    print("-" * 80)
    
    # Check if Chrome driver is available
    test_driver = setup_driver()
    if test_driver:
        test_driver.quit()
        print("✓ Selenium Chrome driver is available")
    else:
        print("⚠ Warning: Selenium not available, using requests only")
    
    # Get all Dow 30 companies
    dow30_companies = get_dow30_companies()
    
    print(f"\nLoaded {len(dow30_companies)} Dow 30 companies")
    print("\nStarting IR page discovery...")
    print("=" * 80)
    
    # Find IR pages for all companies
    results = process_companies(dow30_companies)
    
    # Display results
    print("\n" + "=" * 80)
    print(" RESULTS SUMMARY ")
    print("=" * 80)
    
    # Show found IR pages
    print("\n✓ SUCCESSFULLY FOUND IR PAGES:")
    print("-" * 80)
    print(f"{'Ticker':<8} {'Company':<30} {'IR URL':<70}")
    print("-" * 80)
    found = results[results['Status'] == 'Found']
    for _, row in found.iterrows():
        print(f"{row['Ticker']:<8} {row['Company'][:29]:<30} {row['IR_URL']:<70}")
    
    # Show not found
    not_found = results[results['Status'] == 'Not Found']
    if len(not_found) > 0:
        print("\n✗ COULD NOT FIND IR PAGES:")
        print("-" * 80)
        for _, row in not_found.iterrows():
            print(f"{row['Ticker']:<8} {row['Company'][:29]:<30} {row['Website']}")
    
    # Save results
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    filename = f'dow30_ir_pages_{timestamp}.csv'
    if safe_save_csv(results, filename):
        print(f"\n✓ Saved: {filename}")
    
    simple_filename = 'dow30_ir_pages_latest.csv'
    if safe_save_csv(results, simple_filename):
        print(f"✓ Saved: {simple_filename}")
    
    print("\n" + "=" * 80)
    print(" FINAL STATISTICS ")
    print("=" * 80)
    success = len(found)
    total = len(results)
    print(f"\nSuccess Rate: {success}/{total} ({100*success/total:.1f}%)")
    
    # Protocol statistics
    if success > 0:
        http_count = sum(1 for _, row in found.iterrows() if row['IR_URL'].startswith('http://'))
        https_count = sum(1 for _, row in found.iterrows() if row['IR_URL'].startswith('https://'))
        print(f"HTTP IR pages: {http_count}")
        print(f"HTTPS IR pages: {https_count}")
        
        # Subdomain vs path statistics
        subdomain_count = sum(1 for _, row in found.iterrows() 
                            if any(prefix in urlparse(row['IR_URL']).netloc 
                                  for prefix in ['investor', 'ir', 'stock']))
        path_count = success - subdomain_count
        print(f"Subdomain-based: {subdomain_count}")
        print(f"Path-based: {path_count}")
    
    print("\n✓ Process completed!")
    
    return results


if __name__ == "__main__":
    results = main()

# Requirements: 
# pip install requests beautifulsoup4 pandas selenium
# Also need ChromeDriver installed: https://chromedriver.chromium.org/