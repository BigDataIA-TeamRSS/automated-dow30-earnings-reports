#!/usr/bin/env python3
"""
Enhanced Selenium-based scraper that intelligently navigates IR websites
to find quarterly earnings reports by following promising links
"""

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
import time
import logging
from pathlib import Path
from bs4 import BeautifulSoup
import os
import csv
from urllib.parse import urljoin, urlparse
import re
from datetime import datetime
import random

# Project root (one level up from this `src` directory)
PROJECT_ROOT = Path(__file__).resolve().parents[1]

# Configure logging (log file at project root)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(str(PROJECT_ROOT / 'enhanced_selenium_scraper.log')),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

PATH_TO_CSV = str(PROJECT_ROOT / "dow30_companies.csv")

class DocumentLink:
    """Class to represent document links with metadata"""
    
    def __init__(self, href, text, title, link_type, full_html, source_url=None):
        self.href = href
        self.text = text.strip() if text else ""
        self.title = title.strip() if title else ""
        self.link_type = link_type  # "document" or "navigational"
        self.full_html = full_html
        self.source_url = source_url  # URL where this link was found
        self.file_extension = self._get_file_extension()
        self.document_type = self._classify_document_type()
    
    def _get_file_extension(self):
        """Extract file extension from href"""
        if not self.href:
            return ""
        return self.href.split('.')[-1].lower() if '.' in self.href else ""
    
    def _classify_document_type(self):
        """Classify the type of document based on extension and content"""
        if not self.file_extension:
            return "unknown"
        
        doc_types = {
            'pdf': 'PDF Document',
            'doc': 'Word Document',
            'docx': 'Word Document',
            'xls': 'Excel Spreadsheet',
            'xlsx': 'Excel Spreadsheet',
            'ppt': 'PowerPoint Presentation',
            'pptx': 'PowerPoint Presentation',
            'zip': 'Archive',
            'rar': 'Archive',
            'csv': 'CSV Data',
            'txt': 'Text Document',
            'rtf': 'Rich Text',
            'xml': 'XML Document',
            'json': 'JSON Data',
            'html': 'Web Page',
            'htm': 'Web Page',
            'wav': 'Audio File',
            'mp3': 'Audio File',
        }
        
        return doc_types.get(self.file_extension, f"{self.file_extension.upper()} File")
    
    def is_document(self):
        """Check if this is a document link"""
        return self.link_type == "document"
    
    def is_navigational(self):
        """Check if this is a navigational link"""
        return self.link_type == "navigational"
    
    def to_dict(self):
        """Convert to dictionary for export"""
        return {
            'href': self.href,
            'text': self.text,
            'title': self.title,
            'type': self.link_type,
            'file_extension': self.file_extension,
            'document_type': self.document_type,
            'source_url': self.source_url,
            'full_html': self.full_html
        }
    
    def __str__(self):
        return f"{self.text} ({self.title}) [{self.document_type}] -> {self.href}"
    
    def __hash__(self):
        """Make DocumentLink hashable for set operations"""
        return hash(self.href)
    
    def __eq__(self, other):
        """Check equality based on href"""
        if isinstance(other, DocumentLink):
            return self.href == other.href
        return False

class EnhancedSeleniumScraper:
    """Enhanced Selenium-based scraper with intelligent navigation"""
    
    def __init__(self, headless=True, max_promising_links=10):
        """Initialize Chrome WebDriver"""
        self.driver = None
        self.headless = headless
        self.visited_urls = set()
        self.document_links = set()  # Store unique document links
        self.max_promising_links = max_promising_links  # Configurable limit for promising links
        
        # Exclusion list for URLs that should be filtered out
        # These are typically third-party services or irrelevant domains
        self.exclusion_domains = {
            'q4inc.com',  # Q4 Inc. is a third-party IR platform provider
            'investorcalendar.com',
            'webcast.com',
            'eventbrite.com',
            'zoom.us',
            'teams.microsoft.com',
            'webex.com',
            'gotomeeting.com'
        }
        
        self.setup_driver()
    
    def setup_driver(self):
        """Setup Chrome WebDriver with options"""
        chrome_options = Options()
        
        if self.headless:
            chrome_options.add_argument("--headless")
        
        # Additional options for better compatibility
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--disable-gpu")
        chrome_options.add_argument("--window-size=1920,1080")
        chrome_options.add_argument("--user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
        chrome_options.add_argument("--lang=en-US,en;q=0.9")
        # Stealth: reduce obvious automation signals
        chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
        chrome_options.add_experimental_option("useAutomationExtension", False)
        chrome_options.add_argument("--disable-blink-features=AutomationControlled")
        
        try:
            self.driver = webdriver.Chrome(options=chrome_options)
            # Timeouts
            self.driver.set_page_load_timeout(30)
            self.driver.set_script_timeout(30)
            # Hide webdriver flag in navigator
            try:
                self.driver.execute_cdp_cmd(
                    "Page.addScriptToEvaluateOnNewDocument",
                    {
                        "source": "Object.defineProperty(navigator, 'webdriver', {get: () => undefined});"
                    },
                )
            except Exception:
                # Best-effort; ignore if CDP not available
                pass
            logger.info("Chrome WebDriver initialized successfully")
        except Exception as e:
            logger.error(f"Failed to initialize Chrome WebDriver: {e}")
    
    def get_rendered_content(self, url, wait_time=10, max_retries=2):
        """Get fully rendered page content with retries, backoff, and human-like actions"""
        attempt = 0
        backoff_seconds = 2
        while attempt <= max_retries:
            try:
                logger.info(f"Loading page: {url}")
                self.driver.get(url)
                
                # Wait for page to be interactive
                time.sleep(random.uniform(1.5, 3.5))
                WebDriverWait(self.driver, wait_time).until(
                    lambda driver: driver.execute_script("return document.readyState") in ("interactive", "complete")
                )
                # Give JS time, accept cookies, and perform a human-like scroll
                self._try_accept_cookies()
                self._human_like_scroll()
                time.sleep(random.uniform(0.8, 2.0))
                
                # Final readyState check
                WebDriverWait(self.driver, wait_time).until(
                    lambda driver: driver.execute_script("return document.readyState") == "complete"
                )
                
                page_source = self.driver.page_source
                soup = BeautifulSoup(page_source, 'html.parser')
                return soup
            except TimeoutException as te:
                logger.warning(f"Timeout loading {url} (attempt {attempt+1}/{max_retries+1}): {te}")
                try:
                    # Stop loading to recover
                    self.driver.execute_script("window.stop();")
                except Exception:
                    pass
            except Exception as e:
                logger.warning(f"Error getting content from {url} (attempt {attempt+1}/{max_retries+1}): {e}")
            
            # Backoff before retry
            attempt += 1
            if attempt <= max_retries:
                sleep_for = backoff_seconds * (2 ** (attempt - 1)) + random.uniform(0, 1.25)
                time.sleep(sleep_for)
        
        logger.error(f"Failed to load {url} after {max_retries+1} attempts")
        return None

    def _human_like_scroll(self):
        """Perform a few incremental scrolls to trigger lazy loading and mimic humans."""
        try:
            scroll_steps = random.randint(2, 4)
            total_height = self.driver.execute_script("return Math.max(document.body.scrollHeight, document.documentElement.scrollHeight);")
            viewport_height = self.driver.execute_script("return window.innerHeight || document.documentElement.clientHeight;")
            if not total_height or not viewport_height:
                return
            step_size = max(int(total_height / (scroll_steps + 1)), int(viewport_height * 0.6))
            current = 0
            for _ in range(scroll_steps):
                current = min(current + step_size, total_height)
                self.driver.execute_script("window.scrollTo(0, arguments[0]);", current)
                time.sleep(random.uniform(0.6, 1.4))
            # Scroll back a bit
            self.driver.execute_script("window.scrollTo(0, 0);")
        except Exception:
            pass

    def _try_accept_cookies(self):
        """Attempt to accept cookie consent banners if present."""
        try:
            # Try common button texts using XPath (case-insensitive via translate)
            xpaths = [
                "//button[contains(translate(., 'ACEIPTGRYOKNS', 'aceiptgryokns'), 'accept')]",
                "//button[contains(translate(., 'ACEIPTGRYOKNS', 'aceiptgryokns'), 'agree')]",
                "//button[contains(translate(., 'ACEIPTGRYOKNS', 'aceiptgryokns'), 'consent')]",
                "//button[contains(., 'Accept All')]",
                "//button[contains(., 'I Agree')]",
                "//button[contains(., 'Got it')]",
                "//a[contains(., 'Accept')]",
            ]
            for xp in xpaths:
                try:
                    elem = WebDriverWait(self.driver, 2).until(
                        EC.element_to_be_clickable((By.XPATH, xp))
                    )
                    elem.click()
                    time.sleep(random.uniform(0.2, 0.6))
                    break
                except Exception:
                    continue
        except Exception:
            pass
    
    def classify_link(self, href, base_url):
        """Classify a link as document, navigational, internal, or external"""
        if not href:
            return "invalid"
        
        href_lower = href.lower()
        
        # Check for document file extensions
        document_extensions = ('.pdf', '.doc', '.docx', '.xls', '.xlsx', '.ppt', '.pptx', 
                             '.zip', '.rar', '.csv', '.txt', '.rtf', '.xml', '.json')
        
        if href_lower.endswith(document_extensions):
            return "document"
        
        # Check for document-related keywords in URL
        document_keywords = ['file', 'download', 'document', 'attachment']
        
        if any(keyword in href_lower for keyword in document_keywords):
            return "document"
        
        # Check if it's internal or external
        if href.startswith('/'):
            return "internal"
            # what is the point of this line?
        
        if href.startswith(('http://', 'https://')):
            base_domain = urlparse(base_url).netloc
            link_domain = urlparse(href).netloc
            if base_domain == link_domain:
                return "internal"
            else:
                return "external"
        
        # Handle other relative URLs
        return "internal"
    
    def resolve_url(self, href, base_url):
        """Resolve relative URLs to absolute URLs"""
        if not href:
            return None
        
        if href.startswith(('http://', 'https://')):
            return href
        
        return urljoin(base_url, href)
    
    def is_url_excluded(self, url):
        """Check if a URL should be excluded based on domain exclusion list"""
        if not url:
            return True
        
        try:
            parsed_url = urlparse(url)
            domain = parsed_url.netloc.lower()
            
            # Check if domain is in exclusion list
            for excluded_domain in self.exclusion_domains:
                if excluded_domain in domain:
                    return True
            
            return False
        except Exception:
            return True  # Exclude if URL parsing fails
    
    def create_document_link(self, link_element, base_url, source_url=None):
        """Create a DocumentLink object from a BeautifulSoup link element"""
        href = link_element.get('href', '')
        text = link_element.get_text(strip=True)
        title = link_element.get('title', '')
        full_html = str(link_element)
        
        # Resolve the URL
        full_url = self.resolve_url(href, base_url)
        if not full_url:
            return None
        
        # Classify the link
        link_type = self.classify_link(full_url, base_url)
        
        # Create DocumentLink object
        doc_link = DocumentLink(
            href=full_url,
            text=text,
            title=title,
            link_type=link_type,
            full_html=full_html,
            source_url=source_url
        )
        
        return doc_link
    
    def find_quarterly_links(self, soup, base_url):
        """Find links that might lead to quarterly earnings pages"""
        # year = datetime.now().year
        # quarterly_keywords = [
        #     'quarterly results', 'quarterly report', 'quarterly earnings', 'financial information', 'financial report', 'q1'+year, 'q2'+year, 'q3'+year, 'q4'+year,
        #     '1q'+year, '2q'+year, '3q'+year, '4q'+year, '10-q', '10-k', 'press release', 'webcast'
        # ]
        quarterly_keywords = [
            'quarterly-result', 'quarterly-report', 'income-statement', 'quarterly-earning', 'financial-information', 'financial-report', 'q1', 'q2', 'q3', 'q4',
            '1q', '2q', '3q', '4q', '10-q', '10-k', 'financial-statements'
        ]
        promising_links = []
        all_links = soup.find_all('a', href=True)
        
        for link in all_links:
            href = link.get('href', '')
            text = link.get_text(strip=True).lower()
            title = link.get('title', '').lower()
            a = str(link)

            # Skip invalid links
            link_type = self.classify_link(href, base_url)
            if link_type == "invalid" or link_type == "document":
                continue
            
            # Resolve the URL
            full_url = self.resolve_url(href, base_url)
            if not full_url or full_url in self.visited_urls:
                continue
            
            # Check if URL should be excluded
            if self.is_url_excluded(full_url):
                continue
            
            score = sum(1 for keyword in quarterly_keywords if keyword in a)
            score = sum(1 for keyword in quarterly_keywords if keyword in text.replace(' ', '-'))
            score = sum(1 for keyword in quarterly_keywords if keyword in title.replace(' ', '-'))
            score = sum(1 for keyword in quarterly_keywords if keyword in full_url.replace(' ', '-'))

            # score += 1 if a in quarterly_keywords else 0
            # score += 1 if text.replace(' ', '-') in quarterly_keywords else 0
            # score += 1 if title.replace(' ', '-') in quarterly_keywords else 0
            # Check if link text or title contains quarterly keywords
            # combined_text = f"{text} {title}"
            # score = sum(1 for keyword in quarterly_keywords if keyword in combined_text)
            
            if score > 0:
                promising_links.append({
                    'url': full_url,
                    'text': link.get_text(strip=True),
                    'title': link.get('title', ''),
                    'score': score,
                    'full_html': str(link)
                })
        
        # Sort by score (highest first) and return top candidates
        promising_links.sort(key=lambda x: x['score'], reverse=True)
        
        # Limit the number of promising links to prevent overwhelming the crawler
        # This helps focus on the most relevant quarterly earnings pages while
        # avoiding getting lost in too many navigation paths
        return promising_links[:self.max_promising_links]
    
    def extract_all_links(self, soup, base_url, source_url=None):
        """Extract all links from a page and classify them"""
        all_links = soup.find_all('a', href=True)
        
        for link in all_links:
            doc_link = self.create_document_link(link, base_url, source_url)
            if doc_link:
                # Add to document links set (automatically handles uniqueness)
                self.document_links.add(doc_link)
        
        return len(all_links)  # Return count of links processed
    
    def crawl_company_ir_site(self, company_name, base_url, max_depth=2):
        """Crawl a company's IR site to find quarterly earnings reports"""
        logger.info(f"Starting enhanced crawl for {company_name} at {base_url}")
        
        # Clear document links for this company
        self.document_links.clear()
        
        urls_to_visit = [(base_url, 0)]  # (url, depth)
        
        while urls_to_visit:
            current_url, depth = urls_to_visit.pop(0)
            
            if current_url in self.visited_urls or depth > max_depth:
                continue
            
            logger.info(f"Visiting {current_url} (depth: {depth})")
            self.visited_urls.add(current_url)
            
            # Get page content
            soup = self.get_rendered_content(current_url)
            if not soup:
                logger.error(f"Failed to get rendered content from {current_url}")
                continue
            
            # Extract all links from current page
            links_processed = self.extract_all_links(soup, current_url, current_url)
            logger.info(f"Processed {links_processed} links from {current_url}")
            
            # If this is the homepage (depth 0), look for promising quarterly links
            if depth == 0:
                promising_links = self.find_quarterly_links(soup, current_url)
                logger.info(f"Found {len(promising_links)} promising quarterly links")
                
                # Add promising links to visit queue
                for link in promising_links:  # Limit to top 5 most promising
                    if link['url'] not in self.visited_urls:
                        urls_to_visit.append((link['url'], depth + 1))
                        logger.info(f"Added to queue: {link['text']} -> {link['url']}")
            
            # Human-like delay between requests

            time.sleep(random.randint(2, 4))
        
        # Filter to only document links
        document_links = [link for link in self.document_links if link.is_document()]
        navigational_links = [link for link in self.document_links if link.is_navigational()]
        
        logger.info(f"Completed crawl for {company_name}")
        logger.info(f"Total links found: {len(self.document_links)}")
        logger.info(f"Document links: {len(document_links)}")
        logger.info(f"Navigational links: {len(navigational_links)}")
        
        return document_links  # Return only document links
    
    def save_content(self, content, filename):
        """Save content to file"""
        try:
            with open(filename, 'w', encoding='utf-8') as f:
                f.write(content)
            logger.info(f"Content saved to {filename}")
        except Exception as e:
            print(f"Error saving content: {e}")
            logger.error(f"Error saving content: {e}")
    
    def close(self):
        """Close the WebDriver"""
        if self.driver:
            self.driver.quit()
            logger.info("WebDriver closed")

def get_investor_relation_urls(csv_path="dow30_companies.csv"):
    """
    Reads the dow30_companies.csv file and returns a list of Investor Relations URLs.
    """
    urls = {}
    try:
        with open(csv_path, "r", encoding="utf-8") as f:
            csv_reader = csv.reader(f)
            # Skip header
            next(csv_reader)
            for row in csv_reader:
                # The IR URL is the 4th column (index 3)
                if len(row) >= 4:
                    url = row[3].strip()
                    if url:
                        urls[row[1]] = url
    except Exception as e:
        print(f"Error reading {csv_path}: {e}")
    return urls

def main():
    """Main function to run enhanced scraper"""
    ir_links_dir = Path(PROJECT_ROOT / "ir_links")
    os.makedirs(ir_links_dir, exist_ok=True)
    scraper = EnhancedSeleniumScraper(headless=False)  # Set to True for headless mode
    
    try:
        urls = get_investor_relation_urls(PATH_TO_CSV)
        
        # Test with a few companies first
        # test_companies = ["Apple", "Microsoft", "Amazon", "JPMorgan Chase", "Verizon", "IBM"]
        # test_companies = list(urls.keys())
        # test_companies = ["Microsoft"]
        
        for company, url in urls.items():
            # if company in test_companies:
            if True:
                print(f"\n{'='*60}")
                print(f"Processing {company}: {url}")
                print(f"{'='*60}")
                
                # Crawl the IR site
                document_links = scraper.crawl_company_ir_site(company, url)
                
                # Save document links to file with enhanced metadata
                output_file = ir_links_dir / f"financial_links_{company}.txt"
                with open(output_file, "w", encoding="utf-8") as f:
                    for doc_link in document_links:
                        # Write enhanced format with all metadata
                        f.write(f"title='{doc_link.title}' text='{doc_link.text}' url='{doc_link.href}' type='{doc_link.link_type}' file_extension='{doc_link.file_extension}' document_type='{doc_link.document_type}' source_url='{doc_link.source_url}' full_html='{doc_link.full_html}'\n")
                
                print(f"Saved {len(document_links)} document links to {output_file}")
                
                # Show sample of document types found
                doc_types = {}
                for doc_link in document_links:
                    doc_type = doc_link.document_type
                    doc_types[doc_type] = doc_types.get(doc_type, 0) + 1
                
                print(f"Document types found:")
                for doc_type, count in sorted(doc_types.items()):
                    print(f"  {doc_type}: {count}")
                
                # Reset visited URLs for next company
                scraper.visited_urls.clear()
    
    finally:
        scraper.close()

if __name__ == "__main__":
    main()
