#!/usr/bin/env python3
"""
Simplified Orchestrator for the automated Dow 30 earnings reports pipeline.
Always runs in parallel unless COMPANIES is set.
"""

import csv
import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from datetime import datetime
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Project root (one level up from src directory)
PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = Path(__file__).resolve().parent

# Import the scraper and extractor classes directly
try:
    # Try relative imports first (when used as a module)
    from .enhanced_selenium_scraper import EnhancedSeleniumScraper, get_investor_relation_urls
    from .extract_reports import extract_reports
    from .download_reports import parse_report_file, download_file
except ImportError:
    # Fall back to absolute imports (when run directly)
    from enhanced_selenium_scraper import EnhancedSeleniumScraper, get_investor_relation_urls
    from extract_reports import extract_reports
    from download_reports import parse_report_file, download_file

# Configure logging
# Ensure logs directory exists
(PROJECT_ROOT / 'logs').mkdir(exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(str(PROJECT_ROOT / 'logs' / 'orchestrator.log')),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Configuration
# COMPANIES = ["Honeywell"]  
COMPANIES = None # Set to specific companies list to test, None for all companies
MAX_WORKERS = 10        # Reduced parallel workers to prevent bot detection

def process_company(company_name, company_url):
    """Process a single company through all three stages"""
    logger.info(f"Starting processing for {company_name}")
    
    try:
        # Stage 1: Scraping
        logger.info(f"Stage 1: Starting scraping for {company_name}")
        scraper = EnhancedSeleniumScraper(headless=True)
        document_links = scraper.crawl_company_ir_site(company_name, company_url)
        # Persist scraped links for extraction stage
        ir_dir = PROJECT_ROOT / "ir_links"
        ir_dir.mkdir(exist_ok=True)
        ir_file = ir_dir / f"financial_links_{company_name}.txt"
        try:
            with open(ir_file, "w", encoding="utf-8") as f:
                for doc_link in document_links:
                    f.write(
                        f"title='{doc_link.title}' text='{doc_link.text}' url='{doc_link.href}' type='{doc_link.link_type}' file_extension='{doc_link.file_extension}' document_type='{doc_link.document_type}' source_url='{doc_link.source_url}' full_html='{doc_link.full_html}'\n"
                    )
            logger.info(f"Saved {len(document_links)} document links to {ir_file}")
        except Exception as e:
            logger.warning(f"Failed to save document links for {company_name}: {e}")
        logger.info(f"✅ Stage 1 completed: Scraping successful for {company_name}")
        
        # Stage 2: Extraction
        logger.info(f"Stage 2: Starting extraction for {company_name}")
        extracted_dir = PROJECT_ROOT / "extracted_reports"
        extracted_dir.mkdir(exist_ok=True)
        extract_reports(f"financial_links_{company_name}.txt")
        logger.info(f"✅ Stage 2 completed: Extraction successful for {company_name}")
        
        # Stage 3: Download
        logger.info(f"Stage 3: Starting download for {company_name}")
        downloads_dir = PROJECT_ROOT / "downloads"
        downloads_dir.mkdir(exist_ok=True)
        report_file = PROJECT_ROOT / "extracted_reports" / f"extracted_reports_{company_name}.txt"
        if report_file.exists():
            urls_data = parse_report_file(str(report_file))
            downloaded_count = 0
            for url_data in urls_data:
                if download_file(url_data, company_name, str(PROJECT_ROOT / "downloads")):
                    downloaded_count += 1
            logger.info(f"✅ Stage 3 completed: Downloaded {downloaded_count}/{len(urls_data)} files for {company_name}")
        else:
            logger.warning(f"No report file found for {company_name}")
        
        logger.info(f"✅ {company_name} completed successfully through all stages!")
        return {"name": company_name, "status": "success"}
        
    except Exception as e:
        logger.error(f"❌ {company_name} failed: {str(e)}")
        return {"name": company_name, "status": "failed", "error": str(e)}

def load_companies():
    """Load companies from CSV file"""
    logger.info("Loading companies from dow30_companies.csv")
    companies = []
    csv_file = PROJECT_ROOT / "dow30_companies.csv"
    
    with open(csv_file, 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            companies.append({
                'name': row['Company'],
                'ticker': row['Ticker'],
                'url': row['Investor_Relations_URL']
            })
    
    logger.info(f"Loaded {len(companies)} companies from dow30_companies.csv")
    return companies

def main():
    """Main function - super simple"""
    logger.info("Starting Dow 30 earnings reports pipeline")
    
    # Load companies
    companies = load_companies()
    
    # Filter to test companies if specified
    if COMPANIES:
        companies = [c for c in companies if c['name'] in COMPANIES]
        logger.info(f"Filtering to test companies: {COMPANIES}")
        print(f"Running in TEST mode with companies: {COMPANIES}")
    else:
        print("Processing all Dow 30 companies")
    
    # Always run in parallel
    # Use minimum of MAX_WORKERS and number of companies
    actual_workers = min(MAX_WORKERS, len(companies))
    logger.info(f"Running in PARALLEL mode with {actual_workers} workers (for {len(companies)} companies)")
    print(f"Running in PARALLEL mode with {actual_workers} workers (for {len(companies)} companies)")
    
    start_time = datetime.now()
    completed = []
    failed = []
    
    # Process companies in parallel
    with ThreadPoolExecutor(max_workers=actual_workers) as executor:
        # Submit all tasks
        future_to_company = {
            executor.submit(process_company, company['name'], company['url']): company 
            for company in companies
        }
        
        # Process completed tasks
        for future in as_completed(future_to_company):
            company = future_to_company[future]
            try:
                result = future.result()
                if result['status'] == 'success':
                    completed.append(result)
                    print(f"✅ {result['name']} completed successfully!")
                else:
                    failed.append(result)
                    print(f"❌ {result['name']} failed: {result.get('error', 'Unknown error')}")
            except Exception as e:
                failed.append({"name": company['name'], "status": "failed", "error": str(e)})
                print(f"❌ {company['name']} failed: {str(e)}")
    
    end_time = datetime.now()
    elapsed = end_time - start_time
    
    # Print final summary
    print(f"\n{'='*60}")
    print("PIPELINE COMPLETED")
    print(f"{'='*60}")
    print(f"Total Runtime: {elapsed}")
    print(f"Total Companies: {len(companies)}")
    print(f"Successfully Completed: {len(completed)}")
    print(f"Failed: {len(failed)}")
    
    if completed:
        print(f"\n✅ Successfully Completed:")
        for result in completed:
            print(f"   - {result['name']}")
    
    if failed:
        print(f"\n❌ Failed:")
        for result in failed:
            print(f"   - {result['name']}")
    
    print(f"{'='*60}")

if __name__ == "__main__":
    main()