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
    from .simple_metadata_collector import SimpleMetadataCollector, create_file_metadata
except ImportError:
    # Fall back to absolute imports (when run directly)
    from enhanced_selenium_scraper import EnhancedSeleniumScraper, get_investor_relation_urls
    from extract_reports import extract_reports
    from download_reports import parse_report_file, download_file
    from simple_metadata_collector import SimpleMetadataCollector, create_file_metadata

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
# COMPANIES = ["Apple"]  # Test with Disney first
COMPANIES = None # Set to specific companies list to test, None for all companies
MAX_WORKERS = 10        # Reduced parallel workers to prevent bot detection

def process_company(company_name, company_url, ticker):
    """Process a single company through all three stages with metadata collection"""
    logger.info(f"Starting processing for {company_name}")
    
    # Initialize metadata collector
    metadata_collector = SimpleMetadataCollector()
    
    try:
        # Start metadata collection
        metadata = metadata_collector.start_company_processing(company_name, ticker, company_url)
        
        # Stage 1: Scraping
        logger.info(f"Stage 1: Starting scraping for {company_name}")
        metadata_collector.update_scraping_start()
        
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
        
        # Update scraping metadata
        metadata_collector.update_scraping_complete(document_links, len(scraper.visited_urls), 2)
        logger.info(f"✅ Stage 1 completed: Scraping successful for {company_name}")
        
        # Stage 2: Extraction
        logger.info(f"Stage 2: Starting extraction for {company_name}")
        
        # Read file to get text size for metadata
        with open(ir_file, "r", encoding="utf-8") as f:
            html_content = f.read()
        
        text_size_chars = len(html_content)
        metadata_collector.update_extraction_start(text_size_chars, "google/gemini")
        
        extracted_dir = PROJECT_ROOT / "extracted_reports"
        extracted_dir.mkdir(exist_ok=True)
        
        # Record extraction start time
        extraction_start = datetime.now()
        extract_reports(f"financial_links_{company_name}.txt")
        extraction_duration = (datetime.now() - extraction_start).total_seconds()
        
        # Read original financial links to get source_url and file_extension mapping
        ir_file = PROJECT_ROOT / "ir_links" / f"financial_links_{company_name}.txt"
        url_to_metadata = {}
        if ir_file.exists():
            with open(ir_file, "r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if line:
                        import re
                        url_match = re.search(r"url='([^']+)'", line)
                        source_url_match = re.search(r"source_url='([^']+)'", line)
                        file_extension_match = re.search(r"file_extension='([^']+)'", line)
                        
                        if url_match:
                            url_to_metadata[url_match.group(1)] = {
                                'source_url': source_url_match.group(1) if source_url_match else '',
                                'file_extension': file_extension_match.group(1) if file_extension_match else ''
                            }
        
        # Read extracted reports
        report_file = PROJECT_ROOT / "extracted_reports" / f"extracted_reports_{company_name}.txt"
        extracted_reports_data = []
        if report_file.exists():
            with open(report_file, "r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if line:
                        # Parse the report line (format: Report(title='...', category='...', url='...', year=..., quarter=...))
                        try:
                            # Simple parsing - in real implementation, you'd want more robust parsing
                            import re
                            title_match = re.search(r"title='([^']+)'", line)
                            category_match = re.search(r"category='([^']+)'", line)
                            url_match = re.search(r"url='([^']+)'", line)
                            year_match = re.search(r"year=(\d+)", line)
                            quarter_match = re.search(r"quarter=(\d+)", line)
                            
                            if url_match:
                                url = url_match.group(1)
                                metadata = url_to_metadata.get(url, {})
                                report_data = {
                                    'title': title_match.group(1) if title_match else '',
                                    'category': category_match.group(1) if category_match else '',
                                    'url': url,
                                    'year': int(year_match.group(1)) if year_match else None,
                                    'quarter': int(quarter_match.group(1)) if quarter_match else None,
                                    'source_url': metadata.get('source_url', ''),
                                    'file_extension': metadata.get('file_extension', '')
                                }
                                extracted_reports_data.append(report_data)
                        except Exception as e:
                            logger.warning(f"Failed to parse report line: {line[:100]}... Error: {e}")
        
        metadata_collector.update_extraction_complete(extracted_reports_data, extraction_duration)
        logger.info(f"✅ Stage 2 completed: Extraction successful for {company_name}")
        
        # Stage 3: Download
        logger.info(f"Stage 3: Starting download for {company_name}")
        
        downloads_dir = PROJECT_ROOT / "downloads"
        downloads_dir.mkdir(exist_ok=True)
        
        if report_file.exists():
            urls_data = parse_report_file(str(report_file))
            # Add source_url and file_extension to each URL data
            for url_data in urls_data:
                url = url_data['url']
                metadata = url_to_metadata.get(url, {})
                url_data['source_url'] = metadata.get('source_url', '')
                url_data['file_extension'] = metadata.get('file_extension', '')
            
            metadata_collector.update_download_start(len(urls_data))
            
            downloaded_count = 0
            failed_count = 0
            
            for url_data in urls_data:
                success = download_file(url_data, company_name, str(PROJECT_ROOT / "downloads"))
                
                # Create file metadata
                if success:
                    # Find the downloaded file (this is a simplified approach)
                    # In a real implementation, you'd track the exact file path from download_file
                    company_dir = downloads_dir / company_name
                    if company_dir.exists():
                        # Get the most recently created file in the company directory
                        files = list(company_dir.glob("*"))
                        if files:
                            latest_file = max(files, key=lambda f: f.stat().st_mtime)
                            file_metadata = create_file_metadata(
                                str(latest_file),
                                url_data['url'],
                                url_data['title'],
                                url_data['category'],
                                url_data['year'],
                                url_data['quarter'],
                                url_data.get('source_url', ''),
                                url_data.get('file_extension', '')
                            )
                            metadata_collector.update_download_progress(file_metadata)
                            downloaded_count += 1
                else:
                    failed_count += 1
                    # Create failed file metadata
                    failed_metadata = {
                        'filename': '',
                        'file_path': '',
                        'file_size': 0,
                        'url': url_data['url'],
                        'title': url_data['title'],
                        'category': url_data['category'],
                        'year': url_data['year'],
                        'quarter': url_data['quarter'],
                        'download_timestamp': datetime.now().isoformat(),
                        'source_url': url_data.get('source_url', ''),
                        'file_extension': url_data.get('file_extension', ''),
                        'success': False
                    }
                    metadata_collector.update_download_progress(failed_metadata)
            
            metadata_collector.update_download_complete()
            logger.info(f"✅ Stage 3 completed: Downloaded {downloaded_count}/{len(urls_data)} files for {company_name}")
        else:
            logger.warning(f"No report file found for {company_name}")
            metadata_collector.update_download_failed("No report file found")
        
        # Complete pipeline
        metadata_collector.complete_company_processing(success=True)
        
        logger.info(f"✅ {company_name} completed successfully through all stages!")
        return {"name": company_name, "status": "success"}
        
    except Exception as e:
        logger.error(f"❌ {company_name} failed: {str(e)}")
        metadata_collector.complete_company_processing(success=False, error_message=str(e))
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
            executor.submit(process_company, company['name'], company['url'], company['ticker']): company 
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
    
    print(f"\n💾 Metadata stored in JSON format: {PROJECT_ROOT / 'metadata'}")
    print(f"{'='*60}")

if __name__ == "__main__":
    main()