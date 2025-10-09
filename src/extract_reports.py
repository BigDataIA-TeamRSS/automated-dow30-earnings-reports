import instructor
from pydantic import BaseModel
import os
import logging
import time
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('extract_reports.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class Report(BaseModel):
    title: str 
    category: str
    url: str
    year: int
    quarter: int

def extract_reports(file):
    """Extract financial reports from a company's link file using AI"""
    logger.info(f"Starting extraction for file: {file}")
    
    try:
        # Read the file content
        file_path = f"../ir_links/{file}" if not file.startswith("../ir_links/") else file
        logger.info(f"Reading file: {file_path}")
        
        with open(file_path, "r", encoding="utf-8") as f:
            html = f.read()
            logger.info(f"Successfully read {len(html)} characters from {file_path}")
            print(f"Processing {len(html)} characters of data...")

        # Check for API key
        api_key = os.environ.get("GEMINI_API_KEY")
        if not api_key:
            logger.error("GEMINI_API_KEY is not set in the environment")
            raise RuntimeError("GEMINI_API_KEY is not set in the environment. Export it before running.")
        
        logger.info("API key found, initializing Gemini client")

        # Initialize the AI client
        client = instructor.from_provider(
            "google/gemini-flash-lite-latest",
            # "google/gemini-2.0-flash"
            # api_key=api_key,
        )
        logger.info("Gemini client initialized successfully")

        # Make API call to extract reports
        logger.info("Sending request to Gemini API for report extraction")
        start_time = datetime.now()
        
        resp = client.messages.create(
            messages=[
                {
                    "role": "user",
                    "content": f"Extract the documents you can find for the latest financial quarter only among these a tags. eg: if you have q3fy2024, q4fy2024, q1fy2025, q2fy2025, q3fy2025, q4fy2025, then you should only return the documents for the latest financial quarter i.e. q4fy2025. If you can't find any valid financial documents, return an empty list. If you are unsure due to limited information, open the link and get a sense of the financial documents. Return a comma separated list of all the documents you can find in a structured fashion in this format (title, text, url, full html). \n {html}",
                }
            ],
            response_model=list[Report],
        )
        
        end_time = datetime.now()
        processing_time = (end_time - start_time).total_seconds()
        logger.info(f"API call completed in {processing_time:.2f} seconds")

        # Process and save results
        company_name = file.replace("financial_links_", "").replace(".txt", "")
        output_file = f"../extracted_reports/extracted_reports_{company_name}.txt"
        
        logger.info(f"Found {len(resp)} reports for {company_name}")
        
        with open(output_file, "w", encoding="utf-8") as f:
            for i, report in enumerate(resp, 1):
                logger.info(f"Report {i}: {report.title} ({report.category}) - Q{report.quarter} {report.year}")
                f.write(str(report) + "\n")
        
        logger.info(f"Successfully saved {len(resp)} reports to {output_file}")
        
        return len(resp)
        
    except FileNotFoundError as e:
        logger.error(f"File not found: {e}")
        raise
    except Exception as e:
        logger.error(f"Error extracting reports from {file}: {e}")
        raise


if __name__ == "__main__":
    logger.info("Starting report extraction process")
    
    # Create output directory
    os.makedirs("../extracted_reports", exist_ok=True)
    logger.info("Created extracted_reports directory")
    
    # Get list of files to process
    ir_links_dir = "../ir_links"
    if not os.path.exists(ir_links_dir):
        logger.error(f"Directory {ir_links_dir} does not exist")
        exit(1)
    
    files_to_process = [f for f in os.listdir(ir_links_dir) if f.endswith(".txt")]
    logger.info(f"Found {len(files_to_process)} files to process")
    
    if not files_to_process:
        logger.warning("No .txt files found in ir_links directory")
        exit(0)
    
    # Process each file
    total_reports = 0
    successful_extractions = 0
    failed_extractions = 0
    
    for i, file in enumerate(files_to_process, 1):
        logger.info(f"Processing file {i}/{len(files_to_process)}: {file}")
        print(f"\n{'='*60}")
        print(f"Processing {i}/{len(files_to_process)}: {file}")
        print(f"{'='*60}")
        
        try:
            reports_count = extract_reports(file)
            total_reports += reports_count
            successful_extractions += 1
            logger.info(f"Successfully processed {file} - found {reports_count} reports")
            time.sleep(15)
        except Exception as e:
            failed_extractions += 1
            logger.error(f"Failed to process {file}: {e}")
            print(f"Error processing {file}: {e}")
    
    # Summary
    logger.info("="*60)
    logger.info("EXTRACTION SUMMARY")
    logger.info("="*60)
    logger.info(f"Total files processed: {len(files_to_process)}")
    logger.info(f"Successful extractions: {successful_extractions}")
    logger.info(f"Failed extractions: {failed_extractions}")
    logger.info(f"Total reports extracted: {total_reports}")
    logger.info("="*60)
    
    print(f"\n{'='*60}")
    print("EXTRACTION COMPLETE")
    print(f"{'='*60}")
    print(f"Files processed: {len(files_to_process)}")
    print(f"Successful: {successful_extractions}")
    print(f"Failed: {failed_extractions}")
    print(f"Total reports: {total_reports}")
    print(f"{'='*60}")