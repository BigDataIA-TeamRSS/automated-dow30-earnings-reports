import instructor
from pydantic import BaseModel
import os
import logging
import time
from datetime import datetime
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv(Path(__file__).parent.parent / ".env")
PROJECT_ROOT = Path(__file__).resolve().parents[1]


# def truncate_text_for_free_tier(text: str) -> str:
#     """Truncate text to stay within free tier limits"""
#     char_count = len(text)
    
#     # Free tier limit: 250k tokens per minute
#     # Rough estimation: 1 token ≈ 1.5 characters for English text
#     # So 250k tokens ≈ 375k characters
#     # Be conservative: use 300k characters as limit
    
#     if char_count > 300000:
#         logger.info(f"Text too large ({char_count} chars), truncating to 300k characters")
#         truncated = text[:300000]
#         # Try to end at a reasonable point (end of a tag or word)
#         last_tag = truncated.rfind('>')
#         if last_tag > 250000:  # If we can find a reasonable break point
#             truncated = truncated[:last_tag + 1]
#         logger.info(f"Truncated to {len(truncated)} characters")
#         return truncated
    
#     logger.info(f"Text size OK ({char_count} chars)")
#     return text


def select_model_based_on_size(text: str) -> str:
    """Select appropriate model based on text size"""
    word_count = len(text.split())
    char_count = len(text)
    
    logger.info(f"Text size: {word_count} words, {char_count} characters")
    
    # If more than 150k words OR 200k characters, use higher context model
    if char_count > 200000:
        logger.info(f"Large text detected, using gemini-2.0-flash")
        return "gemini-2.0-flash"
    else:
        logger.info(f"Text size within limits, using gemini-2.5-flash")
        return "gemini-flash-latest"


# Configure logging
# Ensure logs directory exists

PROJECT_ROOT = Path(__file__).resolve().parents[1]
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(str(PROJECT_ROOT / 'logs' / 'extract_reports.log')),
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
        project_root = Path(__file__).resolve().parents[1]
        file_path = project_root / "ir_links" / file
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

        # Rate limiting: ensure 15-second gap between API calls across all workers
        lock_file = ".api_call_lock"
        if os.path.exists(lock_file):
            with open(lock_file, 'r') as f:
                last_call = datetime.fromisoformat(f.read().strip())
            gap = (datetime.now() - last_call).total_seconds()
            if gap < 30:
                time.sleep(30 - gap)
        
        with open(lock_file, 'w') as f:
            f.write(datetime.now().isoformat())

        # Initialize the AI client
        # client = instructor.from_provider(
        #     # "google/gemini-flash-lite-latest",
        #     # "google/gemini-2.0-flash"
        #     "google/gemini-2.5-flash",
        #     # api_key=api_key,
        # )
        # logger.info("Gemini client initialized successfully")

        # Truncate text to stay within free tier limits
        # html = truncate_text_for_free_tier(html)
        selected_model = select_model_based_on_size(html)
        # Initialize the AI client (always use 2.0-flash for free tier)
        client = instructor.from_provider(f"google/{selected_model}")
        logger.info(f"Gemini client initialized with {selected_model}")
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
        project_root = Path(__file__).resolve().parents[1]
        output_file = project_root / "extracted_reports" / f"extracted_reports_{company_name}.txt"
        
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
    import sys
    
    # Get company from command line argument
    if len(sys.argv) > 1 and sys.argv[1] == "--companies" and len(sys.argv) > 2:
        target_company = sys.argv[2]
    else:
        print("Usage: python extract_reports.py --companies <company_name>")
        exit(1)
    
    logger.info("Starting report extraction process")
    
    # Create output directory
    extracted_dir = PROJECT_ROOT / "extracted_reports"
    ir_links_dir = PROJECT_ROOT / "ir_links"
    
    os.makedirs(extracted_dir, exist_ok=True)
    logger.info("Created extracted_reports directory")
    if not os.path.exists(ir_links_dir):
        logger.error(f"Directory {ir_links_dir} does not exist")
        exit(1)
    
    # Look for the specific company file
    company_file = f"financial_links_{target_company}.txt"
    if company_file not in os.listdir(ir_links_dir):
        logger.error(f"File not found for company {target_company}: {company_file}")
        exit(1)
    
    logger.info(f"Processing file: {company_file}")
    print(f"\n{'='*60}")
    print(f"Processing: {company_file}")
    print(f"{'='*60}")
    
    try:
        reports_count = extract_reports(company_file)
        logger.info(f"Successfully processed {company_file} - found {reports_count} reports")
        print(f"✅ Extraction complete: {reports_count} reports found")
    except Exception as e:
        logger.error(f"Failed to process {company_file}: {e}")
        print(f"❌ Error processing {company_file}: {e}")
        exit(1)