#!/usr/bin/env python3
"""
Simple metadata collector for the Dow 30 earnings reports pipeline.
Stores essential metadata in JSON format - no database complexity.
"""

import json
import logging
import hashlib
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Any, Optional

# Project root (one level up from src directory)
PROJECT_ROOT = Path(__file__).resolve().parents[1]

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(str(PROJECT_ROOT / 'logs' / 'simple_metadata_collector.log')),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class SimpleMetadataCollector:
    """Simple metadata collector that stores data in JSON format"""
    
    def __init__(self, output_dir: Optional[Path] = None):
        self.output_dir = output_dir or PROJECT_ROOT / "metadata"
        self.output_dir.mkdir(exist_ok=True)
        self.current_metadata: Dict[str, Any] = {}
        
    def start_company_processing(self, company_name: str, ticker: str, ir_url: str) -> Dict[str, Any]:
        """Start tracking metadata for a company"""
        logger.info(f"Starting metadata collection for {company_name}")
        
        self.current_metadata = {
            "company": company_name,
            "ticker": ticker,
            "ir_url": ir_url,
            "pipeline_start_time": datetime.now().isoformat(),
            "pipeline_end_time": None,
            "status": "in_progress",
            "error_message": None,
            "scraping_start_time": None,
            "scraping_end_time": None,
            "urls_visited": 0,
            "urls_found": 0,
            "extraction_start_time": None,
            "extraction_end_time": None,
            "model_used": None,
            "download_start_time": None,
            "download_end_time": None,
            "downloaded_files": []
        }
        
        return self.current_metadata
    
    def update_scraping_start(self):
        """Mark scraping stage as started"""
        self.current_metadata["scraping_start_time"] = datetime.now().isoformat()
        logger.info(f"Scraping started for {self.current_metadata['company']}")
    
    def update_scraping_complete(self, document_links: List[Any], pages_visited: int, max_depth: int):
        """Update scraping metadata with results"""
        self.current_metadata["scraping_end_time"] = datetime.now().isoformat()
        self.current_metadata["urls_visited"] = pages_visited
        self.current_metadata["urls_found"] = len(document_links)
        logger.info(f"Scraping completed for {self.current_metadata['company']}: {len(document_links)} documents found")
    
    def update_extraction_start(self, text_size_chars: int, ai_model: str):
        """Mark extraction stage as started"""
        self.current_metadata["extraction_start_time"] = datetime.now().isoformat()
        self.current_metadata["model_used"] = ai_model
        logger.info(f"Extraction started for {self.current_metadata['company']} using {ai_model}")
    
    def update_extraction_complete(self, reports: List[Any], api_duration: float):
        """Update extraction metadata with results"""
        self.current_metadata["extraction_end_time"] = datetime.now().isoformat()
        logger.info(f"Extraction completed for {self.current_metadata['company']}: {len(reports)} reports found")
    
    def update_download_start(self, files_to_download: int):
        """Mark download stage as started"""
        self.current_metadata["download_start_time"] = datetime.now().isoformat()
        logger.info(f"Download started for {self.current_metadata['company']}: {files_to_download} files")
    
    def update_download_progress(self, file_info: Dict[str, Any]):
        """Update download progress with file information"""
        if file_info.get('success', False):
            # Calculate checksum for successful downloads
            file_path = file_info.get('file_path', '')
            checksum = self._calculate_checksum(file_path) if file_path else None
            
            file_data = {
                "title": file_info.get('title', ''),
                "size": file_info.get('file_size', 0),
                "checksum": checksum,
                "quarter": file_info.get('quarter'),
                "year": file_info.get('year'),
                "url": file_info.get('url', ''),
                "download_timestamp": file_info.get('download_timestamp', ''),
                "source_page": file_info.get('source_url', ''),
                "file_type": file_info.get('file_extension', '')
            }
            self.current_metadata["downloaded_files"].append(file_data)
    
    def update_download_complete(self):
        """Mark download stage as completed"""
        self.current_metadata["download_end_time"] = datetime.now().isoformat()
        logger.info(f"Download completed for {self.current_metadata['company']}: {len(self.current_metadata['downloaded_files'])} files")
    
    def complete_company_processing(self, success: bool = True, error_message: str = None):
        """Complete company processing and save metadata"""
        self.current_metadata["pipeline_end_time"] = datetime.now().isoformat()
        self.current_metadata["status"] = "completed" if success else "failed"
        self.current_metadata["error_message"] = error_message
        
        # Save metadata to file
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"metadata_{self.current_metadata['company']}_{timestamp}.json"
        filepath = self.output_dir / filename
        
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(self.current_metadata, f, indent=2, ensure_ascii=False)
        
        logger.info(f"Metadata saved to {filepath}")
        return filepath
    
    def _calculate_checksum(self, file_path: str) -> Optional[str]:
        """Calculate MD5 checksum of a file"""
        try:
            if not file_path or not Path(file_path).exists():
                return None
            
            hash_md5 = hashlib.md5()
            with open(file_path, "rb") as f:
                for chunk in iter(lambda: f.read(4096), b""):
                    hash_md5.update(chunk)
            return hash_md5.hexdigest()
        except Exception as e:
            logger.warning(f"Could not calculate checksum for {file_path}: {e}")
            return None

def create_file_metadata(file_path: str, url: str, title: str, category: str, year: int, quarter: int, source_url: str = '', file_extension: str = '') -> Dict[str, Any]:
    """Create file metadata for download tracking"""
    file_size = 0
    if Path(file_path).exists():
        file_size = Path(file_path).stat().st_size
    
    return {
        'filename': Path(file_path).name,
        'file_path': file_path,
        'file_size': file_size,
        'url': url,
        'title': title,
        'category': category,
        'year': year,
        'quarter': quarter,
        'download_timestamp': datetime.now().isoformat(),
        'source_url': source_url,
        'file_extension': file_extension,
        'success': True
    }