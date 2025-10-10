import requests
import os
import re
from urllib.parse import urlparse
from pathlib import Path

def parse_report_file(file_path):
    """Parse txt file and extract URLs with metadata"""
    urls_data = []
    
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            for line_num, line in enumerate(f, 1):
                line = line.strip()
                if not line:
                    continue
                
                # Extract URL from the line using regex
                url_match = re.search(r"url='([^']+)'", line)
                if url_match:
                    url = url_match.group(1)
                    
                    # Skip relative URLs for now (they would need base URL to be resolved)
                    if not url.startswith('http'):
                        print(f"⚠️  Skipping relative URL on line {line_num}: {url}")
                        continue
                    
                    # Extract other metadata
                    title_match = re.search(r"title='([^']+)'", line)
                    category_match = re.search(r"category='([^']+)'", line)
                    year_match = re.search(r"year=(\d+)", line)
                    quarter_match = re.search(r"quarter=(\d+)", line)
                    
                    urls_data.append({
                        'url': url,
                        'title': title_match.group(1) if title_match else '',
                        'category': category_match.group(1) if category_match else '',
                        'year': year_match.group(1) if year_match else '',
                        'quarter': quarter_match.group(1) if quarter_match else '',
                        'line_num': line_num
                    })
                else:
                    print(f"⚠️  No URL found on line {line_num}: {line}")
    
    except FileNotFoundError:
        print(f"❌ Error: File '{file_path}' not found.")
        return []
    except Exception as e:
        print(f"❌ Error reading file '{file_path}': {e}")
        return []
    
    return urls_data

def _filename_from_content_disposition(cd_header):
    """Parse RFC 6266 Content-Disposition for filename / filename* and return a filename if present.

    Returns None if no filename is found.
    """
    if not cd_header:
        return None
    # Try RFC 5987 / 6266 filename* with charset and lang: filename*=UTF-8''encoded%20name.pdf
    m = re.search(r"filename\*\s*=\s*[^']+'[^']+'\s*([^;]+)", cd_header, flags=re.I)
    if m:
        candidate = m.group(1).strip().strip('"')
        return candidate
    # Fallback to plain filename=
    m = re.search(r'filename\s*=\s*"?(?P<fn>[^";]+)"?', cd_header, flags=re.I)
    return m.group("fn").strip() if m else None

def _extension_from_content_type(content_type_value, url_path):
    """Infer a reasonable file extension from Content-Type or URL path.

    This is a best-effort mapping for common types encountered in IR pages.
    """
    ctype = (content_type_value or "").lower()
    # Prefer URL path extension if clearly present
    _, url_ext = os.path.splitext(url_path)
    if url_ext:
        return url_ext
    if "pdf" in ctype:
        return ".pdf"
    if "html" in ctype or "htm" in ctype:
        return ".html"
    if "spreadsheetml" in ctype or "excel" in ctype or "xlsx" in ctype:
        return ".xlsx"
    if "zip" in ctype:
        return ".zip"
    if "msword" in ctype or "wordprocessingml" in ctype or "docx" in ctype:
        return ".docx"
    if "plain" in ctype or "text/" in ctype:
        return ".txt"
    return ".bin"

def _build_target_filename(url, response_headers, title, year, quarter):
    """Determine the best filename using headers, URL, and metadata.

    Priority:
      1) Use metadata-based descriptive name when available (title/year/quarter) with inferred extension.
      2) Else use filename from Content-Disposition.
      3) Else use last path segment from URL (adding extension if missing).
    """
    parsed = urlparse(url)
    last_segment = os.path.basename(parsed.path.rstrip("/"))
    cd = response_headers.get("Content-Disposition", "")
    ctype = response_headers.get("Content-Type", "")
    # Determine extension first
    ext = _extension_from_content_type(ctype, last_segment)
    # 1) Descriptive name if metadata present
    if title and year and quarter:
        base = f"{title}_{year}Q{quarter}"
        filename = f"{base}{ext}"
        return filename
    # 2) Content-Disposition filename
    cd_name = _filename_from_content_disposition(cd)
    if cd_name:
        return cd_name
    # 3) URL last segment or slug
    if last_segment:
        if os.path.splitext(last_segment)[1]:
            return last_segment
        return f"{last_segment}{ext}"
    # Fallback generic
    return f"download{ext}"

def download_file(url_data, company_name, download_dir=None):
    """Download a single file from URL with metadata"""
    url = url_data['url']
    title = url_data['title']
    category = url_data['category']
    year = url_data['year']
    quarter = url_data['quarter']
    
    try:
        # Resolve project root and default directories
        project_root = Path(__file__).resolve().parents[1]
        if download_dir is None:
            download_dir = project_root / "downloads"
        else:
            download_dir = Path(download_dir)
        # Create download directory if it doesn't exist
        os.makedirs(download_dir, exist_ok=True)
        # Use company name for directory (remove problematic characters)
        company_dir_name = re.sub(r'[<>:"/\\|?*]', '_', company_name)
        company_dir_path = os.path.join(str(download_dir), company_dir_name)
        os.makedirs(company_dir_path, exist_ok=True)

        print(f"📥 Downloading: {title} ({category}) - {year}Q{quarter}")
        print(f"   URL: {url}")

        # Decide path based on URL having an extension; retry with robust method if needed
        parsed_for_ext = urlparse(url)
        last_seg = os.path.basename(parsed_for_ext.path.rstrip("/"))
        _, url_ext = os.path.splitext(last_seg)

        def _robust_session_download():
            with requests.Session() as session:
                headers = {
                    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) Python-Requests Downloader",
                    "Accept": "application/pdf,application/octet-stream;q=0.9,application/vnd.openxmlformats-officedocument.spreadsheetml.sheet;q=0.8,text/html;q=0.7,*/*;q=0.5",
                }
                resp = session.get(url, headers=headers, stream=True, timeout=30)
                resp.raise_for_status()

                ctype = resp.headers.get("Content-Type", "").lower()
                if ("text/html" in ctype) and ("pdf" not in ctype):
                    sample = resp.text[:500]
                    print("ℹ️  Note: response is HTML. If this isn't intended, the URL may be an interstitial page.")
                    print(f"   Content-Type: {ctype}")
                    print(f"   Sample: {sample[:200].replace('\n', ' ')}...")

                fn = _build_target_filename(url, resp.headers, title, year, quarter)
                fn = re.sub(r'[<>:"/\\|?*]', '_', fn)
                robust_path = os.path.join(company_dir_path, fn)
                print(f"   Saving as: {fn}")

                bytes_written = 0
                with open(robust_path, 'wb') as fh:
                    for chunk in resp.iter_content(chunk_size=1024 * 64):
                        if chunk:
                            fh.write(chunk)
                            bytes_written += len(chunk)
                return robust_path, bytes_written

        # Case 1: URL lacks extension -> use robust method directly
        if not url_ext:
            file_path, bytes_written = _robust_session_download()
        else:
            # Case 2: URL has extension -> try simple download first
            if title and year and quarter:
                filename = f"{title}_{year}Q{quarter}{url_ext}"
            else:
                filename = last_seg or f"download{url_ext}"
            filename = re.sub(r'[<>:"/\\|?*]', '_', filename)
            file_path = os.path.join(company_dir_path, filename)
            print(f"   Saving as: {filename}")

            need_retry = False
            bytes_written = 0
            try:
                resp = requests.get(url, stream=True, timeout=30)
                resp.raise_for_status()
                ctype = resp.headers.get("Content-Type", "").lower()
                # If expecting a document (e.g., .pdf) but got HTML, mark for retry
                if url_ext.lower() in (".pdf", ".xlsx", ".xls", ".docx", ".doc") and "text/html" in ctype:
                    need_retry = True
                else:
                    with open(file_path, 'wb') as f:
                        for chunk in resp.iter_content(chunk_size=1024 * 64):
                            if chunk:
                                f.write(chunk)
                                bytes_written += len(chunk)
                    if bytes_written == 0:
                        need_retry = True
            except requests.exceptions.RequestException:
                need_retry = True

            if need_retry:
                print("↩️  Retrying with robust header-aware download...")
                file_path, bytes_written = _robust_session_download()

        print(f"✅ Success! File saved as '{file_path}'")
        try:
            size = os.path.getsize(file_path)
            print(f"   Size: {size:,} bytes")
        except OSError:
            pass
        return True

    except requests.exceptions.RequestException as e:
        print(f"❌ Error downloading {title}: {e}")
        return False
    except Exception as e:
        print(f"❌ Unexpected error downloading {title}: {e}")
        return False

def main():
    """Main function to download all reports from extracted reports files"""
    import sys
    
    # Get company from command line argument
    if len(sys.argv) > 1 and sys.argv[1] == "--companies" and len(sys.argv) > 2:
        target_company = sys.argv[2]
    else:
        print("Usage: python download_reports.py --companies <company_name>")
        return
    
    project_root = Path(__file__).resolve().parents[1]
    extracted_dir = project_root / "extracted_reports"
    downloads_dir = project_root / "downloads"
    
    # Look for the specific company file
    company_file = f"extracted_reports_{target_company}.txt"
    if company_file not in os.listdir(extracted_dir):
        print(f"⚠️  File not found for company {target_company}: {company_file}")
        return
    
    print(f"\n{'='*60}")
    print(f"Downloading reports from {company_file} (Company: {target_company})")
    print(f"{'='*60}")
    
    report_file = os.path.join(str(extracted_dir), company_file)
    urls_data = parse_report_file(report_file)
    
    if not urls_data:
        print("❌ No URLs found to download.")
        return
    
    print(f"📋 Found {len(urls_data)} URLs to download:")
    for i, data in enumerate(urls_data, 1):
        print(f"   {i}. {data['title']} ({data['category']}) - {data['year']}Q{data['quarter']}")
    
    print(f"\n🚀 Starting downloads...")
    
    successful_downloads = 0
    failed_downloads = 0

    for i, url_data in enumerate(urls_data, 1):
        print(f"\n--- Download {i}/{len(urls_data)} ---")
        if download_file(url_data, target_company):
            successful_downloads += 1
        else:
            failed_downloads += 1
    
    print(f"\n📊 Download Summary for {target_company}:")
    print(f"   ✅ Successful: {successful_downloads}")
    print(f"   ❌ Failed: {failed_downloads}")
    print(f"   📁 Files saved in: {downloads_dir.resolve()}")

if __name__ == "__main__":
    main()