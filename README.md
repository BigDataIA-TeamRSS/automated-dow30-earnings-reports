# Automated Dow 30 Earnings Reports Pipeline

Automated Airflow pipeline for collecting, parsing, and storing quarterly earnings reports of the Dow Jones 30 companies. The workflow automatically discovers Investor Relations pages, extracts the latest reports, downloads and parses them using Docling, and stores results locally with optional cloud storage upload for streamlined financial research.

---

## 📋 Project Overview

**FinTrust Analytics - Project LANTERN (Part 2)**

This project automates the entire earnings report collection pipeline for the Dow Jones 30 companies, eliminating manual analyst work and providing structured, analysis-ready data.

### What It Does

1. **Programmatically identifies** Investor Relations (IR) pages for each Dow 30 company
2. **Automatically locates** the latest quarterly earnings reports (press releases, slides, supplemental tables)
3. **Downloads and parses** reports using advanced PDF parsing (Docling)
4. **Extracts structured data** including text, tables, and financial metrics
5. **Orchestrates the workflow** using Apache Airflow on a weekly schedule
6. **Stores results** in organized local directories with cloud storage support

---

## 🎥 Demo & Documentation

- **Video Demo**: [Watch on Zoom](https://northeastern.zoom.us/rec/share/JdtkiPJ5PgEAMLTRsgvgHg0yqTbXobjy9geRrrVbLusuH-BK9nx0lUXCesmLwA_g.ku44f96G5YTgtE7G) (Passcode: `R=x#f3R.`)
- **Full Documentation**: [View on Google Drive](https://drive.google.com/file/d/1vJJf8gxLMt2c8jYIuVL-MFgpUXnKecll/view?usp=sharing)

---

## 👥 Team Contributions

| Member | Contribution | Percentage |
|--------|-------------|------------|
| **Somil Shah** | Selenium Scraper, Extraction (Instructor) & Download, Metadata Collection | 33.3% |
| **Riya Kapadnis** | Google Cloud Storage Integration, PDF Parsing via Docling, Testing | 33.3% |
| **Siddhi Dhamale** | Airflow Orchestration, DOW30 Ticker Identification, IR Page Discovery, DAG Implementation | 33.3% |

---

## 🏗️ Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                    Apache Airflow DAG                        │
│                  (dow30_earnings_docker)                     │
└─────────────────────────────────────────────────────────────┘
                            │
        ┌───────────────────┼───────────────────┐
        ▼                   ▼                   ▼
   ┌─────────┐      ┌──────────────┐    ┌──────────────┐
   │  Task 1 │      │   Task 2     │    │   Task 3     │
   │Orchestr-│─────▶│   Docling    │───▶│   Verify     │
   │  ator   │      │   Parser     │    │   Outputs    │
   └─────────┘      └──────────────┘    └──────────────┘
        │                   │                   │
        ▼                   ▼                   ▼
   ┌─────────────────────────────────────────────────┐
   │               Task 4: Generate Report            │
   └─────────────────────────────────────────────────┘
                            │
                            ▼
                    ┌──────────────┐
                    │ Local Storage│
                    │  (Optional:  │
                    │     GCS)     │
                    └──────────────┘
```

### Pipeline Flow

1. **Orchestrator Task**: 
   - Identifies Dow 30 tickers and company websites
   - Discovers IR pages using Selenium-based scraping
   - Extracts latest earnings report links using AI (Instructor + Gemini)
   - Downloads PDF/HTML reports

2. **Docling Parser Task**: 
   - Parses downloaded PDFs using Docling
   - Extracts structured text, tables, and metadata
   - Converts to JSON and CSV formats

3. **Verify Outputs Task**: 
   - Validates file counts and directory structure
   - Logs sample outputs and file statistics

4. **Generate Report Task**: 
   - Creates comprehensive execution summary
   - Logs storage locations and metrics

---

## 🚀 Quick Start

### Prerequisites

- Docker & Docker Compose
- Python 3.11+
- Google Gemini API Key (for AI-powered extraction)
- 8GB+ RAM recommended

### Installation

1. **Clone the repository**
   ```bash
   git clone https://github.com/yourusername/automated-dow30-earnings-reports.git
   cd automated-dow30-earnings-reports
   ```

2. **Set up environment variables**
   ```bash
   # Create .env file
   echo "GEMINI_API_KEY=your_api_key_here" > .env
   ```

3. **Build and start Airflow**
   ```bash
   # Enable BuildKit for faster builds
   export DOCKER_BUILDKIT=1
   export COMPOSE_DOCKER_CLI_BUILD=1
   
   # Build and start services
   docker-compose build
   docker-compose up -d
   ```

4. **Access Airflow UI**
   - URL: http://localhost:8080
   - Username: `airflow`
   - Password: `airflow`

5. **Trigger the DAG**
   - Find `dow30_earnings_docker` in the UI
   - Toggle it ON (unpause)
   - Click the play button to trigger manually

---

## 📁 Project Structure

```
automated-dow30-earnings-reports/
├── dags/
│   └── run_pipeline.py              # Main Airflow DAG
├── scrapers/
│   ├── enhanced_selenium_scraper.py # IR page discovery
│   ├── extract_reports.py           # AI-powered report extraction
│   └── download_reports.py          # Report downloader
├── orchestrator.py                   # Main orchestration script
├── docling_runner.py                 # Docling PDF parser
├── simple_metadata_collector.py      # Metadata collection
├── docker-compose.yaml               # Docker services configuration
├── Dockerfile                        # Custom Airflow image
├── pip-requirements.txt              # Python dependencies
├── output/                           # Generated outputs
│   ├── ir_links/                    # Discovered IR pages
│   ├── extracted_reports/           # Report metadata
│   ├── downloads/                   # Raw PDF/HTML files
│   ├── data/parsed/docling/         # Parsed JSON & tables
│   └── logs/                        # Pipeline execution logs
└── README.md
```

---

## 🔧 Configuration

### Airflow DAG Settings

```python
# Schedule: Weekly on Mondays at 2 AM
schedule_interval='0 2 * * 1'

# Retry configuration
retries=2
retry_delay=timedelta(minutes=5)
```

### Key Environment Variables

| Variable | Description | Required |
|----------|-------------|----------|
| `GEMINI_API_KEY` | Google Gemini API key for AI extraction | Yes |
| `AIRFLOW_HOME` | Airflow home directory (default: /opt/airflow) | No |

---

## 📊 Output Structure

### Directory Organization

```
output/
├── ir_links/
│   ├── company_name_ir_links.json
│   └── ...
├── extracted_reports/
│   ├── company_name_reports.json
│   └── ...
├── downloads/
│   ├── company_name/
│   │   ├── Q4_2024_earnings.pdf
│   │   └── ...
│   └── ...
├── data/parsed/docling/
│   ├── company_name/
│   │   ├── document.json            # Full parsed content
│   │   ├── tables_table_1.csv       # Extracted tables
│   │   └── ...
│   └── ...
└── logs/
    └── pipeline_run_YYYYMMDD_HHMMSS.json
```

### Sample Output

**IR Links JSON**:
```json
{
  "company": "Apple Inc.",
  "ticker": "AAPL",
  "ir_url": "https://investor.apple.com",
  "discovered_at": "2025-11-19T20:00:00Z"
}
```

**Extracted Reports JSON**:
```json
{
  "company": "Apple Inc.",
  "reports": [
    {
      "title": "Q4 2024 Earnings Release",
      "url": "https://investor.apple.com/earnings/q4-2024.pdf",
      "date": "2024-10-31",
      "type": "earnings_release"
    }
  ]
}
```

**Docling Parsed JSON**:
```json
{
  "title": "Apple Inc. Q4 2024 Results",
  "pages": [...],
  "tables": [
    {
      "id": "table_1",
      "title": "Consolidated Statement of Operations",
      "data": [...]
    }
  ],
  "text_content": "..."
}
```

---

## 🛠️ Technical Stack

### Core Technologies
- **Orchestration**: Apache Airflow 2.8.1
- **Containerization**: Docker & Docker Compose
- **Web Scraping**: Selenium 4.16.0, BeautifulSoup4
- **PDF Parsing**: Docling (advanced document layout analysis)
- **AI Extraction**: Instructor 1.7.0 + Google Gemini
- **Data Processing**: Pandas 2.2.0

### Key Libraries
```
apache-airflow==2.8.1
selenium==4.16.0
beautifulsoup4==4.12.3
docling
instructor==1.7.0
google-generativeai==0.8.3
pandas==2.2.0
requests>=2.31.0
pydantic>=2.0.0
```

---

## 🔍 Key Features

### 1. **Fully Automated Discovery**
- No hardcoded IR URLs
- Selenium-based intelligent page navigation
- Handles dynamic JavaScript-rendered content

### 2. **AI-Powered Report Extraction**
- Uses Google Gemini with Instructor for structured extraction
- Identifies latest quarterly reports by date and keywords
- Extracts metadata (title, date, type, URL)

### 3. **Advanced PDF Parsing**
- Docling library for layout-aware parsing
- Extracts tables, text, and document structure
- Preserves formatting and relationships

### 4. **Robust Error Handling**
- Automatic retries on failures
- Comprehensive logging at each step
- Graceful degradation for missing data

### 5. **Scalable Architecture**
- Docker-based deployment
- Parallel processing capability
- Easy to add more companies

---

## 📈 Monitoring & Logs

### Airflow UI Monitoring
- Real-time task status visualization
- Execution history and timing metrics
- Task logs accessible from UI

### Log Locations
- **Airflow logs**: `/opt/airflow/logs/`
- **Pipeline reports**: `/opt/airflow/logs/pipeline_run_*.json`
- **Container logs**: `docker-compose logs -f airflow-scheduler`

### Checking Pipeline Status
```bash
# View scheduler logs
docker-compose logs -f airflow-scheduler

# View webserver logs
docker-compose logs -f airflow-webserver

# Check task execution
docker-compose exec airflow-webserver airflow tasks list dow30_earnings_docker
```

---

## 🐛 Troubleshooting

### Common Issues

**1. Import errors with instructor/vertexai**
```bash
# Rebuild with fixed dependencies
docker-compose down
docker-compose build --no-cache
docker-compose up -d
```

**2. Selenium timeouts**
- Increase timeout values in scraper configuration
- Check network connectivity
- Verify Chromium installation in container

**3. Docling parsing failures**
- Ensure PDFs are not corrupted
- Check available memory (Docling is memory-intensive)
- Review logs for specific error messages

**4. Out of memory errors**
```bash
# Increase Docker memory limit
# Docker Desktop → Settings → Resources → Memory (increase to 8GB+)
```

### Debug Mode

Enable verbose logging in DAG:
```python
# In run_pipeline.py
logging.getLogger().setLevel(logging.DEBUG)
```

---

## Future Enhancements

### Planned Features
- [ ] Google Cloud Storage integration for automatic uploads
- [ ] Natural language processing for earnings sentiment analysis
- [ ] Historical data tracking and trend analysis
- [ ] Support for additional stock indices (S&P 500, NASDAQ 100)
- [ ] Real-time alerting for new earnings releases
- [ ] Interactive dashboard for visualizing parsed data
- [ ] Incremental updates (only fetch new reports)

### Extensibility
The pipeline is designed to be easily extended:
- Add new parsing strategies in `docling_runner.py`
- Integrate additional AI models in `extract_reports.py`
- Add custom post-processing tasks in the DAG
- Connect to databases for structured storage

---

## Development

### Running Tests
```bash
# Test IR page discovery
docker-compose exec airflow-webserver python /opt/airflow/enhanced_selenium_scraper.py

# Test report extraction
docker-compose exec airflow-webserver python /opt/airflow/extract_reports.py

# Test Docling parser
docker-compose exec airflow-webserver python /opt/airflow/docling_runner.py --help
```

### Making Code Changes

Since code is mounted via volumes, changes are reflected immediately:
```bash
# Edit DAG or Python files locally
vim dags/run_pipeline.py

# Restart scheduler to pick up DAG changes
docker-compose restart airflow-scheduler
```

### Rebuilding (Only When Dependencies Change)
```bash
# Update pip-requirements.txt
vim pip-requirements.txt

# Rebuild
docker-compose build
docker-compose up -d
```

---

## Contributing

We welcome contributions! Please follow these guidelines:

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/AmazingFeature`)
3. Commit your changes (`git commit -m 'Add some AmazingFeature'`)
4. Push to the branch (`git push origin feature/AmazingFeature`)
5. Open a Pull Request

---

## License

This project is part of an academic assignment for FinTrust Analytics.

---

## Acknowledgments

- **Docling**: Advanced document parsing library
- **Apache Airflow**: Workflow orchestration
- **Instructor**: Structured AI outputs
- **Google Gemini**: AI-powered extraction
- **FinTrust Analytics**: Project sponsor and requirements

---

## Contact & Support

For questions or issues:
- Create an issue in the GitHub repository
- Refer to the demo video for visual guidance
- Check the documentation PDF for detailed explanations

---
