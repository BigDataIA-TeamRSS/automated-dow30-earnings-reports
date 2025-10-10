# automated-dow30-earnings-reports
Automated Airflow pipeline for collecting, parsing, and storing quarterly earnings reports of the Dow Jones 30 companies. The workflow discovers Investor Relations pages, extracts the latest reports, downloads and parses them, and stores results in cloud storage for streamlined financial research.

Video Links (https://northeastern.zoom.us/rec/share/JdtkiPJ5PgEAMLTRsgvgHg0yqTbXobjy9geRrrVbLusuH-BK9nx0lUXCesmLwA_q.ku44f96G5YTgtE7G 
Passcode: R=x#f3R.) ([somil's part](https://drive.google.com/file/d/1w-eidZJRc-DwoLYcSAEf5HGqKQ0r9_C9/view?usp=drive_link))

Documentation : https://drive.google.com/file/d/1vlJf8gxLMt2c8jYIuVL-MFgpUXnKecIl/view?usp=sharing


## 👥 Team Contributions

| Member | Contribution | Percentage |
|--------|-------------|------------|
| **Somil Shah** | Selenium Scrapper, Extraction (Instructor) & Download, Metadata | 33.3% |
| **Riya Kapadnis** | Store results in Google cloud storage, Parse pdf via Docling | 33.3% |
| **Siddhi Dhamale** | Orchestration using Airflow, DOW30 Ticker Identification, IR page identification | 33.3% |
