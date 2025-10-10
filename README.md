# automated-dow30-earnings-reports
Automated Airflow pipeline for collecting, parsing, and storing quarterly earnings reports of the Dow Jones 30 companies. The workflow discovers Investor Relations pages, extracts the latest reports, downloads and parses them, and stores results in cloud storage for streamlined financial research.

# 4. Rebuild Docker
docker compose down
docker compose build --no-cache
docker compose up -d

# 5. Wait for startup
Start-Sleep -Seconds 40

# 6. Trigger the DAG
# Go to http://localhost:8080 and trigger dow30_earnings_docker