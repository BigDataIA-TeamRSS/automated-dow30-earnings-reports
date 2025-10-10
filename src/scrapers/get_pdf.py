import pandas as pd
import json
import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import urljoin
import re

# Selenium imports
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, WebDriverException
from webdriver_manager.chrome import ChromeDriverManager

# BeautifulSoup for parsing
from bs4 import BeautifulSoup

# --- Mock and OpenAI Functions (Unchanged) ---
def call_llm_for_analysis_mock(company_name: str, page_text: str, pdf_links: list) -> dict:
    print(f"--- MOCK ANALYSIS for {company_name} ---")
    time.sleep(1)
    if not pdf_links: return {"company_name": company_name, "best_pdf_url": None, "reasoning": "Response: No PDF links were found."}
    best_link = pdf_links[0]
    keywords = ['earnings', 'q3', 'quarter', 'results', 'release', 'presentation']
    for link in pdf_links:
        if any(keyword in link.lower() for keyword in keywords):
            best_link = link
            break
    return {"company_name": company_name, "best_pdf_url": best_link, "publication_date": "2025-10-28", "quarter": "Q3 2025", "reasoning": "This is a MOCK response."}

def call_openai_api(company_name: str, page_text: str, pdf_links: list, client) -> dict:
    if not client: raise ValueError("OpenAI client is not initialized.")
    prompt = f"""Analyze the provided webpage text for '{company_name}' to find the latest quarterly earnings report PDF. The current date is October 6, 2025. You should look for Q3 2025 results. From the candidate links, identify the single best URL. Avoid annual reports (10-K).
    WEBPAGE TEXT (first 8000 characters): --- {page_text[:8000]} ---
    CANDIDATE PDF LINKS: --- {json.dumps(pdf_links, indent=2)} ---
    Return a single JSON object with the keys: "company_name", "best_pdf_url", "publication_date", "quarter", and "reasoning"."""
    try:
        from openai import OpenAI
        response = client.chat.completions.create(model="gpt-4-turbo", messages=[{"role": "system", "content": "You are a helpful financial analyst assistant designed to return JSON."}, {"role": "user", "content": prompt}], response_format={"type": "json_object"}, temperature=0.0)
        return json.loads(response.choices[0].message.content)
    except Exception as e: return {"error": f"An error occurred during the OpenAI API call for {company_name}: {str(e)}"}


# --- UPGRADED: Two-Stage Selenium Scraper ---
def fetch_and_process_with_selenium(company_name: str, url: str):
    print(f"Fetching IR page for {company_name} at {url}...")
    
    options = webdriver.ChromeOptions()
    options.add_argument('--headless')
    options.add_argument('--disable-gpu')
    options.add_argument('--window-size=1920,1200')
    options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36")
    options.add_experimental_option('excludeSwitches', ['enable-logging'])

    driver = None
    try:
        try:
            service = Service(ChromeDriverManager().install())
        except Exception as e:
            print(f"CRITICAL ERROR for {company_name}: Could not set up ChromeDriver. Run PowerShell as Admin. Error: {e}")
            return company_name, None, []

        driver = webdriver.Chrome(service=service, options=options)
        driver.get(url)
        WebDriverWait(driver, 20).until(EC.presence_of_element_located((By.TAG_NAME, "body")))

        soup = BeautifulSoup(driver.page_source, 'html.parser')
        earnings_page_link = None
        navigated_to_new_page = False
        earnings_regex = re.compile(r'(q3|third quarter|3rd quarter)[\s\w,]*2025', re.IGNORECASE)
        
        for link in soup.find_all('a', href=True):
            if earnings_regex.search(link.get_text()):
                earnings_page_link = urljoin(url, link['href'])
                print(f"Found potential earnings page for {company_name}: {earnings_page_link}")
                break
        
        if earnings_page_link:
            print(f"Navigating to earnings page for {company_name}...")
            driver.get(earnings_page_link)
            navigated_to_new_page = True
            WebDriverWait(driver, 20).until(EC.presence_of_element_located((By.TAG_NAME, "body")))
            soup = BeautifulSoup(driver.page_source, 'html.parser')

        # --- REVISED PDF FINDING LOGIC ---
        pdf_links = set()
        pdf_keywords = ['earnings release', 'press release', 'financials', 'presentation', 'report', 'q3']
        
        for link in soup.find_all('a', href=True):
            href = link.get('href', '')
            if href.lower().endswith('.pdf'):
                # If we are on a dedicated earnings page, ANY PDF is a strong candidate.
                if navigated_to_new_page:
                    pdf_links.add(urljoin(driver.current_url, href))
                else:
                    # If we are still on the main page, be more strict with keywords.
                    link_text = link.get_text().lower()
                    parent_text = link.find_parent().get_text().lower() if link.find_parent() else ''
                    if any(keyword in link_text for keyword in pdf_keywords) or any(keyword in parent_text for keyword in pdf_keywords):
                        pdf_links.add(urljoin(driver.current_url, href))

        for tag in soup(["script", "style", "nav", "footer", "header"]):
            tag.extract()
        page_text = soup.get_text(separator=' ', strip=True)

        print(f"Found {len(pdf_links)} relevant PDF links for {company_name}.")
        return company_name, page_text, list(pdf_links)

    except TimeoutException:
        print(f"Could not fetch URL for {company_name}: Page load timed out.")
        return company_name, None, []
    except WebDriverException as e:
        # Provide the specific webdriver error message
        error_message = str(e).splitlines()[0]
        print(f"A WebDriver error occurred for {company_name}: {error_message}")
        return company_name, None, []
    except Exception as e:
        print(f"An unexpected error occurred for {company_name}: {e}")
        return company_name, None, []
    finally:
        if driver:
            driver.quit()

# --- Main Execution Logic (Unchanged) ---
def main():
    try:
        df = pd.read_csv('dow30_ir_pages_20251005_173133.csv')
    except FileNotFoundError:
        print("Error: The CSV file was not found.")
        return

    df_subset = df.head(5)
    print(f"Starting processing for the first {len(df_subset)} companies...")

    results = []
    analysis_method = 'openai'
    openai_client = None

    if analysis_method == 'openai':
        try:
            from openai import OpenAI
            if "OPENAI_API_KEY" in os.environ and os.environ["OPENAI_API_KEY"]:
                openai_client = OpenAI()
                print("OpenAI client initialized successfully.")
            else:
                print("WARNING: OPENAI_API_KEY env variable not found. Using MOCK mode.")
                analysis_method = 'mock'
        except ImportError:
            print("WARNING: 'openai' library not installed. Using MOCK mode.")
            analysis_method = 'mock'

    with ThreadPoolExecutor(max_workers=5) as executor:
        future_to_company = {
            executor.submit(fetch_and_process_with_selenium, row['Company'], row['IR_URL']): row
            for _, row in df_subset.iterrows()
        }

        for future in as_completed(future_to_company):
            company_name, page_text, pdf_links = future.result()

            if page_text and pdf_links:
                if analysis_method == 'openai' and openai_client:
                    llm_result = call_openai_api(company_name, page_text, pdf_links, openai_client)
                else:
                    llm_result = call_llm_for_analysis_mock(company_name, page_text, pdf_links)
                results.append(llm_result)
            elif page_text:
                results.append({"company_name": company_name, "best_pdf_url": None, "reasoning": "No relevant PDF links were found on the page."})
            else:
                results.append({"company_name": company_name, "best_pdf_url": None, "reasoning": "Failed to fetch or process the webpage."})

    results_df = pd.DataFrame(results).reindex(columns=['company_name', 'best_pdf_url', 'reasoning', 'publication_date', 'quarter'])
    output_filename = 'dow30_latest_earnings_pdfs.csv'
    results_df.to_csv(output_filename, index=False)

    print("\n--- Processing Complete ---")
    print(f"Results saved to '{output_filename}'")
    print("\nResults:")
    print(results_df.to_string())

if __name__ == '__main__':
    main()