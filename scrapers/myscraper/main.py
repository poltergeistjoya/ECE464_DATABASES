from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import WebDriverException

from webdriver_manager.chrome import ChromeDriverManager
import time
import structlog
import pandas as pd
from pathlib import Path

logger = structlog.get_logger()

def setup_driver():
    options = Options()
    options.add_argument("--headless")
    return webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)

def safe_get(driver, selector, url):
    try:
        return driver.find_element(By.CSS_SELECTOR, selector).text.strip()
    except Exception as e:
        logger.warning(f"{selector} not found", url=url, error=str(e))
        return ""

def safe_get_all(driver, selector, url):
    try:
        return [el.text.strip() for el in driver.find_elements(By.CSS_SELECTOR, selector)]
    except Exception as e:
        logger.warning(f"{selector} list not found", url=url, error=str(e))
        return []

def scrape_dataset_page(driver, url):
    driver.get(url)
    return {
        "title": safe_get(driver, "h1[itemprop='name']", url),
        "organization_type": safe_get(driver, "span.organization-type", url),
        "formats": safe_get_all(driver, "section#dataset-resources span.format-label", url),
        "tags": safe_get_all(driver, "ul.tag-list li a", url),
        "publisher": safe_get(driver, "[title='publisher']", url),
        "url": url,
    }

def main():

    driver = setup_driver()
    MAX_RETRIES = 3
    RETRY_SLEEP = 2  # seconds
    MAX_EMPTY_PAGES = 5
    output_path = Path("out.csv")
    page_number = 1
    empty_page_streak = 0

    if output_path.exists():
        output_path.unlink()

    while empty_page_streak < MAX_EMPTY_PAGES:
            url = f"https://catalog.data.gov/dataset?page={page_number}"
            logger.info("Scraping page", page_number=page_number, url=url)

            dataset_links = []
            for attempt in range(MAX_RETRIES):
                try:
                    driver.get(url)
                    dataset_links = [
                        card.get_attribute("href")
                        for card in driver.find_elements(By.CSS_SELECTOR, "h3.dataset-heading a")
                    ]
                    if dataset_links:
                        break
                except WebDriverException as e:
                    logger.warning("Page load error", attempt=attempt + 1, error=str(e))
                time.sleep(RETRY_SLEEP)

            if not dataset_links:
                logger.warning("No dataset links after retries", page_number=page_number)
                empty_page_streak += 1
                page_number += 1
                continue

            empty_page_streak = 0  # reset streak on success
            page_results = [scrape_dataset_page(driver, link) for link in dataset_links]

            df = pd.DataFrame(page_results)
            df.to_csv(output_path, mode='a', index=False, header=not output_path.exists())
            logger.info("Appended results to CSV", records=len(df), page_number=page_number)

            page_number += 1

    driver.quit()

if __name__ == "__main__":
    main()

