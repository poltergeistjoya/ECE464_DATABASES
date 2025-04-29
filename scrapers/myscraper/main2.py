import time
import os
import psutil
import pandas as pd
import structlog
from pathlib import Path
from multiprocessing import Pool
from selenium import webdriver
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.common.by import By
from selenium.common.exceptions import WebDriverException
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager

logger = structlog.get_logger()


def setup_driver():
    options = Options()
    options.add_argument("--headless")
    options.add_argument("--disable-gpu")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)
    return driver 

def restart_driver(driver):
    driver.quit()
    driver = setup_driver()
    return driver

def get_memory_usage_mb():
    return psutil.Process(os.getpid()).memory_info().rss / 1024 / 1024

def safe_driver_get(driver, url, max_retries=3, sleep_sec=5):
    for attempt in range(max_retries):
        try:
            driver.get(url)
            return driver
        except WebDriverException as e:
            msg = str(e).lower()
            if "invalid session id" in msg or "session deleted" in msg:
                logger.warning("Main driver session lost — restarting", attempt=attempt + 1, url=url)
                driver = restart_driver(driver)
            else:
                logger.warning("Page load error", attempt=attempt + 1, error=msg)
            time.sleep(sleep_sec)
    return None

def get_text_by_label_xpath(driver, label_text):
    try:
        return driver.find_element(
            By.XPATH,
            f"//th[normalize-space(text())='{label_text}']/following-sibling::td"
        ).text.strip()
    except Exception:
        return ""

def safe_get(driver, selector, url):
    try:
        return driver.find_element(By.CSS_SELECTOR, selector).text.strip()
    except Exception:
        return ""

def safe_get_all(driver, selector, url):
    try:
        return [el.text.strip() for el in driver.find_elements(By.CSS_SELECTOR, selector)]
    except Exception:
        return []

def scrape_dataset_page_url(link, max_retries=3):
    attempt = 0
    while attempt < max_retries:
        driver = setup_driver()
        try:
            driver.get(link)
            result = {
                "title": safe_get(driver, "h1[itemprop='name']", link),
                "organization_type": safe_get(driver, "span.organization-type", link),
                "formats": safe_get_all(driver, "section#dataset-resources span.format-label", link),
                "tags": safe_get_all(driver, "ul.tag-list li a", link),
                "publisher_heading": safe_get(driver, "section#organization-info h1.heading", link),
                "publisher": safe_get(driver, "[title='publisher']", link),
                # "date_created": safe_get(driver, "span[itemprop='dateModified'] a", link),
                "date_last_updated": get_text_by_label_xpath(driver, "Metadata Updated Date"),
                "url": link,
            }
            driver.quit()
            return result
        except WebDriverException as e:
            msg = str(e).lower()
            if "invalid session id" in msg or "session deleted" in msg:
                logger.warning("Session lost — restarting driver", url=link, attempt=attempt + 1)
            else:
                logger.warning("Scrape failed, retrying", url=link, attempt=attempt + 1, error=msg)
            attempt += 1
            driver.quit()
        except Exception as e:
            logger.warning("Unexpected error during scrape", url=link, attempt=attempt + 1, error=str(e))
            attempt += 1
            driver.quit()
    raise RuntimeError(f"Failed to scrape {link} after {max_retries} attempts")

def main():
    tic = time.time()
    driver = setup_driver()
    output_path = Path("out2.csv")
    MAX_EMPTY_PAGES = 5
    MAX_RETRIES = 3
    RETRY_SLEEP = 5
    MEMORY_LIMIT_MB = 2500

    page_number = 1
    empty_page_streak = 0
    empty_page_retry = 0

    scraped_urls = set()
    if output_path.exists():
        scraped_df = pd.read_csv(output_path, usecols=["url"])
        scraped_urls = set(scraped_df["url"])

    while empty_page_streak < MAX_EMPTY_PAGES:
        if get_memory_usage_mb() > MEMORY_LIMIT_MB:
            logger.info("Restarting browser due to high memory")
            driver = restart_driver(driver)

        url = f"https://catalog.data.gov/dataset?page={page_number}"
        logger.info("Scraping page", page_number=page_number, url=url)

        new_driver = safe_driver_get(driver, url, max_retries=MAX_RETRIES, sleep_sec=RETRY_SLEEP)
        if not new_driver:
            logger.warning("Failed to load page after retries", page_number=page_number)
            empty_page_streak += 1
            driver = restart_driver(driver)
            continue

        driver = new_driver

        try:
            WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "h3.dataset-heading a"))
            )
            dataset_links = [
                card.get_attribute("href")
                for card in driver.find_elements(By.CSS_SELECTOR, "h3.dataset-heading a")
            ]
        except WebDriverException as e:
            logger.warning("Failed to collect dataset links after page load", error=str(e))
            driver = restart_driver(driver)
            empty_page_streak += 1
            page_number += 1
            continue

        if not dataset_links:
            logger.warning("No dataset links found, retrying", page_number=page_number, retry=empty_page_retry + 1)
            if empty_page_retry < MAX_RETRIES:
                driver = restart_driver(driver)
                empty_page_retry += 1
                continue
            else:
                logger.warning("Max retries for empty page reached", page_number = page_number)
                empty_page_streak +=1
                page_number += 1
                empty_page_retry = 0
                continue

        new_links = [link for link in dataset_links if link not in scraped_urls]
        if not new_links:
            logger.info("All links on this page already scraped", page_number=page_number)
            page_number += 1
            continue

        try:
            with Pool(processes=4) as pool:
                page_results = pool.map(scrape_dataset_page_url, new_links)
        except RuntimeError as e:
            logger.warning("Pool failed, retrying links one by one", error=str(e))
            page_results = []
            for link in new_links:
                try:
                    page_results.append(scrape_dataset_page_url(link))
                except Exception as e2:
                    logger.warning("Final failure", url=link, error=str(e2))

        df = pd.DataFrame(page_results)
        df.to_csv(output_path, mode='a', index=False, header=not output_path.exists())
        scraped_urls.update(new_links)

        logger.info("Appended results to CSV", records=len(df), page_number=page_number)

        page_number += 1
        empty_page_streak = 0

    driver.quit()
    toc = time.time()
    logger.info(f"Scraping completed in {toc - tic:.2f} seconds")

if __name__ == "__main__":
    main()
