from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options

from webdriver_manager.chrome import ChromeDriverManager
import time
import structlog
import pandas as pd
from pathlib import Path

logger = structlog.get_logger()

# setup Chrome
options = Options()
options.add_argument("--headless")
driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)


# go to Page 1
BASE_URL = "https://catalog.data.gov/dataset?page=1"
logger.info(f"BASE_URL: {BASE_URL}")
driver.get(BASE_URL)
# time.sleep(2)

# Get all dataset links on page
dataset_cards = driver.find_elements(By.CSS_SELECTOR, "h3.dataset-heading a")
dataset_links = [card.get_attribute("href") for card in dataset_cards]

results = []

#visit each dataset detail page 
for url in dataset_links:
    driver.get(url)
    # time.sleep(1)

    def safe_get(selector):
        try:
            return driver.find_element(By.CSS_SELECTOR, selector).text.strip()
        except Exception as e:
            logger.warning(e)
            logger.warning(f"Could not find {selector} for {url}")
            return ""
        
    def safe_get_all(selector):
        try:
            return [el.text.strip() for el in driver.find_elements(By.CSS_SELECTOR, selector)]
        except Exception as e:
            logger.warning(e)
            logger.warning(f"Could not find {selector} for {url}")
            return []
        
    title = safe_get("h1[itemprop='name']")
    description = safe_get("div.notes")
    organization_type = safe_get("span.organization-type")
    formats = safe_get_all("li.resource-item")
    topics = safe_get_all("ul.tag-list li a")
    publisher = safe_get("[title='publisher']")
    
    results.append({
        "title": title, 
        # "description": description, 
        "organization_type": organization_type,
        "formats": formats, 
        "topics": topics, 
        "publisher": publisher, 
        "url": url, 
    })
    
driver.quit()

from pprint import pprint
pprint(results)

df = pd.DataFrame(results)
filepath = Path('out.csv')

# Write the DataFrame to the CSV file
df.to_csv(filepath, index=False)

