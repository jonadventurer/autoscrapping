import time
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import TimeoutException
from bs4 import BeautifulSoup
import csv

def setup_driver():
    # Use ChromeDriver path from the environment (set in GitHub workflow)
    chrome_driver_path = os.getenv("CHROME_DRIVER_PATH", "chromedriver")
    
    options = Options()
    options.add_argument("--headless")  # Enable headless mode for GitHub Actions
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument("--disable-extensions")
    options.add_argument("--disable-infobars")
    options.add_argument("--incognito")
    options.add_argument("--start-maximized")
    options.add_argument("--no-sandbox")  # Required for running in some CI environments
    options.add_argument("--disable-dev-shm-usage")  # Prevents resource issues
    service = Service(chrome_driver_path)
    driver = webdriver.Chrome(service=service, options=options)
    driver.set_page_load_timeout(120)  # Set timeout to 120 seconds
    return driver

def fetch_page(driver, url, manual_unlock=False):
    """
    Fetch a web page and return its BeautifulSoup object. Retry on failure.
    """
    try:
        print(f"Fetching URL: {url}")
        driver.get(url)
        if manual_unlock:
            print("Manual intervention required. Solve the CAPTCHA or unlock the page.")
            input("Press Enter after you have solved the CAPTCHA or unlocked the page...")
        time.sleep(10)  # Allow time for the page to load
        return BeautifulSoup(driver.page_source, "html.parser")
    except TimeoutException as e:
        print(f"Timeout occurred for {url}: {e}")
        return None
    except Exception as e:
        print(f"Unexpected error: {e}")
        return None
    
def extract_councils_and_categories(driver, base_url, csv_writer):
    """
    Extract councils, their categories, and subcategories, including result counts.
    Save data directly to CSV during extraction.
    """
    soup = fetch_page(driver, base_url, manual_unlock=True)  # Solve CAPTCHA only once
    if not soup:
        print("Failed to load the base URL. Exiting.")
        return

    # Locate the councils in the specified selector
    council_list = soup.select("ul.scrollat-600 li a")

    if not council_list:
        print("No councils found.")
        return

    for council_link in council_list:
        if 'href' not in council_link.attrs:
            print(f"Skipping council link without href: {council_link}")
            continue

        council_name = council_link.text.strip()
        council_url = "https://www.mycommunitydirectory.com.au" + council_link["href"]
        print(f"DEBUG: Processing Council - {council_name} | URL: {council_url}")

        # Fetch the council page to extract categories
        council_soup = fetch_page(driver, council_url, manual_unlock=False)
        if not council_soup:
            print(f"Failed to load council page for {council_name}. Skipping.")
            continue

        # Extract categories and subcategories for this council
        extract_categories_and_subcategories(driver, council_soup, council_name, council_url, csv_writer)

def extract_categories_and_subcategories(driver, council_soup, council_name, council_url, csv_writer):
    """
    Extract categories and their subcategories, including result counts.
    Save data directly to CSV during extraction.
    """
    # Process each category individually
    category_wrappers = council_soup.find_all("div", class_="category-expand-wrapper")

    if not category_wrappers:
        print(f"No categories found for {council_name}.")
        return

    for wrapper in category_wrappers:
        # Extract category name and URL
        category_link = wrapper.find("a", class_="category-expand")
        if not category_link:
            print(f"No category link found under {council_name}. Skipping.")
            continue

        category_name = category_link.text.strip()
        category_url = "https://www.mycommunitydirectory.com.au" + category_link["href"]
        print(f"DEBUG: Processing Category - {category_name} | URL: {category_url}")

        # Fetch the category page to extract subcategories
        category_soup = fetch_page(driver, category_url, manual_unlock=False)  # No CAPTCHA handling here
        if not category_soup:
            print(f"Failed to load category page for {category_name}. Skipping.")
            continue

        # Extract subcategories only under the current category's context
        subcategory_list = wrapper.find_next_sibling("ul", class_="link-list")
        if not subcategory_list:
            print(f"No subcategories found for {category_name}.")
            continue

        subcategory_links = subcategory_list.find_all("a")
        for sub_link in subcategory_links:
            subcategory_name = sub_link.text.strip()
            subcategory_url = "https://www.mycommunitydirectory.com.au" + sub_link["href"]

            # Skip invalid subcategory links
            if "#dp-inject" in sub_link["href"] or not subcategory_name:
                print(f"  DEBUG: Skipping invalid subcategory link: {sub_link['href']}")
                continue

            print(f"  DEBUG: Subcategory Found Under {category_name}: {subcategory_name} | URL: {subcategory_url}")

            # Extract result count for each subcategory
            result_count = extract_result_count(driver, subcategory_url)

            # Add timestamp
            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

            # Save to CSV immediately
            csv_writer.writerow([council_name, council_url, category_name, category_url, subcategory_name, subcategory_url, result_count, timestamp])
            print(f"Writing to CSV: Council: {council_name}, Council URL: {council_url}, Category: {category_name}, Category URL: {category_url}, Subcategory: {subcategory_name}, Subcategory URL: {subcategory_url}, Results: {result_count}, Time Added: {timestamp}")

def extract_result_count(driver, subcategory_url):
    """
    Extract the number of results for a subcategory.
    """
    print(f"Fetching result count for: {subcategory_url}")
    soup = fetch_page(driver, subcategory_url)
    if not soup:
        print(f"Failed to fetch results for {subcategory_url}. Returning 0.")
        return 0

    result_count_element = soup.select_one("p.search-summary strong")
    if result_count_element:
        result_text = result_count_element.text.strip()
        result_count = int(result_text.split()[0]) if result_text.split()[0].isdigit() else 0
    else:
        result_count = 0
    print(f"  Results Found: {result_count}")
    return result_count

def main():
    base_url = "https://www.mycommunitydirectory.com.au/Victoria/"
    driver = setup_driver()
    try:
        # Open CSV file for writing
        with open("categories_and_subcategories_results2.csv", "w", newline="", encoding="utf-8") as f:
            csv_writer = csv.writer(f)
            csv_writer.writerow(["Council", "Council URL", "Category", "Category URL", "Subcategory", "Subcategory URL", "Result", "Time Added"])  # Write header

            # Extract categories and subcategories
            extract_councils_and_categories(driver, base_url, csv_writer)
    finally:
        driver.quit()

if __name__ == "__main__":
    main()
