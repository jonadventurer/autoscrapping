import re
import time
from datetime import datetime
import logging
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
from bs4 import BeautifulSoup
import requests
from webdriver_manager.chrome import ChromeDriverManager
from itertools import dropwhile

# Define a constant for the council name
COUNCIL_NAME = "Banyule Council"

def setup_google_sheets(output_sheet_name, worksheet_name):
    """Authenticate and connect to Google Sheets using service account credential"""
    creds = ServiceAccountCredentials.from_json_keyfile_name("mycommunitydirectorycredential.json", [
        "https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"
    ])
    client = gspread.authorize(creds)
    sheet = client.open(output_sheet_name).worksheet(worksheet_name)  # Open the specified sheet
    return sheet  # Return the sheet object

def save_to_google_sheets(sheet, data):
    """Save scraped data to Google Sheets"""
    if not data:
        print("No data to save.")
        return

    # Define the headers for the Google Sheet
    headers = ["Timestamp", "main_state", "council_name", "services", "company_name", 
               "service_area", "ndis_provider", "about", "outlet", "details_url", "location", 
               "suburb", "state", "postal_code", "latitude", "longitude", "website", "phone", "subcategory_url"]
    existing_data = sheet.get_all_values()  # Fetch existing data in the sheet
    
    # If the sheet is empty or missing headers, insert headers as the first row
    if not existing_data or existing_data[0] != headers:
        sheet.insert_row(headers, index=1)
    
    # Generate timestamp for each entry
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    rows = [[timestamp] + [entry.get(header, "N/A") for header in headers[1:]] for entry in data]
    sheet.append_rows(rows)  # Append new rows to the sheet
    print("Data successfully saved to Google Sheets.")

def get_latest_scraped_entry(sheet_name, worksheet_name):
    """Fetch the last entry from column S in the Google Sheet to resume scraping."""
    sheet = setup_google_sheets(sheet_name, worksheet_name)
    data = sheet.get_all_values()
    
    if len(data) <= 1:
        return None, None, None
    
    headers = data[0]  # First row as headers
    rows = data[1:]
    
    try:
        col_subcategory = headers.index("subcategory_url")
        col_details = headers.index("details_url")
        col_outlet = headers.index("outlet")
    except ValueError:
        print("⚠️ Column names not found, check headers in Google Sheets.")
        return None, None, None
    
    for row in reversed(rows):
        if len(row) > col_subcategory and row[col_subcategory].strip():
            return (
                row[col_subcategory], row[col_details], row[col_outlet]
            )
    
    return None, None, None

def extract_main_state(council_url):
    """Extract the main state from the council's URL using regex"""
    match = re.search(r"\.au/([^/]+)/", council_url)
    return match.group(1) if match else "N/A"

def fetch_subcategory_links(sheet_name, worksheet_name, last_scraped_subcategory):
    """Retrieve subcategory links from Google Sheets for further scraping"""
    sheet = setup_google_sheets(sheet_name, worksheet_name)
    data = sheet.get_all_values()
    
    if len(data) < 2:
        print("No subcategory links found.")
        return []
    
    headers = data[0]
    rows = data[1:]
    
    col_council = headers.index("Council")
    col_council_url = headers.index("Council URL")
    col_subcategory = headers.index("Subcategory URL")
    col_result = headers.index("Result")
    
    subcategory_links = [
        {
            "main_state": extract_main_state(row[col_council_url]),
            "council_name": row[col_council],
            "subcategory_url": row[col_subcategory],
        }
        for row in rows
        if len(row) > max(col_subcategory, col_result)  # Ensure row is long enough
        and row[col_council] == COUNCIL_NAME
        and row[col_result].strip() != "0"  # Skip if column Result is "0"
    ]
    
    # Apply filtering if last_scraped_subcategory exists
    if last_scraped_subcategory:
        subcategory_links = list(dropwhile(
            lambda entry: entry["subcategory_url"].lower() != last_scraped_subcategory.lower(), 
            subcategory_links
        ))
    
        if not subcategory_links:
            print(f"⚠️ Warning: last_scraped_subcategory '{last_scraped_subcategory}' not found, returning all links.")
            return subcategory_links
    
    return subcategory_links

def fetch_subcategory_metadata(subcategory_url, subcategory_links):
    """This function searches for metadata associated with a given subcategory URL, by iterating through a list of subcategory links. If a match is found"""
    for entry in subcategory_links:
        if entry["subcategory_url"] == subcategory_url:
            return entry
    return {}

def setup_driver():
    options = Options()
    options.add_argument("--headless=new")  
    options.add_argument("--disable-gpu")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument("--incognito")
    options.add_argument("--window-size=1920x1080")
    options.add_argument("--ignore-certificate-errors")
    options.add_argument("--allow-running-insecure-content")

    # Automatically fetch the correct ChromeDriver version
    service = Service(ChromeDriverManager().install())  
    driver = webdriver.Chrome(service=service, options=options)
    driver.set_page_load_timeout(300)
    return driver

def fetch_page(driver, url):
    """Function to fetch a webpage and parse it with BeautifulSoup"""
    try:
        driver.get(url)
        time.sleep(5)  # Allow page to load
        return BeautifulSoup(driver.page_source, "html.parser")
    except Exception as e:
        print(f"Error fetching URL {url}: {e}")
        return None

def scrape_services_list(driver, subcategory_url):
    """Scrape list of services from a subcategory page"""
    try:
        driver.get(subcategory_url)
        time.sleep(5)  # Allow time for the page to load
        
        service_items = driver.find_elements(By.CSS_SELECTOR, "li.search-result")
        print(f"  Found {len(service_items)} service items on {subcategory_url}.")
        services = []

        for item in service_items:
            try:
                # Extract company name
                company_name = item.find_element(By.CSS_SELECTOR, "h4.h4.regular.business-name").text.strip() if item.find_elements(By.CSS_SELECTOR, "h4.h4.regular.business-name") else "N/A"
                
                # Fetch category and subcategory names
                category_name = fetch_category_name(driver)
                subcategory_name = fetch_subcategory_name(driver)
        
                if not subcategory_name:
                    subcategory_name = f"All {category_name}"
                print(f"  Category: {category_name} | Subcategory: {subcategory_name} from {company_name} {subcategory_url}")
                
                service_area_element = item.find_elements(By.CSS_SELECTOR, "p[aria-label]")
                service_area = service_area_element[0].get_attribute("aria-label").strip() if service_area_element else "N/A"
                
                ndis_provider = "No"
                try:
                    # Scroll the item into view for dynamic content loading
                    driver.execute_script("arguments[0].scrollIntoView(true);", item)
                    time.sleep(1)  # Allow some time for content to load

                    # Check for the NDIS icon dynamically using JavaScript
                    ndis_icon_element = item.find_elements(By.CSS_SELECTOR, "img[title='Registered NDIS Provider']")
                    if len(ndis_icon_element) > 0:
                        ndis_provider = "Yes"
                    else:
                        # Alternative check for hidden elements or using parent relationships
                        ndis_parent = item.find_elements(By.XPATH, ".//a[contains(@href, 'AccessingTheNDIS')]")
                        if len(ndis_parent) > 0:
                            ndis_provider = "Yes"
                except Exception as e:
                    print(f"NDIS Icon Detection Error: {e}")

                # Extract service details URL
                detail_link_element = item.find_elements(By.CSS_SELECTOR, "a.orange")
                details_url = detail_link_element[0].get_attribute("href") if detail_link_element else "N/A"

                services.append({
                    "subcategory_url": subcategory_url,
                    "company_name": company_name,
                    "service_area": service_area,
                    "ndis_provider": ndis_provider,
                    "details_url": details_url,
                    "category_name": category_name,
                    "subcategory_name": subcategory_name
                })
            except Exception as e:
                print(f"Error extracting service list info for an item: {e}")
    except Exception as e:
        print(f"Error while loading subcategory URL {subcategory_url}: {e}")
        return []

    return services

def get_actual_website_url(driver, service_url, retries=3):
    """Extract actual website URL by clicking on a redirect link"""
    redirect_url = "N/A"
    for attempt in range(retries):
        try:
            driver.get(service_url)
            
            # Capture redirect URL as a fallback
            redirect_element = driver.find_elements(By.CSS_SELECTOR, "a[rel='ugc'][target='_blank']")
            if redirect_element:
                redirect_url = redirect_element[0].get_attribute("href")
            
            # Wait for website button
            website_elements = driver.find_elements(By.CSS_SELECTOR, "a[aria-label='Go to their website']")
            if website_elements:
                website_element = website_elements[0]
            else:
                print("Website button not found, using fallback URL.")
                return redirect_url  # Use the redirect URL as a fallback

            driver.execute_script("arguments[0].scrollIntoView(true);", website_element)
            time.sleep(2)  # Small delay for stability

            original_window = driver.current_window_handle
            existing_windows = driver.window_handles
            website_element.click()

            # Wait for new tab to open
            WebDriverWait(driver, 10).until(lambda d: len(d.window_handles) > len(existing_windows))
            new_window = [w for w in driver.window_handles if w not in existing_windows][0]
            driver.switch_to.window(new_window)

            # Ensure page loads before extracting URL
            WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.TAG_NAME, "body")))
            final_url = driver.current_url

            driver.close()
            driver.switch_to.window(original_window)
            return final_url

        except Exception as e:
            print(f"Retry {attempt+1} failed: {e}")
            logging.error(f"Retry {attempt+1} failed: {e}")
            time.sleep(2)  # Wait before retrying

    # Return the redirect URL if normal method fails
    print(f"Using redirect URL for {service_url}: {redirect_url}")
    logging.warning(f"Using redirect URL for {service_url}: {redirect_url}")
    return redirect_url

def parse_location(location):
    """Extract suburb, state, postal code from the given location"""
    suburb, state, postal_code = "N/A", "N/A", "N/A"

    # Ensure location is properly formatted
    location = location.strip()

    # First, try to match format with a comma (e.g., "Street, Suburb State Postcode")
    match = re.search(r",\s*([\w\s]+)\s([A-Z]{2,3})\s(\d{4})$", location)
    
    # If match found, extract values
    if match:
        suburb, state, postal_code = match.groups()
    else:
        # If no comma, check simple format "Suburb State Postcode"
        match_simple = re.search(r"([\w\s]+)\s([A-Z]{2,3})\s(\d{4})$", location)
        if match_simple:
            suburb, state, postal_code = match_simple.groups()

    return suburb.strip(), state.strip(), postal_code.strip()

def extract_lat_long_from_maps(driver, service_url):
    """Extract latitude and longitude from Google Maps image URL"""
    try:
        driver.get(service_url)
        
        # Wait for the map image to be present
        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, 
                "#content > div > div.row > div.col-sm-12.col-md-9.col-md-push-3 > div > div.col-xs-12.col-sm-4.listing-support > div.map-panel > img"
            ))
        )
        
        # Locate the <img> tag with the map
        map_img_element = driver.find_element(By.CSS_SELECTOR, 
            "#content > div > div.row > div.col-sm-12.col-md-9.col-md-push-3 > div > div.col-xs-12.col-sm-4.listing-support > div.map-panel > img"
        )
        
        map_img_src = map_img_element.get_attribute("src")
        print(f"Extracted Map URL: {map_img_src}")  # Debugging line
        
        # Extract latitude and longitude using regex
        lat_lon_pattern = re.compile(r'center=(-?\d+\.\d+),(-?\d+\.\d+)')
        match = lat_lon_pattern.search(map_img_src)
        
        if match:
            latitude, longitude = match.groups()
            return latitude, longitude

    except TimeoutException:
        print(f"Timeout: Map image not found for {service_url}")
    except Exception as e:
        print(f"Error extracting lat/lon: {e}")
        logging.error(f"Error extracting lat/lon: {e}")

    return "N/A", "N/A"

def extract_outlet_from_url(details_url):
    """Extract outlet from url"""
    match = re.search(r'/(\d+)/(?:[^/]+)$', details_url)
    return match.group(1) if match else "N/A"

def fetch_category_name(driver):
    """Extract category names"""
    try:
        category_element = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "#content div.crumbtrail span:nth-of-type(4)"))
        )
        return category_element.text.strip()
    except (TimeoutException, NoSuchElementException):
        return "N/A"

def fetch_subcategory_name(driver):
    """Extract subcategory names"""
    try:
        subcategory_element = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "#content div.crumbtrail span:nth-of-type(5)"))
        )
        return subcategory_element.text.strip()
    except (TimeoutException, NoSuchElementException):
        return "N/A"

def scrape_service_details(driver, service_url, retries=3):
    """Scrapes service details from a given URL using a Selenium WebDriver."""
    # Initialize data dictionary with default values
    data = {
        "about": "N/A",
        "location": "N/A",
        "suburb": "N/A",
        "state": "N/A",
        "postal_code": "N/A",
        "website": "N/A",
        "phone": "N/A",
        "latitude": "N/A",
        "longitude": "N/A",
        "outlet": extract_outlet_from_url(service_url), 
    }
    
    for attempt in range(retries):
        try:
            print(f"Fetching service details from: {service_url} (Attempt {attempt+1})")
            driver.get(service_url)
            
            WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.CSS_SELECTOR, "div.description")))

            # Extract basic details
            data["about"] = safe_find_text(driver, By.CSS_SELECTOR, "div.description", "N/A")
            location = safe_find_text(driver, By.CSS_SELECTOR, "div.contact-info [aria-label]", "N/A").replace("Address: ", "").strip()
            data["location"] = location
            data["phone"] = safe_find_text(driver, By.CSS_SELECTOR, "a[href^='tel:']", "N/A")
            
            # Extract additional details
            data["website"] = get_actual_website_url(driver, service_url)
            data["latitude"], data["longitude"] = extract_lat_long_from_maps(driver, service_url)
            data["suburb"], data["state"], data["postal_code"] = parse_location(location)
            
            return data # Return extracted data
        except Exception as e:
            print(f"Retry {attempt+1} failed for {service_url}: {e}")
            logging.error(f"Retry {attempt+1} failed for {service_url}: {e}")
            time.sleep(3) # Wait before retrying
    
    logging.error(f"Failed to extract details for {service_url} after {retries} retries.")
    return data # Return data with default values if all retries fail

def resolve_redirect_url(redirect_url):
    """Resolves the final URL if the given URL redirects."""
    try:
        if redirect_url and redirect_url != "N/A":
            response = requests.get(redirect_url, allow_redirects=True, timeout=30)
            return response.url  # Returns the final resolved URL
        return "N/A"
    except Exception as e:
        print(f"Error resolving redirect URL: {e}")
        logging.error(f"Error resolving redirect URL: {e}")
        return "N/A"

def safe_find_text(driver, by, value, default="N/A"):
    """Attempts to find an element and return its text. If not found, returns default value."""
    try:
        element = WebDriverWait(driver, 10).until(EC.presence_of_element_located((by, value)))
        return element.text.strip()
    except (TimeoutException, NoSuchElementException):
        return default

def safe_find_attribute(driver, by, value, attribute, default="N/A"):
    """Attempts to find an element and return a specified attribute's value. If not found, returns default value."""
    try:
        element = WebDriverWait(driver, 10).until(EC.presence_of_element_located((by, value)))
        return element.get_attribute(attribute)
    except (TimeoutException, NoSuchElementException):
        return default

def safe_extract_by_heading(driver, heading_text, default="N/A"):
    """Extracts text from elements that appear as siblings to a given heading text."""
    try:
        heading_element = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.XPATH, f"//h3[text()='{heading_text}']"))
        )
        sibling_elements = heading_element.find_elements(By.XPATH, "./following-sibling::p")
        return " | ".join([elem.text.strip() for elem in sibling_elements])
    except (TimeoutException, NoSuchElementException):
        return default
    
def get_column_index(sheet, column_name):
    """Find the index of a given column name in the Google Sheet."""
    headers = sheet.row_values(1)  # Get the first row (header)
    for i, header in enumerate(headers, start=1):  # Google Sheets is 1-based index
        if header.strip().lower() == column_name.strip().lower():
            return i
    raise ValueError(f"Column '{column_name}' not found in the sheet.")
    
def get_existing_services(sheet, outlet):
    """Fetch the existing services categories for a given outlet."""
    data = sheet.get_all_records()  # Fetch all rows
    for row in data:
        if str(row.get("outlet", "")).strip().lower() == outlet.strip().lower():
            return str(row.get("services", "")).strip()  # Ensure it's a string
    return ""

def log_skipped_data(sheet, data):
    """Logs skipped entries (if already present) in the "Skipped Link (COUNCIL_NAME)" worksheet."""
    skipped_sheet = sheet.spreadsheet.worksheet(f"Skipped Link ({COUNCIL_NAME})")
    existing_records = skipped_sheet.get_all_values()  # Get all existing records
    
    # Prepare a new entry with relevant fields
    new_entry = [
        data.get("council_name", "N/A"),
        data.get("category_name", "N/A"),
        data.get("subcategory_name", "N/A"),
        data.get("company_name", "N/A"),
        data.get("details_url", "N/A"),
        data.get("outlet", "N/A"),
        data.get("location", "N/A")
    ]

    # Check if the entry already exists (excluding timestamp column)
    if any(row[1:] == new_entry for row in existing_records[1:]):  # Skip header row
        print(f"🔴 Skipped entry exists: {data.get('outlet', 'N/A')}, {data.get('category_name')}, {data.get('subcategory_name')}")
        return  # Exit without saving

    # If not found, log the new entry
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    skipped_sheet.append_row([timestamp] + new_entry)
    print(f"⏭️  Logged update for {data.get('outlet', 'N/A')}, {data.get('category_name')}, {data.get('subcategory_name')}")

def update_google_sheets(sheet, outlet, updated_categories, combined_data):
    """Update the services column with the new categories and log skipped data."""
    data = sheet.get_all_records()
    column_index = get_column_index(sheet, "services")  # Get correct column index

    for i, row in enumerate(data, start=2):  # Start from row 2 (assuming headers in row 1)
        if str(row.get("outlet", "")).strip().lower() == outlet.strip().lower():
            sheet.update_cell(i, column_index, updated_categories) # Update services column
            print(f"✅ Updated {outlet} with categories: {updated_categories}")

            # Log skipped update in the separate worksheet
            log_skipped_data(sheet, combined_data)
            return  # Exit after updating the correct row

def format_unique_categories(service):
    """Extract and format unique categories."""
    unique_categories = set()

    # Fetch category and subcategory
    category = service.get("category_name", "").strip()
    subcategory = service.get("subcategory_name", "").strip()

    # Add valid category and subcategory to the set
    if category and category != "N/A":
        unique_categories.add(category)
    if subcategory and subcategory != "N/A":
        unique_categories.add(subcategory)

    # Return sorted unique categories as a comma-separated string
    return ", ".join(sorted(unique_categories)) if unique_categories else "N/A"

def scrape_and_save(subcategory_url, sheet, subcategory_links):
    """Scrapes services from a given subcategory URL and updates Google Sheets."""
    driver = setup_driver()# Initialize the Selenium WebDriver
    try:
        # Fetch metadata for the subcategory
        metadata = fetch_subcategory_metadata(subcategory_url, subcategory_links)
        
        # Scrape list of services for the given subcategory
        services = scrape_services_list(driver, subcategory_url)
        
        for service in services:
            service_details = scrape_service_details(driver, service["details_url"])
            combined_data = {**metadata, **service, **service_details} # Merge all relevant data
            
            outlet = combined_data.get("outlet", "N/A").strip()

            # Fetch existing categories
            existing_service_categories = get_existing_services(sheet, outlet)
            
            # Format new categories from the scraped data
            new_categories = format_unique_categories(combined_data)

            if existing_service_categories:
                print(f"🔹 Existing {outlet} for categories: {existing_service_categories}")
                # Merge new categories with existing ones
                all_categories = set(existing_service_categories.split(", ")) | set(new_categories.split(", "))
                updated_categories = ", ".join(sorted(all_categories))

                # Update the Google Sheet and log skipped data
                update_google_sheets(sheet, outlet, updated_categories, combined_data)
            else:
                # Save new data if outlet is not found
                combined_data["services"] = new_categories
                save_to_google_sheets(sheet, [combined_data])
                print(f"✅ Saved new outlet: {outlet} with services: {new_categories}")

    finally:
        driver.quit() # Ensure WebDriver is closed properly

def main():
    """Main function that initializes Google Sheets, fetches the last scraped subcategory, and iterates through subcategory URLs to scrape and save service data."""
    # Google Sheets setup
    sheet_name = "VICTORIA Australian Council [MyCommunity] Scrapping - Tracking"
    tracking_sheet = "Tracking Code (0 results)"
    output_sheet_name = "My Community Scrapping (Victoria) state - By Council Tabs"
    output_sheet = COUNCIL_NAME
    sheet = setup_google_sheets(output_sheet_name, output_sheet)

    # Retrieve last scraped subcategory for tracking progress
    last_scraped_subcategory, _, _ = get_latest_scraped_entry(output_sheet_name, output_sheet)
    
    # Fetch subcategory links from tracking sheet
    subcategory_data = fetch_subcategory_links(sheet_name, tracking_sheet, last_scraped_subcategory)
    subcategory_urls = [entry["subcategory_url"] for entry in subcategory_data]

    # Iterate through each subcategory URL and scrape services
    for url in subcategory_urls:
        scrape_and_save(url, sheet, subcategory_data)

    print("Scraping completed.")

if __name__ == "__main__":
    main()