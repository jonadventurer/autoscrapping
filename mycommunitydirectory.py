import re
import requests
import time
import random
import gspread
from bs4 import BeautifulSoup
from datetime import datetime
from oauth2client.service_account import ServiceAccountCredentials
from urllib.parse import urljoin

# Setting the council name
COUNCIL_NAME = "Alpine Council"

# FireCrawl API key
API_KEY = "fc-6483a601863c44a9b03c0d9821dd8cc3"
FIRECRAWL_URL = "https://api.firecrawl.dev/v1/scrape"
HEADERS = {"Authorization": f"Bearer {API_KEY}", "Content-Type": "application/json"}

# Google Sheets configuration
SHEET_NAME = "VICTORIA Australian Council [MyCommunity] Scrapping - Tracking"
TRACKING_SHEET = "Tracking Code (0 results)"
OUTPUT_NAME = "My Community Scrapping (Victoria) state - By Council Tabs"
OUTPUTSHEET_NAME = f"{COUNCIL_NAME} Testing"
SKIPPED_SHEET_NAME = f"Skipped Link ({COUNCIL_NAME}) Testing"
BASE_URL = "https://www.mycommunitydirectory.com.au"

# Authenticate with Google Sheets
scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
creds = ServiceAccountCredentials.from_json_keyfile_name("mycommunitydirectorycredential.json", scope)
client = gspread.authorize(creds)
sheet = client.open(OUTPUT_NAME).worksheet(OUTPUTSHEET_NAME)
skipped_sheet = client.open(OUTPUT_NAME).worksheet(SKIPPED_SHEET_NAME)

def append_to_sheet(data):
    sheet.append_row(data)
    
def append_to_skipped_sheet(data):
    skipped_sheet.append_row(data)
    
def get_last_scraped_entry():
    # Retrieve the last successfully scraped subcategory URL and company name from the output sheet.
    data = sheet.get_all_values()
    if len(data) > 1:
        last_row = data[-1]  # Get last row
        last_subcategory_url = last_row[18]  # Column 19 (zero-indexed 18) contains subcategory URL
        last_company_name = last_row[4]  # Column 5 (zero-indexed 4) contains company name
        return last_subcategory_url, last_company_name
    return None, None

def get_subcategory_urls():    
    sheet = client.open(SHEET_NAME).worksheet(TRACKING_SHEET)
    data = sheet.get_all_values()
   
    subcategory_urls = []
    for row in data[1:]:  # Skip header
        council_name = row[0]
        url = row[5]
        result = row[6]
        
        if url and result != "0" and council_name == COUNCIL_NAME:  # Skip if result is 0
            subcategory_urls.append(url)
    
    return subcategory_urls

def firecrawl_scrape(url, formats=["html"]):
    # Scrapes a URL using FireCrawl with retry logic.
    max_retries = 3
    for attempt in range(max_retries):
        start_time = time.time()
        payload = {"url": url, "formats": formats}
        response = requests.post(FIRECRAWL_URL, json=payload, headers=HEADERS)
        end_time = time.time()
        
        if response.status_code == 200:
            data = response.json()
            if data.get("success"):
                return data  # Return scraped data

        wait_time = random.uniform(10, 20)  # Increased wait time
        time.sleep(wait_time)

    print(f"❌ FireCrawl failed after {max_retries} attempts for {url}")
    return None  # Return None if all retries fail

def get_timestamp():
    # Returns the current timestamp in YYYY-MM-DD HH:MM:SS format.
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

def extract_category_info(soup):
    categories = soup.select("span[itemprop='title']")
    if len(categories) >= 4:
        return {
            "category_name": categories[3].get_text(strip=True),
            "subcategory_name": categories[4].get_text(strip=True) if len(categories) > 4 else "N/A",
            "services": f"{categories[3].get_text(strip=True)}, {categories[4].get_text(strip=True)}" if len(categories) > 4 else categories[3].get_text(strip=True)
        }
    return {"category_name": "N/A", "subcategory_name": "N/A", "services": "N/A"}

def extract_company_info(soup):
    results = soup.select("#results > li")
    company_data = []
    
    for result in results:
        company_name = result.select_one("div.info h4 a.orange.nofollow")
        ndis_provider = "Yes" if result.select_one("div.info > a > img") else "No"
        service_area = result.select_one("div.contact-details > div:nth-child(1) > p.icon.icon-map15")
        
        company_data.append({
            "company_name": company_name.get_text(strip=True) if company_name else "N/A",
            "ndis_provider": ndis_provider,
            "service_area": f"Located in {service_area.get_text(strip=True)}" if service_area else "N/A"
        })
    return company_data

def extract_links(soup, subcategory_urls, max_wait=200, check_interval=60):
    # Wait until links is obtained
    start_time = time.time()
    links = []

    while time.time() - start_time < max_wait:
        links = soup.select("#results li div.info h4 a")
        if links:
            return [link.get("href") for link in links if link.get("href")]

        wait_time = random.uniform(check_interval, check_interval + 5)  # Randomized wait time
        time.sleep(wait_time)  # Wait before rechecking
    
    return []  # Return empty list if no links found

def extract_main_state(url):
    # Extracts the main state from the target URL (e.g., Victoria from the given URL).
    parts = url.split("/")
    if len(parts) > 3:  # Assuming state is always the second part after the domain
        return parts[3]  # Index 3 should be the state name
    return "State not found"

def extract_details_from_link(url):
    data = firecrawl_scrape(url, ["html"])
    if not data or not data.get("success"):
        return None
    
    soup = BeautifulSoup(data["data"].get("html", ""), "html.parser")
    
    location = soup.select_one(".company-info .contact-info p.icon-map15 a, .company-info .contact-info p.icon-map15 span")
    location_text = location.get_text(strip=True) if location else "N/A"
    phone = soup.select_one("a[href^='tel:']")
    outlet_id = url.split("/")[-2] if url.rstrip('/').split('/')[-2].isdigit() else "N/A"
    lat_long_match = re.search(r"center=(-?\d+\.\d+),(-?\d+\.\d+)", str(soup))
    lat, long = (lat_long_match.groups() if lat_long_match else ("N/A", "N/A"))
    about_us = "\n".join([p.get_text(strip=True) for p in soup.select("div.description p")])
    address_details = extract_suburb_state_postal(location_text)
    website_url = extract_website_url(soup)

    return {
        "location": location_text,
        "suburb": address_details["suburb"],
        "state": address_details["state"],
        "postal_code": address_details["postal_code"],
        "phone": phone.get_text(strip=True) if phone else "N/A",
        "website": website_url,
        "outlet_id": outlet_id,
        "latitude": lat,
        "longitude": long,
        "about_us": about_us
    }

def extract_suburb_state_postal(location_text):
    # Ensure we have at least three words for suburb, state, and postal code
    words = location_text.split()    
    if len(words) >= 3:
        return {
            "suburb": words[-3],  # Third last word
            "state": words[-2],   # Second last word
            "postal_code": words[-1]  # Last word
        }
    return {"suburb": "N/A", "state": "N/A", "postal_code": "N/A"}

def extract_website_url(soup):
    # Extracts the website URL from the given HTML using BeautifulSoup and combines it with BASE_URL.
    element = soup.select_one(".company-info .contact-info p.icon.icon-website a")
    return urljoin(BASE_URL, element['href']) if element else None
    
def get_existing_entries():
    # Fetch all existing company names and outlet IDs from the Google Sheet. Returns a dictionary with (company_name, outlet_id) as keys and their services as values.
    data = sheet.get_all_values()
    existing_entries = {}
    
    for row in data[1:]:  # Skip header
        company_name = row[4]  # Column 5: company_name
        outlet_id = row[8]  # Column 9: outlet_id
        services = row[3]  # Column 4: services
        
        if (company_name, outlet_id) in existing_entries:
            existing_entries[(company_name, outlet_id)] += f", {services}"
        else:
            existing_entries[(company_name, outlet_id)] = services
    
    return existing_entries

existing_entries = get_existing_entries()

def scrape_subcategory(url):
    main_data = firecrawl_scrape(url, ["html"])
    if not main_data or not main_data.get("success") or "html" not in main_data["data"]:
        return
    
    wait_time = random.uniform(30, 50)  # Random wait between 10-20 seconds
    time.sleep(wait_time)
    
    soup = BeautifulSoup(main_data["data"].get("html", ""), "html.parser")
    links = extract_links(soup, url)
    category_info = extract_category_info(soup)
    companies = extract_company_info(soup)
    main_state = extract_main_state(url)
    
    # ✅ Fetch existing data once to optimize performance
    existing_entries = sheet.get_all_values()
    existing_companies = {(row[4], row[8]): row[3] for row in existing_entries}  # Extract (company_name, outlet_id) → services
    
    # ✅ Fetch existing skipped entries to avoid duplicates
    skipped_entries = skipped_sheet.get_all_values()
    existing_skipped_data = {
        (row[2], row[3], row[4], row[6]) for row in skipped_entries[1:]
    }  # (category_name, subcategory_name, company_name, outlet_id)

    for company, link in zip(companies, links):
        # Retry extracting details up to 3 times if it fails
        details = None
        max_retries = 3
        for attempt in range(max_retries):
            details = extract_details_from_link(link)
            if details:
                break  # Success, stop retrying)
            time.sleep(random.uniform(5, 10))  # Wait before retrying
        
        if not details:
            continue  # Skip this entry and move to the next one

        # 🚩 Check for existing entry before saving
        existing_services = existing_companies.get((company["company_name"], details["outlet_id"]))
        new_services = category_info["services"]

        if existing_services:
            existing_services_list = existing_services.split(", ")
            new_services_list = new_services.split(", ")
            updated_services_list = list(set(existing_services_list + new_services_list))
            updated_services = ", ".join(updated_services_list)
            
            existing_companies[(company["company_name"], details["outlet_id"])] = updated_services
            
            cell = sheet.find(company["company_name"], in_column=5)
            if cell:
                sheet.update_cell(cell.row, 4, updated_services)
                
            # ✅ **Check if already in SKIPPED_SHEET before saving**
            skipped_data_entry = (
                category_info["category_name"],
                category_info["subcategory_name"],
                company["company_name"],
                details["outlet_id"]
            )
            if skipped_data_entry not in existing_skipped_data:
                skipped_data = [
                    datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    COUNCIL_NAME,
                    category_info["category_name"],
                    category_info["subcategory_name"],
                    company["company_name"],
                    link,
                    details["outlet_id"],
                    details["location"]
                ]
                append_to_skipped_sheet(skipped_data)
            else:
                continue  # Skip saving duplicate data
        else:
            # ✅ New entry, append to Google Sheets
            row_data = [
                datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                main_state,
                COUNCIL_NAME,
                new_services,
                company["company_name"],
                company["service_area"],
                company["ndis_provider"],
                details["about_us"],
                details["outlet_id"],
                link,
                details["location"],
                details['suburb'],
                details['state'],
                details['postal_code'],
                details["latitude"],
                details["longitude"],
                details["website"],
                details["phone"],
                url
            ]
            append_to_sheet(row_data)
    return companies  # Return extracted company data

# Fetch all subcategory URLs
subcategory_urls = get_subcategory_urls()
last_scraped_url, last_scraped_company = get_last_scraped_entry()

# If there's a last scraped URL and company, resume from there
if last_scraped_url and last_scraped_company and last_scraped_url in subcategory_urls:
    last_index = subcategory_urls.index(last_scraped_url)
    subcategory_urls = subcategory_urls[last_index:]  # Start from the last scraped URL

    # Ensure we only continue from the next company if multiple exist in the same subcategory
    first_subcategory_companies = scrape_subcategory(last_scraped_url)  # Get all companies in last URL
    skip_companies = True
    for company in first_subcategory_companies:
        if skip_companies:
            if company["company_name"] == last_scraped_company:
                skip_companies = False  # Stop skipping once we reach the last scraped company
            continue

        # Scrape remaining companies in the same subcategory
        scrape_subcategory(last_scraped_url)

    # Continue with the rest of the URLs
    for url in subcategory_urls[1:]:  # Skip the already processed subcategory
        scrape_subcategory(url)
else:
    # Start from the beginning if no last entry is found
    for url in subcategory_urls:
        scrape_subcategory(url)
