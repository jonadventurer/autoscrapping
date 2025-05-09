import re
import os
import json
import requests
import time
import random
import gspread
from bs4 import BeautifulSoup
from datetime import datetime
from oauth2client.service_account import ServiceAccountCredentials
from urllib.parse import urljoin

# ─── Firecrawl auth via ENV VAR ─────────────────────────────────────────────
FIRECRAWL_URL = "https://api.firecrawl.dev/v1/scrape"
API_KEY      = os.getenv("FIRECRAWL_API_KEY")
if not API_KEY:
    raise RuntimeError("Missing FIRECRAWL_API_KEY env var")
HEADERS = {
    "Authorization": f"Bearer {API_KEY}",
    "Content-Type":    "application/json",
}

# ─── Google Sheets auth via ENV VAR holding full JSON ────────────────────────
SCOPE      = [
    "https://spreadsheets.google.com/feeds",
    "https://www.googleapis.com/auth/drive",
]
creds_json = os.getenv("GOOGLE_CREDENTIALS")
if not creds_json:
    raise RuntimeError("Missing GOOGLE_CREDENTIALS env var")
creds_dict = json.loads(creds_json)
creds      = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, SCOPE)
client     = gspread.authorize(creds)

COUNCIL_NAME       = "Gabo Island Council"
SHEET_NAME         = "VICTORIA Australian Council [MyCommunity] Scrapping - Tracking"
TRACKING_SHEET     = "Tracking Code (0 results)"
OUTPUT_NAME        = "My Community Scrapping (Victoria) state - By Council Tabs"
OUTPUTSHEET_NAME   = f"{COUNCIL_NAME}"
SKIPPED_SHEET_NAME = f"Skipped Link ({COUNCIL_NAME})"
BASE_URL           = "https://www.mycommunitydirectory.com.au"

# Open the output sheet for storing scraped data.
sheet = client.open(OUTPUT_NAME).worksheet(OUTPUTSHEET_NAME)  # Open the output sheet for storing scraped data.
skipped_sheet = client.open(OUTPUT_NAME).worksheet(SKIPPED_SHEET_NAME)  # Open the skipped entries sheet for logging unsuccessful scraping attempts.

def append_to_sheet(data):
    """Appends a row of data to the Google Sheets output sheet."""
    sheet.append_row(data)
    
def append_to_skipped_sheet(data):
    """Appends a row of data to the skipped sheet for logging skipped entries."""
    skipped_sheet.append_row(data)
    
def get_last_scraped_entry():
    """Retrieve the last successfully scraped subcategory URL and company name from the output sheet."""
    data = sheet.get_all_values()
    if len(data) > 1:
        last_row = data[-1]  # Get last row
        last_subcategory_url = last_row[18]  # Column 19 (zero-indexed 18) contains subcategory URL
        last_company_name = last_row[4]  # Column 5 (zero-indexed 4) contains company name
        return last_subcategory_url, last_company_name
    return None, None

def get_subcategory_urls():
    """Fetches all subcategory URLs from the tracking sheet, filtering by the council name."""
    sheet = client.open(SHEET_NAME).worksheet(TRACKING_SHEET)  # Open Google Sheet and select worksheet
    data = sheet.get_all_values()  # Fetch all data from the sheet
   
    subcategory_urls = []
    for row in data[1:]:  # Skip header
        council_name = row[0]  # Extract the council name
        url = row[5]  # Extract the subcategory URL
        result = row[6]  # Extract the result column value
        
        if url and result != "0" and council_name == COUNCIL_NAME:  # Skip if result is 0
            subcategory_urls.append(url)
    
    return subcategory_urls

def firecrawl_scrape(url, formats=["html"]):
    """Scrapes a URL using FireCrawl with retry logic."""
    max_retries = 3  # Maximum number of retry attempts
    for attempt in range(max_retries):
        start_time = time.time()  # Record the start time of the request
        payload = {"url": url, "formats": formats}  # Prepare API request payload
        response = requests.post(FIRECRAWL_URL, json=payload, headers=HEADERS)  # Send request to FireCrawl
        end_time = time.time()  # Record the end time of the request

        # If response is successful and contains "success" field, return the data
        if response.status_code == 200:
            data = response.json()
            if data.get("success"):
                return data  # Return scraped data

        # If unsuccessful, wait for a random time before retrying
        wait_time = random.uniform(10, 20)  # Random wait between 10-20 seconds
        time.sleep(wait_time)
    return None # Return "N/A" if all retries fail

def get_timestamp():
    """Returns the current timestamp in YYYY-MM-DD HH:MM:SS format."""
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

def extract_category_info(soup):
    """Extracts category and subcategory information from the page."""
    categories = soup.select("span[itemprop='title']")  # Select all breadcrumb category elements
    if len(categories) >= 4:
        return {
            "category_name": categories[3].get_text(strip=True),  # Extract category name
            "subcategory_name": categories[4].get_text(strip=True) if len(categories) > 4 else "N/A",  # Extract subcategory name if available
            "services": f"{categories[3].get_text(strip=True)}, {categories[4].get_text(strip=True)}" if len(categories) > 4 else categories[3].get_text(strip=True)  # Combine category and subcategory
        }
    # Return default "N/A" values if category information is missing
    return {"category_name": "N/A", "subcategory_name": "N/A", "services": "N/A"}

def extract_company_info(soup):
    """Extracts company details like name, NDIS provider status, and service area."""
    results = soup.select("#results > li")  # Select all list items within the #results selector
    company_data = []
    
    for result in results:
        company_name = result.select_one("div.info h4 a.orange.nofollow")  # Extract the company name
        ndis_provider = "Yes" if result.select_one("div.info > a > img") else "No"  # Check if the company is an NDIS provider
        service_area = result.select_one("div.contact-details > div:nth-child(1) > p.icon.icon-map15")  # Extract the service area
        
        company_data.append({
            "company_name": company_name.get_text(strip=True) if company_name else "N/A",  # Extract company name
            "ndis_provider": ndis_provider,  # Store "Yes" or "No" based on NDIS provider presence
            "service_area": f"Located in {service_area.get_text(strip=True)}" if service_area else "N/A"  # Extract service area
        })
    return company_data  # Return the list of extracted company details

def extract_links(soup, subcategory_urls, max_wait=200, check_interval=60):
    """Extracts business profile links from the subcategory page. With a retry of total 200 seconds, 60 seconds each try."""
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
    """Extracts the main state from the target URL (e.g., Victoria from the given URL)."""
    parts = url.split("/")
    if len(parts) > 3:  # Assuming state is always the second part after the domain
        return parts[3]  # Index 3 should be the state name
    return "N/A"

def extract_details_from_link(url):
    """Extracts company details from the business profile page."""
    data = firecrawl_scrape(url, ["html"])  # Scrape the webpage using FireCrawl
    if not data or not data.get("success"):  # If scraping fails or response is invalid, return default "N/A" values
        return {
            "location": "N/A",
            "suburb": "N/A",
            "state": "N/A",
            "postal_code": "N/A",
            "phone": "N/A",
            "website": "N/A",
            "outlet_id": "N/A",
            "latitude": "N/A",
            "longitude": "N/A",
            "about_us": "N/A"
        }
    # Parse the scraped HTML content
    soup = BeautifulSoup(data["data"].get("html", ""), "html.parser")
    # Extract the business location from the contact details section
    location = soup.select_one(".company-info .contact-info p.icon-map15 a, .company-info .contact-info p.icon-map15 span")
    location_text = location.get_text(strip=True) if location else "N/A"
    # Extract phone number (if available)
    phone = soup.select_one("a[href^='tel:']")
    # Extract outlet ID from the URL (assuming it is the second last part of the URL and is numeric)
    outlet_id = url.split("/")[-2] if url.rstrip('/').split('/')[-2].isdigit() else "N/A"
    # Extract latitude and longitude from map data (if available)
    lat_long_match = re.search(r"center=(-?\d+\.\d+),(-?\d+\.\d+)", str(soup))
    lat, long = (lat_long_match.groups() if lat_long_match else ("N/A", "N/A"))
    # Extract company description (if available)
    about_us = "\n".join([p.get_text(strip=True) for p in soup.select("div.description p")])
    # Extract suburb, state, and postal code from location text
    address_details = extract_suburb_state_postal(location_text)
    # Extract website URL (if available)
    website_url = extract_website_url(soup)

    # Return all extracted details as a dictionary
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
    """Ensure we have at least three words for suburb, state, and postal code"""
    words = location_text.split()  # Split location text into words
    # Ensure we have at least 3 words for suburb, state, and postal code
    if len(words) >= 3:
        return {
            "suburb": words[-3],  # Third last word
            "state": words[-2],   # Second last word
            "postal_code": words[-1]  # Last word
        }
    return {"suburb": "N/A", "state": "N/A", "postal_code": "N/A"}

def extract_website_url(soup):
    """Extracts the website URL from the given HTML using BeautifulSoup and combines it with BASE_URL."""
    element = soup.select_one(".company-info .contact-info p.icon.icon-website a")
    return urljoin(BASE_URL, element['href']) if element else "N/A"
    
def get_existing_entries():
    """Fetch all existing company names and outlet IDs from the Google Sheet. Returns a dictionary with (company_name, outlet_id) as keys and their services as values."""
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
    main_data = firecrawl_scrape(url, ["html"])  # returns dict on success, None on failure

    if not isinstance(main_data, dict) \
       or not main_data.get("success") \
       or "html" not in main_data.get("data", {}):
         return []  # return empty list so callers can continue safely
    
    wait_time = random.uniform(30, 50)  # Random wait between 30-50seconds
    time.sleep(wait_time)
    
    soup = BeautifulSoup(main_data["data"].get("html", ""), "html.parser")  # Parse the scraped HTML content
    # Extract data from the page
    links = extract_links(soup, url)  # Extracts service links from the page
    category_info = extract_category_info(soup)  # Extracts category metadata
    companies = extract_company_info(soup)  # Extracts company information
    main_state = extract_main_state(url)  # Extracts state information from URL
    
    # Fetch existing data once to optimize performance
    existing_entries = sheet.get_all_values()
    existing_companies = {(row[4], row[8]): row[3] for row in existing_entries}  # Extract (company_name, outlet_id) → services
    
    # Fetch existing skipped entries to avoid duplicates
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
                break  # Stop retrying if successful
            time.sleep(random.uniform(5, 10))  # Wait before retrying
        
        if not details:
            continue  # Skip this entry and move to the next one

        # Check for existing entry before saving
        existing_services = existing_companies.get((company["company_name"], details["outlet_id"]))
        new_services = category_info["services"]
        # Check if any new services are missing from existing ones
        if existing_services:
            # Update existing entry with new services
            existing_services_list = existing_services.split(", ")
            new_services_list = new_services.split(", ")
            updated_services_list = list(set(existing_services_list + new_services_list))
            updated_services = ", ".join(updated_services_list)
            # Update the dictionary
            existing_companies[(company["company_name"], details["outlet_id"])] = updated_services
            # Find and update the existing entry in Google Sheets
            cell = sheet.find(company["company_name"], in_column=5)
            if cell and set(existing_services_list) != set(updated_services_list):
                sheet.update_cell(cell.row, 4, updated_services)
                
            # Check if already in SKIPPED_SHEET before saving
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
                append_to_skipped_sheet(skipped_data)  # Save skipped entry
            else:
                continue  # Skip saving duplicate data
        else:
            # New entry, append to Google Sheets
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
            append_to_sheet(row_data)  # Save new entry
    return companies  # Return extracted company data

# Fetch all subcategory URLs
subcategory_urls = get_subcategory_urls()
last_scraped_url, last_scraped_company = get_last_scraped_entry()

# Figure out where to start: after the last scraped URL, or at the very beginning
start = 0
if last_scraped_url and last_scraped_url in subcategory_urls:
    idx = subcategory_urls.index(last_scraped_url)
    start = idx + 1  # resume at the next URL, not the same one

# If start is beyond the list, nothing to do
if start >= len(subcategory_urls):
    print("✅ All subcategories already scraped.")
else:
    print(f"🔄 Resuming from index {start}: {subcategory_urls[start:]}")
    for url in subcategory_urls[start:]:
        scrape_subcategory(url)

