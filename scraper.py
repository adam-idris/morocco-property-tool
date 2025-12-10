#--------------Imports--------------#

import logging
import os
from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from time import sleep
from random import uniform
from typing import List, Optional, Tuple

import pandas as pd
import requests
import tqdm
import boto3
from botocore.client import Config
from requests.adapters import HTTPAdapter, Retry
from concurrent.futures import ThreadPoolExecutor, as_completed
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from supabase import create_client, Client
from urllib.parse import unquote
import re

#--------------Supabase Setup--------------#

load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

if not SUPABASE_URL or not SUPABASE_KEY:
    raise RuntimeError("Supabase URL or Key not found in environment variables.")

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

#-----------Cloudflare R2 Setup-----------#

CF_ACCOUNT_ID = os.getenv("CF_ACCOUNT_ID")
CF_ACCESS_KEY_ID = os.getenv("CF_ACCESS_KEY")
CF_SECRET_ACCESS_KEY = os.getenv("CF_SECRET_KEY")
CF_BUCKET = os.getenv("CF_BUCKET")
CF_BASE_URL = os.getenv("CF_ENDPOINT")

if not (CF_ACCOUNT_ID and CF_ACCESS_KEY_ID and CF_SECRET_ACCESS_KEY and CF_BUCKET):
    raise RuntimeError("Cloudflare R2 credentials or bucket name missing.")


r2_client = boto3.client(
    "s3",
    endpoint_url=CF_BASE_URL,
    aws_access_key_id=CF_ACCESS_KEY_ID,
    aws_secret_access_key=CF_SECRET_ACCESS_KEY,
    config=Config(signature_version="s3v4"),
)

#--------------Constants and Session Setup--------------#

USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 15_3_1) "
    "AppleWebKit/605.1.15 (KHTML, like Gecko) "
    "Version/17.4 Safari/605.1.15"
)

REQUEST_TIMEOUT = 15  # seconds
MIN_SLEEP = 1  # seconds
MAX_SLEEP = 3  # seconds

session = requests.Session()
session.headers.update({"User-Agent": USER_AGENT})
retries = Retry(
    total=3,
    backoff_factor=0.5,
    status_forcelist=[429, 500, 502, 503, 504],
    allowed_methods=["HEAD", "GET", "OPTIONS"],
)
adapter = HTTPAdapter(max_retries=retries)
session.mount("http://", adapter)
session.mount("https://", adapter)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)

#--------------Data Classes--------------#
@dataclass
class PropertyDetails:
    title: Optional[str]
    description: Optional[str]
    property_type: Optional[str]
    city: Optional[str]
    area: Optional[str]
    size: Optional[int]
    rooms: Optional[int]
    bedrooms: Optional[int]
    bathrooms: Optional[int]
    price: Optional[int]
    features: Optional[str]
    condition: Optional[str]
    age: Optional[str]
    orientation: Optional[str]
    flooring: Optional[str]
    floor_number: Optional[int]
    number_of_floors: Optional[int]
    lat: Optional[float]
    lon: Optional[float]
    url: str
    agent_type: Optional[str] = None      # 'agency' / 'particular'
    agent_name: Optional[str] = None
    agent_url: Optional[str] = None

#--------------Helper Functions--------------#

def load_existing_external_ids(
    source: str = "mubawab",
    listing_type: Optional[str] = "rent",
) -> set[str]:
    ids: set[str] = set()
    try:
        query = (
            supabase.table("normalised_listings")
            .select("external_id")
            .eq("source", source)
        )
        if listing_type is not None:
            query = query.eq("listing_type", listing_type)
        res = query.execute()
        for row in res.data:
            eid = row.get("external_id")
            if eid:
                ids.add(eid)
        logging.info("Loaded %d existing external_ids into cache.", len(ids))
    except Exception as e:
        logging.error("Failed to load existing ids: %s", e)
    return ids

def is_already_scraped(external_id: str, existing_ids: set[str]) -> bool:
    """
    Check in-memory cache first (O(1)). If absent, treat as new.
    We rely on upsert in Supabase to make reruns idempotent anyway.
    """
    return external_id in existing_ids

def process_single_listing(
    listing_meta: dict,
    source: str = "mubawab",
    listing_type: str = "rent",
) -> Optional[dict]:
    url = listing_meta["url"]
    external_id = listing_meta["external_id"]
    listing_tier = listing_meta["listing_tier"]

    resp = fetch(url)
    if resp is None:
        return None

    soup = BeautifulSoup(resp.text, "html.parser")
    image_urls = get_image_links(soup)

    agent_type, agent_name, agent_url = extract_agency_info(soup)

    # Parse property details
    details = parse_property_page(url, resp.text)
    if details is None:
        return None

    # Attach agency info to details dataclass
    details.agent_type = agent_type
    details.agent_name = agent_name
    details.agent_url = agent_url

    # 1) Save raw listing (including agency + tier)
    save_raw_listing(
        source=source,
        listing_type=listing_type,
        external_id=external_id,
        link=url,
        response_text=resp.text,
        image_urls=image_urls,
        listing_tier=listing_tier,
        agent_type=agent_type,
        agent_name=agent_name,
        agent_url=agent_url,
    )

    # 2) Upload images to R2 and create listing_images rows
    main_image_path = process_listing_images(external_id, image_urls)

    # 3) Upsert into normalised_listings
    upsert_normalised_listing(
        details=details,
        external_id=external_id,
        source=source,
        listing_type=listing_type,
        main_image_path=main_image_path,
        listing_tier=listing_tier,
    )

    return asdict(details)

def classify_listing_box(box: BeautifulSoup) -> tuple[str, bool]:
    """
    Based on the class list of a listingBox, return (tier, is_adboost).
    tier: 'super_premium', 'premium', 'standard'
    """
    classes = box.get("class", [])
    classes_set = {c.lower() for c in classes}

    is_adboost = "adboostbox" in classes_set

    if "spremium" in classes_set:
        tier = "super_premium"
    elif "premium" in classes_set:
        tier = "premium"
    else:
        tier = "standard"

    return tier, is_adboost

def get_mubawab_external_id(url: str) -> Optional[str]:
    """
    Extracts a combined ID such as:
    - 'a8037244'
    - 'pa4634098'
    from any URL containing /a/ or /pa/ or similar.
    """
    match = re.search(r"/(a|pa)/(\d+)", url)
    if match:
        prefix = match.group(1)
        number = match.group(2)
        return f"{prefix}{number}"
    return None

def save_raw_listing(
    source: str,
    external_id: str,
    link: str,
    response_text: str,
    image_urls: list[str],
    *,
    listing_tier: str,
    is_adboost: bool,
    details: PropertyDetails,
) -> None:
    """
    Inserts or upserts the raw listing into the raw_listings table.

    IMPORTANT: agent_url lives here (in payload_json), not in normalised_listings.
    """
    payload = {
        "url": link,
        "html": response_text,
        "image_urls": image_urls,
        "scraped_at_client": datetime.now(timezone.utc).isoformat(),
        "listing_tier": listing_tier,
        "is_adboost": is_adboost,
        "agent_type": details.agent_type,
        "agent_name": details.agent_name,
        "agent_url": details.agent_url,
    }

    try:
        supabase.table("raw_listings").upsert(
            {
                "external_id": external_id,
                "source": source,
                "payload_json": payload,
            },
            on_conflict="external_id",
        ).execute()
    except Exception as e:
        logging.error("Error saving raw listing to Supabase for %s: %s", link, e)


def upsert_normalised_listing(
    details: PropertyDetails,
    external_id: str,
    *,
    source: str = "mubawab",
    listing_type: str = "rent",
    listing_tier: str,
    is_adboost: bool,
    main_image_path: str | None = None,
) -> None:
    """
    Write a clean, ML-ready row to normalised_listings.

    NOTE: agent_url is NOT stored here.
    """
    payload = {
        "external_id": external_id,
        "source": source,
        "listing_type": listing_type,
        "title": details.title,
        "description": details.description,
        "property_type": details.property_type,
        "city": details.city,
        "area": details.area,
        "size": details.size,
        "rooms": details.rooms,
        "bedrooms": details.bedrooms,
        "bathrooms": details.bathrooms,
        "price": details.price,
        "features": details.features,
        "condition": details.condition,
        "age": details.age,
        "orientation": details.orientation,
        "flooring": details.flooring,
        "floor_number": details.floor_number,
        "number_of_floors": details.number_of_floors,
        "lat": details.lat,
        "lon": details.lon,
        "url": details.url,
        "main_image_path": main_image_path,
        
        "agent_type": details.agent_type,
        "agent_name": details.agent_name,
        "listing_tier": listing_tier,
        "is_adboost": is_adboost,
    }

    supabase.table("normalised_listings").upsert(
        payload,
        on_conflict="external_id",
    ).execute()
        
#--------------Scraper Functions--------------#

def fetch(url: str) -> Optional[requests.Response]:
    """HTTP GET request with logging, timeout, and error handling."""
    try:
        resp = session.get(url, timeout=REQUEST_TIMEOUT)
        resp.raise_for_status()
        return resp
    except requests.RequestException as exc:
        logging.error("Request failed for %s: %s", url, exc)
        return None

def get_links(
    base_url: str,
    max_pages: int,
    existing_ids: set[str],
) -> list[dict]:
    """
    Scrape ALL listing cards from Mubawab (up to max_pages) and return
    a list of dicts with metadata:

      {
        "url":          full_listing_url,
        "external_id":  "a1234567",
        "listing_tier": "premium",
        "is_adboost":   False,
      }

    We skip:
      - already-scraped external_ids,
      - adBoostBox cards (sale ads in rent pages).
    """
    from bs4 import BeautifulSoup
    new_listings: list[dict] = []

    for page in range(1, max_pages + 1):
        page_url = f"{base_url}:p:{page}"
        logging.info("Processing page %s", page_url)

        resp = fetch(page_url)
        if resp is None:
            logging.warning("Failed to fetch page %s, stopping pagination.", page_url)
            break

        soup = BeautifulSoup(resp.text, "html.parser")

        # div.listingBox[linkref] was your working selector
        boxes = soup.select("div.listingBox[linkref]")
        if not boxes:
            logging.info("No listings found on page %s, stopping.", page)
            break

        logging.info("Found %d listing elements on page %d", len(boxes), page)

        for box in boxes:
            url = box.get("linkref")
            if not url:
                continue

            if url.startswith("/"):
                url = "https://www.mubawab.ma" + url

            external_id = get_mubawab_external_id(url)
            if not external_id:
                continue

            # skip if already normalised
            if external_id in existing_ids:
                continue

            listing_tier, is_adboost = classify_listing_box(box)

            # drop adBoostBox from the rental dataset
            if is_adboost:
                continue

            new_listings.append(
                {
                    "url": url,
                    "external_id": external_id,
                    "listing_tier": listing_tier,
                    "is_adboost": is_adboost,
                }
            )

        sleep(uniform(MIN_SLEEP, MAX_SLEEP))

    logging.info("Total NEW links collected this run: %d", len(new_listings))
    return new_listings

def get_image_links(soup: BeautifulSoup) -> list[str]:
    """
    Extracts all image URLs from the masonryPhoto container.
    """
    urls: list[str] = []
    container = soup.find("div", id="masonryPhoto")
    if not container:
        return urls

    for img in container.find_all("img"):
        src = img.get("src")
        if src:
            urls.append(src)

    # Deduplicate while preserving order
    seen = set()
    unique_urls = []
    for u in urls:
        if u not in seen:
            unique_urls.append(u)
            seen.add(u)

    return unique_urls
    
#--------------Data Cleaning Functions--------------#

def clean_integer(number_str: Optional[str]) -> Optional[int]:
    """
    Cleans all numerate fields by removing any non-digit characters, and 
    converting it into an integer value.

    Args:
        number_str (str): The price string of the property.

    Returns:
        int or None: The cleaned price as an integer, or None if invalid.
    """
    
    if not number_str:
        return None
    try:
        # Remove all non-digit characters
        cleaned = re.sub(r'[^\d]', '', number_str)
        return int(cleaned) if cleaned else None
    except (ValueError, TypeError):
        return None
    
def clean_text(text: Optional[str]) -> Optional[str]:
    return text.strip() if text else None

def clean_att(s: str) -> str:
    return " ".join(s.split()).strip()

def clean_age(age_str):
    """
    Extract age range like '0-5' only if 'years' present and exactly 2 numbers exist.
    """
    if not age_str:
        return None

    age_str_lower = age_str.lower()
    if 'years' not in age_str_lower:
        return None

    numbers = re.findall(r'\d+', age_str)
    if len(numbers) == 2:
        return f"{int(numbers[0])}-{int(numbers[1])}"
    return None
        
def clean_rooms(description: Optional[str]) -> Optional[int]:
    if not description:
        return None
    pattern = r'(\d+)\s*(?:\w+\s)?rooms?\b'
    match = re.search(pattern, description, re.IGNORECASE)
    if match:
        return int(match.group(1))
    return None

def clean_condition(cond_str):
    try:
        if not cond_str:
            return None
        
        if cond_str == 'Good condition':
            cond_str = 'Good'
            return cond_str
        elif cond_str == 'Due for reform':
            cond_str == 'Old'
            return cond_str
        elif cond_str == 'New':
            return cond_str
        else:
            return None
    except ValueError:
        return None
    
def parse_area_and_city(raw_area_text: Optional[str]) -> Tuple[Optional[str], Optional[str]]:
    if not raw_area_text:
        logging.warning("raw_area_text is None or empty.")
        return None, None
    
    raw_area_text = raw_area_text.strip()
    pattern = r'^(.*)\s+in\s+(.*)$'
    match = re.search(pattern, raw_area_text, re.IGNORECASE)
    
    if match:
        area = match.group(1).strip()
        city = match.group(2).strip()
        logging.debug(
            "Parsed area: '%s', city: '%s' from raw_area_text: '%s'",
            area,
            city,
            raw_area_text,
        )
    else:
        area = None
        city = raw_area_text.strip()
        logging.debug(
            "No 'in' found. Set area to None and city to '%s' from raw_area_text: '%s'",
            city,
            raw_area_text,
        )
    return area, city

def extract_coordinates(soup: BeautifulSoup) -> Tuple[Optional[float], Optional[float]]:
    lat: Optional[float] = None
    lon: Optional[float] = None
    
    scripts = soup.find_all("script")
    for s in scripts:
        if s.string and "waze.com/ul" in s.string:
            match = re.search(r"waze\.com/ul\?ll=([^&]+)", s.string)
            if match:
                ll = unquote(match.group(1))
                try:
                    lat_str, lon_str = ll.split(",")                    
                    lat = float(lat_str)
                    lon = float(lon_str)
                    break
                except ValueError:
                    logging.error(f"Error parsing lat/long from: {ll}")
    
    return lat, lon

def process_listing_images(external_id: str, image_urls: list[str]) -> str | None:
    """
    For a given listing:
      - uploads each image to Storage
      - inserts/updates listing_images rows
    Returns the storage_path of the first image (for main_image_path),
    or None if no images processed.
    """
    main_image_path: str | None = None

    for idx, url in enumerate(image_urls):
        # Skip if this image_index already exists
        existing = (
            supabase.table("listing_images")
            .select("external_id")
            .eq("external_id", external_id)
            .eq("image_index", idx)
            .execute()
        )
        if existing.data:
            # If we already have a row, use its storage_path for main_image_path if needed
            if main_image_path is None and idx == 0:
                main_image_path = (
                    supabase.table("listing_images")
                    .select("storage_path")
                    .eq("external_id", external_id)
                    .eq("image_index", idx)
                    .limit(1)
                    .execute()
                    .data[0]["storage_path"]
                )
            continue

        storage_path = upload_avif_to_r2(external_id, idx, url)
        if not storage_path:
            continue

        supabase.table("listing_images").upsert(
            {
                "external_id": external_id,
                "image_index": idx,
                "original_url": url,
                "storage_path": storage_path,
            },
            on_conflict="external_id,image_index",
        ).execute()

        if main_image_path is None and idx == 0:
            main_image_path = storage_path

    return main_image_path
    
#--------------Main Scraper Logic--------------#

def parse_property_page(link: str, html: str) -> Optional[PropertyDetails]:
    """Parse a single property detail HTML into a PropertyDetails object."""
    soup = BeautifulSoup(html, 'html.parser')
    
    price_tag = soup.find('h3', class_='orangeTit')
    area_tag = soup.find('h3', class_='greyTit')
    title_tag = soup.find('h1', class_='searchTitle')
    
    if not (price_tag and area_tag and title_tag):
        logging.warning(f"Missing critical tags on page {link}, skipping.")
        return None
    
    raw_price = price_tag.get_text(strip=True)
    raw_area_text = area_tag.get_text(strip=True)
    raw_title = title_tag.get_text(strip=True)
    
    price = clean_integer(raw_price)
    title = clean_text(raw_title)
    area, city = parse_area_and_city(raw_area_text)
    
    #-- Description --#
    text_content: Optional[str] = None
    description_div = soup.find('div', class_='wordBreak')
    if description_div:
        text_content = description_div.get_text(separator=" ").strip()
    else:
        for p in soup.find_all('p'):
            if len(p.text.strip()) > 50:
                text_content = p.get_text(separator=" ").strip()
                break
            
    #-- Main features --#
    features_block = soup.find("div", class_="adFeatures")
    label_value: dict[str, str] = {}
    if features_block:
        for content in features_block.select("div.adMainFeatureContent"):
            label_tag = content.find("p", class_="adMainFeatureContentLabel")
            value_tag = content.find("p", class_="adMainFeatureContentValue")
            if not label_tag or not value_tag:
                continue
            label = clean_att(label_tag.get_text())
            value = clean_att(value_tag.get_text())
            label_value[label] = value
            
    prop_type = label_value.get("Type of property")
    condition = clean_condition(label_value.get("Condition"))
    age_raw = label_value.get("Age")
    orientation = label_value.get("Orientation")
    flooring = label_value.get("Flooring")
    floor_number = clean_integer(label_value.get("Floor number"))
    number_of_floors = clean_integer(label_value.get("Number of floors"))
    
    #-- Additional details --#
    size: Optional[int] = None
    rooms: Optional[int] = None
    bedrooms: Optional[int] = None
    bathrooms: Optional[int] = None
    
    details_block = soup.find_all('div', class_='adDetailFeature')
    for detail in details_block:
        text = detail.get_text(strip=True)
        span = detail.find('span')
        if not span:
            continue
        value = span.get_text(strip=True)
        
        if 'm²' in text:
            size = clean_integer(value)
        if 'Pieces' in text or 'Piece' in text:
            rooms = clean_integer(value)
        if 'Rooms' in text or 'Room' in text:
            bedrooms = clean_integer(value)
        if 'Bathrooms' in text or 'Bathroom' in text:
            bathrooms = clean_integer(value)
            
    if rooms is None and text_content:
        rooms = clean_rooms(text_content)
        
    #-- Feature List --#
    features_tags = soup.find_all('p', class_='fSize11 centered')
    features_list = [clean_text(tag.get_text()) for tag in features_tags]
    feature_str = ', '.join(filter(None, features_list)) if features_list else None
    
    lat, lon = extract_coordinates(soup)

    # 👇 NEW: agent metadata
    agent_type, agent_name, agent_url = extract_agent_info(soup)
    
    return PropertyDetails(
        title=title,
        description=text_content,
        property_type=prop_type,
        city=city,
        area=area,
        size=size,
        rooms=rooms,
        bedrooms=bedrooms,
        bathrooms=bathrooms,
        price=price,
        features=feature_str,
        condition=condition,
        age=clean_age(str(age_raw)) if age_raw else None,
        orientation=orientation,
        flooring=flooring,
        floor_number=floor_number,
        number_of_floors=number_of_floors,
        lat=lat,
        lon=lon,
        url=link,
        agent_type=agent_type,
        agent_name=agent_name,
        agent_url=agent_url,
    )

def get_details(
    listings_meta: list[dict],
    source: str = "mubawab",
    listing_type: str = "rent",
) -> pd.DataFrame:
    rows: list[dict] = []
    for listing_meta in tqdm.tqdm(listings_meta, desc="Scraping property details"):
        try:
            result = process_single_listing(listing_meta, source, listing_type)
            if result is not None:
                rows.append(result)
        except Exception as e:
            logging.error("Unhandled exception processing %s: %s", listing_meta["url"], e)
    return pd.DataFrame(rows)

def extract_agent_info(soup: BeautifulSoup) -> tuple[Optional[str], Optional[str], Optional[str]]:
    """
    Extract agent_type, agent_name, agent_url from the property page.

    Returns:
        (agent_type, agent_name, agent_url)

        agent_type ∈ {"agency", "individual", None}
    """
    agent_type = None
    agent_name = None
    agent_url = None

    info = soup.find("div", class_="businessInfo")
    if not info:
        return agent_type, agent_name, agent_url

    # The agency / individual name is in span.link.businessName
    span = info.select_one("span.link.businessName")
    if not span:
        return agent_type, agent_name, agent_url

    # If there's an <a>, it's an agency with its own page
    a_tag = span.find("a", href=True)
    if a_tag:
        agent_name = a_tag.get_text(strip=True)
        agent_url = a_tag["href"]
        # look for "Agency" / "Particular" text nearby
        type_text = span.get_text(" ", strip=True).lower()
        if "agency" in type_text:
            agent_type = "agency"
        elif "particular" in type_text:
            agent_type = "individual"
        else:
            agent_type = "agency"  # sensible default
    else:
        # No link → usually a private individual
        agent_name = span.get_text(strip=True)
        agent_type = "individual"

    return agent_type, agent_name, agent_url

#--------------Main Execution--------------#

def main() -> None:
    # Base rent URL – adjust if you add other listing types later
    base_url = "https://www.mubawab.ma/en/cc/real-estate-for-rent"

    # Full catalog: you said ~535 pages for rent
    max_pages = 536

    logging.info("Loading existing external_ids cache from normalised_listings...")
    existing_ids = load_existing_external_ids(source="mubawab", listing_type="rent")
    logging.info("Loaded %d existing external_ids into cache.", len(existing_ids))

    logging.info("Starting full scan link scraping (skipping already-scraped IDs)...")
    listings = get_links(
        base_url=base_url,
        max_pages=max_pages,
        existing_ids=existing_ids,
    )
    logging.info("Collected %d NEW listings to scrape.", len(listings))

    if not listings:
        logging.info("No new listings found. Exiting.")
        return

    logging.info(
        "Scraping property details and writing to Supabase + R2 for %d listings...",
        len(listings),
    )
    df = get_details(
        listings,
        source="mubawab",
        listing_type="rent",
    )
    logging.info("Scraped %d NEW properties this run.", len(df))
    logging.info("Done.")


if __name__ == "__main__":
    main()
