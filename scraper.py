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
    floor_number: Optional[int]        # <- changed from Optional[str]
    number_of_floors: Optional[int]
    lat: Optional[float]
    lon: Optional[float]
    url: str

#--------------Helper Functions--------------#

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


def save_raw_listing(source: str, link: str, response_text: str) -> None:
    """
    Inserts or upserts the raw listing into the raw_listings table.
    """
    external_id = get_mubawab_external_id(link)

    if external_id is None:
        logging.warning(f"Could not extract external_id from URL: {link}")
        return

    payload = {
        "url": link,
        "html": response_text,
        "scraped_at_client": datetime.now(timezone.utc).isoformat(),
    }

    try:
        supabase.table("raw_listings").upsert(
            {
                "external_id": external_id,
                "source": source,
                "payload_json": payload,
            },
            on_conflict="external_id",   # <- CHANGED
        ).execute()
    except Exception as e:
        logging.error(f"Error saving raw listing to Supabase for {link}: {e}")
        
def upsert_normalised_listing(
    details: PropertyDetails,
    source: str = "mubawab",
    listing_type: str = "sale",   # for now this script is only for-sale
) -> None:
    """
    Upserts a cleaned property into normalised_listings.
    """
    external_id = get_mubawab_external_id(details.url)
    if not external_id:
        logging.warning("No external_id for normalized listing: %s", details.url)
        return

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
    }

    try:
        supabase.table("normalised_listings").upsert(
            payload,
            on_conflict="external_id",   # PK for this table too
        ).execute()
    except Exception as e:
        logging.error(
            "Error upserting normalized listing for %s (external_id=%s): %s",
            details.url,
            external_id,
            e,
        )
        
#--------------Scraper Functions--------------#

def fetch(url: str) -> Optional[requests.Response]:
    """HTTP GET request with logging, timeout, and error handling."""
    try:
        resp = session.get(url, timeout=REQUEST_TIMEOUT)
        resp.raise_for_status()
        return resp
    except requests.RequestException as exc:
        logging.error(f"Request failed for {url}: {e}")
        return None

def get_links(
    base_url: str,
    max_pages: int = 50,
    source: str = "mubawab",
) -> list[str]:
    """
    Scrape links newest to oldest until we hit the last external_id
    we saw on a previous run.
    """
    last_external_id = get_last_external_id(source=source)
    if last_external_id:
        logging.info("Last seen external_id for %s: %s", source, last_external_id)
    else:
        logging.info("No previous external_id found for %s (first run).", source)

    prop_links: list[str] = []
    page = 1
    stop_pagination = False

    while page <= max_pages and not stop_pagination:
        page_url = f"{base_url}:p:{page}"
        resp = fetch(page_url)
        if resp is None:
            break

        soup = BeautifulSoup(resp.content, "html.parser")
        listings = soup.find_all("div", class_="listingBox sPremium")

        if not listings:
            logging.info("No listings found on page %s. Stopping.", page)
            break

        logging.info("Processing page %s (%d listings)", page, len(listings))

        for listing in listings:
            link = listing.get("linkref")
            if not link:
                continue

            external_id = get_mubawab_external_id(link)
            if not external_id:
                logging.warning("Could not extract external_id for %s", link)
                prop_links.append(link)
                continue

            if last_external_id and external_id == last_external_id:
                logging.info(
                    "Hit last seen listing %s (external_id=%s). Stopping pagination.",
                    link,
                    external_id,
                )
                stop_pagination = True
                break

            prop_links.append(link)

        page += 1
        sleep(uniform(MIN_SLEEP, MAX_SLEEP))

    return prop_links

    
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
        logging.debug(f"Parsed area: '{area}', city: '{city}' from raw_area_text: '{raw_area_text}'")
    else:
        area = None
        city = raw_area_text.strip()
        logging.debug(f"No 'in' found. Set area to None and city to '{city}' from raw_area_text: '{raw_area_text}'")
    return area, city

def extract_coordinates(soup: BeautifulSoup) -> Tuple[Optional[float], Optional[float]]:
    lat = Optional[float] = None
    lon = Optional[float] = None
    
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
    condition = label_value.get("Condition")
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
    
    details = soup.find_all('div', class_='adDetailFeature')
    for detail in details:
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
        number_of_floors=clean_integer(number_of_floors),
        lat=lat,
        lon=lon,
        url=link
        
    )

def get_details(links: List[str]) -> pd.DataFrame:
    """
    Scrapes the important features of each property.

    Args:
        links (str): The URLs of each property to be scraped.

    Returns:
        DataFrame: DataFrame containing property features.
    """
    properties: List[dict] = []
    
    for link in tqdm.tqdm(links, desc="Scraping property details"):
        
        resp = fetch(link)
        if resp is None:
            continue
        
        save_raw_listing(source="mubawab", link=link, response_text=resp.text)
        
        try:
            details = parse_property_page(link, resp.text)
            if details is None:
                continue
            
            upsert_normalised_listing(details, source="mubawab")
            
            properties.append(asdict(details))
        
        except Exception as exc:
            logging.error(f"Error parsing property data from {link}: {exc}")
            
        sleep(uniform(MIN_SLEEP, MAX_SLEEP))
            
    return pd.DataFrame(properties)

def get_last_external_id(source: str = "mubawab") -> Optional[str]:
    """
    Returns the external_id of the most recently scraped listing for a source,
    or None if table is empty.
    """
    res = (
        supabase.table("raw_listings")
        .select("external_id, scraped_at")
        .eq("source", source)
        .order("scraped_at", desc=True)
        .limit(1)
        .execute()
    )

    if not res.data:
        return None

    return res.data[0]["external_id"]

#--------------Main Execution--------------#

def main() -> None:
    base_url = "https://www.mubawab.ma/en/cc/real-estate-for-rent"
    max_pages = 50

    logging.info("Starting incremental link scraping...")
    links = get_links(base_url, max_pages=max_pages, source="mubawab")
    logging.info("Found %d NEW property links.", len(links))

    logging.info("Scraping property details and writing to Supabase...")
    df = get_details(links)
    logging.info("Scraped %d properties.", len(df))
    logging.info("Done.")
    
if __name__ == "__main__":
    main()