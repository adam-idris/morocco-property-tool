# repair_missing_normalised.py
# ----------------------------
# Repairs all missing rows in normalised_listings using the HTML stored
# in raw_listings.payload_json. Does NOT fetch pages again.

import os
import logging
from dataclasses import asdict
from datetime import datetime, timezone

from supabase import create_client, Client
from dotenv import load_dotenv
from bs4 import BeautifulSoup
import re
import json

# ----------------------------
# ENV + CLIENT SETUP
# ----------------------------

load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

if not SUPABASE_URL or not SUPABASE_KEY:
    raise RuntimeError("Missing Supabase credentials")

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)

# ----------------------------
# IMPORT PARSERS FROM MAIN SCRAPER
# (If this file is separate, you can paste the same functions here)
# ----------------------------

from scraper import (
    PropertyDetails,
    parse_property_page,
    clean_integer,
    clean_text,
    clean_rooms,
    clean_age,
    clean_condition,
    parse_area_and_city,
    extract_coordinates,
    get_image_links,
)

# ----------------------------
# FIND MISSING NORMALISED LISTINGS
# ----------------------------

def get_missing_normalised_ids() -> list[str]:
    """Return a list of external_ids that exist in raw_listings but not in normalised_listings."""
    logging.info("Checking for missing normalised listings...")

    raw_res = supabase.table("raw_listings").select("external_id").execute()
    norm_res = supabase.table("normalised_listings").select("external_id").execute()

    raw_ids = {row["external_id"] for row in raw_res.data}
    norm_ids = {row["external_id"] for row in norm_res.data}

    missing = sorted(list(raw_ids - norm_ids))
    logging.info("Found %d missing normalised listings.", len(missing))
    return missing

# ----------------------------
# REPAIR A SINGLE LISTING
# ----------------------------

def repair_single_listing(external_id: str):
    logging.info("Repairing %s ...", external_id)

    # 1) Retrieve stored HTML + metadata
    res = supabase.table("raw_listings").select("*").eq("external_id", external_id).limit(1).execute()
    if not res.data:
        logging.error("Missing raw_listing row for %s", external_id)
        return False

    payload = res.data[0]["payload_json"]
    html = payload.get("html")
    url = payload.get("url")

    if not html:
        logging.error("No HTML stored for %s", external_id)
        return False

    # 2) Parse the HTML into structured fields
    details = parse_property_page(url, html)
    if details is None:
        logging.error("Failed to parse stored HTML for %s", external_id)
        return False

    # 3) Insert/Upsert into normalised_listings (WITHOUT images)
    update_payload = asdict(details)
    update_payload.update(
        {
            "external_id": external_id,
            "source": "mubawab",
            "listing_type": "rent",
            "main_image_path": None,  # will fix later
        }
    )

    supabase.table("normalised_listings").upsert(
        update_payload,
        on_conflict="external_id",
    ).execute()

    # 4) Repair image table if missing
    image_urls = payload.get("image_urls", [])
    repair_listing_images(external_id, image_urls)

    logging.info("Repaired %s successfully.", external_id)
    return True

# ----------------------------
# REPAIR LISTING IMAGES (NO R2 UPLOAD)
# Uses stored URLs only, and only inserts when row missing
# ----------------------------

def repair_listing_images(external_id: str, image_urls: list[str]):
    if not image_urls:
        logging.info("%s has no stored images in raw_listings.", external_id)
        return

    main_image_path = None

    for idx, url in enumerate(image_urls):
        # Check if exists
        exists = (
            supabase.table("listing_images")
            .select("storage_path")
            .eq("external_id", external_id)
            .eq("image_index", idx)
            .execute()
            .data
        )
        if exists:
            if idx == 0:
                main_image_path = exists[0]["storage_path"]
            continue

        # Insert placeholder entry using the original URL only
        # storage_path remains NULL (since no R2 upload here)
        supabase.table("listing_images").insert(
            {
                "external_id": external_id,
                "image_index": idx,
                "original_url": url,
                "storage_path": None,
            }
        ).execute()

    # Patch main image if missing
    if main_image_path:
        supabase.table("normalised_listings").update(
            {"main_image_path": main_image_path}
        ).eq("external_id", external_id).execute()

# ----------------------------
# MAIN EXECUTION
# ----------------------------

def main():
    missing_ids = get_missing_normalised_ids()
    if not missing_ids:
        logging.info("Nothing to repair — all normalised listings are present.")
        return

    repaired = 0

    for ext_id in missing_ids:
        if repair_single_listing(ext_id):
            repaired += 1

    logging.info("Repair completed: %d/%d listings fixed.", repaired, len(missing_ids))


if __name__ == "__main__":
    main()
