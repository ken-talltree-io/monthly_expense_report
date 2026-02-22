#!/usr/bin/env python3
"""
Financial Dashboard & Subscription Auditor
Reads credit card CSV exports and generates a self-contained HTML dashboard
with personal & corporate financial overview.

Usage:
    python3 dashboard.py                     # Basic dashboard
    python3 dashboard.py --ai                # With AI recommendations
    python3 dashboard.py --path /some/folder # Different CSV folder
"""

import argparse
import csv
import glob
import json
import os
import ssl
import sys
from collections import defaultdict
from datetime import datetime
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

# ── Merchant normalization ───────────────────────────────────────────────────
# Maps substrings in raw merchant names to a clean canonical name.
MERCHANT_ALIASES = {
    "AMAZON.CA": "Amazon",
    "AMZN MKTP CA": "Amazon",
    "AMAZON.CA PRIME": "Amazon Prime",
    "APPLE.COM/BILL": "Apple Subscriptions",
    "APPLE.COM/CA": "Apple Store",
    "NETFLIX": "Netflix",
    "DISNEY PLUS": "Disney+",
    "BELL MEDIA": "Bell Media (Crave)",
    "RMI-SPORTSNET": "Sportsnet NOW",
    "WWW.MUBI.COM": "MUBI",
    "FIDO MOBILE": "Fido Mobile",
    "TELUS PRE-AUTH": "Telus Home Internet",
    "TELUS MOBILITY": "Telus Mobility",
    "LYFT": "Lyft",
    "UBER CANADA": "Uber",
    "PETRO-CANADA": "Petro-Canada",
    "LONDON DRUGS": "London Drugs",
    "SHOPPERS DRUG MART": "Shoppers Drug Mart",
    "REAL CDN": "Real Canadian Superstore",
    "MARKETPLACE IGA": "IGA",
    "IGA 1070": "IGA",
    "TIM HORTONS": "Tim Hortons",
    "STARBUCKS": "Starbucks",
    "CITY OF VAN PAYBYPHONE": "PayByPhone Parking",
    "CITY OF VAN-PARKS": "City of Vancouver Parks",
    "IMPARK": "Impark Parking",
    "COMPASS": "TransLink Compass",
    "BCF - ": "BC Ferries",
    "BCF-": "BC Ferries",
    "DOLLARAMA": "Dollarama",
    "CANADIAN TIRE": "Canadian Tire",
    "WWW.CANADIANTIRE": "Canadian Tire",
    "SPORT CHEK": "Sport Chek",
    "WWW.SPORTCHEK": "Sport Chek",
    "ARDENE": "Ardene",
    "CONTINENTAL COFFEE": "Continental Coffee",
    "COBS BREAD": "Cobs Bread",
    "MEC MOUNTAIN": "MEC",
    "MOUNTAIN EQUIPMENT": "MEC",
    "INDIGO": "Indigo",
    "FORECAST COFFEE": "Forecast Coffee",
    "JJ BEAN": "JJ Bean Coffee",
    "PRADO CAFE": "Prado Cafe",
    "MOJA COFFEE": "Moja Coffee",
    "SEPHORA": "Sephora",
    "PET VALU": "Pet Valu",
    "RONA": "Rona",
    "TACOFINO": "Tacofino",
    "HONK PARKING": "Honk Parking",
    "SQ *MRPETS": "MrPets",
    "SQ *NEMESIS": "Nemesis Coffee",
    "SQ *FC HEADLAND": "FC Headland",
    "SQ *FLOURIST": "Flourist",
    "SQ *PAULIE": "Paulie's Barbershop",
    "SQ *MORE CAFE": "More Cafe & Bakeshop",
    "SQ *KITS BEACH": "Kits Beach Coffee",
    "SQ *PRADO": "Prado Cafe",
    "SQ *JJ BEAN": "JJ Bean Coffee",
    "SQ *FORECAST": "Forecast Coffee",
    "TST-TACOFINO": "Tacofino",
    "TST-SULA": "Sula Indian Restaurant",
    "AIR CAN": "Air Canada",
    "AIR-SERV": "Air-Serv (Tire Inflation)",
    "SUNPEAKSRESORT": "Sun Peaks Resort",
    "SUN PEAKS": "Sun Peaks Resort",
    "SAFEWAY": "Safeway",
    "SAVE ON FOODS": "Save-On-Foods",
    "SUPER VALU": "Super Valu",
    "WHOLE FOODS": "Whole Foods",
    "OPENHEARTPROJECT": "Open Heart Project",
    "SHINE AUTO WASH": "Shine Auto Wash",
    "ZIPBY": "Zipby (Bridge Toll)",
    "DOMINOS": "Domino's Pizza",
    "OLD NAVY": "Old Navy",
    "NWEST PARKING": "New West Parking",
    "DOLLAR TREE": "Dollar Tree",
    "ICBC": "ICBC",
    "BCAA-INSURANCE": "BCAA Insurance",
    "WAWANESA": "Wawanesa Insurance",
    "BIG WHITE": "Big White Ski Resort",
    "MT SEYMOUR": "Mt Seymour",
    "SPIRIT OF MT SEYMOUR": "Mt Seymour",
    "SCANDINAVE SPA": "Scandinave Spa",
    "TICKETMASTER": "Ticketmaster",
    "SEATGEEK": "SeatGeek",
    "HOME DEPOT": "Home Depot",
    "KAL TIRE": "KAL Tire",
    "EXPEDIA": "Expedia",
    "BEST WESTERN": "Best Western",
    "LULULEMON": "Lululemon",
    "LAMAISONSIMONS": "Simons",
    "UNIQLO": "Uniqlo",
    "7-ELEVEN": "7-Eleven",
    "EVENTBRITE": "Eventbrite",
    "PANAGO": "Panago Pizza",
    "HARBOUR AIR": "Harbour Air",
    "THEUPSSTORE": "UPS Store",
    "NEW VISAGE": "New Visage Skincare",
    "CAMBIE BROADWAY DENTAL": "Cambie Broadway Dental",
    "YALETOWN DENTISTRY": "Yaletown Dentistry",
    "TOT 2 TEEN DENTAL": "Tot 2 Teen Dental",
    "BC LIQUOR": "BC Liquor Store",
    "LIBERTY WINE": "Liberty Wine Merchants",
    "LEGACY LIQUOR": "Legacy Liquor Store",
    "FA BARTLETT TREE": "Bartlett Tree Experts",
    "BEAR COUNTRY PROPERTY": "Bear Country Property Mgmt",
    "CAULFEILD VETERINARY": "Caulfeild Vet Hospital",
    "TESLA": "Tesla",
    "BUSY BEE CLEANERS": "Busy Bee Cleaners",
    "BRIEF MEDIA": "Brief Media",
    "HARBOUR OYSTER": "Harbour Oyster + Bar",
    "BRINY OYSTER": "Briny Oyster + Bar",
    "PHO37": "Pho 37",
    "LA MEZCALERIA": "La Mezcaleria",
    "CACTUS CLUB": "Cactus Club",
    "MAKEA-A-WISH": "Make-A-Wish Foundation",
    "MAKE-A-WISH": "Make-A-Wish Foundation",
    "YMCA": "YMCA",
    "CINEPLEX": "Cineplex",
    "THRIFTY FOODS": "Thrifty Foods",
    "PET PANTRY": "Pet Pantry",
    "LONG & MCQUADE": "Long & McQuade",
    "VESSI": "Vessi Footwear",
    "URBAN PLANET": "Urban Planet",
    "REXALL": "Rexall Pharmacy",
    "SHOE COMPANY": "Shoe Company",
    "SPIRIT HALLOWEEN": "Spirit Halloween",
    "PAYPAL": "PayPal",
    "DECATHLON": "Decathlon",
    "CAPRI VALLEY": "Capri Valley Lanes",
    "OTTER CO-OP": "Otter Co-op",
    # Debit card merchants
    "STRATFORD HALL": "Stratford Hall (Tuition)",
    "HYUNDAI PMNT": "Hyundai Car Payment",
    "FORTISBC": "FortisBC",
    "B.C. HYDRO": "BC Hydro",
    "DR. LIAT TZUR": "Dr. Liat Tzur (Orthodontics)",
    "ZENSURANCE": "Zensurance",
    "FRESHBOOKS": "FreshBooks",
    "FN": "Mortgage (First National)",
}

# Business expenses — excluded from personal spending totals
BUSINESS_MERCHANTS = {"Zensurance", "FreshBooks"}

# Debt payoff thresholds — AFT_OUT amounts above these are one-time payoffs,
# not regular spending. Regular payments (below threshold) remain as fixed costs.
DEBT_PAYOFF_THRESHOLDS = {
    "Mortgage (First National)": 5000,
    "Hyundai Car Payment": 5000,
}

CORPORATE_TAKE_HOME_RATE = 0.60  # Est. personal take-home after corp tax + personal tax on T4/T5


def normalize_merchant(raw: str) -> str:
    upper = raw.upper().strip()
    # Check longer keys first so "AMAZON.CA PRIME" matches before "AMAZON.CA"
    for key in sorted(MERCHANT_ALIASES, key=len, reverse=True):
        if key.upper() in upper:
            return MERCHANT_ALIASES[key]
    # Fall back: title-case the raw name, strip trailing codes
    cleaned = raw.strip().rstrip("0123456789 *#")
    return cleaned.title() if cleaned else raw.strip().title()


# ── Category mapping ─────────────────────────────────────────────────────────
# Keywords checked against the NORMALIZED merchant name (case-insensitive).
CATEGORY_RULES = [
    ("Restaurants & Dining", [
        "Pho 37", "La Mezcaleria", "Cactus Club", "Sal Y Limon", "Black Bean",
        "Harbour Oyster", "Briny Oyster", "Sula", "Sandbar", "Chancho",
        "Belgian Fries", "Noodlebox", "Kojima", "Meson Spanish", "Ikhaya",
        "End Of The Line", "Viet Family", "Zabb", "Il Mundo", "Subway",
        "Domino", "Panago", "Mountain High Pizza", "Sarpino", "Grillworks",
        "Isetta", "Abbott St", "Mikes Place", "The Office Restaurant",
        "Home Restaurant", "The Mad Hen", "Powder Hounds", "Flying Otter",
        "Artigiano", "Local Kitsilano", "Cahilty", "OEB", "Green Moustache",
        "Platform 7", "New York Fries", "Club De Playa", "Craft Maison",
        "Alice And Brohm", "Bluebird Market", "The Burrow",
        "The Gumboot Cafe", "The Gumboot Restaurant", "Latin America",
        "Baked Cookies", "Toasty By Sprout", "Santa Barbara Market",
    ]),
    ("Coffee Shops", [
        "Continental Coffee", "JJ Bean", "Prado Cafe", "Forecast Coffee",
        "Moja Coffee", "Starbucks", "Nemesis Coffee", "Kits Beach Coffee",
        "Bean Scene", "White Rabbit Coffee", "Bolacco", "Crema Cafe",
        "Parsonage Cafe",
    ]),
    ("Bakeries & Treats", [
        "Cobs Bread", "Purebread", "Livia Sweets", "To Live For Bakery",
        "The First Ravioli", "Oh Sweet Day", "Earnest Ice Cream",
        "The Bench Bakehouse", "Uprising Breads", "Terra Breads",
        "More Cafe", "That Churro", "The Bread Company", "Melt Confectionary",
        "Siegel", "Dilly Dally",
    ]),
    ("Groceries", [
        "Real Canadian Superstore", "IGA", "Safeway", "Save-On-Foods",
        "Super Valu", "Whole Foods", "Thrifty Foods", "Sweet Cherubim",
        "City Avenue Market", "Fig Mart", "Persia Foods", "Mostafa",
        "The Grocery Store", "New Triple A", "Dundas KK", "Otter Co-op",
        "Good Fridays", "Choices Drive", "Flourist",
    ]),
    ("Liquor & Alcohol", [
        "BC Liquor", "Liberty Wine", "Legacy Liquor", "Sundance Liquor",
        "Strange Fellows", "Strathcona Beer", "Commercial Drive Licoric",
    ]),
    ("Telecom", [
        "Telus", "Fido Mobile",
    ]),
    ("Streaming & Subscriptions", [
        "Netflix", "Disney+", "Bell Media", "Sportsnet NOW", "MUBI",
        "Apple Subscriptions", "Amazon Prime", "Open Heart Project",
        "Brief Media",
    ]),
    ("Pets", [
        "MrPets", "Pet Valu", "Pet Pantry", "Caulfeild Vet",
    ]),
    ("Transportation", [
        "Lyft", "Uber", "BC Ferries", "Air Canada", "TransLink Compass",
        "Harbour Air", "Expedia",
    ]),
    ("Parking & Gas", [
        "PayByPhone", "Impark", "Honk Parking", "Petro-Canada",
        "Chevron", "Nwest Parking", "New West Parking", "Zipby",
    ]),
    ("Clothing", [
        "Ardene", "Old Navy", "Uniqlo", "Simons", "Lululemon", "Vessi",
        "JQ Clothing", "Mintage Vintage", "Funktional", "Urban Planet",
        "Shoe Company", "Quidditas", "Gatley", "Sool Of Thread",
        "Spool Of Thread", "Dressew",
    ]),
    ("Sports & Outdoor", [
        "MEC", "Sport Chek", "Decathlon", "Sports Junkies", "Canucks",
        "Long & McQuade", "Drive Drum",
    ]),
    ("Ski Resorts", [
        "Sun Peaks", "Mt Seymour", "Big White",
    ]),
    ("Home Improvement", [
        "Home Depot", "Rona", "Dal-Tile", "Lighting Warehouse",
        "Magnet Hardware", "Skyland Building", "Bartlett Tree",
        "Bear Country Property", "Figaros Garden",
    ]),
    ("Health & Beauty", [
        "London Drugs", "Shoppers Drug Mart", "Rexall", "New Visage",
        "Harlow Skin", "Body Energy", "Spice Beauty", "Paulie",
        "Caulfeild Pharmasave", "Hemlock Hospital",
        "Mount Pleasant Visio",
    ]),
    ("Dental", [
        "Yaletown Dentistry", "Cambie Broadway Dental", "Tot 2 Teen Dental",
        "Dr. Liat Tzur", "Sunrise Orthodontics",
    ]),
    ("Insurance", [
        "ICBC", "BCAA Insurance", "Wawanesa",
    ]),
    ("Entertainment", [
        "Ticketmaster", "SeatGeek", "Eventbrite", "Cineplex",
        "Scandinave Spa", "Butchart Gardens", "Candytopia", "Capri Valley",
        "Spirit Halloween", "Games On The Drive", "Got Craft",
        "Red Horses Gallery", "Mosaic Books", "International Travel Maps",
        "The Anza Club",
    ]),
    ("Amazon", [
        "Amazon",
    ]),
    ("Kids", [
        "Dilly Dally Kids",
    ]),
    ("Auto", [
        "KAL Tire", "Shine Auto Wash", "Air-Serv", "Tesla", "Sony Wash",
        "Hyundai Car Payment",
    ]),
    ("Travel & Hotels", [
        "Best Western", "Nomade Cabo", "Merpago", "Clip Mx",
        "La Comer", "Tastes On The Fly", "0835_YVR",
    ]),
    ("Donations", [
        "Make-A-Wish",
    ]),
    ("Education", [
        "Stratford Hall",
    ]),
    ("Utilities", [
        "FortisBC", "BC Hydro",
    ]),
    ("Medical", [
    ]),
    ("Housing", [
        "Mortgage",
    ]),
]

# ── Category consolidation ───────────────────────────────────────────────────
# Maps fine-grained categories to broader groups for cleaner reporting.
# Any category not listed passes through unchanged.
CATEGORY_CONSOLIDATION = {
    "Education": "Kids & Education",
    "Kids": "Kids & Education",
    "Restaurants & Dining": "Food & Dining",
    "Coffee Shops": "Food & Dining",
    "Bakeries & Treats": "Food & Dining",
    "Groceries": "Food & Dining",
    "Liquor & Alcohol": "Food & Dining",
    "Home Improvement": "Housing & Utilities",
    "Housing": "Housing & Utilities",
    "Utilities": "Housing & Utilities",
    "Transportation": "Transportation",
    "Auto": "Transportation",
    "Parking & Gas": "Transportation",
    "Health & Beauty": "Health & Wellness",
    "Dental": "Health & Wellness",
    "Medical": "Health & Wellness",
    "Entertainment": "Recreation",
    "Sports & Outdoor": "Recreation",
    "Ski Resorts": "Recreation",
    "Travel & Hotels": "Travel",
    "Clothing": "Shopping",
    "Amazon": "Shopping",
    "Insurance": "Insurance",
    "Streaming & Subscriptions": "Subscriptions & Telecom",
    "Telecom": "Subscriptions & Telecom",
    "Pets": "Pets",
    "Donations": "Donations",
}


def load_user_categories(folder: str) -> dict:
    """Load merchant → category overrides from categories.csv if it exists."""
    overrides = {}
    path = os.path.join(folder, "categories.csv")
    if not os.path.exists(path):
        return overrides
    with open(path, newline="", encoding="utf-8-sig") as f:
        for row in csv.DictReader(f):
            merchant = (row.get("merchant") or "").strip()
            category = (row.get("category") or "").strip()
            if merchant and category and not merchant.startswith("# "):
                overrides[merchant.lower()] = category
    if overrides:
        print(f"Loaded {len(overrides)} category overrides from categories.csv")
    return overrides


def load_notes(folder: str) -> dict:
    """Load merchant → note from notes.csv if it exists."""
    notes = {}
    path = os.path.join(folder, "notes.csv")
    if not os.path.exists(path):
        return notes
    with open(path, newline="", encoding="utf-8-sig") as f:
        for row in csv.DictReader(f):
            merchant = (row.get("merchant") or "").strip()
            note = (row.get("note") or "").strip()
            if merchant and note and not merchant.startswith("# "):
                notes[merchant.lower()] = note
    if notes:
        print(f"Loaded {len(notes)} notes from notes.csv")
    return notes


def load_budgets(folder: str) -> dict:
    """Load category → monthly budget target from budgets.csv if it exists."""
    budgets = {}
    path = os.path.join(folder, "budgets.csv")
    if not os.path.exists(path):
        return budgets
    with open(path, newline="", encoding="utf-8-sig") as f:
        for row in csv.DictReader(f):
            category = (row.get("category") or "").strip()
            target = (row.get("monthly_target") or "").strip().replace("$", "").replace(",", "")
            if category and target and not category.startswith("# "):
                try:
                    budgets[category] = float(target)
                except ValueError:
                    pass
    if budgets:
        print(f"Loaded {len(budgets)} budget targets from budgets.csv")
    return budgets


# Global user overrides — populated in main() before categorization
_user_categories: dict = {}


def categorize(merchant: str) -> str:
    # Check user overrides first (from categories.csv)
    lower = merchant.lower()
    if lower in _user_categories:
        return _user_categories[lower]
    for category, keywords in CATEGORY_RULES:
        for kw in keywords:
            if kw.lower() in lower:
                return category
    return "Uncategorized"


# ── CSV Parsing ──────────────────────────────────────────────────────────────

def parse_csvs(folder: str) -> list[dict]:
    """Read credit card and debit card CSVs, return unified transaction list."""
    transactions = []

    # Collect CSV files from subdirectories
    all_files = []
    for subdir in ["credit card", "debit card"]:
        subpath = os.path.join(folder, subdir)
        if os.path.isdir(subpath):
            all_files.extend(sorted(glob.glob(os.path.join(subpath, "*.csv"))))
    # Backward compat: check root folder for credit-card-*.csv
    root_csvs = sorted(glob.glob(os.path.join(folder, "credit-card-*.csv")))
    if root_csvs:
        all_files.extend(root_csvs)
    if not all_files:
        skip = {"categories", "notes", "budgets"}
        all_files = sorted(f for f in glob.glob(os.path.join(folder, "*.csv"))
                           if not any(os.path.basename(f).startswith(s) for s in skip))
    if not all_files:
        print(f"Error: No CSV files found in {folder}")
        sys.exit(1)

    credit_count = debit_count = 0
    business_total = 0.0
    debt_payoffs = []  # track individual debt payoff events
    for fpath in all_files:
        with open(fpath, newline="", encoding="utf-8-sig") as f:
            reader = csv.DictReader(f)
            headers = reader.fieldnames or []

            if "transaction_date" in headers:
                # ── Credit card format ──
                credit_count += 1
                for row in reader:
                    amount = float(row["amount"])
                    txn_type = row.get("type", "Purchase")
                    if amount < 0 or txn_type.strip().lower() == "payment":
                        continue
                    date = datetime.strptime(row["transaction_date"], "%Y-%m-%d")
                    raw_merchant = row["details"]
                    merchant = normalize_merchant(raw_merchant)
                    if merchant in BUSINESS_MERCHANTS:
                        business_total += amount
                        continue
                    category = categorize(merchant)
                    category = CATEGORY_CONSOLIDATION.get(category, category)
                    transactions.append({
                        "date": date,
                        "month": date.strftime("%Y-%m"),
                        "raw_merchant": raw_merchant,
                        "merchant": merchant,
                        "category": category,
                        "amount": amount,
                        "source": "credit",
                    })

            elif "transaction" in headers:
                # ── Debit card format ──
                debit_count += 1
                for row in reader:
                    txn_type = row["transaction"]
                    amount = float(row["amount"])
                    description = row["description"]

                    if txn_type == "SPEND":
                        date = datetime.strptime(row["date"], "%Y-%m-%d")
                        merchant = normalize_merchant(description)
                        if merchant in BUSINESS_MERCHANTS:
                            business_total += abs(amount)
                            continue
                        category = categorize(merchant)
                        category = CATEGORY_CONSOLIDATION.get(category, category)
                        transactions.append({
                            "date": date,
                            "month": date.strftime("%Y-%m"),
                            "raw_merchant": description,
                            "merchant": merchant,
                            "category": category,
                            "amount": abs(amount),
                            "source": "debit",
                        })
                    elif txn_type == "AFT_OUT":
                        # Extract merchant from "Pre-authorized Debit to MERCHANT"
                        merchant_raw = description
                        if "Pre-authorized Debit to " in description:
                            merchant_raw = description.split("Pre-authorized Debit to ", 1)[1]
                        date = datetime.strptime(row["date"], "%Y-%m-%d")
                        merchant = normalize_merchant(merchant_raw)
                        amt = abs(amount)
                        # Exclude large one-time debt payoffs
                        threshold = DEBT_PAYOFF_THRESHOLDS.get(merchant)
                        if threshold and amt > threshold:
                            debt_payoffs.append({
                                "merchant": merchant,
                                "amount": amt,
                                "date": date,
                            })
                            continue
                        category = categorize(merchant)
                        category = CATEGORY_CONSOLIDATION.get(category, category)
                        transactions.append({
                            "date": date,
                            "month": date.strftime("%Y-%m"),
                            "raw_merchant": description,
                            "merchant": merchant,
                            "category": category,
                            "amount": amt,
                            "source": "debit",
                            "fixed_cost": True,
                        })

    print(f"Found {credit_count} credit card and {debit_count} debit card CSV files")
    if business_total > 0:
        print(f"Excluded ${business_total:,.2f} in business expenses (Zensurance, FreshBooks)")
    if debt_payoffs:
        total_payoffs = sum(d["amount"] for d in debt_payoffs)
        print(f"Excluded ${total_payoffs:,.2f} in debt payoffs (mortgage/auto — paid off)")
    return sorted(transactions, key=lambda t: t["date"]), debt_payoffs


# ── Income & Transfer Extraction ─────────────────────────────────────────────

def extract_passive_income(folder: str) -> dict | None:
    """Extract annual passive income from investment portfolio CSV.

    Reads investments/*.csv and sums yield (annual income) for personal accounts.
    Excludes Corporate, RESP, and Property accounts.
    For accounts with TBD yield, estimates from return rate * total value.
    """
    invest_dir = os.path.join(folder, "investments")
    if not os.path.isdir(invest_dir):
        return None

    csv_files = sorted(glob.glob(os.path.join(invest_dir, "*.csv")))
    if not csv_files:
        return None

    EXCLUDE_TYPES = {"Corporate", "Property", "RESP"}
    ACCESSIBLE_TYPES = {"Non-reg", "Cash", "TFSA"}  # spendable without tax penalty

    accessible = []
    rrsp = []

    for fpath in csv_files:
        with open(fpath, newline="", encoding="utf-8-sig") as f:
            reader = csv.reader(f)
            next(reader)  # skip header row

            for row in reader:
                if len(row) < 10:
                    continue

                account = row[0].strip().replace("\n", " ")
                asset_type = row[2].strip().replace("\n", " ")

                # Skip excluded types and totals row
                if asset_type in EXCLUDE_TYPES or not account:
                    continue

                # Parse total value
                val_str = row[4].strip().replace("$", "").replace(",", "")
                try:
                    total_value = float(val_str)
                except (ValueError, TypeError):
                    continue
                if total_value <= 0:
                    continue

                # Parse yield — either explicit dollar amount or TBD
                yield_str = row[9].strip()
                if yield_str.upper() == "TBD" or not yield_str:
                    # Estimate from return rate * total value
                    rate_str = row[8].strip().replace("%", "")
                    try:
                        rate = float(rate_str) / 100
                        annual_yield = total_value * rate
                    except (ValueError, TypeError):
                        annual_yield = 0.0
                else:
                    cleaned = yield_str.replace("$", "").replace(",", "")
                    try:
                        annual_yield = float(cleaned)
                    except (ValueError, TypeError):
                        annual_yield = 0.0

                if annual_yield <= 0:
                    continue

                entry = {
                    "account": account,
                    "type": asset_type,
                    "value": total_value,
                    "annual_yield": round(annual_yield, 2),
                }

                if asset_type in ACCESSIBLE_TYPES:
                    accessible.append(entry)
                elif asset_type == "RRSP":
                    rrsp.append(entry)

    if not accessible and not rrsp:
        return None

    accessible_yield = sum(a["annual_yield"] for a in accessible)
    rrsp_yield = sum(a["annual_yield"] for a in rrsp)

    return {
        "annual_income": round(accessible_yield, 2),
        "monthly_income": round(accessible_yield / 12, 2),
        "accounts": sorted(accessible, key=lambda a: a["annual_yield"], reverse=True),
        "rrsp_annual": round(rrsp_yield, 2),
        "rrsp_monthly": round(rrsp_yield / 12, 2),
        "rrsp_accounts": sorted(rrsp, key=lambda a: a["annual_yield"], reverse=True),
    }


def extract_transfers(folder: str) -> dict:
    """Extract monthly transfer summary from debit card CSVs.

    Returns dict of month -> {"in": float, "out": float}.
    Covers TRFOUT, TRFIN, TRFINTF, E_TRFOUT, E_TRFIN, EFTOUT.
    """
    TRANSFER_TYPES = {"TRFOUT", "TRFIN", "TRFINTF", "E_TRFOUT", "E_TRFIN", "EFTOUT"}
    transfers = defaultdict(lambda: {"in": 0.0, "out": 0.0})
    debit_dir = os.path.join(folder, "debit card")
    if not os.path.isdir(debit_dir):
        return {}

    for fpath in sorted(glob.glob(os.path.join(debit_dir, "*.csv"))):
        with open(fpath, newline="", encoding="utf-8-sig") as f:
            for row in csv.DictReader(f):
                txn_type = row["transaction"]
                if txn_type not in TRANSFER_TYPES:
                    continue
                amount = float(row["amount"])
                date = datetime.strptime(row["date"], "%Y-%m-%d")
                month = date.strftime("%Y-%m")

                if amount > 0:
                    transfers[month]["in"] += amount
                else:
                    transfers[month]["out"] += abs(amount)

    return {m: {"in": round(v["in"], 2), "out": round(v["out"], 2)}
            for m, v in transfers.items()}


def extract_corporate_income(folder: str) -> dict | None:
    """Extract corporate income from corporate account CSVs.

    Reads CSVs from corporate/ subdirectory.
    - Tall Tree Technology: CONT = client revenue (positive amounts)
    - Britton Holdings (Growth): DIV = dividend income (positive amounts)
    """
    corp_dir = os.path.join(folder, "corporate")
    if not os.path.isdir(corp_dir):
        return None

    csv_files = sorted(glob.glob(os.path.join(corp_dir, "*.csv")))
    if not csv_files:
        return None

    revenue_monthly = defaultdict(float)
    dividends_monthly = defaultdict(float)

    for fpath in csv_files:
        fname = os.path.basename(fpath)
        is_tall_tree = "Tall Tree" in fname
        is_bh = "Britton Holdings" in fname

        if not is_tall_tree and not is_bh:
            continue

        with open(fpath, newline="", encoding="utf-8-sig") as f:
            for row in csv.DictReader(f):
                txn_type = row.get("transaction", "").strip()
                date_str = row.get("date", "").strip()
                amount_str = row.get("amount", "").strip()

                if not date_str or not amount_str:
                    continue

                try:
                    amount = float(amount_str)
                except (ValueError, TypeError):
                    continue

                if amount <= 0:
                    continue

                month = datetime.strptime(date_str, "%Y-%m-%d").strftime("%Y-%m")

                if is_tall_tree and txn_type == "CONT":
                    revenue_monthly[month] += amount
                elif is_bh and txn_type == "DIV":
                    dividends_monthly[month] += amount

    if not revenue_monthly and not dividends_monthly:
        return None

    revenue_total = sum(revenue_monthly.values())
    dividends_total = sum(dividends_monthly.values())
    total_income = revenue_total + dividends_total

    all_months = sorted(set(list(revenue_monthly.keys()) + list(dividends_monthly.keys())))
    num_months = len(all_months)

    return {
        "revenue_monthly": {m: round(v, 2) for m, v in sorted(revenue_monthly.items())},
        "dividends_monthly": {m: round(v, 2) for m, v in sorted(dividends_monthly.items())},
        "revenue_total": round(revenue_total, 2),
        "dividends_total": round(dividends_total, 2),
        "total_income": round(total_income, 2),
        "monthly_avg": round(total_income / num_months, 2) if num_months else 0,
        "months": num_months,
    }


# ── Analysis ─────────────────────────────────────────────────────────────────

def analyze(transactions: list[dict], transfers: dict | None = None,
            debt_payoffs: list | None = None) -> dict:
    transfers = transfers or {}
    debt_payoffs = debt_payoffs or []
    months_set = sorted({t["month"] for t in transactions})
    total = sum(t["amount"] for t in transactions)
    monthly_totals = defaultdict(float)
    category_totals = defaultdict(float)
    category_counts = defaultdict(int)
    category_monthly = defaultdict(lambda: defaultdict(float))
    merchant_totals = defaultdict(float)
    merchant_counts = defaultdict(int)
    merchant_monthly = defaultdict(lambda: defaultdict(float))
    monthly_txns = defaultdict(list)

    for t in transactions:
        monthly_totals[t["month"]] += t["amount"]
        category_totals[t["category"]] += t["amount"]
        category_counts[t["category"]] += 1
        category_monthly[t["category"]][t["month"]] += t["amount"]
        merchant_totals[t["merchant"]] += t["amount"]
        merchant_counts[t["merchant"]] += 1
        merchant_monthly[t["merchant"]][t["month"]] += t["amount"]
        monthly_txns[t["month"]].append(t)

    # 3-month trend: avg of last 3 months vs avg of previous 3 months
    monthly_list = [(m, monthly_totals[m]) for m in months_set]
    if len(monthly_list) >= 6:
        recent_avg = sum(v for _, v in monthly_list[-3:]) / 3
        prior_avg = sum(v for _, v in monthly_list[-6:-3]) / 3
        mom_change = ((recent_avg - prior_avg) / prior_avg) * 100 if prior_avg else 0
    elif len(monthly_list) >= 2:
        recent_avg = sum(v for _, v in monthly_list[-3:]) / len(monthly_list[-3:])
        prior_avg = sum(v for _, v in monthly_list[:-3]) / len(monthly_list[:-3]) if len(monthly_list) > 3 else monthly_list[0][1]
        mom_change = ((recent_avg - prior_avg) / prior_avg) * 100 if prior_avg else 0
    else:
        mom_change = 0

    # Subscription detection — find merchants with consistent recurring charges
    # Track per-merchant charge counts per month (to filter out shopping visits)
    merchant_monthly_counts = defaultdict(lambda: defaultdict(int))
    merchant_categories = {}
    for t in transactions:
        merchant_monthly_counts[t["merchant"]][t["month"]] += 1
        merchant_categories[t["merchant"]] = t["category"]

    # Categories that are NOT subscription-like (regular shopping/dining)
    NON_SUB_CATEGORIES = {
        "Food & Dining", "Shopping", "Recreation",
        "Housing & Utilities", "Transportation", "Travel",
        "Kids & Education", "Donations",
    }

    # Known service/subscription merchant keywords (always consider these)
    KNOWN_SUB_KEYWORDS = [
        "telus", "fido", "netflix", "disney", "bell media", "sportsnet",
        "apple sub", "amazon prime", "mubi", "open heart", "brief media",
    ]

    subscriptions = []
    for merchant, by_month in merchant_monthly.items():
        present_months = [m for m in months_set if by_month.get(m, 0) > 0]
        if len(present_months) < 2:
            continue
        amounts = [by_month[m] for m in present_months]
        avg_amount = sum(amounts) / len(amounts)
        if avg_amount < 5:
            continue

        # Coefficient of variation (amount consistency)
        variance = sum((a - avg_amount) ** 2 for a in amounts) / len(amounts)
        cv = (variance ** 0.5) / avg_amount if avg_amount > 0 else 999

        # Average charges per month
        avg_charges = sum(merchant_monthly_counts[merchant][m] for m in present_months) / len(present_months)

        cat = merchant_categories.get(merchant, "Uncategorized")
        is_known_sub = any(kw in merchant.lower() for kw in KNOWN_SUB_KEYWORDS)
        is_non_sub_category = cat in NON_SUB_CATEGORIES

        # Decision logic
        is_subscription = False
        if is_known_sub:
            # Always include known services regardless of consistency
            is_subscription = True
        elif is_non_sub_category:
            # For retail/dining categories, require very tight consistency
            # (catches things like barbershop monthly appointments)
            if cv < 0.10 and len(present_months) >= 3 and avg_charges <= 1.2:
                is_subscription = True
        else:
            # For service-like categories (telecom, health, insurance, etc.)
            if cv < 0.20 and len(present_months) >= 3 and avg_charges <= 1.3:
                is_subscription = True
            elif cv < 0.40 and len(present_months) >= 4 and avg_charges <= 1.2:
                is_subscription = True

        if not is_subscription:
            continue

        history = {m: round(by_month.get(m, 0), 2) for m in months_set}

        # Detect status and alerts
        status = "stable"
        alerts = []
        # Price change detection — flag significant changes but avoid noise
        # from alternating amounts (e.g., two phone lines billing different months)
        if len(amounts) >= 2:
            min_a, max_a = min(amounts), max(amounts)
            # Check if amounts just alternate between ~2 values (not a real change)
            unique_approx = set()
            for a in amounts:
                matched = False
                for u in unique_approx:
                    if abs(a - u) / u < 0.10:
                        matched = True
                        break
                if not matched:
                    unique_approx.add(a)
            if len(unique_approx) <= 2 and max_a / min_a < 1.5:
                # Alternating pattern — just note the range
                if max_a / min_a > 1.20:
                    alerts.append(f"Varies ${min_a:.2f} – ${max_a:.2f}")
                    status = "price_change"
            else:
                # True price changes — flag significant jumps
                for i in range(1, len(amounts)):
                    prev_a, curr_a = amounts[i-1], amounts[i]
                    if prev_a > 0 and abs(curr_a - prev_a) / prev_a > 0.20:
                        direction = "increased" if curr_a > prev_a else "decreased"
                        alerts.append(f"${prev_a:.2f} \u2192 ${curr_a:.2f} ({direction})")
                        status = "price_change"
        # New subscription (first appeared in last 2 months)
        if present_months[0] in months_set[-2:]:
            status = "new"
            alerts.append("New recurring charge")
        # Stopped subscription (absent in last month)
        if present_months[-1] != months_set[-1]:
            status = "stopped"
            alerts.append(f"Last charge: {present_months[-1]}")

        subscriptions.append({
            "merchant": merchant,
            "avg": round(avg_amount, 2),
            "history": history,
            "status": status,
            "alerts": alerts,
            "months_active": len(present_months),
        })

    subscriptions.sort(key=lambda s: s["avg"], reverse=True)

    # Top merchants
    top_merchants = sorted(merchant_totals.items(), key=lambda x: x[1], reverse=True)[:20]
    top_merchants = [(m, round(t, 2), merchant_counts[m]) for m, t in top_merchants]

    # Categories sorted by total
    categories = sorted(category_totals.items(), key=lambda x: x[1], reverse=True)
    num_months = len(months_set)
    categories = [(c, round(t, 2), round(t / num_months, 2), category_counts[c]) for c, t in categories]

    # Source breakdown (credit vs debit spending by month)
    source_monthly = defaultdict(lambda: defaultdict(float))
    for t in transactions:
        source_monthly[t.get("source", "credit")][t["month"]] += t["amount"]

    # Fixed costs (AFT_OUT transactions)
    fixed_merchants = defaultdict(lambda: defaultdict(float))
    for t in transactions:
        if t.get("fixed_cost"):
            fixed_merchants[t["merchant"]][t["month"]] += t["amount"]
    fixed_total = sum(t["amount"] for t in transactions if t.get("fixed_cost"))

    fixed_cost_detail = sorted(
        [(m, round(sum(amounts.values()), 2),
          {mo: round(amounts.get(mo, 0), 2) for mo in months_set})
         for m, amounts in fixed_merchants.items()],
        key=lambda x: x[1], reverse=True
    )

    return {
        "months": months_set,
        "total": round(total, 2),
        "monthly_avg": round(total / num_months, 2) if num_months else 0,
        "mom_change": round(mom_change, 1),
        "monthly_totals": {m: round(monthly_totals[m], 2) for m in months_set},
        "categories": categories,
        "category_monthly": {c: {m: round(category_monthly[c].get(m, 0), 2) for m in months_set} for c in category_totals},
        "subscriptions": subscriptions,
        "top_merchants": top_merchants,
        "monthly_txns": {m: sorted(monthly_txns[m], key=lambda t: t["date"]) for m in months_set},
        "transfers": transfers,
        "fixed_costs": {m: round(sum(v.get(m, 0) for v in fixed_merchants.values()), 2) for m in months_set},
        "fixed_cost_detail": fixed_cost_detail,
        "fixed_total": round(fixed_total, 2),
        "discretionary_total": round(total - fixed_total, 2),
        "source_breakdown": {s: {m: round(source_monthly[s].get(m, 0), 2) for m in months_set} for s in source_monthly},
        "debt_payoffs": debt_payoffs,
    }


# ── AI Recommendations ──────────────────────────────────────────────────────

def get_ai_recommendations(data: dict) -> str | None:
    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        print("Error: ANTHROPIC_API_KEY environment variable not set.")
        print("Set it with: export ANTHROPIC_API_KEY=sk-ant-...")
        sys.exit(1)

    # Build a concise summary to send (not raw transactions)
    summary = {
        "total_spend": data["total"],
        "months": data["months"],
        "monthly_avg": data["monthly_avg"],
        "mom_change_pct": data["mom_change"],
        "monthly_totals": data["monthly_totals"],
        "categories": [
            {"name": c, "total": t, "monthly_avg": a, "txn_count": n}
            for c, t, a, n in data["categories"]
        ],
        "subscriptions": [
            {"merchant": s["merchant"], "avg_monthly": s["avg"],
             "status": s["status"], "alerts": s["alerts"],
             "history": s["history"]}
            for s in data["subscriptions"]
        ],
        "top_merchants": [
            {"name": m, "total": t, "txn_count": c}
            for m, t, c in data["top_merchants"]
        ],
        "fixed_costs": [
            {"merchant": m, "total": t} for m, t, _ in data.get("fixed_cost_detail", [])
        ],
        "fixed_total": data.get("fixed_total", 0),
        "discretionary_total": data.get("discretionary_total", 0),
    }

    prompt = f"""Analyze this household spending data (credit card + debit card combined) and provide actionable recommendations.

The data includes income, fixed costs (tuition, car payment, utilities), and discretionary spending. Savings rate is calculated where income data is available.

DATA:
{json.dumps(summary, indent=2)}

Please provide exactly 10 numbered recommendations — no more, no less. Each should be specific, actionable, and reference actual merchant names and dollar amounts from the data. Cover a mix of:
- Fixed cost optimization (insurance, utilities, recurring debits)
- Subscription cost-saving actions (price increases to negotiate, services to cancel/downgrade)
- Spending pattern optimizations (consolidation, alternatives)
- Income and savings rate observations
- Notable month-over-month changes worth investigating

Format your response in clean HTML as a single <ol> list with 10 <li> items. Use <strong> for emphasis on merchant names and dollar amounts. Be concise — one short paragraph per recommendation."""

    payload = json.dumps({
        "model": "claude-sonnet-4-20250514",
        "max_tokens": 2000,
        "messages": [{"role": "user", "content": prompt}],
    }).encode("utf-8")

    req = Request(
        "https://api.anthropic.com/v1/messages",
        data=payload,
        headers={
            "Content-Type": "application/json",
            "x-api-key": api_key,
            "anthropic-version": "2023-06-01",
        },
        method="POST",
    )

    print("Calling Claude API for recommendations...")
    try:
        ctx = ssl.create_default_context()
        with urlopen(req, context=ctx, timeout=60) as resp:
            body = json.loads(resp.read().decode("utf-8"))
            return body["content"][0]["text"]
    except HTTPError as e:
        error_body = e.read().decode("utf-8") if e.fp else ""
        print(f"API error ({e.code}): {error_body}")
        sys.exit(1)
    except URLError as e:
        print(f"Network error: {e.reason}")
        sys.exit(1)


# ── HTML Generation ──────────────────────────────────────────────────────────

def generate_html(data: dict, ai_html: str | None = None,
                   notes: dict | None = None, budgets: dict | None = None,
                   passive_income: dict | None = None,
                   corporate_income: dict | None = None) -> str:
    notes = notes or {}
    budgets = budgets or {}
    months = data["months"]
    month_labels = [datetime.strptime(m, "%Y-%m").strftime("%b %Y") for m in months]

    # Color palette
    COLORS = [
        "#4e79a7", "#f28e2b", "#e15759", "#76b7b2", "#59a14f",
        "#edc948", "#b07aa1", "#ff9da7", "#9c755f", "#bab0ac",
        "#86bcb6", "#8cd17d", "#b6992d", "#499894", "#d37295",
        "#a0cbe8", "#ffbe7d", "#d4a6c8", "#fabfd2", "#d7b5a6",
        "#79706e", "#c49c94", "#aec7e8", "#ff7f0e",
    ]

    def status_badge(status):
        colors = {"stable": "#27ae60", "price_change": "#f39c12", "new": "#e74c3c", "stopped": "#e74c3c"}
        labels = {"stable": "Stable", "price_change": "Price Change", "new": "New", "stopped": "Stopped"}
        c = colors.get(status, "#95a5a6")
        l = labels.get(status, status.title())
        return f'<span style="background:{c};color:#fff;padding:2px 8px;border-radius:12px;font-size:0.8em;font-weight:600">{l}</span>'

    def money(val):
        return f"${val:,.2f}"

    def sparkline(values: list[float], width: int = 80, height: int = 24) -> str:
        """Generate an inline SVG sparkline from a list of values."""
        if not values or max(values) == 0:
            return ""
        max_v = max(values)
        min_v = min(values)
        rng = max_v - min_v if max_v != min_v else 1
        n = len(values)
        points = []
        for i, v in enumerate(values):
            x = round(i / max(n - 1, 1) * (width - 4) + 2, 1)
            y = round(height - 2 - ((v - min_v) / rng) * (height - 4), 1)
            points.append(f"{x},{y}")
        if n >= 2:
            trend = values[-1] - values[0]
            color = "#e15759" if trend > rng * 0.1 else "#27ae60" if trend < -rng * 0.1 else "#7f8c8d"
        else:
            color = "#7f8c8d"
        return (f'<svg width="{width}" height="{height}" style="vertical-align:middle">'
                f'<polyline points="{" ".join(points)}" fill="none" stroke="{color}" '
                f'stroke-width="2" stroke-linecap="round" stroke-linejoin="round"/>'
                f'<circle cx="{points[-1].split(",")[0]}" cy="{points[-1].split(",")[1]}" '
                f'r="2.5" fill="{color}"/></svg>')

    def budget_bar(actual: float, target: float) -> str:
        """Generate an inline budget progress bar."""
        pct = min(actual / target * 100, 150) if target > 0 else 0
        color = "#27ae60" if pct <= 90 else "#f39c12" if pct <= 105 else "#e15759"
        bar_width = min(pct / 150 * 100, 100)
        over = f" ({actual/target*100:.0f}%)" if pct > 0 else ""
        return (f'<div style="display:flex;align-items:center;gap:6px">'
                f'<div style="flex:1;background:#eee;border-radius:4px;height:8px;min-width:60px">'
                f'<div style="width:{bar_width:.0f}%;background:{color};border-radius:4px;height:100%"></div>'
                f'</div>'
                f'<span style="font-size:0.78em;color:{color};white-space:nowrap">{money(target)}{over}</span>'
                f'</div>')

    # ── Data preparation ──
    cat_labels = json.dumps([c[0] for c in data["categories"]])
    cat_values = json.dumps([c[1] for c in data["categories"]])
    cat_colors = json.dumps(COLORS[:len(data["categories"])])
    monthly_values = json.dumps([data["monthly_totals"][m] for m in months])
    month_labels_json = json.dumps(month_labels)

    # Source breakdown for stacked bar chart
    source_breakdown = data.get("source_breakdown", {})
    credit_monthly = json.dumps([source_breakdown.get("credit", {}).get(m, 0) for m in months])
    debit_monthly = json.dumps([source_breakdown.get("debit", {}).get(m, 0) for m in months])
    has_debit = "debit" in source_breakdown

    # Fixed costs data
    fixed_detail = data.get("fixed_cost_detail", [])
    fixed_total = data.get("fixed_total", 0)
    discretionary_total = data.get("discretionary_total", 0)
    fixed_pct = round(fixed_total / data["total"] * 100, 1) if data["total"] > 0 else 0

    # Transfers data
    transfers = data.get("transfers", {})

    # Debt payoff data
    debt_payoffs = data.get("debt_payoffs", [])
    INTEREST_RATES = {
        "Mortgage (First National)": 0.0325,
        "Hyundai Car Payment": 0.0399,
    }
    debt_payoff_total = sum(d["amount"] for d in debt_payoffs)
    annual_interest_saved = sum(
        d["amount"] * INTEREST_RATES.get(d["merchant"], 0) for d in debt_payoffs
    )

    # Burn rate — recent 3-month average, excluding paid-off debt payments
    debt_merchants = set(DEBT_PAYOFF_THRESHOLDS.keys()) if debt_payoffs else set()
    recent_months = months[-3:]
    recent_adjusted = []
    for m in recent_months:
        m_total = data["monthly_totals"].get(m, 0)
        if debt_merchants:
            debt_in_month = sum(t["amount"] for t in data["monthly_txns"].get(m, [])
                                if t["merchant"] in debt_merchants)
            m_total -= debt_in_month
        recent_adjusted.append(m_total)
    burn_rate = sum(recent_adjusted) / len(recent_adjusted) if recent_adjusted else 0

    # ── Build table rows ──

    # Subscription table rows
    sub_rows = ""
    for s in data["subscriptions"]:
        month_cells = ""
        for m in months:
            val = s["history"].get(m, 0)
            if val > 0:
                month_cells += f"<td style='text-align:right'>{money(val)}</td>"
            else:
                month_cells += "<td style='text-align:center;color:#ccc'>—</td>"
        alert_html = "<br>".join(f"<small style='color:#e74c3c'>{a}</small>" for a in s["alerts"]) if s["alerts"] else ""
        note = notes.get(s["merchant"].lower(), "")
        note_html = f"<br><small style='color:#4e79a7;font-style:italic'>Note: {note}</small>" if note else ""
        sub_rows += f"""<tr>
            <td><strong>{s['merchant']}</strong>{('<br>' + alert_html) if alert_html else ''}{note_html}</td>
            <td style="text-align:right">{money(s['avg'])}</td>
            {month_cells}
            <td style="text-align:center">{status_badge(s['status'])}</td>
        </tr>"""

    # Top merchants table
    top_rows = ""
    for i, (m, t, c) in enumerate(data["top_merchants"], 1):
        note = notes.get(m.lower(), "")
        note_html = f"<br><small style='color:#4e79a7;font-style:italic'>{note}</small>" if note else ""
        top_rows += f"<tr><td>{i}</td><td>{m}{note_html}</td><td style='text-align:right'>{money(t)}</td><td style='text-align:center'>{c}</td></tr>"

    # Category table with sparklines and budget bars
    has_budgets = bool(budgets)
    cat_rows = ""
    for c, t, a, n in data["categories"]:
        monthly_vals = [data["category_monthly"].get(c, {}).get(m, 0) for m in months]
        spark = sparkline(monthly_vals)
        budget_cell = ""
        if has_budgets:
            target = budgets.get(c)
            no_budget = '<span style="color:#ccc">—</span>'
            budget_cell = f"<td>{budget_bar(a, target) if target else no_budget}</td>"
        cat_rows += f"<tr><td>{c}</td><td style='text-align:right'>{money(t)}</td><td style='text-align:right'>{money(a)}</td>{budget_cell}<td style='text-align:center'>{spark}</td><td style='text-align:center'>{n}</td></tr>"

    # Monthly detail sections (last 3 months only)
    monthly_sections = ""
    recent_months = months[-3:]
    for m in recent_months:
        label = datetime.strptime(m, "%Y-%m").strftime("%B %Y")
        txns = data["monthly_txns"][m]
        m_total = data["monthly_totals"][m]
        txn_rows = ""
        for t in txns:
            source_tag = f" <small style='color:#76b7b2'>[D]</small>" if t.get("source") == "debit" else ""
            txn_rows += f"<tr><td>{t['date'].strftime('%b %d')}</td><td>{t['merchant']}{source_tag}</td><td style='color:#888;font-size:0.85em'>{t['category']}</td><td style='text-align:right'>{money(t['amount'])}</td></tr>"
        monthly_sections += f"""
        <details class="month-detail">
            <summary><strong>{label}</strong> — {money(m_total)} ({len(txns)} transactions)</summary>
            <table class="data-table">
                <thead><tr><th>Date</th><th>Merchant</th><th>Category</th><th style="text-align:right">Amount</th></tr></thead>
                <tbody>{txn_rows}</tbody>
            </table>
        </details>"""

    # Sub month headers
    sub_month_headers = "".join(f"<th style='text-align:right'>{datetime.strptime(m, '%Y-%m').strftime('%b %Y')}</th>" for m in months)

    # Trend indicator
    trend_arrow = "\u2191" if data["mom_change"] > 0 else "\u2193" if data["mom_change"] < 0 else "\u2192"
    trend_color = "#e74c3c" if data["mom_change"] > 5 else "#27ae60" if data["mom_change"] < -5 else "#f39c12"

    # Fixed costs table rows
    fixed_rows = ""
    num_months = len(months)
    for merchant, total_amt, by_month in fixed_detail:
        monthly_avg = total_amt / num_months
        fixed_rows += f"<tr><td>{merchant}</td><td style='text-align:right'>{money(total_amt)}</td><td style='text-align:right'>{money(monthly_avg)}</td></tr>"

    # AI section
    ai_section = ""
    if ai_html:
        ai_section = f"""
        <section id="recommendations" class="card">
            <h2>AI-Powered Recommendations</h2>
            <div class="ai-recommendations">{ai_html}</div>
        </section>"""

    # ── Income vs burn rate (the main story) ──
    monthly_passive = passive_income["monthly_income"] if passive_income else 0
    annual_passive = passive_income["annual_income"] if passive_income else 0
    rrsp_monthly = passive_income["rrsp_monthly"] if passive_income else 0
    rrsp_annual = passive_income["rrsp_annual"] if passive_income else 0

    # Corporate income components — trailing 3-month average (same window as burn rate)
    if corporate_income:
        corp_months_all = sorted(set(
            list(corporate_income["revenue_monthly"].keys()) +
            list(corporate_income["dividends_monthly"].keys())
        ))
        corp_trailing = corp_months_all[-3:]  # last 3 months
        corp_trailing_n = len(corp_trailing)
        corp_revenue_avg = round(sum(corporate_income["revenue_monthly"].get(m, 0) for m in corp_trailing) / corp_trailing_n, 2) if corp_trailing_n else 0
        corp_div_avg = round(sum(corporate_income["dividends_monthly"].get(m, 0) for m in corp_trailing) / corp_trailing_n, 2) if corp_trailing_n else 0
    else:
        corp_months_all = []
        corp_trailing = []
        corp_trailing_n = 0
        corp_revenue_avg = 0
        corp_div_avg = 0

    corp_revenue_takehome = round(corp_revenue_avg * CORPORATE_TAKE_HOME_RATE, 2)
    corp_monthly_takehome = corp_revenue_takehome + corp_div_avg
    combined_monthly = monthly_passive + corp_monthly_takehome
    has_income = passive_income or corporate_income

    if has_income and burn_rate > 0:
        coverage_pct = combined_monthly / burn_rate * 100
        gap = combined_monthly - burn_rate
        if coverage_pct >= 100:
            coverage_color = "#27ae60"
            coverage_label = f"Surplus: {money(gap)}/mo"
        elif coverage_pct >= 75:
            coverage_color = "#f39c12"
            coverage_label = f"Gap: {money(abs(gap))}/mo"
        else:
            coverage_color = "#e74c3c"
            coverage_label = f"Gap: {money(abs(gap))}/mo"
    else:
        coverage_pct = 0
        coverage_color = "#95a5a6"
        coverage_label = ""

    # Hero card: income vs burn rate
    hero_card = ""
    if has_income:
        bar_fill = min(coverage_pct, 100)
        # Income breakdown lines
        income_breakdown = ""
        if monthly_passive > 0:
            income_breakdown += f'<div style="font-size:0.82em;color:var(--muted);margin-top:4px">Personal passive: {money(monthly_passive)}/mo <span style="color:#76b7b2">({money(annual_passive)}/yr)</span></div>'
            income_breakdown += '<div style="font-size:0.75em;color:var(--muted);font-style:italic;margin-top:1px;margin-left:4px">projected portfolio yield</div>'
        if corp_revenue_avg > 0:
            income_breakdown += f'<div style="font-size:0.82em;color:var(--muted);margin-top:2px">Corporate revenue: {money(corp_revenue_avg)}/mo gross → {money(corp_revenue_takehome)}/mo est. take-home ({int(CORPORATE_TAKE_HOME_RATE*100)}%) <span style="color:#f28e2b">(Tall Tree)</span></div>'
        if corp_div_avg > 0:
            income_breakdown += f'<div style="font-size:0.82em;color:var(--muted);margin-top:2px">Corporate dividends: {money(corp_div_avg)}/mo <span style="color:#b07aa1">(Britton Holdings)</span></div>'
        if corp_revenue_avg > 0 or corp_div_avg > 0:
            income_breakdown += f'<div style="font-size:0.75em;color:var(--muted);font-style:italic;margin-top:1px;margin-left:4px">3-month trailing avg ({int(CORPORATE_TAKE_HOME_RATE*100)}% take-home on revenue)</div>'
        rrsp_note = ""
        if rrsp_annual > 0:
            rrsp_note = f'<div style="font-size:0.82em;color:var(--muted);margin-top:6px">+ {money(rrsp_monthly)}/mo growing in RRSPs <span style="color:#b07aa1">({money(rrsp_annual)}/yr)</span></div>'
        income_label = "Combined Income" if (monthly_passive > 0 and corp_monthly_takehome > 0) else "Accessible Investment Income" if monthly_passive > 0 else "Corporate Income"
        hero_card = f"""
    <div class="card" style="margin-bottom:20px">
        <div style="display:flex;align-items:center;justify-content:space-between;flex-wrap:wrap;gap:20px">
            <div style="flex:1;min-width:200px">
                <div style="font-size:0.85em;color:var(--muted);text-transform:uppercase;letter-spacing:0.5px;margin-bottom:4px">{income_label}</div>
                <div style="font-size:2.2em;font-weight:700;color:#27ae60">{money(combined_monthly)}<span style="font-size:0.4em;font-weight:400;color:var(--muted)">/mo</span></div>
                {income_breakdown}
                {rrsp_note}
            </div>
            <div style="flex:0 0 60px;text-align:center">
                <div style="font-size:1.8em;color:var(--muted)">vs</div>
            </div>
            <div style="flex:1;min-width:200px;text-align:right">
                <div style="font-size:0.85em;color:var(--muted);text-transform:uppercase;letter-spacing:0.5px;margin-bottom:4px">Burn Rate</div>
                <div style="font-size:2.2em;font-weight:700;color:#e15759">{money(burn_rate)}<span style="font-size:0.4em;font-weight:400;color:var(--muted)">/mo</span></div>
                <div style="font-size:0.85em;color:var(--muted)">3-month trailing average</div>
            </div>
        </div>
        <div style="margin-top:20px">
            <div style="display:flex;justify-content:space-between;margin-bottom:6px">
                <span style="font-size:0.85em;font-weight:600;color:{coverage_color}">Coverage: {coverage_pct:.0f}%</span>
                <span style="font-size:0.85em;color:{coverage_color}">{coverage_label}</span>
            </div>
            <div style="background:#eee;border-radius:6px;height:12px;overflow:hidden">
                <div style="width:{bar_fill:.0f}%;background:{coverage_color};height:100%;border-radius:6px;transition:width 0.3s"></div>
            </div>
        </div>
    </div>"""

    # ── Overview stats ──
    overview_stats = f"""
    <div class="stat"><div class="value">{money(data['total'])}</div><div class="label">Total Spend ({len(months)} months)</div></div>
    <div class="stat"><div class="value">{money(data['monthly_avg'])}</div><div class="label">Monthly Average</div></div>
    <div class="stat"><div class="value" style="color:{trend_color}">{trend_arrow} {abs(data['mom_change']):.0f}%</div><div class="label">3-Month Trend</div></div>"""
    if debt_payoff_total > 0:
        overview_stats += f"""
    <div class="stat"><div class="value" style="color:#27ae60">{money(debt_payoff_total)}</div><div class="label">Debt Paid Off</div></div>"""

    # ── Debt Freedom section ──
    debt_section = ""
    if debt_payoffs:
        # Group payoffs by merchant
        from collections import defaultdict as _dd
        payoff_by_merchant = _dd(lambda: {"total": 0.0, "last_date": None})
        for d in debt_payoffs:
            payoff_by_merchant[d["merchant"]]["total"] += d["amount"]
            dt = d["date"]
            prev = payoff_by_merchant[d["merchant"]]["last_date"]
            if prev is None or dt > prev:
                payoff_by_merchant[d["merchant"]]["last_date"] = dt
        debt_rows = ""
        for merchant, info in sorted(payoff_by_merchant.items(), key=lambda x: x[1]["total"], reverse=True):
            rate = INTEREST_RATES.get(merchant, 0)
            annual_saved = info["total"] * rate
            paid_off_date = info["last_date"].strftime("%b %Y")
            debt_rows += f"<tr><td>{merchant}</td><td style='text-align:right'>{money(info['total'])}</td><td style='text-align:center'>{rate*100:.2f}%</td><td style='text-align:right'>{money(annual_saved)}</td><td style='text-align:center'>{paid_off_date}</td></tr>"
        monthly_saved = annual_interest_saved / 12
        debt_section = f"""
<section id="debt-freedom" class="card">
    <h2>Debt Freedom</h2>
    <p style="color:var(--muted);margin-bottom:15px">Debts paid off during this period — saving <strong style="color:#27ae60">{money(annual_interest_saved)}/year</strong> ({money(monthly_saved)}/month) in interest</p>
    <table class="data-table" style="max-width:700px">
        <thead><tr><th>Debt</th><th style="text-align:right">Principal</th><th style="text-align:center">Rate</th><th style="text-align:right">Annual Savings</th><th style="text-align:center">Paid Off</th></tr></thead>
        <tbody>{debt_rows}</tbody>
        <tfoot><tr style="font-weight:700"><td>Total</td><td style="text-align:right">{money(debt_payoff_total)}</td><td></td><td style="text-align:right">{money(annual_interest_saved)}</td><td></td></tr></tfoot>
    </table>
</section>"""

    # ── Fixed vs Discretionary section ──
    fixed_section = ""
    if fixed_detail:
        fixed_section = f"""
<section id="fixed-discretionary" class="card">
    <h2>Fixed vs Discretionary</h2>
    <p style="color:var(--muted);margin-bottom:15px">{fixed_pct}% of total spending is fixed (pre-authorized recurring debits)</p>
    <div class="chart-row">
        <div>
            <table class="data-table">
                <thead><tr><th>Fixed Cost</th><th style="text-align:right">Total</th><th style="text-align:right">Monthly Avg</th></tr></thead>
                <tbody>{fixed_rows}</tbody>
                <tfoot><tr style="font-weight:700"><td>Total Fixed</td><td style="text-align:right">{money(fixed_total)}</td><td style="text-align:right">{money(fixed_total / num_months if num_months else 0)}</td></tr></tfoot>
            </table>
        </div>
        <div>
            <div class="chart-container"><canvas id="fixedChart"></canvas></div>
        </div>
    </div>
</section>"""

    # ── Corporate Income section ──
    corporate_section = ""
    if corporate_income:
        corp_months_sorted = sorted(set(
            list(corporate_income["revenue_monthly"].keys()) +
            list(corporate_income["dividends_monthly"].keys())
        ))
        corp_rows = ""
        for m in corp_months_sorted:
            m_label = datetime.strptime(m, "%Y-%m").strftime("%b %Y")
            rev = corporate_income["revenue_monthly"].get(m, 0)
            div = corporate_income["dividends_monthly"].get(m, 0)
            m_total = rev + div
            rev_cell = money(rev) if rev > 0 else '<span style="color:#ccc">\u2014</span>'
            div_cell = money(div) if div > 0 else '<span style="color:#ccc">\u2014</span>'
            corp_rows += f"<tr><td>{m_label}</td><td style='text-align:right'>{rev_cell}</td><td style='text-align:right'>{div_cell}</td><td style='text-align:right;font-weight:600'>{money(m_total)}</td></tr>"
        # Revenue trend warning: detect if latest month < 80% of prior month
        corp_revenue_warning = ""
        if len(corp_months_sorted) >= 2:
            latest_rev = corporate_income["revenue_monthly"].get(corp_months_sorted[-1], 0)
            prior_rev = corporate_income["revenue_monthly"].get(corp_months_sorted[-2], 0)
            if prior_rev > 0 and latest_rev < prior_rev * 0.80:
                decline_pct = round((1 - latest_rev / prior_rev) * 100)
                corp_revenue_warning = f'<div style="color:#e74c3c;font-size:0.9em;margin-top:10px;font-weight:600">⚠ Revenue declining: down {decline_pct}% month-over-month</div>'
        corp_trailing_total_avg = round(corp_revenue_avg + corp_div_avg, 2)
        corporate_section = f"""
<section id="corporate-income" class="card">
    <h2>Corporate Income</h2>
    <p style="color:var(--muted);margin-bottom:15px">Revenue from Tall Tree Technology (client payments) and dividends from Britton Holdings Growth (investment portfolio)</p>
    {corp_revenue_warning}
    <table class="data-table" style="max-width:600px">
        <thead><tr><th>Month</th><th style="text-align:right">Revenue (Tall Tree)</th><th style="text-align:right">Dividends (BH Growth)</th><th style="text-align:right">Total</th></tr></thead>
        <tbody>{corp_rows}</tbody>
        <tfoot>
            <tr style="font-weight:700"><td>Total</td><td style="text-align:right">{money(corporate_income['revenue_total'])}</td><td style="text-align:right">{money(corporate_income['dividends_total'])}</td><td style="text-align:right">{money(corporate_income['total_income'])}</td></tr>
            <tr style="color:var(--muted)"><td>Trailing Avg (3-mo)</td><td style="text-align:right">{money(corp_revenue_avg)}</td><td style="text-align:right">{money(corp_div_avg)}</td><td style="text-align:right">{money(corp_trailing_total_avg)}</td></tr>
        </tfoot>
    </table>
</section>"""

    # ── TOC links ──
    toc_items = '<li><a href="#overview">Overview</a></li>'
    if corporate_income:
        toc_items += '\n    <li><a href="#corporate-income">Corporate Income</a></li>'
    if debt_payoffs:
        toc_items += '\n    <li><a href="#debt-freedom">Debt Freedom</a></li>'
    toc_items += '\n    <li><a href="#charts">Charts</a></li>'
    if fixed_detail:
        toc_items += '\n    <li><a href="#fixed-discretionary">Fixed vs Discretionary</a></li>'
    toc_items += '\n    <li><a href="#categories">Categories</a></li>'
    toc_items += '\n    <li><a href="#subscriptions">Subscriptions</a></li>'
    if ai_html:
        toc_items += '\n    <li><a href="#recommendations">AI Recommendations</a></li>'
    toc_items += '\n    <li><a href="#top-merchants">Top Merchants</a></li>'
    toc_items += '\n    <li><a href="#monthly-detail">Monthly Detail</a></li>'

    # ── Chart.js for stacked monthly bar ──
    if has_debit:
        monthly_chart_js = f"""
    new Chart(document.getElementById('monthlyChart'), {{
        type: 'bar',
        data: {{
            labels: {month_labels_json},
            datasets: [
                {{ label: 'Credit Card', data: {credit_monthly}, backgroundColor: '#4e79a7', borderRadius: 4 }},
                {{ label: 'Debit Card', data: {debit_monthly}, backgroundColor: '#76b7b2', borderRadius: 4 }}
            ]
        }},
        options: {{
            responsive: true,
            plugins: {{
                tooltip: {{ callbacks: {{ label: ctx => ctx.dataset.label + ': $' + ctx.parsed.y.toLocaleString(undefined, {{minimumFractionDigits:2}}) }} }}
            }},
            scales: {{
                x: {{ stacked: true }},
                y: {{ stacked: true, beginAtZero: true, ticks: {{ callback: v => '$' + (v/1000).toFixed(0) + 'k' }} }}
            }}
        }}
    }});"""
    else:
        monthly_chart_js = f"""
    new Chart(document.getElementById('monthlyChart'), {{
        type: 'bar',
        data: {{
            labels: {month_labels_json},
            datasets: [{{ label: 'Monthly Spend', data: {monthly_values}, backgroundColor: '#4e79a7', borderRadius: 6 }}]
        }},
        options: {{
            responsive: true,
            plugins: {{ legend: {{ display: false }}, tooltip: {{ callbacks: {{ label: ctx => '$' + ctx.parsed.y.toLocaleString(undefined, {{minimumFractionDigits:2}}) }} }} }},
            scales: {{ y: {{ beginAtZero: true, ticks: {{ callback: v => '$' + (v/1000).toFixed(0) + 'k' }} }} }}
        }}
    }});"""

    # ── Chart.js for fixed/discretionary pie ──
    fixed_chart_js = ""
    if fixed_detail:
        fixed_chart_js = f"""
    new Chart(document.getElementById('fixedChart'), {{
        type: 'doughnut',
        data: {{
            labels: ['Fixed Costs', 'Discretionary'],
            datasets: [{{ data: [{fixed_total}, {discretionary_total}], backgroundColor: ['#4e79a7', '#76b7b2'], borderWidth: 2, borderColor: '#fff' }}]
        }},
        options: {{
            responsive: true,
            plugins: {{
                legend: {{ position: 'bottom' }},
                tooltip: {{ callbacks: {{ label: ctx => ctx.label + ': $' + ctx.parsed.toLocaleString(undefined, {{minimumFractionDigits:2}}) }} }}
            }}
        }}
    }});"""

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Financial Dashboard — {months[0]} to {months[-1]}</title>
<style>
:root {{
    --bg: #f5f6fa;
    --card: #ffffff;
    --text: #2c3e50;
    --muted: #7f8c8d;
    --border: #e1e8ed;
    --accent: #4e79a7;
}}
* {{ margin: 0; padding: 0; box-sizing: border-box; }}
body {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; background: var(--bg); color: var(--text); line-height: 1.6; padding: 20px; max-width: 1200px; margin: 0 auto; }}
h1 {{ font-size: 1.8em; margin-bottom: 5px; }}
h2 {{ font-size: 1.3em; margin-bottom: 15px; color: var(--accent); border-bottom: 2px solid var(--accent); padding-bottom: 5px; }}
.subtitle {{ color: var(--muted); margin-bottom: 25px; }}
.card {{ background: var(--card); border-radius: 12px; padding: 25px; margin-bottom: 20px; box-shadow: 0 1px 3px rgba(0,0,0,0.08); }}
.stats {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 15px; margin-bottom: 20px; }}
.stat {{ background: var(--card); border-radius: 10px; padding: 20px; text-align: center; box-shadow: 0 1px 3px rgba(0,0,0,0.08); }}
.stat .value {{ font-size: 1.8em; font-weight: 700; color: var(--accent); }}
.stat .label {{ font-size: 0.85em; color: var(--muted); margin-top: 5px; }}
.chart-container {{ position: relative; max-width: 100%; margin: 0 auto; }}
.chart-row {{ display: grid; grid-template-columns: 1fr 1fr; gap: 20px; margin-bottom: 20px; }}
@media (max-width: 768px) {{ .chart-row {{ grid-template-columns: 1fr; }} }}
.data-table {{ width: 100%; border-collapse: collapse; font-size: 0.9em; }}
.data-table th {{ background: var(--bg); padding: 10px 12px; text-align: left; font-weight: 600; position: sticky; top: 0; }}
.data-table td {{ padding: 8px 12px; border-bottom: 1px solid var(--border); }}
.data-table tbody tr:hover {{ background: #f8f9fa; }}
.month-detail {{ margin-bottom: 8px; }}
.month-detail summary {{ cursor: pointer; padding: 12px 15px; background: var(--bg); border-radius: 8px; font-size: 0.95em; }}
.month-detail summary:hover {{ background: #e8ecf1; }}
.month-detail[open] summary {{ border-radius: 8px 8px 0 0; }}
.month-detail .data-table {{ border: 1px solid var(--border); border-top: none; }}
.ai-recommendations {{ line-height: 1.7; }}
.ai-recommendations h3 {{ color: var(--accent); margin: 20px 0 10px; }}
.ai-recommendations li {{ margin-bottom: 6px; }}
canvas {{ max-width: 100%; }}
.noscript-table {{ margin-top: 10px; }}
nav.toc {{ background: var(--card); border-radius: 12px; padding: 15px 25px; margin-bottom: 20px; box-shadow: 0 1px 3px rgba(0,0,0,0.08); }}
nav.toc ul {{ list-style: none; display: flex; flex-wrap: wrap; gap: 8px; }}
nav.toc a {{ text-decoration: none; color: var(--accent); background: var(--bg); padding: 5px 14px; border-radius: 20px; font-size: 0.88em; font-weight: 500; transition: background 0.15s; }}
nav.toc a:hover {{ background: var(--accent); color: #fff; }}
</style>
</head>
<body>
<h1>Financial Dashboard</h1>
<p class="subtitle">Personal &amp; corporate financial overview: {month_labels[0]} – {month_labels[-1]} | Generated {datetime.now().strftime('%b %d, %Y at %I:%M %p')}</p>

<nav class="toc"><ul>
    {toc_items}
</ul></nav>

<!-- Overview -->
<div id="overview"></div>
{hero_card}
<div class="stats">
    {overview_stats}
</div>

{corporate_section}

{debt_section}

<!-- Charts -->
<div id="charts" class="chart-row">
    <div class="card">
        <h2>Monthly Spending{' (Credit + Debit)' if has_debit else ''}</h2>
        <div class="chart-container"><canvas id="monthlyChart"></canvas></div>
    </div>
    <div class="card">
        <h2>Category Breakdown</h2>
        <div class="chart-container"><canvas id="categoryChart"></canvas></div>
    </div>
</div>

{fixed_section}

<!-- Category Table -->
<section id="categories" class="card">
    <h2>Category Breakdown</h2>
    <table class="data-table">
        <thead><tr><th>Category</th><th style="text-align:right">Total</th><th style="text-align:right">Monthly Avg</th>{'<th>vs Budget</th>' if has_budgets else ''}<th style="text-align:center">Trend</th><th style="text-align:center">Txns</th></tr></thead>
        <tbody>{cat_rows}</tbody>
        <tfoot><tr style="font-weight:700"><td>Total</td><td style="text-align:right">{money(data['total'])}</td><td style="text-align:right">{money(data['monthly_avg'])}</td>{'<td></td>' if has_budgets else ''}<td></td><td></td></tr></tfoot>
    </table>
</section>

<!-- Subscription Audit -->
<section id="subscriptions" class="card">
    <h2>Subscription Audit</h2>
    <p style="color:var(--muted);margin-bottom:15px">Recurring charges detected across your statements. <span style="background:#27ae60;color:#fff;padding:1px 6px;border-radius:8px;font-size:0.8em">Stable</span> <span style="background:#f39c12;color:#fff;padding:1px 6px;border-radius:8px;font-size:0.8em">Price Change</span> <span style="background:#e74c3c;color:#fff;padding:1px 6px;border-radius:8px;font-size:0.8em">New / Stopped</span></p>
    <div style="overflow-x:auto">
    <table class="data-table">
        <thead><tr><th>Service</th><th style="text-align:right">Avg/Mo</th>{sub_month_headers}<th style="text-align:center">Status</th></tr></thead>
        <tbody>{sub_rows}</tbody>
    </table>
    </div>
</section>

{ai_section}

<!-- Top Merchants -->
<section id="top-merchants" class="card">
    <h2>Top 20 Merchants</h2>
    <table class="data-table">
        <thead><tr><th>#</th><th>Merchant</th><th style="text-align:right">Total</th><th style="text-align:center">Transactions</th></tr></thead>
        <tbody>{top_rows}</tbody>
    </table>
</section>

<!-- Monthly Detail -->
<section id="monthly-detail" class="card">
    <h2>Monthly Detail</h2>
    {monthly_sections}
</section>

<script src="https://cdn.jsdelivr.net/npm/chart.js@4/dist/chart.umd.min.js"></script>
<script>
document.addEventListener('DOMContentLoaded', function() {{
    if (typeof Chart === 'undefined') return;

    // Monthly spending bar chart
    {monthly_chart_js}

    // Category donut chart
    new Chart(document.getElementById('categoryChart'), {{
        type: 'doughnut',
        data: {{
            labels: {cat_labels},
            datasets: [{{
                data: {cat_values},
                backgroundColor: {cat_colors},
                borderWidth: 2,
                borderColor: '#fff',
            }}]
        }},
        options: {{
            responsive: true,
            plugins: {{
                legend: {{ position: 'right', labels: {{ font: {{ size: 11 }}, boxWidth: 12 }} }},
                tooltip: {{ callbacks: {{ label: ctx => ctx.label + ': $' + ctx.parsed.toLocaleString(undefined, {{minimumFractionDigits:2}}) }} }}
            }}
        }}
    }});

    {fixed_chart_js}
}});
</script>

<footer style="text-align:center;padding:30px;color:var(--muted);font-size:0.85em">
    Generated by Financial Dashboard &amp; Subscription Auditor
</footer>
</body>
</html>"""
    return html


# ── Main ─────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Financial Dashboard & Subscription Auditor")
    parser.add_argument("--path", default=".", help="Folder containing CSV files (default: current directory)")
    parser.add_argument("--ai", action="store_true", help="Generate AI-powered recommendations (requires ANTHROPIC_API_KEY)")
    args = parser.parse_args()

    folder = os.path.abspath(args.path)
    print(f"Reading CSVs from: {folder}")

    # Load user category overrides from categories.csv
    global _user_categories
    _user_categories = load_user_categories(folder)

    # Load notes and budgets
    user_notes = load_notes(folder)
    user_budgets = load_budgets(folder)

    transactions, debt_payoffs = parse_csvs(folder)
    print(f"Loaded {len(transactions)} transactions")

    # Extract transfer data from debit card CSVs
    transfers = extract_transfers(folder)
    if transfers:
        print(f"Found transfer data across {len(transfers)} months")

    # Extract passive income from investment portfolio
    passive_income = extract_passive_income(folder)
    if passive_income:
        print(f"Portfolio passive income: ${passive_income['annual_income']:,.2f}/year (${passive_income['monthly_income']:,.2f}/month) from {len(passive_income['accounts'])} accounts")

    # Extract corporate income from corporate accounts
    corporate_income = extract_corporate_income(folder)
    if corporate_income:
        print(f"Corporate income: ${corporate_income['total_income']:,.2f} total ({corporate_income['months']} months) — Revenue: ${corporate_income['revenue_total']:,.2f}, Dividends: ${corporate_income['dividends_total']:,.2f}")

    data = analyze(transactions, transfers=transfers,
                   debt_payoffs=debt_payoffs)
    print(f"Total spend: ${data['total']:,.2f} across {len(data['months'])} months")
    print(f"Found {len(data['subscriptions'])} recurring charges")
    if data.get("fixed_cost_detail"):
        print(f"Fixed costs: ${data['fixed_total']:,.2f} | Discretionary: ${data['discretionary_total']:,.2f}")

    ai_html = None
    if args.ai:
        ai_html = get_ai_recommendations(data)

    html = generate_html(data, ai_html, notes=user_notes, budgets=user_budgets,
                         passive_income=passive_income,
                         corporate_income=corporate_income)
    output_path = os.path.join(folder, "dashboard.html")
    with open(output_path, "w", encoding="utf-8") as f:
        f.write(html)

    print(f"\nDashboard written to: {output_path}")
    print("Open it in your browser to view the report.")


if __name__ == "__main__":
    main()
