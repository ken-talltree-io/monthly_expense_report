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
import re
import ssl
import subprocess
import sys
from collections import defaultdict
from datetime import datetime, timedelta
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

# ── Merchant normalization ───────────────────────────────────────────────────
# Maps substrings in raw merchant names to a clean canonical name.
MERCHANT_ALIASES = {
    "AMAZON.CA": "Amazon",
    "AMZN MKTP CA": "Amazon",
    "AMAZON.CA PRIME": "Amazon Prime",
    "APPLE.COM/BILL": "Apple Media",
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
    # ScotiaBank merchants
    "RBCINS-LIFE": "RBC Life Insurance",
    "SUNLIFE MED INS": "Sun Life Insurance",
    "HYUNDAI CAPITAL": "Hyundai Car Payment",
    "COSTCO WHOLESALE": "Costco",
    "COSTCOWHOLESALE": "Costco",
    "FPOS COSTCO": "Costco",
    "STEADYHAND": "Steadyhand Investments",
    "REALCDNSUPERSTORE": "Real Canadian Superstore",
    "REALCDNLIQUORSTORE": "Real Canadian Liquor Store",
    "SPAUTOPIA": "Spautopia Spa",
    "B.C.HYDRO&POWER": "BC Hydro",
    "B.C. HYDRO-PAP": "BC Hydro",
    "BC HYDRO EV": "BC Hydro",
    "FORTISBCHOLDINGSINC": "FortisBC",
    "FORTISBC ENERGY": "FortisBC",
    "HYUNDAICAPITAL-RETAILCOLL": "Hyundai Car Payment",
    "STRATFORDHALL-BLACKBAUD": "Stratford Hall (Tuition)",
    "HKRETAILFUNDINGLP": "Hyundai Car Payment",
    "SCOTIABANK INSURANCE": "Scotiabank Insurance",
    # Debit card merchants
    "STRATFORD HALL": "Stratford Hall (Tuition)",
    "HYUNDAI PMNT": "Hyundai Car Payment",
    "FORTISBC": "FortisBC",
    "B.C. HYDRO": "BC Hydro",
    "DR. LIAT TZUR": "Dr. Liat Tzur (Orthodontics)",
    "ZENSURANCE": "Zensurance",
    "FRESHBOOKS": "FreshBooks",
    "FN": "Mortgage (First National)",
    "VANCOUVER PROPERTY TAXES": "Vancouver Property Taxes",
    "VANCOUVERPROPERTYTAXES": "Vancouver Property Taxes",
    "STRATFORDHALL": "Stratford Hall (Tuition)",
    # Common merchants missing aliases
    "AIRBNB": "Airbnb",
    "IKEA": "IKEA",
    "ARITZIA": "Aritzia",
    "WINNERS": "Winners",
    "NORTH SHORE KIA": "North Shore Kia",
    "NORTHSHORE KIA": "North Shore Kia",
    "SMASH VOLLEYBALL": "Smash Volleyball",
    "MOUNT WASHINGTON": "Mount Washington",
    "REVELSTOKE MOUNTAIN": "Revelstoke Mountain Resort",
    "CHATGPT": "OpenAI (ChatGPT)",
    "OPENAI": "OpenAI (ChatGPT)",
    "CURSOR.COM": "Cursor IDE",
    "CURSOR.SH": "Cursor IDE",
    "FRONTENDMASTERS": "Frontend Masters",
    "RESUME.IO": "Resume.io",
    "NUBA": "Nuba Restaurant",
    "LE PHO": "Le Pho Restaurant",
    "MAENAM": "Maenam Restaurant",
    "MARCELLO": "Marcello Ristorante",
    "1ST AVE ANIMAL": "1st Ave Animal Hospital",
    "KEATS CAMP": "Keats Camps",
    "LARGE TUTORING": "Large Tutoring",
    "PAINTED BOAT": "Painted Boat Resort",
    "HOTEL PALACE": "Hotel Palace Royal",
    "ACE HOTEL": "Ace Hotel",
    "RICHMOND OLYMPIC": "Richmond Olympic Oval",
    "LEAH MARKS": "Leah Marks Counselling",
    # ScotiaBank no-space format aliases
    "NORTHSHOREKIA": "North Shore Kia",
    # Additional common merchants
    "REVELATION LODGE": "Revelation Lodge",
    "MODO OLYMPIC": "Modo Car Share",
    "MODO_YOGA": "Modo Yoga",
    "YUM ICE CREAM": "Yum Ice Creamery",
    "BREKA BAKERY": "Breka Bakery",
    "DEVILLE COFFEE": "Deville Coffee",
    "BEAN AROUND": "Bean Around The World",
    "COAST GOODS": "Coast Goods",
    "MAH MILKBAR": "Mah Milkbar",
    "STRATECHERY": "Stratechery",
    "PULPFICTION": "Pulp Fiction Books",
    "SWANK'S SALON": "Swank's Salon",
    "BETA5 CHOCOLAT": "Beta5 Chocolates",
    "HOMESENSE": "HomeSense",
    "PARKING FALSE CREEK": "False Creek Parking",
    "VANCOUVER PB RECREATIO": "Vancouver Parks Board",
    "DR. MARINA LIARSKY": "Dr. Marina Liarsky (Chiropractor)",
    "DRIVE PHARMACY": "The Drive Pharmacy",
    "ANCHOR EATERY": "Anchor Eatery",
    "BC PLACE": "BC Place",
    "GRANT STREET WELLNESS": "Grant Street Wellness",
    "TK'S GOURMET TURKISH": "TK's Gourmet Turkish",
    "CAFE AMERICANO": "Cafe Americano",
    "INTERMARCHE": "Intermarche",
    "OPHELIA": "Ophelia Restaurant",
    "RIO THEATRE": "Rio Theatre",
    "BUMP N GRIND": "Bump N Grind Cafe",
    "GENERATIONS OPTOMETRY": "Generations Optometry",
    "COMMON SENSE PLUS": "Common Sense Plus",
    "STEAMGAMES": "Steam Games",
    "VANCOUVER SCHOOL BOARD": "Vancouver School Board",
    "CLAUDE.AI": "Claude AI (Anthropic)",
    "NVIDIA": "Nvidia",
    "SHELL RECHARGE": "Shell EV Charging",
    "CHARGEPOINT": "ChargePoint EV Charging",
    "ON THE RUN EV": "On The Run EV Charging",
    "GROUNDS FOR COFFEE": "Grounds For Coffee",
    "THIERRY CHOCOLAT": "Thierry Chocolates",
    "HAVANA VANCOUVER": "Havana Restaurant",
    "ANGUS T": "Angus T (Fish & Chips)",
    "KOKOPELLI": "Kokopelli Salon",
    "MAVI JEANS": "Mavi Jeans",
    "LA BAGUETTE": "La Baguette Catering",
    "FLYING APRON": "The Flying Apron",
    "PURR CLOTHING": "Purr Clothing",
    "SECOND NATURE HOME": "Second Nature Home",
    "RAINFLORIST": "Rainflorist",
    "COAST GOODS": "Coast Goods",
    "CHANCE CAFE": "Chance Cafe",
    "SILVER CREEK TRAVEL": "Silver Creek Travel Centre",
    "HOT CHOCOLATES": "Hot Chocolates",
}

# Business expenses — excluded from personal spending totals
BUSINESS_MERCHANTS = {"Zensurance", "FreshBooks"}

# Merchants that are always fixed costs, regardless of transaction source
FIXED_COST_MERCHANTS = {
    "Wawanesa Insurance", "BCAA Insurance", "ICBC",
    "RBC Life Insurance", "Sun Life Insurance",
    "Vancouver Property Taxes",
}

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
        "Le Pho", "Nuba", "Maenam", "Marcello",
        "Chai Restaurant", "Le Don Vegan", "Mediterranean Speciality",
        "Chickpea", "Thaigo", "Fortune Wok", "Sushi Loku",
        "Anchor Eatery", "Ophelia Restaurant", "TK's Gourmet Turkish",
        "Mah Milkbar", "Cafe Americano", "Intermarche",
        "Havana Restaurant", "Angus T", "La Baguette",
        "The Flying Apron", "Hot Chocolates", "Chance Cafe",
    ]),
    ("Cafes & Treats", [
        "Continental Coffee", "JJ Bean", "Prado Cafe", "Forecast Coffee",
        "Moja Coffee", "Starbucks", "Nemesis Coffee", "Kits Beach Coffee",
        "Bean Scene", "White Rabbit Coffee", "Bolacco", "Crema Cafe",
        "Parsonage Cafe",
        "Breka Bakery", "Deville Coffee", "Bean Around", "Bump N Grind",
        "Grounds For Coffee", "Matchstick", "Laughing Bean",
        "Cobs Bread", "Purebread", "Livia Sweets", "To Live For Bakery",
        "The First Ravioli", "Oh Sweet Day", "Earnest Ice Cream",
        "The Bench Bakehouse", "Uprising Breads", "Terra Breads",
        "More Cafe", "That Churro", "The Bread Company", "Melt Confectionary",
        "Siegel", "Dilly Dally",
        "Yum Ice Creamery", "Beta5 Chocolates", "Thierry Chocolates",
    ]),
    ("Groceries", [
        "Real Canadian Superstore", "IGA", "Safeway", "Save-On-Foods",
        "Super Valu", "Whole Foods", "Thrifty Foods", "Sweet Cherubim",
        "City Avenue Market", "Fig Mart", "Persia Foods", "Mostafa",
        "The Grocery Store", "New Triple A", "Dundas KK", "Otter Co-op",
        "Good Fridays", "Choices Drive", "Flourist",
        "Costco",
    ]),
    ("Liquor & Alcohol", [
        "BC Liquor", "Liberty Wine", "Legacy Liquor", "Sundance Liquor",
        "Strange Fellows", "Strathcona Beer", "Commercial Drive Licoric",
        "Real Canadian Liquor Store",
    ]),
    ("Telecom", [
        "Telus", "Fido Mobile",
    ]),
    ("Streaming & Subscriptions", [
        "Netflix", "Disney+", "Bell Media", "Sportsnet NOW", "MUBI",
        "Amazon Prime", "Open Heart Project",
        "Brief Media", "Stratechery",
        "OpenAI", "Cursor IDE", "Frontend Masters", "Resume.io",
        "Claude AI", "Nvidia",
    ]),
    ("Pets", [
        "MrPets", "Pet Valu", "Pet Pantry", "Caulfeild Vet",
        "1st Ave Animal Hospital",
    ]),
    ("Transportation", [
        "Lyft", "Uber", "BC Ferries", "Air Canada", "TransLink Compass",
        "Harbour Air", "Expedia",
        "Modo Car Share",
    ]),
    ("Auto", [
        "PayByPhone", "Impark", "Honk Parking", "Petro-Canada",
        "Chevron", "Nwest Parking", "New West Parking", "Zipby",
        "False Creek Parking", "Silver Creek Travel",
        "Shell EV Charging", "ChargePoint EV", "On The Run EV",
        "KAL Tire", "Shine Auto Wash", "Air-Serv", "Tesla", "Sony Wash",
        "Hyundai Car Payment", "North Shore Kia",
    ]),
    ("Clothing", [
        "Ardene", "Old Navy", "Uniqlo", "Simons", "Lululemon", "Vessi",
        "JQ Clothing", "Mintage Vintage", "Funktional", "Urban Planet",
        "Shoe Company", "Quidditas", "Gatley", "Sool Of Thread",
        "Spool Of Thread", "Dressew",
        "Aritzia", "Winners",
        "Mavi Jeans", "Purr Clothing",
    ]),
    ("Sports & Outdoor", [
        "MEC", "Sport Chek", "Decathlon", "Sports Junkies", "Canucks",
        "Long & McQuade", "Drive Drum",
        "Sun Peaks", "Mt Seymour", "Big White",
        "Mount Washington", "Revelstoke Mountain",
    ]),
    ("Home Improvement", [
        "Home Depot", "Rona", "Dal-Tile", "Lighting Warehouse",
        "Magnet Hardware", "Skyland Building", "Bartlett Tree",
        "Bear Country Property", "Figaros Garden",
        "IKEA", "HomeSense", "Coast Goods", "Second Nature Home",
        "Rainflorist",
    ]),
    ("Health & Wellness", [
        "London Drugs", "Shoppers Drug Mart", "Rexall", "New Visage",
        "Harlow Skin", "Body Energy", "Spice Beauty", "Paulie",
        "Caulfeild Pharmasave", "Hemlock Hospital",
        "Mount Pleasant Visio",
        "Spautopia", "Leah Marks",
        "Swank's Salon", "Dr. Marina Liarsky", "Grant Street Wellness",
        "Modo Yoga", "The Drive Pharmacy", "Generations Optometry",
        "Kokopelli Salon",
        "Yaletown Dentistry", "Cambie Broadway Dental", "Tot 2 Teen Dental",
        "Dr. Liat Tzur", "Sunrise Orthodontics",
    ]),
    ("Insurance", [
        "ICBC", "BCAA Insurance", "Wawanesa", "RBC Life Insurance", "Sun Life Insurance",
        "Scotiabank Insurance",
    ]),
    ("Entertainment", [
        "Apple Media", "Ticketmaster", "SeatGeek", "Eventbrite", "Cineplex",
        "Scandinave Spa", "Butchart Gardens", "Candytopia", "Capri Valley",
        "Spirit Halloween", "Games On The Drive", "Got Craft",
        "Red Horses Gallery", "Mosaic Books", "International Travel Maps",
        "The Anza Club",
        "Smash Volleyball", "Richmond Olympic Oval",
        "BC Place", "Rio Theatre", "Steam Games", "Pulp Fiction Books",
        "Vancouver Parks Board",
    ]),
    ("Online Shopping", [
        "Amazon",
    ]),
    ("Kids", [
        "Dilly Dally Kids",
    ]),
    ("Travel & Hotels", [
        "Best Western", "Nomade Cabo", "Merpago", "Clip Mx",
        "La Comer", "Tastes On The Fly", "0835_YVR",
        "Airbnb", "Painted Boat", "Hotel Palace", "Ace Hotel", "Keats Camp",
        "Revelation Lodge",
    ]),
    ("Donations", [
        "Make-A-Wish",
    ]),
    ("Education", [
        "Stratford Hall",
        "Large Tutoring",
        "Vancouver School Board",
    ]),
    ("Utilities", [
        "FortisBC", "BC Hydro",
    ]),
    ("Housing", [
        "Mortgage", "Vancouver Property Taxes",
    ]),
]

# ── Category consolidation ───────────────────────────────────────────────────
# Maps fine-grained categories to broader groups for cleaner reporting.
# Any category not listed passes through unchanged.
CATEGORY_CONSOLIDATION = {
    "Education": "Kids & Education",
    "Kids": "Kids & Education",
    "Restaurants & Dining": "Food & Dining",
    "Cafes & Treats": "Food & Dining",
    "Groceries": "Food & Dining",
    "Liquor & Alcohol": "Food & Dining",
    "Home Improvement": "Housing & Utilities",
    "Housing": "Housing & Utilities",
    "Utilities": "Housing & Utilities",
    "Transportation": "Transportation",
    "Auto": "Auto",
    "Health & Wellness": "Health & Wellness",
    "Entertainment": "Recreation",
    "Sports & Outdoor": "Recreation",
    "Travel & Hotels": "Travel",
    "Clothing": "Shopping",
    "Online Shopping": "Shopping",
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

    # Collect CSV files from transactions/personal/
    all_files = []
    txn_personal = os.path.join(folder, "transactions", "personal")
    if os.path.isdir(txn_personal):
        all_files.extend(sorted(glob.glob(os.path.join(txn_personal, "**", "*.csv"), recursive=True)))
    # Backward compat: check old directory structure
    if not all_files:
        for subdir in ["credit card", "debit card"]:
            subpath = os.path.join(folder, subdir)
            if os.path.isdir(subpath):
                all_files.extend(sorted(glob.glob(os.path.join(subpath, "*.csv"))))
                all_files.extend(sorted(glob.glob(os.path.join(subpath, "*", "*.csv"))))
    if not all_files:
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
                    entry = {
                        "date": date,
                        "month": date.strftime("%Y-%m"),
                        "raw_merchant": raw_merchant,
                        "merchant": merchant,
                        "category": category,
                        "amount": amount,
                        "source": "credit",
                    }
                    if merchant in FIXED_COST_MERCHANTS:
                        entry["fixed_cost"] = True
                    transactions.append(entry)

            elif "transaction" in headers:
                # ── Debit card format ──
                debit_count += 1
                for row in reader:
                    txn_type = row["transaction"]
                    amt_str = row["amount"].strip()
                    if not amt_str:
                        continue
                    amount = float(amt_str)
                    description = row["description"]

                    if txn_type == "SPEND":
                        date = datetime.strptime(row["date"], "%Y-%m-%d")
                        merchant = normalize_merchant(description)
                        if merchant in BUSINESS_MERCHANTS:
                            business_total += abs(amount)
                            continue
                        category = categorize(merchant)
                        category = CATEGORY_CONSOLIDATION.get(category, category)
                        entry = {
                            "date": date,
                            "month": date.strftime("%Y-%m"),
                            "raw_merchant": description,
                            "merchant": merchant,
                            "category": category,
                            "amount": abs(amount),
                            "source": "debit",
                        }
                        if merchant in FIXED_COST_MERCHANTS:
                            entry["fixed_cost"] = True
                        transactions.append(entry)
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
                    elif txn_type == "OBP_OUT":
                        # Online bill payments (e.g. property taxes)
                        # Extract merchant from "Online bill payment for MERCHANT, account ..."
                        merchant_raw = description
                        if "Online bill payment for " in description:
                            merchant_raw = description.split("Online bill payment for ", 1)[1]
                            merchant_raw = merchant_raw.split(",")[0]
                        date = datetime.strptime(row["date"], "%Y-%m-%d")
                        merchant = normalize_merchant(merchant_raw)
                        category = categorize(merchant)
                        category = CATEGORY_CONSOLIDATION.get(category, category)
                        amt = abs(amount)
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
                    elif txn_type == "E_TRFOUT":
                        date = datetime.strptime(row["date"], "%Y-%m-%d")
                        merchant = "Interac e-Transfer"
                        category = categorize(merchant)
                        category = CATEGORY_CONSOLIDATION.get(category, category)
                        amt = abs(amount)
                        transactions.append({
                            "date": date,
                            "month": date.strftime("%Y-%m"),
                            "raw_merchant": description,
                            "merchant": merchant,
                            "category": category,
                            "amount": amt,
                            "source": "debit",
                        })

    print(f"Found {credit_count} credit card and {debit_count} debit card CSV files")
    if business_total > 0:
        print(f"Excluded ${business_total:,.2f} in business expenses (Zensurance, FreshBooks)")
    if debt_payoffs:
        total_payoffs = sum(d["amount"] for d in debt_payoffs)
        print(f"Excluded ${total_payoffs:,.2f} in debt payoffs (mortgage/auto — paid off)")
    return sorted(transactions, key=lambda t: t["date"]), debt_payoffs


# ── Statement Balance Parsing ────────────────────────────────────────────────

def parse_statement_balances(folder: str) -> dict[str, dict]:
    """Parse statement PDFs to get authoritative account balances.

    Scans statements/ for Wealthsimple, Steadyhand, and Scotiabank PDFs.
    Returns a dict keyed by account suffix with:
        {"balance": float, "date": str, "source": str}
    For each suffix, keeps only the most recent statement.
    """
    stmt_dir = os.path.join(folder, "statements")
    if not os.path.isdir(stmt_dir):
        return {}

    # Quick check that pdftotext is available
    try:
        subprocess.run(["pdftotext", "-v"], capture_output=True, timeout=5)
    except (FileNotFoundError, subprocess.TimeoutExpired):
        return {}

    results: dict[str, dict] = {}  # suffix -> {balance, date, source, return_pct, dividends_annual}

    def _pdf_text(path: str) -> str:
        """Extract text from a PDF using pdftotext -layout."""
        try:
            r = subprocess.run(
                ["pdftotext", "-layout", path, "-"],
                capture_output=True, text=True, timeout=30,
            )
            return r.stdout if r.returncode == 0 else ""
        except (subprocess.TimeoutExpired, OSError):
            return ""

    # ── Wealthsimple (individual PDFs per account) ──────────────────────────
    # Scan both personal and corporate Wealthsimple statement directories
    ws_pdfs: dict[str, list[tuple[str, str]]] = defaultdict(list)
    for ownership in ["personal", "corporate"]:
        ws_dir = os.path.join(stmt_dir, ownership, "Wealthsimple")
        if not os.path.isdir(ws_dir):
            continue
        for fname in os.listdir(ws_dir):
            if not fname.lower().endswith(".pdf"):
                continue
            if "_CRM2_" in fname:
                continue  # skip CRM2 annual reports (return % managed in portfolio.csv)
            parts = fname.split("_")
            if len(parts) < 3:
                continue
            suffix = parts[0]
            # Extract YYYY-MM from filename (3rd segment)
            date_seg = parts[2] if len(parts) > 2 else ""
            ws_pdfs[suffix].append((date_seg, os.path.join(ws_dir, fname)))

    for suffix, files in ws_pdfs.items():
        # Use the most recent statement for balance
        files.sort(key=lambda x: x[0], reverse=True)
        date_seg, pdf_path = files[0]

        text = _pdf_text(pdf_path)
        if not text:
            continue

        # Parse balance + book cost: "Total Portfolio  $market  100.00  $book  100.00"
        m = re.search(
            r"Total Portfolio\s+\$([0-9,]+\.\d{2})\s+100\.00\s+\$([0-9,]+\.\d{2})\s+100\.00",
            text,
        )
        if not m:
            continue
        balance = float(m.group(1).replace(",", ""))
        book_cost = float(m.group(2).replace(",", ""))

        # Handle USD accounts: convert to CAD using statement exchange rate
        is_usd = suffix.upper().endswith("USD")
        if is_usd:
            fx = re.search(
                r"\$1 USD = \$([0-9.]+) CAD", text
            )
            if fx:
                fx_rate = float(fx.group(1))
                balance = round(balance * fx_rate, 2)
                book_cost = round(book_cost * fx_rate, 2)
            else:
                # Check other Wealthsimple PDFs for an exchange rate
                for other_suffix, other_files in ws_pdfs.items():
                    if other_suffix == suffix:
                        continue
                    other_text = _pdf_text(other_files[0][1])
                    fx = re.search(r"\$1 USD = \$([0-9.]+) CAD", other_text)
                    if fx:
                        fx_rate = float(fx.group(1))
                        balance = round(balance * fx_rate, 2)
                        book_cost = round(book_cost * fx_rate, 2)
                        break
                else:
                    continue  # Can't convert; skip this account

        # Parse statement end date
        dm = re.search(
            r"(\d{4}-\d{2}-\d{2})\s*-\s*(\d{4}-\d{2}-\d{2})", text
        )
        stmt_date = dm.group(2) if dm else date_seg

        # Simple return rate: (market_value - book_cost) / book_cost
        simple_return = round((balance - book_cost) / book_cost * 100, 2) if book_cost > 0 else None

        results[suffix] = {
            "balance": balance,
            "date": stmt_date,
            "source": "Wealthsimple statement",
            "return_pct": simple_return,
            "return_source": "estimated",
            "dividends_annual": None,
        }

        # Parse dividends + interest from ALL monthly statements for this account
        total_income = 0.0
        months_seen = set()
        for ds, fp in files:
            pdf_text = text if fp == pdf_path else _pdf_text(fp)
            if not pdf_text:
                continue
            # Track unique months
            months_seen.add(ds[:7] if len(ds) >= 7 else ds)
            monthly_income = 0.0
            div_m = re.search(r"Dividends\s+\$([\d,]+\.\d{2})", pdf_text)
            int_m = re.search(r"Interest Earned\s+\$([\d,]+\.\d{2})", pdf_text)
            if div_m:
                monthly_income += float(div_m.group(1).replace(",", ""))
            if int_m:
                monthly_income += float(int_m.group(1).replace(",", ""))
            total_income += monthly_income

        if months_seen and total_income > 0:
            results[suffix]["dividends_annual"] = round(
                total_income / len(months_seen) * 12, 2
            )

    # ── Steadyhand (consolidated quarterly PDFs) ────────────────────────────
    sh_dir = os.path.join(stmt_dir, "personal", "Steadyhand")
    if os.path.isdir(sh_dir):
        # Find the most recent quarterly PDF by parsing month names
        MONTH_ORDER = {
            "january": 1, "february": 2, "march": 3, "april": 4,
            "may": 5, "june": 6, "july": 7, "august": 8,
            "september": 9, "october": 10, "november": 11, "december": 12,
        }
        sh_pdfs = []
        for fname in os.listdir(sh_dir):
            if not fname.lower().endswith(".pdf"):
                continue
            name = os.path.splitext(fname)[0]
            parts = name.split()
            if len(parts) == 2:
                month_str, year_str = parts[0].lower(), parts[1]
                if month_str in MONTH_ORDER:
                    try:
                        sort_key = int(year_str) * 100 + MONTH_ORDER[month_str]
                        sh_pdfs.append((sort_key, os.path.join(sh_dir, fname)))
                    except ValueError:
                        pass

        if sh_pdfs:
            sh_pdfs.sort(reverse=True)
            _, latest_pdf = sh_pdfs[0]
            text = _pdf_text(latest_pdf)

            if text:
                # Parse "As of" date
                date_m = re.search(r"As of (\w+ \d{1,2},?\s*\d{4})", text)
                stmt_date = date_m.group(1) if date_m else ""

                # Parse "Your Accounts" table: 7-digit account number + market value
                for row_m in re.finditer(
                    r"^(\d{7})\s+.+?\s+([\d,]+\.\d{2})\s*$",
                    text, re.MULTILINE,
                ):
                    acct_num = row_m.group(1)
                    balance = float(row_m.group(2).replace(",", ""))
                    if balance <= 0:
                        continue
                    results[acct_num] = {
                        "balance": balance,
                        "date": stmt_date,
                        "source": "Steadyhand statement",
                        "return_pct": None,
                        "dividends_annual": None,
                    }

                # Parse per-account 1-year returns (first match per account wins)
                for acct_m in re.finditer(r"Account (\d{7})\s+\w", text):
                    acct_num = acct_m.group(1)
                    if acct_num not in results or results[acct_num]["return_pct"] is not None:
                        continue
                    si = re.search(r"1 Year\s+([\d.]+)", text[acct_m.start():])
                    if si:
                        results[acct_num]["return_pct"] = float(si.group(1))

                # Parse per-account dividends from "Distribution - Reinvested" lines
                # Quarterly statements: sum amounts, annualize by ×4
                for acct_num in list(results.keys()):
                    if not results[acct_num]["source"].startswith("Steadyhand"):
                        continue
                    acct_pattern = f"Account {acct_num}"
                    acct_pos = text.find(acct_pattern)
                    if acct_pos == -1:
                        continue
                    # Scope to this account's section (up to next account or end)
                    next_acct = re.search(r"Account \d{7}", text[acct_pos + len(acct_pattern):])
                    end_pos = (acct_pos + len(acct_pattern) + next_acct.start()) if next_acct else len(text)
                    section = text[acct_pos:end_pos]
                    total_dist = 0.0
                    for dist_m in re.finditer(r"Distribution - Reinvested\s+[\d/]+\s+([\d,]+\.\d{2})", section):
                        total_dist += float(dist_m.group(1).replace(",", ""))
                    if total_dist > 0:
                        results[acct_num]["dividends_annual"] = round(total_dist * 4, 2)

    # ── Scotiabank Chequing (e-statement PDFs) ───────────────────────────
    MONTH_NAMES = {
        "january": 1, "february": 2, "march": 3, "april": 4,
        "may": 5, "june": 6, "july": 7, "august": 8,
        "september": 9, "october": 10, "november": 11, "december": 12,
    }
    for ownership in ["personal", "corporate"]:
        sc_dir = os.path.join(stmt_dir, ownership, "Scotiabank Chequing")
        if not os.path.isdir(sc_dir):
            continue

        # Collect PDFs and sort by date (most recent first)
        sc_pdfs = []
        for fname in os.listdir(sc_dir):
            if not fname.lower().endswith(".pdf"):
                continue
            name = os.path.splitext(fname)[0]
            # Personal: "February 2026 e-statement"
            # Corporate: "Tall Tree Technology - DebitCard - January 2026 e-statement"
            m = re.search(r"(\w+)\s+(\d{4})\s+e-statement", name, re.IGNORECASE)
            if m:
                month_str = m.group(1).lower()
                year_str = m.group(2)
                if month_str in MONTH_NAMES:
                    sort_key = int(year_str) * 100 + MONTH_NAMES[month_str]
                    sc_pdfs.append((sort_key, os.path.join(sc_dir, fname)))
        if not sc_pdfs:
            continue

        sc_pdfs.sort(reverse=True)
        _, latest_pdf = sc_pdfs[0]
        text = _pdf_text(latest_pdf)
        if not text:
            continue

        # Parse account number (Scotiabank format: XXXXX XXXXX XX)
        acct_m = re.search(r"(\d{5})\s+(\d{5})\s+(\d{2})", text)
        if not acct_m:
            continue
        acct_num = acct_m.group(1) + acct_m.group(2) + acct_m.group(3)

        if ownership == "personal":
            # Personal: "Closing Balance on February 17, 2026:  $2,382.71"
            bal_m = re.search(
                r"Closing Balance on (.+?)[\s:]+\$([0-9,]+\.\d{2})", text
            )
            if bal_m:
                balance = float(bal_m.group(2).replace(",", ""))
                stmt_date = bal_m.group(1).strip()
                results[acct_num] = {
                    "balance": balance,
                    "date": stmt_date,
                    "source": "Scotiabank statement",
                    "return_pct": None,
                    "dividends_annual": None,
                }
        else:
            # Corporate: last balance from transaction lines
            # Format: MM/DD/YYYY  DESCRIPTION  amount  amount  balance
            last_balance = None
            stmt_date = ""
            # Parse statement end date from line with account number
            # Format: "Business Account  40360 01202 19  Dec 31 2025  Jan 30 2026"
            to_m = re.search(
                r"(\d{5}\s+\d{5}\s+\d{2})\s+\w{3}\s+\d{1,2}\s+\d{4}\s+(\w{3}\s+\d{1,2}\s+\d{4})",
                text,
            )
            if to_m:
                stmt_date = to_m.group(2)
            for line in text.split("\n"):
                line = line.strip()
                if re.match(r"\d{2}/\d{2}/\d{4}\s+", line):
                    # Find rightmost dollar amount (the balance column)
                    amounts = re.findall(r"([\d,]+\.\d{2})", line)
                    if amounts:
                        last_balance = float(amounts[-1].replace(",", ""))
            if last_balance is not None:
                results[acct_num] = {
                    "balance": last_balance,
                    "date": stmt_date,
                    "source": "Scotiabank statement",
                    "return_pct": None,
                    "dividends_annual": None,
                }

    # ── BC Property Assessments (sidecar CSV beside scanned PDFs) ───────────
    bc_dir = os.path.join(stmt_dir, "personal", "British Columbia")
    bc_csv = os.path.join(bc_dir, "property_assessments.csv")
    if os.path.isfile(bc_csv):
        # Collect all rows, then keep only the most recent year per suffix
        bc_rows: dict[str, tuple[int, dict]] = {}  # suffix -> (year, row_data)
        with open(bc_csv, newline="", encoding="utf-8-sig") as f:
            for row in csv.DictReader(f):
                suffix = row.get("Suffix", "").strip()
                if not suffix:
                    continue
                year = row.get("Year", "").strip()
                try:
                    year_int = int(year)
                except (ValueError, TypeError):
                    continue
                if suffix in bc_rows and bc_rows[suffix][0] >= year_int:
                    continue
                val_str = row.get("Assessed Value", "").strip().replace("$", "").replace(",", "")
                try:
                    balance = float(val_str)
                except (ValueError, TypeError):
                    continue
                change_str = row.get("Change", "").strip().replace("%", "")
                try:
                    return_pct = float(change_str)
                except (ValueError, TypeError):
                    return_pct = None
                bc_rows[suffix] = (year_int, {
                    "balance": balance,
                    "date": f"{year} assessment",
                    "source": "BC Assessment",
                    "return_pct": return_pct,
                    "return_source": "BC Assessment",
                    "dividends_annual": None,
                })
        for suffix, (_, entry) in bc_rows.items():
            results[suffix] = entry

    return results


# ── Income & Transfer Extraction ─────────────────────────────────────────────

def extract_passive_income(folder: str, source: str = "csv") -> dict | None:
    """Extract annual passive income from investment portfolio.

    source="csv":        All financials from portfolio.csv (no PDF parsing).
    source="statements": Balances/returns/income from PDF statements;
                         portfolio.csv still provides account list & metadata.
    """
    portfolio_path = os.path.join(folder, "portfolio.csv")
    if not os.path.exists(portfolio_path):
        return None

    ACCESSIBLE_TYPES = {"Non-reg", "Cash", "TFSA"}  # spendable without tax penalty

    accessible = []
    registered = []  # RRSP + RESP
    corporate_accts = []
    property_accts = []

    # Parse statement balances only in statements mode
    stmt_balances = parse_statement_balances(folder) if source == "statements" else {}

    # Build suffix → statement balance lookup (also match suffixes that are
    # a trailing substring of the statement key, e.g. CSV "6905CAD" matches
    # statement key "HQ8KF6905CAD")
    def _find_stmt(csv_suffix: str) -> dict | None:
        if not csv_suffix:
            return None
        if csv_suffix in stmt_balances:
            return stmt_balances[csv_suffix]
        for key, val in stmt_balances.items():
            if key.endswith(csv_suffix):
                return val
        return None

    with open(portfolio_path, newline="", encoding="utf-8-sig") as f:
        reader = csv.reader(f)
        header = next(reader)
        h = [c.strip().lower().replace("\n", " ") for c in header]
        col_account = 0
        col_type = next((i for i, c in enumerate(h) if "asset" in c or c == "type"), 2)
        col_value = next((i for i, c in enumerate(h) if "total" in c and "value" in c), 4)
        col_return = next((i for i, c in enumerate(h) if "return" in c), None)
        col_start_date = next((i for i, c in enumerate(h) if "start date" in c), None)
        col_brokerage = next((i for i, c in enumerate(h) if "brokerage" in c), 1)
        col_suffix = next((i for i, c in enumerate(h) if "suffix" in c), None)
        col_strategy = next((i for i, c in enumerate(h) if "strategy" in c), None)
        col_yield = next((i for i, c in enumerate(h) if "yield" in c), None)

        for row in reader:
            if len(row) <= max(col_account, col_type, col_value):
                continue

            account = row[col_account].strip().replace("\n", " ")
            asset_type = row[col_type].strip().replace("\n", " ")

            # Skip totals row
            if not account:
                continue

            # Parse total value: portfolio.csv overrides, then statement, then 0
            val_str = row[col_value].strip().replace("$", "").replace(",", "")
            csv_value = None
            try:
                csv_value = float(val_str)
            except (ValueError, TypeError):
                pass

            acct_suffix = row[col_suffix].strip() if col_suffix is not None and col_suffix < len(row) else ""
            stmt = _find_stmt(acct_suffix)

            if source == "csv":
                # CSV mode: balance from portfolio.csv only
                if csv_value is not None and csv_value > 0:
                    total_value = csv_value
                    balance_source = "csv"
                    statement_date = ""
                else:
                    total_value = 0.0
                    balance_source = ""
                    statement_date = ""
            else:
                # Statement mode: balance from statement only
                if stmt:
                    total_value = stmt["balance"]
                    balance_source = stmt["source"]
                    statement_date = stmt["date"]
                else:
                    total_value = 0.0
                    balance_source = ""
                    statement_date = ""

            if total_value <= 0:
                continue

            # Parse investment start date
            start_date = None
            if col_start_date is not None and col_start_date < len(row):
                date_str = row[col_start_date].strip()
                for fmt in ("%B %d, %Y", "%b %d, %Y", "%b %d %Y", "%Y-%m-%d"):
                    try:
                        start_date = datetime.strptime(date_str, fmt).date()
                        break
                    except ValueError:
                        continue

            # Return %
            rate_str = row[col_return].strip().replace("%", "") if col_return is not None and col_return < len(row) else ""
            csv_return = None
            if rate_str and rate_str != "TBD":
                try:
                    csv_return = float(rate_str)
                except (ValueError, TypeError):
                    pass

            if source == "csv":
                # CSV mode: return from portfolio.csv only
                if csv_return is not None:
                    return_pct = csv_return
                    return_source = "csv"
                else:
                    return_pct = 0.0
                    return_source = ""
            else:
                # Statement mode: return from statement only
                if stmt and stmt.get("return_pct") is not None:
                    return_pct = stmt["return_pct"]
                    return_source = stmt.get("return_source", stmt["source"])
                else:
                    return_pct = 0.0
                    return_source = ""

            # Income vs Growth split
            total_return_annual = total_value * return_pct / 100
            strategy = row[col_strategy].strip() if col_strategy is not None and col_strategy < len(row) else ""

            csv_yield = None
            if col_yield is not None and col_yield < len(row):
                yield_str = row[col_yield].strip().replace("%", "")
                if yield_str:
                    try:
                        csv_yield = float(yield_str)
                    except (ValueError, TypeError):
                        pass

            if source == "csv":
                # CSV mode: income from yield% or interest strategy
                if csv_yield is not None:
                    income_annual = total_value * csv_yield / 100
                    income_source = "yield"
                elif strategy == "Interest":
                    income_annual = total_return_annual
                    income_source = "interest"
                else:
                    income_annual = 0.0
                    income_source = ""
                growth_annual = total_return_annual - income_annual
            else:
                # Statement mode: income from statement dividends only
                if stmt and stmt.get("dividends_annual") is not None:
                    income_annual = stmt["dividends_annual"]
                    income_source = "dividends"
                else:
                    income_annual = 0.0
                    income_source = ""
                # Only compute growth when we have a statement return %
                if return_pct > 0:
                    growth_annual = total_return_annual - income_annual
                else:
                    growth_annual = 0.0

            brokerage = row[col_brokerage].strip().replace("\n", " ") if col_brokerage < len(row) else ""

            entry = {
                "account": account,
                "brokerage": brokerage,
                "type": asset_type,
                "value": total_value,
                "income_annual": round(income_annual, 2),
                "growth_annual": round(growth_annual, 2),
                "return_pct": round(return_pct, 2),
                "return_source": return_source,
                "income_source": income_source,
                "strategy": strategy,
                "start_date": start_date,
                "balance_source": balance_source,
                "statement_date": statement_date,
            }

            # Route to appropriate bucket
            if asset_type == "Corporate":
                corporate_accts.append(entry)
            elif asset_type == "Property":
                property_accts.append(entry)
            elif asset_type in ("RRSP", "RESP"):
                registered.append(entry)
            elif (income_annual > 0 or growth_annual > 0) and asset_type in ACCESSIBLE_TYPES:
                accessible.append(entry)

    if not accessible and not registered and not corporate_accts and not property_accts:
        return None

    accessible_income = sum(a["income_annual"] for a in accessible)
    accessible_growth = sum(a["growth_annual"] for a in accessible)
    registered_income = sum(a["income_annual"] for a in registered)
    registered_growth = sum(a["growth_annual"] for a in registered)
    accessible_balance = sum(a["value"] for a in accessible)
    registered_balance = sum(a["value"] for a in registered)
    corporate_balance = sum(a["value"] for a in corporate_accts)
    property_balance = sum(a["value"] for a in property_accts)

    return {
        "annual_income": round(accessible_income, 2),
        "monthly_income": round(accessible_income / 12, 2) if accessible_income else 0,
        "annual_growth": round(accessible_growth, 2),
        "accounts": sorted(accessible, key=lambda a: a["return_pct"], reverse=True),
        "accessible_balance": round(accessible_balance, 2),
        "registered_annual": round(registered_income, 2),
        "registered_monthly": round(registered_income / 12, 2) if registered_income else 0,
        "registered_growth": round(registered_growth, 2),
        "registered_accounts": sorted(registered, key=lambda a: a["return_pct"], reverse=True),
        "registered_balance": round(registered_balance, 2),
        "corporate_accounts": corporate_accts,
        "corporate_balance": round(corporate_balance, 2),
        "property_accounts": property_accts,
        "property_balance": round(property_balance, 2),
    }


def extract_transfers(folder: str) -> tuple[dict, list]:
    """Extract monthly transfer summary from debit card CSVs.

    Returns (aggregates, incoming_etransfers) where:
    - aggregates: dict of month -> {"in": float, "out": float}
    - incoming_etransfers: list of {"date": date, "amount": float} for E_TRFIN
    Covers TRFOUT, TRFIN, TRFINTF, E_TRFOUT, E_TRFIN, EFTOUT.
    """
    TRANSFER_TYPES = {"TRFOUT", "TRFIN", "TRFINTF", "E_TRFOUT", "E_TRFIN", "EFTOUT"}
    transfers = defaultdict(lambda: {"in": 0.0, "out": 0.0})
    incoming_etransfers = []

    # Scan transactions/personal/ recursively (transfers come from debit-format CSVs, auto-detected)
    txn_personal = os.path.join(folder, "transactions", "personal")
    if not os.path.isdir(txn_personal):
        # Backward compat: try old path
        txn_personal = os.path.join(folder, "debit card")
        if not os.path.isdir(txn_personal):
            return {}, []

    debit_csvs = sorted(glob.glob(os.path.join(txn_personal, "**", "*.csv"), recursive=True))
    for fpath in debit_csvs:
        with open(fpath, newline="", encoding="utf-8-sig") as f:
            reader = csv.DictReader(f)
            if "transaction" not in (reader.fieldnames or []):
                continue  # skip non-debit CSVs
            for row in reader:
                txn_type = row["transaction"]
                if txn_type not in TRANSFER_TYPES:
                    continue
                amount = float(row["amount"])
                date = datetime.strptime(row["date"], "%Y-%m-%d")
                month = date.strftime("%Y-%m")

                if amount > 0:
                    transfers[month]["in"] += amount
                    if txn_type == "E_TRFIN":
                        incoming_etransfers.append({"date": date.date(), "amount": amount})
                else:
                    transfers[month]["out"] += abs(amount)

    aggregates = {m: {"in": round(v["in"], 2), "out": round(v["out"], 2)}
                  for m, v in transfers.items()}
    incoming_etransfers.sort(key=lambda t: t["date"], reverse=True)
    return aggregates, incoming_etransfers


def extract_bank_interest(folder: str) -> list:
    """Extract INT (interest) transactions from personal and corporate debit CSVs.

    Returns list of {"date": date, "amount": float, "account": str} sorted newest-first.
    """
    interest_txns = []

    for subdir in ["personal", "corporate"]:
        txn_dir = os.path.join(folder, "transactions", subdir)
        if not os.path.isdir(txn_dir):
            continue
        for fpath in sorted(glob.glob(os.path.join(txn_dir, "**", "*.csv"), recursive=True)):
            with open(fpath, newline="", encoding="utf-8-sig") as f:
                reader = csv.DictReader(f)
                if "transaction" not in (reader.fieldnames or []):
                    continue
                # Derive account name from parent folder
                account = os.path.basename(os.path.dirname(fpath))
                for row in reader:
                    if row["transaction"] != "INT":
                        continue
                    amount = float(row["amount"])
                    if amount <= 0:
                        continue
                    date = datetime.strptime(row["date"], "%Y-%m-%d").date()
                    interest_txns.append({"date": date, "amount": amount, "account": account})

    interest_txns.sort(key=lambda t: t["date"], reverse=True)
    return interest_txns


def extract_corporate_income(folder: str) -> dict | None:
    """Extract corporate income from corporate account CSVs.

    Reads CSVs from corporate/ subdirectory.
    - Tall Tree Technology: CONT = client revenue (positive amounts)
    - Britton Holdings (Growth): DIV = dividend income (positive amounts)
    """
    corp_dir = os.path.join(folder, "transactions", "corporate")
    if not os.path.isdir(corp_dir):
        # Backward compat: try old path
        corp_dir = os.path.join(folder, "corporate")
        if not os.path.isdir(corp_dir):
            return None

    csv_files = sorted(glob.glob(os.path.join(corp_dir, "**", "*.csv"), recursive=True))
    if not csv_files:
        return None

    revenue_monthly = defaultdict(float)
    dividends_monthly = defaultdict(float)
    first_revenue = None  # (date, amount)
    first_dividend = None  # (date, amount)
    earliest_txn_date = None

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

                dt = datetime.strptime(date_str, "%Y-%m-%d").date()
                month = dt.strftime("%Y-%m")

                if earliest_txn_date is None or dt < earliest_txn_date:
                    earliest_txn_date = dt

                if is_tall_tree and txn_type == "CONT":
                    revenue_monthly[month] += amount
                    if first_revenue is None or dt < first_revenue[0]:
                        first_revenue = (dt, amount)
                elif is_bh and txn_type == "DIV":
                    dividends_monthly[month] += amount
                    if first_dividend is None or dt < first_dividend[0]:
                        first_dividend = (dt, amount)

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
        "first_revenue": {"date": first_revenue[0], "amount": first_revenue[1]} if first_revenue else None,
        "first_dividend": {"date": first_dividend[0], "amount": first_dividend[1]} if first_dividend else None,
        "earliest_txn_date": earliest_txn_date,
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

    # Categories that are NOT subscription-like (regular spending, not services)
    # Use consolidated category names (post CATEGORY_CONSOLIDATION mapping)
    NON_SUB_CATEGORIES = {
        "Food & Dining", "Groceries", "Shopping", "Recreation", "Pets",
        "Health & Wellness", "Housing & Utilities", "Transportation", "Travel",
        "Kids & Education", "Donations",
    }

    # Merchants that look like subscriptions but aren't (consistent price, recurring use)
    NOT_SUBSCRIPTIONS = ["shine auto wash", "amazon vancouver", "openai", "cursor", "stratechery", "false creek"]

    # Known service/subscription merchant keywords (always consider these)
    KNOWN_SUB_KEYWORDS = [
        "telus", "fido", "netflix", "disney", "bell media", "sportsnet",
        "amazon prime", "mubi", "open heart", "brief media",
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
        is_excluded = any(kw in merchant.lower() for kw in NOT_SUBSCRIPTIONS)
        is_known_sub = any(kw in merchant.lower() for kw in KNOWN_SUB_KEYWORDS)
        is_non_sub_category = cat in NON_SUB_CATEGORIES

        # Decision logic
        is_subscription = False
        if is_excluded:
            pass
        elif is_known_sub:
            # Always include known services regardless of consistency
            is_subscription = True
        elif is_non_sub_category:
            # For retail/dining/grocery categories, require very tight consistency
            # and more months of evidence (catches barbershop, excludes one-off shops)
            if cv < 0.10 and len(present_months) >= 4 and avg_charges <= 1.2:
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
        # Stopped subscription (absent in last completed month)
        # Don't mark as stopped if the only missing month is the current
        # (incomplete) month — the charge may not have posted yet.
        current_month = datetime.now().strftime("%Y-%m")
        last_complete = months_set[-2] if months_set[-1] == current_month and len(months_set) > 1 else months_set[-1]
        if present_months[-1] != months_set[-1] and present_months[-1] < last_complete:
            status = "stopped"
            alerts.append(f"Last charge: {present_months[-1]}")

        subscriptions.append({
            "merchant": merchant,
            "avg": round(avg_amount, 2),
            "history": history,
            "status": status,
            "alerts": alerts,
            "months_active": len(present_months),
            "category": cat,
        })

    subscriptions.sort(key=lambda s: s["avg"], reverse=True)

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

def get_ai_recommendations(data: dict, passive_income: dict | None = None,
                           corporate_income: dict | None = None,
                           incoming_etransfers: list | None = None,
                           bank_interest: list | None = None) -> str | None:
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
            {"name": c, "total": t, "monthly_avg": a, "txn_count": n,
             "monthly": {m: round(data["category_monthly"].get(c, {}).get(m, 0), 2) for m in data["months"][-6:]}}
            for c, t, a, n in data["categories"]
        ],
        "subscriptions": [
            {"merchant": s["merchant"], "avg_monthly": s["avg"],
             "status": s["status"], "alerts": s["alerts"],
             "history": s["history"]}
            for s in data["subscriptions"]
        ],
        "fixed_costs": [
            {"merchant": m, "total": t} for m, t, _ in data.get("fixed_cost_detail", [])
        ],
        "fixed_total": data.get("fixed_total", 0),
        "discretionary_total": data.get("discretionary_total", 0),
    }

    # Passive investment income — per-account detail for portfolio-specific advice
    if passive_income:
        summary["passive_income"] = {
            "annual_income": passive_income["annual_income"],
            "annual_growth": passive_income.get("annual_growth", 0),
            "monthly_income": passive_income["monthly_income"],
            "accessible_balance": passive_income.get("accessible_balance", 0),
            "accounts": [
                {"name": a["account"], "type": a["type"],
                 "balance": a["value"], "income_annual": a["income_annual"],
                 "growth_annual": a["growth_annual"], "return_pct": a["return_pct"],
                 "strategy": a.get("strategy", ""), "brokerage": a.get("brokerage", ""),
                 "start_date": str(a["start_date"]) if a.get("start_date") else ""}
                for a in passive_income["accounts"]
            ],
            "registered_annual": passive_income.get("registered_annual", 0),
            "registered_growth": passive_income.get("registered_growth", 0),
            "registered_monthly": passive_income.get("registered_monthly", 0),
            "registered_balance": passive_income.get("registered_balance", 0),
            "registered_accounts": [
                {"name": a["account"], "type": a["type"],
                 "balance": a["value"], "income_annual": a["income_annual"],
                 "growth_annual": a["growth_annual"], "return_pct": a["return_pct"],
                 "strategy": a.get("strategy", ""), "brokerage": a.get("brokerage", ""),
                 "start_date": str(a["start_date"]) if a.get("start_date") else ""}
                for a in passive_income.get("registered_accounts", [])
            ],
            "net_worth": {
                "accessible": passive_income.get("accessible_balance", 0),
                "registered": passive_income.get("registered_balance", 0),
                "corporate": passive_income.get("corporate_balance", 0),
                "property": passive_income.get("property_balance", 0),
            },
        }

    # Corporate income
    if corporate_income:
        rev = corporate_income["revenue_monthly"]
        div = corporate_income["dividends_monthly"]
        rev_months = sorted(rev.keys())
        div_months = sorted(div.keys())

        # Trailing 3-month averages
        rev_last3 = [rev[m] for m in rev_months[-3:]] if len(rev_months) >= 3 else list(rev.values())
        div_last3 = [div[m] for m in div_months[-3:]] if len(div_months) >= 3 else list(div.values())
        rev_avg3 = round(sum(rev_last3) / len(rev_last3), 2) if rev_last3 else 0
        div_avg3 = round(sum(div_last3) / len(div_last3), 2) if div_last3 else 0

        take_home_rate = 0.60
        summary["corporate_income"] = {
            "revenue_monthly": rev,
            "dividends_monthly": div,
            "revenue_total": corporate_income["revenue_total"],
            "dividends_total": corporate_income["dividends_total"],
            "revenue_avg_last3": rev_avg3,
            "dividends_avg_last3": div_avg3,
            "take_home_rate": take_home_rate,
            "estimated_take_home_monthly": round(rev_avg3 * take_home_rate + div_avg3, 2),
        }

        # Revenue trend: latest vs prior month
        if len(rev_months) >= 2:
            latest_rev = rev[rev_months[-1]]
            prior_rev = rev[rev_months[-2]]
            if prior_rev > 0:
                decline_pct = round((prior_rev - latest_rev) / prior_rev * 100, 1)
                summary["revenue_trend"] = {
                    "latest_month": rev_months[-1],
                    "latest_revenue": latest_rev,
                    "prior_month": rev_months[-2],
                    "prior_revenue": prior_rev,
                    "change_pct": -decline_pct if latest_rev < prior_rev else round((latest_rev - prior_rev) / prior_rev * 100, 1),
                }

    # Incoming e-transfers (reimbursements)
    if incoming_etransfers:
        etransfer_in_by_month = defaultdict(float)
        for t in incoming_etransfers:
            m = str(t["date"])[:7]
            etransfer_in_by_month[m] += t["amount"]
        summary["incoming_etransfers"] = {
            "total": round(sum(t["amount"] for t in incoming_etransfers), 2),
            "count": len(incoming_etransfers),
            "monthly": {m: round(v, 2) for m, v in sorted(etransfer_in_by_month.items())},
        }

    # Bank interest income
    if bank_interest:
        bi_by_month = defaultdict(float)
        for t in bank_interest:
            m = str(t["date"])[:7]
            bi_by_month[m] += t["amount"]
        summary["bank_interest"] = {
            "total": round(sum(t["amount"] for t in bank_interest), 2),
            "count": len(bank_interest),
            "monthly": {m: round(v, 2) for m, v in sorted(bi_by_month.items())},
        }

    # Burn rate & coverage — exclude paid-off debt merchant payments
    monthly_totals = data.get("monthly_totals", {})
    monthly_txns = data.get("monthly_txns", {})
    debt_payoff_merchants = set(DEBT_PAYOFF_THRESHOLDS.keys()) if data.get("debt_payoffs") else set()
    spend_months = sorted(monthly_totals.keys())
    adjusted = {}
    for m in spend_months:
        m_total = monthly_totals.get(m, 0)
        if debt_payoff_merchants:
            debt_in_month = sum(t["amount"] for t in monthly_txns.get(m, [])
                                if t["merchant"] in debt_payoff_merchants)
            m_total -= debt_in_month
        adjusted[m] = m_total
    if len(spend_months) >= 3:
        burn_rate = round(sum(adjusted[m] for m in spend_months[-3:]) / 3, 2)
    elif spend_months:
        burn_rate = round(sum(adjusted.values()) / len(spend_months), 2)
    else:
        burn_rate = 0

    combined_monthly = 0.0
    if passive_income:
        combined_monthly += passive_income["monthly_income"]
    if corporate_income and "estimated_take_home_monthly" in summary.get("corporate_income", {}):
        combined_monthly += summary["corporate_income"]["estimated_take_home_monthly"]
    num_months_for_avg = len(data.get("months", [])) or 1
    if incoming_etransfers:
        combined_monthly += sum(t["amount"] for t in incoming_etransfers) / num_months_for_avg
    if bank_interest:
        combined_monthly += sum(t["amount"] for t in bank_interest) / num_months_for_avg

    coverage_pct = round(combined_monthly / burn_rate * 100, 1) if burn_rate > 0 else 0
    accessible_balance = passive_income.get("accessible_balance", 0) if passive_income else 0
    net_monthly_draw = max(burn_rate - combined_monthly, 0)
    runway_months = round(accessible_balance / net_monthly_draw, 1) if net_monthly_draw > 0 else None
    summary["burn_rate_coverage"] = {
        "burn_rate_monthly": burn_rate,
        "combined_monthly_income": round(combined_monthly, 2),
        "coverage_pct": coverage_pct,
        "monthly_surplus_or_gap": round(combined_monthly - burn_rate, 2),
        "accessible_savings": accessible_balance,
        "runway_months": runway_months,
    }

    # Debts already paid off during this period (no longer owed)
    debt_payoffs = data.get("debt_payoffs", [])
    if debt_payoffs:
        from collections import defaultdict as _dd2
        _payoff_by_merchant = _dd2(lambda: {"total": 0.0, "last_date": None})
        for d in debt_payoffs:
            _payoff_by_merchant[d["merchant"]]["total"] += d["amount"]
            dt = d["date"]
            prev = _payoff_by_merchant[d["merchant"]]["last_date"]
            if prev is None or dt > prev:
                _payoff_by_merchant[d["merchant"]]["last_date"] = dt
        summary["debts_paid_off"] = {
            "total_eliminated": round(sum(d["amount"] for d in debt_payoffs), 2),
            "debts": [
                {"merchant": m, "principal": round(info["total"], 2), "paid_off": str(info["last_date"])}
                for m, info in _payoff_by_merchant.items()
            ],
            "note": "These debts have already been fully paid off during this period. They are NOT outstanding balances.",
        }

    # Corporate milestones
    if corporate_income:
        milestones = {}
        if corporate_income.get("earliest_txn_date"):
            milestones["launch_date"] = str(corporate_income["earliest_txn_date"])
        if corporate_income.get("first_revenue"):
            fr = corporate_income["first_revenue"]
            milestones["first_revenue"] = {"date": str(fr["date"]), "amount": fr["amount"]}
        if corporate_income.get("first_dividend"):
            fd = corporate_income["first_dividend"]
            milestones["first_dividend"] = {"date": str(fd["date"]), "amount": fd["amount"]}
        if milestones:
            summary.setdefault("corporate_income", {})["milestones"] = milestones

    prompt = f"""Analyze this personal & corporate financial dashboard and provide actionable recommendations.

Context: This dashboard covers a self-employed consultant pursuing financial sustainability, defined as: passive income >= burn rate. Income comes from three streams: (1) passive portfolio yield from personal investments — this is the SUSTAINABLE income, (2) corporate consulting revenue (Tall Tree Technology) at ~60% take-home after tax/expenses — this is ACTIVE income that bridges the gap, and (3) corporate dividend income (Britton Holdings). The "burn_rate_coverage" section shows how much of the burn rate is covered by passive income alone — coverage_pct is passive-only. Corporate income bridges the remaining gap but is not considered sustainable. "accessible_savings" is the total balance in Non-registered, Cash, and TFSA accounts that can be drawn without tax penalty; "runway_months" shows how long savings last if all income stopped (null if passive income already covers expenses). Revenue trend shows month-over-month changes in consulting income. "debts_paid_off" lists debts that were fully eliminated during this period (with per-debt principal and payoff dates) — these are no longer owed and should be celebrated, not treated as outstanding obligations. "corporate_income.milestones" shows the corporate journey timeline (launch date, first revenue, first dividend). The spending data includes fixed costs (tuition, car payment, utilities) and discretionary spending across credit and debit cards. The "passive_income.accounts" array contains per-account detail (name, type, balance, annual_yield, return_pct) for accessible accounts and RRSP accounts — use this to identify underperforming or overconcentrated positions. The "passive_income.net_worth" object shows the full balance breakdown across accessible, RRSP, corporate, property, and RESP holdings. Each category includes a "monthly" object with per-month spending for the last 6 months — use this to spot categories trending up or down.

DATA:
{json.dumps(summary, indent=2)}

Provide a MAXIMUM of 5 recommendations — no more than 5. Each should be specific, actionable, and reference actual numbers and merchant names from the data. Prioritize the most impactful insights from:
- Sustainability gap (passive income vs burn rate — what would close the gap: higher yield, lower burn, or both)
- Corporate bridge risk (revenue trend, client concentration — what happens if this bridge narrows)
- Portfolio income observations (per-account yields, underperforming accounts, rebalancing opportunities, RRSP vs accessible allocation)
- Net worth composition (concentration risk, liquidity, growth vs income allocation)
- Category spending trends (categories trending up or down over recent months)
- Corporate tax optimization (take-home rate, dividend timing, reinvesting to grow passive income)
- Fixed cost optimization (insurance, utilities, recurring debits)
- Subscription cost-saving actions (price increases to negotiate, services to cancel/downgrade)
- Spending pattern optimizations (consolidation, alternatives)

Format your response in clean HTML as a single <ol> list with at most 5 <li> items. Use <strong> for emphasis on merchant names and dollar amounts. Be concise — one short paragraph per recommendation.

IMPORTANT: On each <li>, include a data-sections attribute containing a comma-separated list of dashboard section IDs that the recommendation relates to. Use ONLY these IDs:
- subscriptions — Subscription Audit
- categories — Category Heatmap / spending categories
- fixed-discretionary — Fixed vs Discretionary costs
- corporate-income — Corporate Income
- passive-income — Investment Portfolio
- interac-transfers — Outgoing e-Transfers
- incoming-etransfers — Incoming e-Transfers
- bank-interest — Bank Interest
- debt-freedom — Debt Freedom

Example: <li data-sections="subscriptions,categories">Cancel Netflix...</li>"""

    payload = json.dumps({
        "model": "claude-sonnet-4-6",
        "max_tokens": 3000,
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
                   corporate_income: dict | None = None,
                   incoming_etransfers: list | None = None,
                   bank_interest: list | None = None,
                   folder: str = ".") -> str:
    notes = notes or {}
    budgets = budgets or {}
    months = data["months"][-12:]  # Cap display window to last 12 months
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
    source_breakdown = data.get("source_breakdown", {})

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

    # Adjusted totals — exclude paid-off debt merchant payments consistently
    debt_merchants = set(DEBT_PAYOFF_THRESHOLDS.keys()) if debt_payoffs else set()
    adjusted_monthly = {}
    for m in months:
        m_total = data["monthly_totals"].get(m, 0)
        if debt_merchants:
            debt_in_month = sum(t["amount"] for t in data["monthly_txns"].get(m, [])
                                if t["merchant"] in debt_merchants)
            m_total -= debt_in_month
        adjusted_monthly[m] = m_total

    # Apply 2% VISA cash-back reduction to credit card spend
    CASHBACK_RATE = 0.02
    credit_by_month = source_breakdown.get("credit", {})
    cashback_monthly = {m: round(credit_by_month.get(m, 0) * CASHBACK_RATE, 2) for m in months}
    cashback_total = sum(cashback_monthly.values())
    for m in months:
        adjusted_monthly[m] -= cashback_monthly[m]

    adjusted_total = sum(adjusted_monthly.values())
    adjusted_avg = adjusted_total / len(months) if months else 0

    # Burn rate — recent 3-month trailing average
    recent_months = months[-3:]
    burn_rate = sum(adjusted_monthly[m] for m in recent_months) / len(recent_months) if recent_months else 0

    # ── Build table rows ──

    # Sub months (last 6 only) and headers
    sub_months = months[-6:]
    sub_month_headers = "".join(f"<th style='text-align:right'>{datetime.strptime(m, '%Y-%m').strftime('%b %Y')}</th>" for m in sub_months)

    # Subscription table rows — grouped by status
    sub_by_status = defaultdict(list)
    for s in data["subscriptions"]:
        sub_by_status[s["status"]].append(s)

    status_order = ["new", "price_change", "stopped", "stable"]
    status_labels = {"stable": "Stable", "price_change": "Price Change", "new": "New", "stopped": "Stopped"}

    sub_rows = ""
    total_monthly = sum(s["avg"] for s in data["subscriptions"])
    for status in status_order:
        subs = sub_by_status.get(status, [])
        if not subs:
            continue
        group_total = sum(s["avg"] for s in subs)
        num_cols = len(sub_months) + 2  # Service + Avg + months
        label = status_labels.get(status, status.title())
        sub_rows += f'<tr style="background:var(--bg);font-weight:600"><td colspan="{num_cols}">{status_badge(status)} {label} — {money(group_total)}/mo ({len(subs)})</td></tr>'
        for s in subs:
            month_cells = ""
            for m in sub_months:
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
        </tr>"""

    # Interac e-Transfer detail table (grouped by month, sorted by date)
    etransfer_txns = sorted(
        [t for txns in data["monthly_txns"].values() for t in txns if t["merchant"] == "Interac e-Transfer"],
        key=lambda t: t["date"], reverse=True
    )
    etransfer_total = sum(t["amount"] for t in etransfer_txns)
    # Load e-transfer annotations (date+amount -> note)
    etransfer_notes = {}
    notes_path = os.path.join(folder, "etransfer-notes.csv")
    if os.path.exists(notes_path):
        with open(notes_path, newline="") as f:
            for row in csv.DictReader(f):
                amt = row["amount"].replace("$", "").replace(",", "")
                key = (row["date"], amt)
                if row.get("note", "").strip():
                    etransfer_notes[key] = row["note"].strip()
    etransfer_by_month = {}
    for t in etransfer_txns:
        m = str(t["date"])[:7]
        etransfer_by_month.setdefault(m, []).append(t)
    etransfer_rows = ""
    for m in sorted(etransfer_by_month, reverse=True):
        txns = etransfer_by_month[m]
        month_label = datetime.strptime(m, "%Y-%m").strftime("%b %Y")
        month_total = sum(t["amount"] for t in txns)
        etransfer_rows += f'<tr style="background:var(--bg);font-weight:600"><td colspan="2">{month_label}</td><td style="text-align:right">{money(month_total)}</td></tr>'
        for t in txns:
            date_str = str(t["date"])[:10]
            amt_str = f'{t["amount"]:.2f}'
            note = etransfer_notes.get((date_str, amt_str), "")
            note_html = f'<span style="color:var(--muted);font-style:italic">{note}</span>' if note else ""
            etransfer_rows += f'<tr><td>{date_str}</td><td>{note_html}</td><td style="text-align:right">{money(t["amount"])}</td></tr>'

    # Category heatmap (last 6 months)
    has_budgets = bool(budgets)
    heatmap_months = months[-6:]
    heatmap_month_headers = "".join(
        f"<th style='text-align:right'>{datetime.strptime(m, '%Y-%m').strftime('%b')}</th>"
        for m in heatmap_months
    )
    # Compute global max for single heatmap scale across all cells
    heatmap_global_max = 0
    for c, t, a, n in data["categories"]:
        for m in heatmap_months:
            val = data["category_monthly"].get(c, {}).get(m, 0)
            if val > heatmap_global_max:
                heatmap_global_max = val
    heatmap_row_data = []
    for c, t, a, n in data["categories"]:
        monthly_vals = [data["category_monthly"].get(c, {}).get(m, 0) for m in heatmap_months]
        cat_total = sum(monthly_vals)
        cat_avg = cat_total / len(monthly_vals) if monthly_vals else 0
        cells = ""
        for val in monthly_vals:
            intensity = (val / heatmap_global_max) if heatmap_global_max > 0 else 0
            bg = f"rgba(78, 121, 167, {intensity:.2f})"
            text_color = "#fff" if intensity > 0.5 else "var(--text)"
            cell_text = money(val) if val > 0 else '<span style="color:#ccc">\u2014</span>'
            cells += f"<td style='text-align:right;background:{bg};color:{text_color}'>{cell_text}</td>"
        avg_cell = f"<td style='text-align:right;font-weight:600'>{money(cat_avg)}</td>"
        total_cell = f"<td style='text-align:right;font-weight:600'>{money(cat_total)}</td>"
        heatmap_row_data.append((cat_total, f"<tr><td>{c}</td>{cells}{avg_cell}{total_cell}</tr>"))
    heatmap_row_data.sort(key=lambda x: x[0], reverse=True)
    heatmap_rows = "".join(row for _, row in heatmap_row_data)

    # ── Monthly Spotlight data prep ──
    current_month = datetime.now().strftime("%Y-%m")
    # Pick the most recent month with data (max statement date)
    spotlight_html = ""
    spot_month = months[-1] if months else None
    if spot_month:
        is_partial = spot_month == current_month
        spot_label = datetime.strptime(spot_month, "%Y-%m").strftime("%B %Y")
        if is_partial:
            spot_label += " (in progress)"
        spot_total = data["monthly_totals"].get(spot_month, 0)

        # Delta vs prior month
        spot_idx = months.index(spot_month) if spot_month in months else -1
        prior_month = months[spot_idx - 1] if spot_idx > 0 else None
        prior_total = data["monthly_totals"].get(prior_month, 0) if prior_month else 0
        delta_prior = spot_total - prior_total if prior_month else 0
        delta_prior_pct = (delta_prior / prior_total * 100) if prior_total > 0 else 0

        # Delta vs 3-month average (use months before the spotlight month)
        prev_months = months[:spot_idx] if spot_idx > 0 else []
        avg_months = prev_months[-3:] if len(prev_months) >= 3 else prev_months
        avg_3mo = sum(data["monthly_totals"].get(m, 0) for m in avg_months) / len(avg_months) if avg_months else 0
        delta_avg = spot_total - avg_3mo if avg_3mo > 0 else 0
        delta_avg_pct = (delta_avg / avg_3mo * 100) if avg_3mo > 0 else 0

        def delta_badge(val, pct):
            """Green for decrease (good), red for increase (bad)."""
            if val > 0:
                color = "#e15759"
                arrow = "\u2191"
            elif val < 0:
                color = "#27ae60"
                arrow = "\u2193"
            else:
                return '<span style="color:var(--muted)">\u2014</span>'
            return (f'<span style="color:{color};font-weight:600">{arrow} {money(abs(val))} '
                    f'({abs(pct):.0f}%)</span>')

        # Top 5 categories for spotlight month
        spot_cats = []
        for c, _, _, _ in data["categories"]:
            val = data["category_monthly"].get(c, {}).get(spot_month, 0)
            prior_val = data["category_monthly"].get(c, {}).get(prior_month, 0) if prior_month else 0
            if val > 0:
                spot_cats.append((c, val, val - prior_val))
        spot_cats.sort(key=lambda x: x[1], reverse=True)
        spot_cats = spot_cats[:5]

        top_cats_rows = ""
        for cat_name, cat_val, cat_delta in spot_cats:
            mom_cell = '<td style="text-align:right;color:var(--muted)">&mdash;</td>'
            if prior_month and cat_delta != 0:
                d_color = "#e15759" if cat_delta > 0 else "#27ae60"
                d_arrow = "\u2191" if cat_delta > 0 else "\u2193"
                mom_cell = f'<td style="text-align:right"><span style="color:{d_color};font-size:0.85em">{d_arrow} {money(abs(cat_delta))}</span></td>'
            budget_cell = ""
            if has_budgets:
                target = budgets.get(cat_name)
                no_budget = '<span style="color:#ccc">\u2014</span>'
                budget_cell = f"<td>{budget_bar(cat_val, target) if target else no_budget}</td>"
            top_cats_rows += f"<tr><td>{cat_name}</td><td style='text-align:right'>{money(cat_val)}</td>{mom_cell}{budget_cell}</tr>"

        # Top 5 biggest transactions for spotlight month
        spot_txns = sorted(data["monthly_txns"].get(spot_month, []), key=lambda t: t["amount"], reverse=True)[:5]
        top_txn_rows = ""
        for t in spot_txns:
            top_txn_rows += f"<tr><td>{t['merchant']}</td><td style='text-align:center'>{t['date'].strftime('%b %d')}</td><td style='text-align:right'>{money(t['amount'])}</td></tr>"

        spotlight_html = f"""
<section class="card" style="margin-bottom:20px">
    <h2>Monthly Spotlight: {spot_label}</h2>
    <p style="color:var(--muted);font-style:italic;margin-bottom:15px">Your most recent month at a glance — top categories and biggest transactions vs prior month.</p>
    <div style="display:grid;grid-template-columns:repeat(auto-fit, minmax(200px, 1fr));gap:15px;margin-bottom:20px">
        <div class="stat"><div class="value">{money(spot_total)}</div><div class="label">Total Spend</div></div>
        <div class="stat"><div class="value">{delta_badge(delta_prior, delta_prior_pct)}</div><div class="label">vs Prior Month</div></div>
        <div class="stat"><div class="value">{delta_badge(delta_avg, delta_avg_pct)}</div><div class="label">vs 3-Month Avg</div></div>
    </div>
    <div class="chart-row">
        <div>
            <h3 style="font-size:1em;margin-bottom:10px;color:var(--accent)">Top Categories</h3>
            <table class="data-table">
                <thead><tr><th>Category</th><th style="text-align:right">Amount</th><th style="text-align:right">MoM</th>{'<th>vs Budget</th>' if has_budgets else ''}</tr></thead>
                <tbody>{top_cats_rows}</tbody>
            </table>
        </div>
        <div>
            <h3 style="font-size:1em;margin-bottom:10px;color:var(--accent)">Biggest Transactions</h3>
            <table class="data-table">
                <thead><tr><th>Merchant</th><th style="text-align:center">Date</th><th style="text-align:right">Amount</th></tr></thead>
                <tbody>{top_txn_rows}</tbody>
            </table>
        </div>
    </div>
</section>"""

    # Trend indicator
    trend_arrow = "\u2191" if data["mom_change"] > 0 else "\u2193" if data["mom_change"] < 0 else "\u2192"
    trend_color = "#e74c3c" if data["mom_change"] > 5 else "#27ae60" if data["mom_change"] < -5 else "#f39c12"

    # Fixed costs table rows
    fixed_rows = ""
    num_months = len(months)
    for merchant, total_amt, by_month in fixed_detail:
        monthly_avg = total_amt / num_months
        fixed_rows += f"<tr><td>{merchant}</td><td style='text-align:right'>{money(total_amt)}</td><td style='text-align:right'>{money(monthly_avg)}</td></tr>"

    # Fixed vs discretionary per-month data (last 6 months) for stacked bar chart
    fixed_disc_months = months[-6:]
    fixed_disc_labels = json.dumps([datetime.strptime(m, "%Y-%m").strftime("%b") for m in fixed_disc_months])
    fixed_per_month = []
    disc_per_month = []
    for m in fixed_disc_months:
        fixed_m = data.get("fixed_costs", {}).get(m, 0)
        total_m = data["monthly_totals"].get(m, 0)
        disc_m = total_m - fixed_m
        fixed_per_month.append(round(fixed_m, 2))
        disc_per_month.append(round(max(disc_m, 0), 2))
    fixed_per_month_json = json.dumps(fixed_per_month)
    disc_per_month_json = json.dumps(disc_per_month)

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
    registered_monthly = passive_income["registered_monthly"] if passive_income else 0
    registered_annual = passive_income["registered_annual"] if passive_income else 0

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

    # Other actual income — monthly averages over the reporting period
    num_months_total = len(months) or 1
    etransfer_in_monthly_avg = round(sum(t["amount"] for t in (incoming_etransfers or [])) / num_months_total, 2)
    bank_interest_monthly_avg = round(sum(t["amount"] for t in (bank_interest or [])) / num_months_total, 2)
    other_income_monthly = etransfer_in_monthly_avg + bank_interest_monthly_avg

    combined_monthly = monthly_passive + corp_monthly_takehome + other_income_monthly
    has_income = passive_income or corporate_income or incoming_etransfers or bank_interest


    # Combined sustainability metrics (passive + corporate income vs burn rate)
    if combined_monthly > 0 and burn_rate > 0:
        coverage_pct = combined_monthly / burn_rate * 100
        sustainability_gap = combined_monthly - burn_rate
        if coverage_pct >= 100:
            coverage_color = "#27ae60"
            coverage_label = f"Surplus: {money(sustainability_gap)}/mo"
        elif coverage_pct >= 50:
            coverage_color = "#f39c12"
            coverage_label = f"Gap: {money(abs(sustainability_gap))}/mo to sustainability"
        else:
            coverage_color = "#e74c3c"
            coverage_label = f"Gap: {money(abs(sustainability_gap))}/mo to sustainability"
    else:
        coverage_pct = 0
        coverage_color = "#95a5a6"
        coverage_label = ""

    # Hero card: passive income vs burn rate
    hero_card = ""
    accessible_balance = passive_income.get("accessible_balance", 0) if passive_income else 0
    if has_income:
        bar_fill = min(coverage_pct, 100)
        # Savings runway line
        savings_line = ""
        if accessible_balance > 0 and burn_rate > 0:
            net_draw = max(burn_rate - combined_monthly, 0)
            if net_draw > 0:
                runway = accessible_balance / net_draw
                savings_line = f'<div style="font-size:0.85em;color:var(--muted);margin-top:4px">Accessible savings: {money(accessible_balance)} &middot; {runway:.0f} months runway</div>'
            else:
                savings_line = f'<div style="font-size:0.85em;color:var(--muted);margin-top:4px">Accessible savings: {money(accessible_balance)}</div>'
        other_income_block = ""
        if other_income_monthly > 0:
            # Build subtitle showing breakdown
            other_parts = []
            if etransfer_in_monthly_avg > 0:
                other_parts.append(f"e-transfers {money(etransfer_in_monthly_avg)}")
            if bank_interest_monthly_avg > 0:
                other_parts.append(f"interest {money(bank_interest_monthly_avg)}")
            other_subtitle = " + ".join(other_parts)
            other_income_block = f"""
            <div style="flex:0 0 40px;text-align:center">
                <div style="font-size:1.8em;color:var(--muted)">+</div>
            </div>
            <div style="flex:1;min-width:160px;text-align:center">
                <div style="font-size:0.85em;color:var(--muted);text-transform:uppercase;letter-spacing:0.5px;margin-bottom:4px">Other Income</div>
                <div style="font-size:2.2em;font-weight:700;color:#27ae60">{money(other_income_monthly)}<span style="font-size:0.4em;font-weight:400;color:var(--muted)">/mo</span></div>
                <div style="font-size:0.85em;color:var(--muted)">{other_subtitle}</div>
            </div>"""
        hero_card = f"""
    <div class="card" style="margin-bottom:20px">
        <div style="display:flex;align-items:center;justify-content:space-between;flex-wrap:wrap;gap:20px">
            <div style="flex:1;min-width:160px">
                <div style="font-size:0.85em;color:var(--muted);text-transform:uppercase;letter-spacing:0.5px;margin-bottom:4px">Income</div>
                <div style="font-size:2.2em;font-weight:700;color:#27ae60">{money(corp_monthly_takehome)}<span style="font-size:0.4em;font-weight:400;color:var(--muted)">/mo</span></div>
                <div style="font-size:0.85em;color:var(--muted)">corporate take-home</div>
            </div>
            <div style="flex:0 0 40px;text-align:center">
                <div style="font-size:1.8em;color:var(--muted)">+</div>
            </div>
            <div style="flex:1;min-width:160px;text-align:center">
                <div style="font-size:0.85em;color:var(--muted);text-transform:uppercase;letter-spacing:0.5px;margin-bottom:4px">Passive Income</div>
                <div style="font-size:2.2em;font-weight:700;color:#27ae60">{money(monthly_passive)}<span style="font-size:0.4em;font-weight:400;color:var(--muted)">/mo</span></div>
                <div style="font-size:0.85em;color:var(--muted)">portfolio yield</div>
            </div>
            {other_income_block}
            <div style="flex:0 0 40px;text-align:center">
                <div style="font-size:1.8em;color:var(--muted)">vs</div>
            </div>
            <div style="flex:1;min-width:160px;text-align:right">
                <div style="font-size:0.85em;color:var(--muted);text-transform:uppercase;letter-spacing:0.5px;margin-bottom:4px">Burn Rate</div>
                <div style="font-size:2.2em;font-weight:700;color:#e15759">{money(burn_rate)}<span style="font-size:0.4em;font-weight:400;color:var(--muted)">/mo</span></div>
                <div style="font-size:0.85em;color:var(--muted)">3-month trailing avg (net of 2% cash-back)</div>
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
            {savings_line}
        </div>
    </div>"""

    # ── Sustainability Projection ──
    sustainability_card = ""
    sustainability_chart_js = ""
    if passive_income and accessible_balance > 0 and burn_rate > 0:
        annual_income_proj = passive_income["annual_income"]
        annual_growth_proj = passive_income.get("annual_growth", 0)
        annual_yield_rate = annual_income_proj / accessible_balance if accessible_balance else 0
        annual_total_return_rate = (annual_income_proj + annual_growth_proj) / accessible_balance if accessible_balance else 0
        monthly_yield_rate = annual_yield_rate / 12
        monthly_total_return_rate = annual_total_return_rate / 12
        total_monthly_income = corp_monthly_takehome + monthly_passive + other_income_monthly

        proj_balance = accessible_balance
        proj_labels = []
        proj_passive = []
        proj_burn = []
        crossover_month = None
        already_sustainable = (monthly_passive >= burn_rate)
        now = datetime.now()
        max_months = 120

        for i in range(max_months):
            m_date = datetime(now.year, now.month, 1) + timedelta(days=32 * i)
            m_date = m_date.replace(day=1)
            proj_labels.append(m_date.strftime("%b %Y"))
            passive_this_month = proj_balance * monthly_yield_rate
            proj_passive.append(round(passive_this_month, 2))
            proj_burn.append(round(burn_rate, 2))
            if crossover_month is None and passive_this_month >= burn_rate and i > 0:
                crossover_month = i
            net_savings = max((corp_monthly_takehome + passive_this_month + other_income_monthly) - burn_rate, 0)
            proj_balance = proj_balance * (1 + monthly_total_return_rate) + net_savings
            if crossover_month is not None and i >= crossover_month + 12:
                break

        # Build summary line
        if already_sustainable:
            summary_html = '<div style="font-size:1.1em;font-weight:600;color:#27ae60;margin:10px 0">You\'re already sustainable! Passive income covers your burn rate.</div>'
        elif crossover_month is not None:
            cross_date = datetime(now.year, now.month, 1) + timedelta(days=32 * crossover_month)
            cross_date = cross_date.replace(day=1)
            years = crossover_month // 12
            mos = crossover_month % 12
            time_str = ""
            if years > 0:
                time_str += f"{years}y "
            time_str += f"{mos}m"
            summary_html = f'<div style="font-size:1.1em;font-weight:600;color:#27ae60;margin:10px 0">Sustainability projected in {time_str} ({cross_date.strftime("%b %Y")})</div>'
        else:
            summary_html = '<div style="font-size:1.1em;font-weight:600;color:#e74c3c;margin:10px 0">Not projected within 10 years at current rates</div>'

        proj_labels_json = json.dumps(proj_labels)
        proj_passive_json = json.dumps(proj_passive)
        proj_burn_json = json.dumps(proj_burn)

        # Point radius array: large green dot at crossover
        point_radius = [0] * len(proj_passive)
        point_bg = ["#27ae60"] * len(proj_passive)
        if crossover_month is not None and crossover_month < len(point_radius):
            point_radius[crossover_month] = 8
        point_radius_json = json.dumps(point_radius)
        point_bg_json = json.dumps(point_bg)

        sustainability_card = f"""
    <div class="card" style="margin-bottom:20px">
        <h2>Sustainability Projection</h2>
        <p style="color:var(--muted);font-style:italic;margin-bottom:10px">Forward projection assuming {annual_yield_rate*100:.1f}% yield, {annual_total_return_rate*100:.1f}% total return, and ${burn_rate:,.0f}/mo burn rate.</p>
        {summary_html}
        <div class="chart-container">
            <canvas id="sustainabilityChart" height="100"></canvas>
        </div>
    </div>"""

        sustainability_chart_js = f"""
    new Chart(document.getElementById('sustainabilityChart'), {{
        type: 'line',
        data: {{
            labels: {proj_labels_json},
            datasets: [
                {{
                    label: 'Passive Income',
                    data: {proj_passive_json},
                    borderColor: '#27ae60',
                    backgroundColor: 'rgba(39, 174, 96, 0.1)',
                    fill: true,
                    tension: 0.3,
                    pointRadius: {point_radius_json},
                    pointBackgroundColor: {point_bg_json},
                    pointBorderColor: '#27ae60',
                    borderWidth: 2
                }},
                {{
                    label: 'Burn Rate',
                    data: {proj_burn_json},
                    borderColor: '#e74c3c',
                    borderDash: [6, 4],
                    fill: false,
                    pointRadius: 0,
                    borderWidth: 2
                }}
            ]
        }},
        options: {{
            responsive: true,
            plugins: {{
                legend: {{ position: 'bottom' }},
                tooltip: {{
                    callbacks: {{
                        label: function(ctx) {{
                            var val = '$' + (ctx.parsed.y / 1000).toFixed(1) + 'k';
                            return ctx.dataset.label + ': ' + val;
                        }},
                        afterBody: function(items) {{
                            if (items.length >= 2) {{
                                var gap = items[0].parsed.y - items[1].parsed.y;
                                var sign = gap >= 0 ? '+' : '';
                                return 'Gap: ' + sign + '$' + (gap / 1000).toFixed(1) + 'k';
                            }}
                        }}
                    }}
                }}
            }},
            scales: {{
                x: {{
                    ticks: {{
                        maxTicksLimit: 12,
                        maxRotation: 45
                    }}
                }},
                y: {{
                    beginAtZero: true,
                    ticks: {{
                        callback: function(v) {{ return '$' + (v / 1000).toFixed(1) + 'k'; }}
                    }}
                }}
            }}
        }}
    }});"""

    # ── Net Worth card ──
    net_worth_card = ""
    if passive_income:
        nw_accessible = passive_income.get("accessible_balance", 0)
        nw_registered = passive_income.get("registered_balance", 0)
        nw_property = passive_income.get("property_balance", 0)
        nw_corporate = passive_income.get("corporate_balance", 0)
        nw_total = nw_accessible + nw_registered + nw_property + nw_corporate

        def fmt_compact(val):
            if val >= 1_000_000:
                return f"${val/1_000_000:.2f}M"
            elif val >= 1_000:
                return f"${val/1_000:.0f}K"
            else:
                return money(val)

        nw_metrics = f"""
            <div style="flex:1;min-width:120px;text-align:center">
                <div style="font-size:0.78em;color:var(--muted);text-transform:uppercase;letter-spacing:0.5px">Accessible</div>
                <div style="font-size:1.4em;font-weight:600">{fmt_compact(nw_accessible)}</div>
            </div>
            <div style="flex:1;min-width:120px;text-align:center">
                <div style="font-size:0.78em;color:var(--muted);text-transform:uppercase;letter-spacing:0.5px">Registered</div>
                <div style="font-size:1.4em;font-weight:600">{fmt_compact(nw_registered)}</div>
            </div>"""
        if nw_property > 0:
            nw_metrics += f"""
            <div style="flex:1;min-width:120px;text-align:center">
                <div style="font-size:0.78em;color:var(--muted);text-transform:uppercase;letter-spacing:0.5px">Property</div>
                <div style="font-size:1.4em;font-weight:600">{fmt_compact(nw_property)}</div>
            </div>"""
        if nw_corporate > 0:
            nw_metrics += f"""
            <div style="flex:1;min-width:120px;text-align:center">
                <div style="font-size:0.78em;color:var(--muted);text-transform:uppercase;letter-spacing:0.5px">Corporate</div>
                <div style="font-size:1.4em;font-weight:600">{fmt_compact(nw_corporate)}</div>
            </div>"""
        nw_metrics += f"""
            <div style="flex:1;min-width:120px;text-align:center">
                <div style="font-size:0.78em;color:var(--accent);text-transform:uppercase;letter-spacing:0.5px;font-weight:600">Total</div>
                <div style="font-size:1.6em;font-weight:700;color:var(--accent)">{fmt_compact(nw_total)}</div>
            </div>"""

        # Stacked bar segments
        nw_segments = []
        nw_colors = {
            "Accessible": "#4e79a7",
            "Registered": "#76b7b2",
            "Property": "#f28e2b",
            "Corporate": "#59a14f",
        }
        for label, val, color in [
            ("Accessible", nw_accessible, nw_colors["Accessible"]),
            ("Registered", nw_registered, nw_colors["Registered"]),
            ("Property", nw_property, nw_colors["Property"]),
            ("Corporate", nw_corporate, nw_colors["Corporate"]),
        ]:
            if val > 0 and nw_total > 0:
                pct = val / nw_total * 100
                nw_segments.append(
                    f'<div style="width:{pct:.1f}%;background:{color};height:100%;display:inline-block" '
                    f'title="{label}: {fmt_compact(val)} ({pct:.0f}%)"></div>'
                )
        nw_bar_html = "".join(nw_segments)

        # Legend
        nw_legend_items = []
        for label, val, color in [
            ("Accessible", nw_accessible, nw_colors["Accessible"]),
            ("Registered", nw_registered, nw_colors["Registered"]),
            ("Property", nw_property, nw_colors["Property"]),
            ("Corporate", nw_corporate, nw_colors["Corporate"]),
        ]:
            if val > 0:
                nw_legend_items.append(
                    f'<span style="display:inline-flex;align-items:center;gap:4px;margin-right:14px">'
                    f'<span style="width:10px;height:10px;border-radius:2px;background:{color};display:inline-block"></span>'
                    f'<span style="font-size:0.8em;color:var(--muted)">{label}</span></span>'
                )
        nw_legend = "".join(nw_legend_items)

        net_worth_card = f"""
    <div class="card" style="margin-bottom:20px">
        <h2 style="margin-bottom:15px">Net Worth</h2>
        <div style="display:flex;align-items:center;justify-content:space-around;flex-wrap:wrap;gap:10px;margin-bottom:18px">
            {nw_metrics}
        </div>
        <div style="background:#eee;border-radius:6px;height:18px;overflow:hidden;font-size:0;line-height:0;white-space:nowrap">
            {nw_bar_html}
        </div>
        <div style="margin-top:8px;text-align:center">{nw_legend}</div>
    </div>"""

    # ── Overview stats ──
    overview_stats = f"""
    <div class="stat"><div class="value">{money(adjusted_total)}</div><div class="label">Total Spend ({len(months)} months)</div></div>
    <div class="stat"><div class="value">{money(adjusted_avg)}</div><div class="label">Monthly Average</div></div>
    <div class="stat"><div class="value" style="color:{trend_color}">{trend_arrow} {abs(data['mom_change']):.0f}%</div><div class="label">3-Mo Avg vs Prior 3-Mo</div></div>"""
    if debt_payoff_total > 0:
        overview_stats += f"""
    <div class="stat"><div class="value" style="color:#27ae60">{money(debt_payoff_total)}</div><div class="label">Debt Paid Off</div></div>"""

    if cashback_total > 0:
        overview_stats += f"""
    <div class="stat"><div class="value" style="color:#27ae60">{money(cashback_total)}</div><div class="label">VISA Cash-Back ({len(months)} months)</div></div>"""

    # ── Total income stat (actual cash received) ──
    total_income_actual = 0.0
    if corporate_income:
        total_income_actual += corporate_income["total_income"]
    if incoming_etransfers:
        total_income_actual += sum(t["amount"] for t in incoming_etransfers)
    if bank_interest:
        total_income_actual += sum(t["amount"] for t in bank_interest)
    if total_income_actual > 0:
        income_num_months = len(months) or 1
        income_monthly_avg = total_income_actual / income_num_months
        overview_stats += f"""
    <div class="stat"><div class="value" style="color:#27ae60">{money(total_income_actual)}</div><div class="label">Total Income ({len(months)} months)</div></div>
    <div class="stat"><div class="value" style="color:#27ae60">{money(income_monthly_avg)}</div><div class="label">Avg Monthly Income</div></div>"""

    # ── Milestones Timeline (unified) ──
    timeline_events = []  # list of (date, icon, title, detail, color)

    # Debt payoff events
    if debt_payoffs:
        from collections import defaultdict as _dd
        payoff_by_merchant = _dd(lambda: {"total": 0.0, "last_date": None})
        for d in debt_payoffs:
            payoff_by_merchant[d["merchant"]]["total"] += d["amount"]
            dt = d["date"]
            prev = payoff_by_merchant[d["merchant"]]["last_date"]
            if prev is None or dt > prev:
                payoff_by_merchant[d["merchant"]]["last_date"] = dt
        for merchant, info in payoff_by_merchant.items():
            rate = INTEREST_RATES.get(merchant, 0)
            annual_saved = info["total"] * rate
            detail = f"{money(info['total'])} principal eliminated"
            if annual_saved > 0:
                detail += f" &mdash; saving {money(annual_saved)}/yr in interest"
            timeline_events.append((info["last_date"], "\u2705", f"{merchant} Paid Off", detail, "#27ae60"))

    # Corporate milestones
    if corporate_income:
        earliest = corporate_income.get("earliest_txn_date")
        first_rev = corporate_income.get("first_revenue")
        first_div = corporate_income.get("first_dividend")
        if earliest:
            timeline_events.append((earliest, "\U0001f3e2", "Corporate Ventures Launch", "Tall Tree Technology &amp; Britton Holdings accounts opened", "#4e79a7"))
        if first_rev:
            timeline_events.append((first_rev["date"], "\U0001f4b5", "First Tall Tree Revenue", f"First client payment received &mdash; {money(first_rev['amount'])}", "#27ae60"))
        if first_div:
            timeline_events.append((first_div["date"], "\U0001f4c8", "First Corporate Dividend", f"First investment dividend from Britton Holdings &mdash; {money(first_div['amount'])}", "#4e79a7"))

    # Passive income milestone
    if passive_income and monthly_passive > 0:
        timeline_events.append((datetime.now().date(), "\U0001f33f", "Portfolio Yielding Passive Income", f"{money(monthly_passive)}/mo from {passive_income.get('account_count', 0)} accounts ({money(annual_passive)}/yr)", "#27ae60"))

    # Sustainability milestone (if already sustainable)
    if passive_income and burn_rate > 0 and combined_monthly >= burn_rate:
        timeline_events.append((datetime.now().date(), "\u2b50", "Sustainability Achieved", f"Combined income ({money(combined_monthly)}/mo) covers burn rate ({money(burn_rate)}/mo)", "#f28e2b"))

    def _to_date(d):
        return d.date() if isinstance(d, datetime) else d
    timeline_events.sort(key=lambda e: _to_date(e[0]))

    milestones_section = ""
    if timeline_events:
        # Group events by quarter
        from collections import OrderedDict
        quarters = OrderedDict()
        for date_val, icon, title, detail, color in timeline_events:
            d = _to_date(date_val)
            h = 1 if d.month <= 6 else 2
            h_key = (d.year, h)
            quarters.setdefault(h_key, []).append((date_val, icon, title, detail, color))

        timeline_rows = ""
        for (year, h), events in quarters.items():
            q_label = f"H{h} {year}"
            timeline_rows += f"""
            <tr>
                <td colspan="3" style="padding:20px 16px 8px;font-weight:700;font-size:0.95em;color:var(--accent);border-bottom:2px solid var(--accent);letter-spacing:0.3px">{q_label}</td>
            </tr>"""
            for date_val, icon, title, detail, color in events:
                date_str = date_val.strftime("%b %d") if hasattr(date_val, 'strftime') else str(date_val)
                timeline_rows += f"""
            <tr>
                <td style="white-space:nowrap;color:var(--muted);font-size:0.9em;padding:12px 20px 12px 28px;vertical-align:top">{date_str}</td>
                <td style="font-size:1.3em;padding:12px 12px;vertical-align:top;text-align:center">{icon}</td>
                <td style="padding:12px 16px 12px 8px">
                    <div style="font-weight:600;color:{color};font-size:1.0em">{title}</div>
                    <div style="color:var(--muted);font-size:0.88em;margin-top:2px">{detail}</div>
                </td>
            </tr>"""

        # Summary stats
        summary_parts = []
        if debt_payoffs:
            monthly_saved = annual_interest_saved / 12
            summary_parts.append(f"{money(debt_payoff_total)} debt eliminated &mdash; saving {money(annual_interest_saved)}/yr ({money(monthly_saved)}/mo) in interest")

        summary_html = ""
        if summary_parts:
            summary_html = '<p style="color:var(--muted);font-style:italic;margin-bottom:18px">' + ". ".join(summary_parts) + ".</p>"

        milestones_section = f"""
<section class="card">
    <h2>Timeline</h2>
    {summary_html}
    <table style="width:100%;border-collapse:separate;border-spacing:0">
        <tbody>{timeline_rows}</tbody>
    </table>
</section>"""

    # ── Fixed vs Discretionary section ──
    fixed_section = ""
    if fixed_detail:
        fixed_section = f"""
<section id="fixed-discretionary" class="card">
    <h2>Fixed vs Discretionary</h2>
    <p style="color:var(--muted);font-style:italic;margin-bottom:15px">{fixed_pct}% of total spending is fixed (pre-authorized recurring debits)</p>
    <div class="chart-row">
        <div>
            <table class="data-table">
                <thead><tr><th>Fixed Cost</th><th style="text-align:right">Total</th><th style="text-align:right">Monthly Avg</th></tr></thead>
                <tbody>{fixed_rows}</tbody>
                <tfoot><tr style="font-weight:700"><td>Total Fixed</td><td style="text-align:right">{money(fixed_total)}</td><td style="text-align:right">{money(fixed_total / num_months if num_months else 0)}</td></tr></tfoot>
            </table>
        </div>
        <div>
            <div class="chart-container"><canvas id="fixedDiscChart"></canvas></div>
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
    <p style="color:var(--muted);font-style:italic;margin-bottom:15px">Revenue from Tall Tree Technology (client payments) and dividends from Britton Holdings Growth (investment portfolio)</p>
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

    # ── Incoming e-Transfers section ──
    incoming_etransfers = incoming_etransfers or []
    etransfer_income_section = ""
    if incoming_etransfers:
        # Load annotations from etransfer-notes-in.csv (date,amount,note)
        etransfer_in_notes = {}
        in_notes_path = os.path.join(folder, "etransfer-notes-in.csv")
        if os.path.exists(in_notes_path):
            with open(in_notes_path, newline="") as f:
                for row in csv.DictReader(f):
                    amt = row["amount"].replace("$", "").replace(",", "")
                    key = (row["date"], amt)
                    if row.get("note", "").strip():
                        etransfer_in_notes[key] = row["note"].strip()
        # Group by month
        etransfer_in_by_month = {}
        for t in incoming_etransfers:
            m = str(t["date"])[:7]
            etransfer_in_by_month.setdefault(m, []).append(t)
        etransfer_in_rows = ""
        etransfer_in_total = sum(t["amount"] for t in incoming_etransfers)
        for m in sorted(etransfer_in_by_month, reverse=True):
            txns = etransfer_in_by_month[m]
            month_label = datetime.strptime(m, "%Y-%m").strftime("%b %Y")
            month_total = sum(t["amount"] for t in txns)
            etransfer_in_rows += f'<tr style="background:var(--bg);font-weight:600"><td colspan="2">{month_label}</td><td style="text-align:right">{money(month_total)}</td></tr>'
            for t in txns:
                date_str = str(t["date"])[:10]
                amt_str = f'{t["amount"]:.2f}'
                note = etransfer_in_notes.get((date_str, amt_str), "")
                note_html = f'<span style="color:var(--muted);font-style:italic">{note}</span>' if note else ""
                etransfer_in_rows += f'<tr><td>{date_str}</td><td>{note_html}</td><td style="text-align:right">{money(t["amount"])}</td></tr>'
        etransfer_income_section = f"""
<section id="incoming-etransfers" class="card">
    <h2>Incoming e-Transfers</h2>
    <p style="color:var(--muted);font-style:italic;margin-bottom:15px">Interac e-Transfer reimbursements received &mdash; {len(incoming_etransfers)} transactions totalling {money(etransfer_in_total)}</p>
    <table class="data-table" style="max-width:600px">
        <thead><tr><th>Date</th><th>Note</th><th style="text-align:right">Amount</th></tr></thead>
        <tbody>{etransfer_in_rows}</tbody>
    </table>
</section>"""

    # ── Bank Interest section ──
    bank_interest = bank_interest or []
    bank_interest_section = ""
    if bank_interest:
        bi_by_month = {}
        for t in bank_interest:
            m = str(t["date"])[:7]
            bi_by_month.setdefault(m, []).append(t)
        bi_total = sum(t["amount"] for t in bank_interest)
        bi_rows = ""
        for m in sorted(bi_by_month, reverse=True):
            txns = bi_by_month[m]
            month_label = datetime.strptime(m, "%Y-%m").strftime("%b %Y")
            month_total = sum(t["amount"] for t in txns)
            bi_rows += f'<tr style="background:var(--bg);font-weight:600"><td colspan="2">{month_label}</td><td style="text-align:right">{money(month_total)}</td></tr>'
            for t in sorted(txns, key=lambda x: x["date"], reverse=True):
                date_str = str(t["date"])[:10]
                bi_rows += f'<tr><td>{date_str}</td><td>{t["account"]}</td><td style="text-align:right">{money(t["amount"])}</td></tr>'
        bank_interest_section = f"""
<section id="bank-interest" class="card">
    <h2>Bank Interest</h2>
    <p style="color:var(--muted);font-style:italic;margin-bottom:15px">Interest earned on cash and savings accounts &mdash; {len(bank_interest)} payments totalling {money(bi_total)}</p>
    <table class="data-table" style="max-width:600px">
        <thead><tr><th>Date</th><th>Account</th><th style="text-align:right">Amount</th></tr></thead>
        <tbody>{bi_rows}</tbody>
    </table>
</section>"""

    # ── Passive Income section ──
    def balance_cell(a: dict) -> str:
        """Render a balance <td> with source annotation."""
        src = a.get("balance_source", "")
        dt = a.get("statement_date", "")
        val = money(a["value"])
        if src and src != "portfolio.csv":
            note = dt if dt else src
            return (f"<td style='text-align:right'>{val}"
                    f"<br><span style='font-size:0.75em;color:var(--muted)'>{note}</span></td>")
        else:
            return (f"<td style='text-align:right;font-style:italic'>{val}"
                    f"<br><span style='font-size:0.75em;color:#e67e22'>csv</span></td>")

    def return_cell(a: dict) -> str:
        """Render a return % <td> with source annotation."""
        pct = a.get("return_pct", 0)
        src = a.get("return_source", "")
        if not src and pct == 0:
            return "<td style='text-align:right;color:var(--muted)'>—</td>"
        if src == "csv":
            return (f"<td style='text-align:right;font-style:italic'>{pct:.1f}%"
                    f"<br><span style='font-size:0.75em;color:#e67e22'>csv</span></td>")
        note = src.replace(" statement", "").replace(" report", "")
        return (f"<td style='text-align:right'>{pct:.1f}%"
                f"<br><span style='font-size:0.75em;color:var(--muted)'>{note}</span></td>")

    def income_cell(a: dict) -> str:
        """Render Income/yr <td> with source annotation."""
        val = a["income_annual"]
        src = a.get("income_source", "")
        if src == "dividends":
            note = "dividends"
        elif src == "yield":
            note = "yield est."
        elif src == "interest":
            note = "interest"
        else:
            note = ""
        if val == 0 and not note:
            return "<td style='text-align:right;color:var(--muted)'>—</td>"
        annotation = f"<br><span style='font-size:0.75em;color:var(--muted)'>{note}</span>" if note else ""
        return f"<td style='text-align:right'>{money(val)}{annotation}</td>"

    def growth_cell(a: dict) -> str:
        """Render Growth/yr <td>."""
        val = a["growth_annual"]
        if val == 0:
            return "<td style='text-align:right;color:var(--muted)'>—</td>"
        color = "#27ae60" if val > 0 else "#e74c3c"
        return f"<td style='text-align:right;color:{color}'>{money(val)}</td>"

    def vs_avg_cell(a: dict, avg_return: float) -> str:
        """Render vs Avg as +/- percentage points of return vs bucket average."""
        diff_pp = a["return_pct"] - avg_return
        new_badge = ""
        if a.get('start_date'):
            age_days = (datetime.now().date() - a['start_date']).days
            age_months = age_days // 30
            if age_months < 6:
                label = f"{age_months} mo" if age_months > 0 else "< 1 mo"
                new_badge = f"<br><span style='font-size:0.8em;color:var(--muted)'>est. {label} ago — monitor</span>"
        if diff_pp >= 0:
            return f"<td style='text-align:right;color:#27ae60'>+{diff_pp:.1f} pp</td>"
        else:
            return f"<td style='text-align:right;color:#e67e22'>{diff_pp:.1f} pp{new_badge}</td>"

    passive_section = ""
    if passive_income:
        # Accessible accounts table rows (sorted by return % desc)
        acc_total_balance = passive_income["accessible_balance"]
        acc_total_income = passive_income["annual_income"]
        acc_total_growth = passive_income.get("annual_growth", 0)
        acc_monthly = passive_income["monthly_income"]
        acc_total_return = acc_total_income + acc_total_growth
        acc_avg_return = (acc_total_return / acc_total_balance * 100) if acc_total_balance else 0

        acc_sorted = sorted(passive_income["accounts"],
                            key=lambda a: a['return_pct'],
                            reverse=True)

        acc_rows = ""
        for a in acc_sorted:
            acc_rows += (
                f"<tr><td>{a['account']}</td><td>{a.get('brokerage','')}</td><td>{a['type']}</td>"
                f"{balance_cell(a)}"
                f"{return_cell(a)}"
                f"{income_cell(a)}"
                f"{growth_cell(a)}"
                f"{vs_avg_cell(a, acc_avg_return)}</tr>"
            )

        # Registered accounts table (RRSP + RESP — TFSAs are in Accessible)
        reg_html = ""
        if passive_income.get("registered_accounts"):
            reg_total_return = passive_income['registered_annual'] + passive_income.get('registered_growth', 0)
            reg_avg_return = (reg_total_return / passive_income['registered_balance'] * 100) if passive_income['registered_balance'] else 0

            reg_sorted = sorted(passive_income["registered_accounts"],
                                 key=lambda a: a['return_pct'],
                                 reverse=True)
            reg_rows = ""
            for a in reg_sorted:
                reg_rows += (
                    f"<tr><td>{a['account']}</td><td>{a.get('brokerage','')}</td><td>{a['type']}</td>"
                    f"{balance_cell(a)}"
                    f"{return_cell(a)}"
                    f"{income_cell(a)}"
                    f"{growth_cell(a)}"
                    f"{vs_avg_cell(a, reg_avg_return)}</tr>"
                )
            reg_html = f"""
    <h3 style="margin-top:30px">Registered Accounts <span style="font-weight:400;color:var(--muted);font-size:0.85em">(RRSP, RESP — not accessible without tax penalty)</span></h3>
    <table class="data-table" style="max-width:100%">
        <thead><tr><th>Account</th><th>Brokerage</th><th>Type</th><th style="text-align:right">Balance</th><th style="text-align:right">Return</th><th style="text-align:right">Income/yr</th><th style="text-align:right">Growth/yr</th><th style="text-align:right">vs Avg</th></tr></thead>
        <tbody>{reg_rows}</tbody>
        <tfoot>
            <tr style="font-weight:700"><td colspan="3">Total Registered</td><td style="text-align:right">{money(passive_income['registered_balance'])}</td><td style="text-align:right">{reg_avg_return:.1f}%</td><td style="text-align:right">{money(passive_income['registered_annual'])}</td><td style="text-align:right">{money(passive_income.get('registered_growth', 0))}</td><td></td></tr>
            <tr style="color:var(--muted)"><td colspan="7">Monthly Income</td><td style="text-align:right">{money(passive_income['registered_monthly'])}</td></tr>
        </tfoot>
    </table>"""

        passive_section = f"""
<section id="passive-income" class="card">
    <h2>Investment Portfolio</h2>
    <p style="color:var(--muted);font-style:italic;margin-bottom:15px">Yield and growth from personal investment accounts — accessible and registered holdings</p>
    <h3>Accessible Accounts</h3>
    <table class="data-table" style="max-width:100%">
        <thead><tr><th>Account</th><th>Brokerage</th><th>Type</th><th style="text-align:right">Balance</th><th style="text-align:right">Return</th><th style="text-align:right">Income/yr</th><th style="text-align:right">Growth/yr</th><th style="text-align:right">vs Avg</th></tr></thead>
        <tbody>{acc_rows}</tbody>
        <tfoot>
            <tr style="font-weight:700"><td colspan="3">Total Accessible</td><td style="text-align:right">{money(acc_total_balance)}</td><td style="text-align:right">{acc_avg_return:.1f}%</td><td style="text-align:right">{money(acc_total_income)}</td><td style="text-align:right">{money(acc_total_growth)}</td><td></td></tr>
            <tr style="color:var(--muted)"><td colspan="7">Monthly Income</td><td style="text-align:right">{money(acc_monthly)}</td></tr>
        </tfoot>
    </table>
    {reg_html}
</section>"""

    # ── Income chart section (removed — not useful) ──
    income_chart_section = ""

    # ── Tab buttons for conditional tabs ──
    income_tab_btn = ''
    if corporate_income or passive_income or incoming_etransfers or bank_interest:
        income_tab_btn = '<button data-tab="tab-income">Income</button>'
    milestones_tab_btn = ''
    if milestones_section:
        milestones_tab_btn = '<button data-tab="tab-milestones">Milestones</button>'
    ai_tab_btn = ''
    if ai_html:
        ai_tab_btn = '<button data-tab="tab-ai">AI Recommendations</button>'

    # ── Chart.js for fixed/discretionary stacked bar ──
    fixed_chart_js = ""
    if fixed_detail:
        fixed_chart_js = f"""
    new Chart(document.getElementById('fixedDiscChart'), {{
        type: 'bar',
        data: {{
            labels: {fixed_disc_labels},
            datasets: [
                {{ label: 'Fixed', data: {fixed_per_month_json}, backgroundColor: '#4e79a7', borderRadius: 4 }},
                {{ label: 'Discretionary', data: {disc_per_month_json}, backgroundColor: '#76b7b2', borderRadius: 4 }}
            ]
        }},
        options: {{
            responsive: true,
            plugins: {{
                legend: {{ position: 'bottom' }},
                tooltip: {{ callbacks: {{ label: ctx => ctx.dataset.label + ': $' + ctx.parsed.y.toLocaleString(undefined, {{minimumFractionDigits:2}}) }} }}
            }},
            scales: {{
                x: {{ stacked: true }},
                y: {{ stacked: true, beginAtZero: true, ticks: {{ callback: v => '$' + (v/1000).toFixed(0) + 'k' }} }}
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
.ai-recommendations {{ line-height: 1.6; }}
.ai-recommendations ol {{ list-style: none; counter-reset: rec; padding: 0; margin: 0; }}
.ai-recommendations li {{ counter-increment: rec; background: var(--bg); border-radius: 10px; padding: 16px 18px 16px 52px; margin-bottom: 12px; position: relative; border: 1px solid var(--border); }}
.ai-recommendations li::before {{ content: counter(rec); position: absolute; left: 16px; top: 16px; width: 26px; height: 26px; background: var(--accent); color: #fff; border-radius: 50%; font-size: 0.82em; font-weight: 700; display: flex; align-items: center; justify-content: center; }}
.ai-recommendations li:last-child {{ margin-bottom: 0; }}
.ai-badge {{ font-size: 0.65em; background: #f39c12; color: #fff; padding: 2px 8px; border-radius: 10px; margin-left: 8px; cursor: pointer; vertical-align: middle; text-decoration: none; }}
.ai-badge:hover {{ background: #e67e22; }}
canvas {{ max-width: 100%; }}
.noscript-table {{ margin-top: 10px; }}
.tab-nav {{ display: flex; flex-wrap: wrap; gap: 8px; background: var(--card); border-radius: 12px; padding: 15px 25px; margin-bottom: 20px; box-shadow: 0 1px 3px rgba(0,0,0,0.08); }}
.tab-nav button {{ border: none; cursor: pointer; font-family: inherit; color: var(--accent); background: var(--bg); padding: 7px 18px; border-radius: 20px; font-size: 0.88em; font-weight: 500; transition: background 0.15s, color 0.15s; }}
.tab-nav button:hover {{ background: var(--accent); color: #fff; }}
.tab-nav button.active {{ background: var(--accent); color: #fff; }}
.tab-panel {{ display: none; }}
.tab-panel.active {{ display: block; }}
</style>
</head>
<body>
<h1>Financial Dashboard</h1>
<p class="subtitle">Personal &amp; corporate financial overview: {month_labels[0]} – {month_labels[-1]} | Generated {datetime.now().strftime('%b %d, %Y at %I:%M %p')}</p>

<div class="tab-nav">
    <button class="active" data-tab="tab-big-picture">The Big Picture</button>
    {income_tab_btn}
    <button data-tab="tab-spending">Spending</button>
    {milestones_tab_btn}
    {ai_tab_btn}
</div>

<!-- ═══ THE BIG PICTURE ═══ -->
<div class="tab-panel active" id="tab-big-picture">
<div id="overview"></div>
{hero_card}
{sustainability_card}
{net_worth_card}
<div class="stats">
    {overview_stats}
</div>

</div>

<!-- ═══ INCOME ═══ -->
{'<div class="tab-panel" id="tab-income">' + income_chart_section + corporate_section + passive_section + etransfer_income_section + bank_interest_section + '</div>' if (corporate_income or passive_income or incoming_etransfers or bank_interest) else ''}

<!-- ═══ SPENDING ANALYSIS ═══ -->
<div class="tab-panel" id="tab-spending">

{spotlight_html}

<section id="categories" class="card">
    <h2>Category Heatmap</h2>
    <p style="color:var(--muted);font-style:italic;margin-bottom:15px">Spending intensity by category over the last 6 months, sorted by total. Darker cells = higher spend.</p>
    <div style="overflow-x:auto">
    <table class="data-table">
        <thead><tr><th>Category</th>{heatmap_month_headers}<th style="text-align:right">Avg</th><th style="text-align:right">6m Total</th></tr></thead>
        <tbody>{heatmap_rows}</tbody>
    </table>
    </div>
</section>

{fixed_section}

<section id="subscriptions" class="card">
    <h2>Subscription Audit</h2>
    <p style="color:var(--muted);font-style:italic;margin-bottom:15px">Recurring charges detected across your statements, grouped by status.</p>
    <div style="overflow-x:auto">
    <table class="data-table">
        <thead><tr><th>Service</th><th style="text-align:right">Avg/Mo</th>{sub_month_headers}</tr></thead>
        <tbody>{sub_rows}</tbody>
        <tfoot><tr style="font-weight:700"><td>Total Subscriptions</td><td style="text-align:right">{money(total_monthly)}/mo</td><td colspan="{len(sub_months)}"></td></tr></tfoot>
    </table>
    </div>
</section>

{'<section id="interac-transfers" class="card"><h2>Interac e-Transfer Details</h2><p style="color:var(--muted);font-style:italic;margin-bottom:15px">All outgoing e-Transfers &mdash; ' + str(len(etransfer_txns)) + ' transactions totalling ' + money(etransfer_total) + '</p><table class="data-table"><thead><tr><th>Date</th><th>Note</th><th style="text-align:right">Amount</th></tr></thead><tbody>' + etransfer_rows + '</tbody></table></section>' if etransfer_txns else ''}
</div>

<!-- ═══ MILESTONES ═══ -->
{'<div class="tab-panel" id="tab-milestones">' + milestones_section + '</div>' if milestones_section else ''}

<!-- ═══ AI RECOMMENDATIONS ═══ -->
{'<div class="tab-panel" id="tab-ai">' + ai_section + '</div>' if ai_html else ''}


<script>
document.addEventListener('DOMContentLoaded', function() {{
    document.querySelectorAll('.tab-nav button').forEach(function(btn) {{
        btn.addEventListener('click', function() {{
            document.querySelectorAll('.tab-nav button').forEach(function(b) {{ b.classList.remove('active'); }});
            document.querySelectorAll('.tab-panel').forEach(function(p) {{ p.classList.remove('active'); }});
            btn.classList.add('active');
            var panel = document.getElementById(btn.dataset.tab);
            panel.classList.add('active');
            // Defer chart resize to next frame so browser reflows display:block first
            if (typeof Chart !== 'undefined') {{
                setTimeout(function() {{
                    panel.querySelectorAll('canvas').forEach(function(c) {{
                        var chart = Chart.getChart(c);
                        if (chart) {{ chart.resize(); chart.update('none'); }}
                    }});
                }}, 50);
            }}
        }});
    }});
}});
</script>
<script>
document.addEventListener('DOMContentLoaded', function() {{
    var items = document.querySelectorAll('.ai-recommendations li[data-sections]');
    items.forEach(function(li, idx) {{
        var tipNum = idx + 1;
        var sections = li.getAttribute('data-sections').split(',');
        li.id = 'ai-tip-' + tipNum;
        sections.forEach(function(id) {{
            id = id.trim();
            var section = document.getElementById(id);
            if (!section) return;
            var h2 = section.querySelector('h2');
            if (!h2) return;
            var badge = document.createElement('a');
            badge.className = 'ai-badge';
            badge.textContent = 'AI tip #' + tipNum;
            badge.href = '#';
            badge.addEventListener('click', function(e) {{
                e.preventDefault();
                var aiBtn = document.querySelector('.tab-nav button[data-tab="tab-ai"]');
                if (aiBtn) aiBtn.click();
                setTimeout(function() {{
                    li.scrollIntoView({{ behavior: 'smooth', block: 'center' }});
                    li.style.outline = '2px solid #f39c12';
                    setTimeout(function() {{ li.style.outline = ''; }}, 2000);
                }}, 100);
            }});
            h2.appendChild(badge);
        }});
    }});
}});
</script>
<script src="https://cdn.jsdelivr.net/npm/chart.js@4/dist/chart.umd.min.js"></script>
<script>
document.addEventListener('DOMContentLoaded', function() {{
    if (typeof Chart === 'undefined') return;

    {fixed_chart_js}

    {sustainability_chart_js}

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
    parser.add_argument("--source", choices=["csv", "statements"], default="csv",
                        help="Financial data source: csv (portfolio.csv) or statements (PDF statements)")
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

    # Apply category overrides from etransfer-notes.csv
    etransfer_notes_path = os.path.join(folder, "etransfer-notes.csv")
    if os.path.exists(etransfer_notes_path):
        override_map = {}
        with open(etransfer_notes_path, newline="") as f:
            for row in csv.DictReader(f):
                cat = row.get("category", "").strip()
                if cat:
                    amt = row["amount"].replace("$", "").replace(",", "")
                    override_map[(row["date"], amt)] = cat
        if override_map:
            count = 0
            for t in transactions:
                if t["merchant"] == "Interac e-Transfer":
                    key = (str(t["date"])[:10], f'{t["amount"]:.2f}')
                    if key in override_map:
                        t["category"] = CATEGORY_CONSOLIDATION.get(override_map[key], override_map[key])
                        count += 1
            if count:
                print(f"Applied {count} e-transfer category overrides")

    # Extract transfer data from debit card CSVs
    transfers, incoming_etransfers = extract_transfers(folder)
    if transfers:
        print(f"Found transfer data across {len(transfers)} months")
    if incoming_etransfers:
        print(f"Found {len(incoming_etransfers)} incoming e-transfers")

    # Extract bank interest from personal + corporate debit CSVs
    bank_interest = extract_bank_interest(folder)
    if bank_interest:
        bi_total = sum(t["amount"] for t in bank_interest)
        print(f"Found {len(bank_interest)} bank interest payments totalling ${bi_total:,.2f}")

    # Extract passive income from investment portfolio
    passive_income = extract_passive_income(folder, source=args.source)
    if passive_income:
        print(f"Portfolio passive income ({args.source}): ${passive_income['annual_income']:,.2f}/year (${passive_income['monthly_income']:,.2f}/month) from {len(passive_income['accounts'])} accounts")

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
        ai_html = get_ai_recommendations(data, passive_income=passive_income,
                                          corporate_income=corporate_income,
                                          incoming_etransfers=incoming_etransfers,
                                          bank_interest=bank_interest)

    html = generate_html(data, ai_html, notes=user_notes, budgets=user_budgets,
                         passive_income=passive_income,
                         corporate_income=corporate_income,
                         incoming_etransfers=incoming_etransfers,
                         bank_interest=bank_interest,
                         folder=folder)
    output_path = os.path.join(folder, "dashboard.html")
    with open(output_path, "w", encoding="utf-8") as f:
        f.write(html)

    print(f"\nDashboard written to: {output_path}")
    print("Open it in your browser to view the report.")


if __name__ == "__main__":
    main()
