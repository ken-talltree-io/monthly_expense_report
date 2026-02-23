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
    ("Coffee Shops", [
        "Continental Coffee", "JJ Bean", "Prado Cafe", "Forecast Coffee",
        "Moja Coffee", "Starbucks", "Nemesis Coffee", "Kits Beach Coffee",
        "Bean Scene", "White Rabbit Coffee", "Bolacco", "Crema Cafe",
        "Parsonage Cafe",
        "Breka Bakery", "Deville Coffee", "Bean Around", "Bump N Grind",
        "Grounds For Coffee", "Matchstick", "Laughing Bean",
    ]),
    ("Bakeries & Treats", [
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
        "Apple Subscriptions", "Amazon Prime", "Open Heart Project",
        "Brief Media", "Stratechery",
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
    ("Parking & Gas", [
        "PayByPhone", "Impark", "Honk Parking", "Petro-Canada",
        "Chevron", "Nwest Parking", "New West Parking", "Zipby",
        "False Creek Parking", "Silver Creek Travel",
        "Shell EV Charging", "ChargePoint EV", "On The Run EV",
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
    ]),
    ("Ski Resorts", [
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
    ("Health & Beauty", [
        "London Drugs", "Shoppers Drug Mart", "Rexall", "New Visage",
        "Harlow Skin", "Body Energy", "Spice Beauty", "Paulie",
        "Caulfeild Pharmasave", "Hemlock Hospital",
        "Mount Pleasant Visio",
        "Spautopia", "Leah Marks",
        "Swank's Salon", "Dr. Marina Liarsky", "Grant Street Wellness",
        "Modo Yoga", "The Drive Pharmacy", "Generations Optometry",
        "Kokopelli Salon",
    ]),
    ("Dental", [
        "Yaletown Dentistry", "Cambie Broadway Dental", "Tot 2 Teen Dental",
        "Dr. Liat Tzur", "Sunrise Orthodontics",
    ]),
    ("Software & SaaS", [
        "OpenAI", "Cursor IDE", "Frontend Masters", "Resume.io",
        "Claude AI", "Nvidia",
    ]),
    ("Insurance", [
        "ICBC", "BCAA Insurance", "Wawanesa", "RBC Life Insurance", "Sun Life Insurance",
        "Scotiabank Insurance",
    ]),
    ("Entertainment", [
        "Ticketmaster", "SeatGeek", "Eventbrite", "Cineplex",
        "Scandinave Spa", "Butchart Gardens", "Candytopia", "Capri Valley",
        "Spirit Halloween", "Games On The Drive", "Got Craft",
        "Red Horses Gallery", "Mosaic Books", "International Travel Maps",
        "The Anza Club",
        "Smash Volleyball", "Richmond Olympic Oval",
        "BC Place", "Rio Theatre", "Steam Games", "Pulp Fiction Books",
        "Vancouver Parks Board",
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
        "North Shore Kia",
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
    ("Medical", [
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
    "Software & SaaS": "Subscriptions & Telecom",
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

        # Parse balance: "Total Portfolio" followed by $amount and 100.00
        m = re.search(
            r"Total Portfolio\s+\$([0-9,]+\.\d{2})\s+100\.00", text
        )
        if not m:
            continue
        balance = float(m.group(1).replace(",", ""))

        # Handle USD accounts: convert to CAD using statement exchange rate
        is_usd = suffix.upper().endswith("USD")
        if is_usd:
            fx = re.search(
                r"\$1 USD = \$([0-9.]+) CAD", text
            )
            if fx:
                balance = round(balance * float(fx.group(1)), 2)
            else:
                # Check other Wealthsimple PDFs for an exchange rate
                for other_suffix, other_files in ws_pdfs.items():
                    if other_suffix == suffix:
                        continue
                    other_text = _pdf_text(other_files[0][1])
                    fx = re.search(r"\$1 USD = \$([0-9.]+) CAD", other_text)
                    if fx:
                        balance = round(balance * float(fx.group(1)), 2)
                        break
                else:
                    continue  # Can't convert; skip this account

        # Parse statement end date
        dm = re.search(
            r"(\d{4}-\d{2}-\d{2})\s*-\s*(\d{4}-\d{2}-\d{2})", text
        )
        stmt_date = dm.group(2) if dm else date_seg

        results[suffix] = {
            "balance": balance,
            "date": stmt_date,
            "source": "Wealthsimple statement",
            "return_pct": None,
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

                # Parse per-account since-inception returns (first match per account wins)
                for acct_m in re.finditer(r"Account (\d{7})\s+\w", text):
                    acct_num = acct_m.group(1)
                    if acct_num not in results or results[acct_num]["return_pct"] is not None:
                        continue
                    si = re.search(r"Since Inception\s+([\d.]+)", text[acct_m.start():])
                    if si:
                        results[acct_num]["return_pct"] = float(si.group(1))

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

    return results


# ── Income & Transfer Extraction ─────────────────────────────────────────────

def extract_passive_income(folder: str) -> dict | None:
    """Extract annual passive income from investment portfolio CSV.

    Reads portfolio.csv and sums yield (annual income) for personal accounts.
    Excludes Corporate, RESP, and Property accounts.
    For accounts with TBD yield, estimates from return rate * total value.
    """
    portfolio_path = os.path.join(folder, "portfolio.csv")
    if not os.path.exists(portfolio_path):
        return None

    ACCESSIBLE_TYPES = {"Non-reg", "Cash", "TFSA"}  # spendable without tax penalty

    accessible = []
    registered = []  # RRSP + RESP
    corporate_accts = []
    property_accts = []

    # Parse statement balances (authoritative source for totals)
    stmt_balances = parse_statement_balances(folder)

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

            if csv_value is not None and csv_value > 0:
                total_value = csv_value
                balance_source = "portfolio.csv"
                statement_date = ""
            elif stmt:
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

            # Return %: portfolio.csv overrides, then statement, then 0
            rate_str = row[col_return].strip().replace("%", "") if col_return is not None and col_return < len(row) else ""
            csv_return = None
            if rate_str and rate_str != "TBD":
                try:
                    csv_return = float(rate_str)
                except (ValueError, TypeError):
                    pass
            if csv_return is not None:
                return_pct = csv_return
                return_source = "portfolio.csv"
            elif stmt and stmt.get("return_pct") is not None:
                return_pct = stmt["return_pct"]
                return_source = stmt["source"]
            else:
                return_pct = 0.0
                return_source = ""

            # Income vs Growth split
            # Priority: statement dividends → Yield % from CSV → Interest strategy → unknown
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

            if stmt and stmt.get("dividends_annual") is not None:
                income_annual = stmt["dividends_annual"]
                income_source = "dividends"
            elif csv_yield is not None:
                income_annual = total_value * csv_yield / 100
                income_source = "yield"
            elif strategy == "Interest":
                income_annual = total_return_annual
                income_source = "interest"
            else:
                income_annual = 0.0
                income_source = ""

            growth_annual = total_return_annual - income_annual

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


def extract_transfers(folder: str) -> dict:
    """Extract monthly transfer summary from debit card CSVs.

    Returns dict of month -> {"in": float, "out": float}.
    Covers TRFOUT, TRFIN, TRFINTF, E_TRFOUT, E_TRFIN, EFTOUT.
    """
    TRANSFER_TYPES = {"TRFOUT", "TRFIN", "TRFINTF", "E_TRFOUT", "E_TRFIN", "EFTOUT"}
    transfers = defaultdict(lambda: {"in": 0.0, "out": 0.0})

    # Scan transactions/personal/ recursively (transfers come from debit-format CSVs, auto-detected)
    txn_personal = os.path.join(folder, "transactions", "personal")
    if not os.path.isdir(txn_personal):
        # Backward compat: try old path
        txn_personal = os.path.join(folder, "debit card")
        if not os.path.isdir(txn_personal):
            return {}

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

    # Categories that are NOT subscription-like (regular spending, not services)
    # Use consolidated category names (post CATEGORY_CONSOLIDATION mapping)
    NON_SUB_CATEGORIES = {
        "Food & Dining", "Groceries", "Shopping", "Recreation", "Pets",
        "Health & Wellness", "Housing & Utilities", "Transportation", "Travel",
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
                           corporate_income: dict | None = None) -> str | None:
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
                 "growth_annual": a["growth_annual"], "return_pct": a["return_pct"]}
                for a in passive_income["accounts"]
            ],
            "registered_annual": passive_income.get("registered_annual", 0),
            "registered_growth": passive_income.get("registered_growth", 0),
            "registered_monthly": passive_income.get("registered_monthly", 0),
            "registered_balance": passive_income.get("registered_balance", 0),
            "registered_accounts": [
                {"name": a["account"], "type": a["type"],
                 "balance": a["value"], "income_annual": a["income_annual"],
                 "growth_annual": a["growth_annual"], "return_pct": a["return_pct"]}
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
        summary["debts_paid_off"] = {
            "total_eliminated": round(sum(d["amount"] for d in debt_payoffs), 2),
            "note": "These debts have already been fully paid off during this period. They are NOT outstanding balances.",
        }

    prompt = f"""Analyze this personal & corporate financial dashboard and provide actionable recommendations.

Context: This dashboard covers a self-employed consultant pursuing financial sustainability, defined as: passive income >= burn rate. Income comes from three streams: (1) passive portfolio yield from personal investments — this is the SUSTAINABLE income, (2) corporate consulting revenue (Tall Tree Technology) at ~60% take-home after tax/expenses — this is ACTIVE income that bridges the gap, and (3) corporate dividend income (Britton Holdings). The "burn_rate_coverage" section shows how much of the burn rate is covered by passive income alone — coverage_pct is passive-only. Corporate income bridges the remaining gap but is not considered sustainable. "accessible_savings" is the total balance in Non-registered, Cash, and TFSA accounts that can be drawn without tax penalty; "runway_months" shows how long savings last if all income stopped (null if passive income already covers expenses). Revenue trend shows month-over-month changes in consulting income. "debts_paid_off" lists debts that were fully eliminated during this period — these are no longer owed and should be celebrated, not treated as outstanding obligations. The spending data includes fixed costs (tuition, car payment, utilities) and discretionary spending across credit and debit cards. The "passive_income.accounts" array contains per-account detail (name, type, balance, annual_yield, return_pct) for accessible accounts and RRSP accounts — use this to identify underperforming or overconcentrated positions. The "passive_income.net_worth" object shows the full balance breakdown across accessible, RRSP, corporate, property, and RESP holdings. Each category includes a "monthly" object with per-month spending for the last 6 months — use this to spot categories trending up or down.

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

Format your response in clean HTML as a single <ol> list with at most 5 <li> items. Use <strong> for emphasis on merchant names and dollar amounts. Be concise — one short paragraph per recommendation."""

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
    combined_monthly = monthly_passive + corp_monthly_takehome
    has_income = passive_income or corporate_income


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
    <div class="stat"><div class="value" style="color:{trend_color}">{trend_arrow} {abs(data['mom_change']):.0f}%</div><div class="label">3-Month Trend</div></div>"""
    if debt_payoff_total > 0:
        overview_stats += f"""
    <div class="stat"><div class="value" style="color:#27ae60">{money(debt_payoff_total)}</div><div class="label">Debt Paid Off</div></div>"""

    if cashback_total > 0:
        overview_stats += f"""
    <div class="stat"><div class="value" style="color:#27ae60">{money(cashback_total)}</div><div class="label">VISA Cash-Back ({len(months)} months)</div></div>"""

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
        src = a.get("return_source", "portfolio.csv")
        if src and src != "portfolio.csv":
            note = src.replace(" statement", "").replace(" report", "")
            return (f"<td style='text-align:right'>{pct:.1f}%"
                    f"<br><span style='font-size:0.75em;color:var(--muted)'>{note}</span></td>")
        else:
            return (f"<td style='text-align:right;font-style:italic'>{pct:.1f}%"
                    f"<br><span style='font-size:0.75em;color:#e67e22'>csv</span></td>")

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
    <h2>Passive Income &amp; Accessible Savings</h2>
    <p style="color:var(--muted);margin-bottom:15px">Income from personal investment accounts — accessible balance breakdown</p>
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
    if corporate_income or passive_income:
        income_tab_btn = '<button data-tab="tab-income">Income</button>'
    milestones_tab_btn = ''
    if debt_payoffs:
        milestones_tab_btn = '<button data-tab="tab-milestones">Milestones</button>'
    ai_tab_btn = ''
    if ai_html:
        ai_tab_btn = '<button data-tab="tab-ai">AI Recommendations</button>'

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
.ai-recommendations {{ line-height: 1.6; }}
.ai-recommendations ol {{ list-style: none; counter-reset: rec; padding: 0; margin: 0; }}
.ai-recommendations li {{ counter-increment: rec; background: var(--bg); border-radius: 10px; padding: 16px 18px 16px 52px; margin-bottom: 12px; position: relative; border: 1px solid var(--border); }}
.ai-recommendations li::before {{ content: counter(rec); position: absolute; left: 16px; top: 16px; width: 26px; height: 26px; background: var(--accent); color: #fff; border-radius: 50%; font-size: 0.82em; font-weight: 700; display: flex; align-items: center; justify-content: center; }}
.ai-recommendations li:last-child {{ margin-bottom: 0; }}
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
{net_worth_card}
<div class="stats">
    {overview_stats}
</div>

</div>

<!-- ═══ INCOME ═══ -->
{'<div class="tab-panel" id="tab-income">' + income_chart_section + corporate_section + passive_section + '</div>' if (corporate_income or passive_income) else ''}

<!-- ═══ SPENDING ANALYSIS ═══ -->
<div class="tab-panel" id="tab-spending">
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

<section id="categories" class="card">
    <h2>Category Breakdown</h2>
    <table class="data-table">
        <thead><tr><th>Category</th><th style="text-align:right">Total</th><th style="text-align:right">Monthly Avg</th>{'<th>vs Budget</th>' if has_budgets else ''}<th style="text-align:center">Trend</th><th style="text-align:center">Txns</th></tr></thead>
        <tbody>{cat_rows}</tbody>
        <tfoot><tr style="font-weight:700"><td>Total</td><td style="text-align:right">{money(data['total'])}</td><td style="text-align:right">{money(data['monthly_avg'])}</td>{'<td></td>' if has_budgets else ''}<td></td><td></td></tr></tfoot>
    </table>
</section>

<section id="subscriptions" class="card">
    <h2>Subscription Audit</h2>
    <p style="color:var(--muted);margin-bottom:15px">Recurring charges detected across your statements, grouped by status.</p>
    <div style="overflow-x:auto">
    <table class="data-table">
        <thead><tr><th>Service</th><th style="text-align:right">Avg/Mo</th>{sub_month_headers}</tr></thead>
        <tbody>{sub_rows}</tbody>
        <tfoot><tr style="font-weight:700"><td>Total Subscriptions</td><td style="text-align:right">{money(total_monthly)}/mo</td><td colspan="{len(sub_months)}"></td></tr></tfoot>
    </table>
    </div>
</section>
</div>

<!-- ═══ MILESTONES ═══ -->
{'<div class="tab-panel" id="tab-milestones">' + debt_section + '</div>' if debt_payoffs else ''}

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
        ai_html = get_ai_recommendations(data, passive_income=passive_income,
                                          corporate_income=corporate_income)

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
