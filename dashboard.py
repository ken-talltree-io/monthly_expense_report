#!/usr/bin/env python3
"""
Expense Dashboard & Subscription Auditor
Reads credit card CSV exports and generates a self-contained HTML dashboard.

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
}


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
    ]),
    ("Travel & Hotels", [
        "Best Western", "Nomade Cabo", "Merpago", "Clip Mx",
        "La Comer", "Tastes On The Fly", "0835_YVR",
    ]),
    ("Donations", [
        "Make-A-Wish",
    ]),
]


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
    """Read all CSV files and return a list of transaction dicts."""
    transactions = []
    files = sorted(glob.glob(os.path.join(folder, "credit-card-*.csv")))
    if not files:
        # Fall back to all CSVs except categories.csv
        files = sorted(f for f in glob.glob(os.path.join(folder, "*.csv"))
                       if not os.path.basename(f).startswith("categories"))
    if not files:
        print(f"Error: No CSV files found in {folder}")
        sys.exit(1)

    for fpath in files:
        with open(fpath, newline="", encoding="utf-8-sig") as f:
            reader = csv.DictReader(f)
            for row in reader:
                amount = float(row["amount"])
                txn_type = row.get("type", "Purchase")
                if amount < 0 or txn_type.strip().lower() == "payment":
                    continue  # skip payments/credits
                date = datetime.strptime(row["transaction_date"], "%Y-%m-%d")
                raw_merchant = row["details"]
                merchant = normalize_merchant(raw_merchant)
                transactions.append({
                    "date": date,
                    "month": date.strftime("%Y-%m"),
                    "raw_merchant": raw_merchant,
                    "merchant": merchant,
                    "category": categorize(merchant),
                    "amount": amount,
                })
    return sorted(transactions, key=lambda t: t["date"])


# ── Analysis ─────────────────────────────────────────────────────────────────

def analyze(transactions: list[dict]) -> dict:
    months_set = sorted({t["month"] for t in transactions})
    total = sum(t["amount"] for t in transactions)
    monthly_totals = defaultdict(float)
    category_totals = defaultdict(float)
    category_counts = defaultdict(int)
    merchant_totals = defaultdict(float)
    merchant_counts = defaultdict(int)
    merchant_monthly = defaultdict(lambda: defaultdict(float))
    monthly_txns = defaultdict(list)

    for t in transactions:
        monthly_totals[t["month"]] += t["amount"]
        category_totals[t["category"]] += t["amount"]
        category_counts[t["category"]] += 1
        merchant_totals[t["merchant"]] += t["amount"]
        merchant_counts[t["merchant"]] += 1
        merchant_monthly[t["merchant"]][t["month"]] += t["amount"]
        monthly_txns[t["month"]].append(t)

    # Month-over-month trend
    monthly_list = [(m, monthly_totals[m]) for m in months_set]
    if len(monthly_list) >= 2:
        last, prev = monthly_list[-1][1], monthly_list[-2][1]
        mom_change = ((last - prev) / prev) * 100 if prev else 0
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
        "Restaurants & Dining", "Coffee Shops", "Bakeries & Treats",
        "Groceries", "Liquor & Alcohol", "Clothing", "Sports & Outdoor",
        "Entertainment", "Amazon", "Home Improvement", "Parking & Gas",
        "Kids", "Travel & Hotels", "Donations", "Ski Resorts",
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

    return {
        "months": months_set,
        "total": round(total, 2),
        "monthly_avg": round(total / num_months, 2) if num_months else 0,
        "mom_change": round(mom_change, 1),
        "monthly_totals": {m: round(monthly_totals[m], 2) for m in months_set},
        "categories": categories,
        "subscriptions": subscriptions,
        "top_merchants": top_merchants,
        "monthly_txns": {m: sorted(monthly_txns[m], key=lambda t: t["date"]) for m in months_set},
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
    }

    prompt = f"""Analyze this household credit card spending data and provide actionable recommendations.

DATA:
{json.dumps(summary, indent=2)}

Please provide exactly 10 numbered recommendations — no more, no less. Each should be specific, actionable, and reference actual merchant names and dollar amounts from the data. Cover a mix of:
- Subscription cost-saving actions (price increases to negotiate, services to cancel/downgrade)
- Spending pattern optimizations (consolidation, alternatives)
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

def generate_html(data: dict, ai_html: str | None = None) -> str:
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

    # Category data for charts
    cat_labels = json.dumps([c[0] for c in data["categories"]])
    cat_values = json.dumps([c[1] for c in data["categories"]])
    cat_colors = json.dumps(COLORS[:len(data["categories"])])
    monthly_values = json.dumps([data["monthly_totals"][m] for m in months])
    month_labels_json = json.dumps(month_labels)

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
        sub_rows += f"""<tr>
            <td><strong>{s['merchant']}</strong>{('<br>' + alert_html) if alert_html else ''}</td>
            <td style="text-align:right">{money(s['avg'])}</td>
            {month_cells}
            <td style="text-align:center">{status_badge(s['status'])}</td>
        </tr>"""

    # Top merchants table
    top_rows = ""
    for i, (m, t, c) in enumerate(data["top_merchants"], 1):
        top_rows += f"<tr><td>{i}</td><td>{m}</td><td style='text-align:right'>{money(t)}</td><td style='text-align:center'>{c}</td></tr>"

    # Category table
    cat_rows = ""
    for c, t, a, n in data["categories"]:
        cat_rows += f"<tr><td>{c}</td><td style='text-align:right'>{money(t)}</td><td style='text-align:right'>{money(a)}</td><td style='text-align:center'>{n}</td></tr>"

    # Monthly detail sections
    monthly_sections = ""
    for m in months:
        label = datetime.strptime(m, "%Y-%m").strftime("%B %Y")
        txns = data["monthly_txns"][m]
        m_total = data["monthly_totals"][m]
        txn_rows = ""
        for t in txns:
            txn_rows += f"<tr><td>{t['date'].strftime('%b %d')}</td><td>{t['merchant']}</td><td style='color:#888;font-size:0.85em'>{t['category']}</td><td style='text-align:right'>{money(t['amount'])}</td></tr>"
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
    trend_arrow = "↑" if data["mom_change"] > 0 else "↓" if data["mom_change"] < 0 else "→"
    trend_color = "#e74c3c" if data["mom_change"] > 5 else "#27ae60" if data["mom_change"] < -5 else "#f39c12"

    # AI section
    ai_section = ""
    if ai_html:
        ai_section = f"""
        <section id="recommendations" class="card">
            <h2>AI-Powered Recommendations</h2>
            <div class="ai-recommendations">{ai_html}</div>
        </section>"""

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Expense Dashboard — {months[0]} to {months[-1]}</title>
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
<h1>Expense Dashboard</h1>
<p class="subtitle">Credit card spending analysis: {month_labels[0]} – {month_labels[-1]} | Generated {datetime.now().strftime('%b %d, %Y at %I:%M %p')}</p>

<nav class="toc"><ul>
    <li><a href="#overview">Overview</a></li>
    <li><a href="#charts">Charts</a></li>
    <li><a href="#categories">Categories</a></li>
    <li><a href="#subscriptions">Subscriptions</a></li>
    {'<li><a href="#recommendations">AI Recommendations</a></li>' if ai_html else ''}
    <li><a href="#top-merchants">Top Merchants</a></li>
    <li><a href="#monthly-detail">Monthly Detail</a></li>
</ul></nav>

<!-- Overview Stats -->
<div id="overview"></div>
<div class="stats">
    <div class="stat"><div class="value">{money(data['total'])}</div><div class="label">Total Spend</div></div>
    <div class="stat"><div class="value">{money(data['monthly_avg'])}</div><div class="label">Monthly Average</div></div>
    <div class="stat"><div class="value" style="color:{trend_color}">{trend_arrow} {abs(data['mom_change'])}%</div><div class="label">Month-over-Month</div></div>
    <div class="stat"><div class="value">{len(data['subscriptions'])}</div><div class="label">Recurring Charges</div></div>
</div>

<!-- Charts -->
<div id="charts" class="chart-row">
    <div class="card">
        <h2>Monthly Spending</h2>
        <div class="chart-container"><canvas id="monthlyChart"></canvas></div>
        <noscript><table class="data-table noscript-table"><thead><tr><th>Month</th><th>Amount</th></tr></thead><tbody>{''.join(f'<tr><td>{ml}</td><td style="text-align:right">{money(data["monthly_totals"][m])}</td></tr>' for m, ml in zip(months, month_labels))}</tbody></table></noscript>
    </div>
    <div class="card">
        <h2>Category Breakdown</h2>
        <div class="chart-container"><canvas id="categoryChart"></canvas></div>
        <noscript><table class="data-table noscript-table"><thead><tr><th>Category</th><th>Amount</th></tr></thead><tbody>{''.join(f'<tr><td>{c[0]}</td><td style="text-align:right">{money(c[1])}</td></tr>' for c in data["categories"])}</tbody></table></noscript>
    </div>
</div>

<!-- Category Table -->
<section id="categories" class="card">
    <h2>Category Breakdown</h2>
    <table class="data-table">
        <thead><tr><th>Category</th><th style="text-align:right">Total</th><th style="text-align:right">Monthly Avg</th><th style="text-align:center">Transactions</th></tr></thead>
        <tbody>{cat_rows}</tbody>
        <tfoot><tr style="font-weight:700"><td>Total</td><td style="text-align:right">{money(data['total'])}</td><td style="text-align:right">{money(data['monthly_avg'])}</td><td></td></tr></tfoot>
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

    // Monthly bar chart
    new Chart(document.getElementById('monthlyChart'), {{
        type: 'bar',
        data: {{
            labels: {month_labels_json},
            datasets: [{{
                label: 'Monthly Spend',
                data: {monthly_values},
                backgroundColor: '#4e79a7',
                borderRadius: 6,
            }}]
        }},
        options: {{
            responsive: true,
            plugins: {{
                legend: {{ display: false }},
                tooltip: {{ callbacks: {{ label: ctx => '$' + ctx.parsed.y.toLocaleString(undefined, {{minimumFractionDigits:2}}) }} }}
            }},
            scales: {{
                y: {{
                    beginAtZero: true,
                    ticks: {{ callback: v => '$' + (v/1000).toFixed(0) + 'k' }}
                }}
            }}
        }}
    }});

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
}});
</script>

<footer style="text-align:center;padding:30px;color:var(--muted);font-size:0.85em">
    Generated by Expense Dashboard &amp; Subscription Auditor
</footer>
</body>
</html>"""
    return html


# ── Main ─────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Expense Dashboard & Subscription Auditor")
    parser.add_argument("--path", default=".", help="Folder containing CSV files (default: current directory)")
    parser.add_argument("--ai", action="store_true", help="Generate AI-powered recommendations (requires ANTHROPIC_API_KEY)")
    args = parser.parse_args()

    folder = os.path.abspath(args.path)
    print(f"Reading CSVs from: {folder}")

    # Load user category overrides from categories.csv
    global _user_categories
    _user_categories = load_user_categories(folder)

    transactions = parse_csvs(folder)
    print(f"Loaded {len(transactions)} transactions")

    data = analyze(transactions)
    print(f"Total spend: ${data['total']:,.2f} across {len(data['months'])} months")
    print(f"Found {len(data['subscriptions'])} recurring charges")

    ai_html = None
    if args.ai:
        ai_html = get_ai_recommendations(data)

    html = generate_html(data, ai_html)
    output_path = os.path.join(folder, "dashboard.html")
    with open(output_path, "w", encoding="utf-8") as f:
        f.write(html)

    print(f"\nDashboard written to: {output_path}")
    print("Open it in your browser to view the report.")


if __name__ == "__main__":
    main()
