#!/usr/bin/env python3
"""Generate investments/portfolio.csv from Steadyhand & Wealthsimple statement PDFs + manual overrides."""

import argparse
import csv
import os
import re
import subprocess
import sys


# ── Account name mappings ───────────────────────────────────────────────────
# Map WS account numbers to friendly names (from existing CSV naming convention)
WS_ACCOUNT_NAMES = {
    "HQ8G33Q05CAD": "ETF Income",
    "WK0536LQ4CAD": "Kids RESP",
    "WK4ZQ2P35CAD": "Joint Chequing",
    "WK4ZQZM43CAD": "Ken RRSP (managed)",
    "WK4ZSSJ33USD": "USD Savings",
    "WK61NR900CAD": "Tall Tree (Savings)",
    "WK6MMM500CAD": "Bond Income",
    "WK6RJ7L40CAD": "Ken RRSP (Summit)",
    "WK6S90WK6CAD": "Ken TFSA (Summit)",
}

# Map Steadyhand account numbers to friendly names
SH_ACCOUNT_NAMES = {
    "1054451": "Lisa RRSP (Spousal)",
    "1055896": "Ken TFSA (100% Builders)",
    "1055904": "Ken RRSP 1",
    "1093210": "Lisa inheritance 2 (100% Builders)",
    "1162270": "Stratford Savings",
}

# Map PDF account type text to our Asset Type column
WS_TYPE_MAP = {
    "MANAGED TFSA ACCOUNT": "TFSA",
    "MANAGED RRSP ACCOUNT": "RRSP",
    "MANAGED RESP ACCOUNT": "RESP",
    "MANAGED NON-REGISTERED ACCOUNT": "Non-reg",
    "NON-REGISTERED ACCOUNT": "Non-reg",
    "NON-REGISTERED CASH ACCOUNT": "Non-reg",
    "USD SAVINGS ACCOUNT": "Cash",
    "CHEQUING ACCOUNT": "Cash",
    "ORDER EXECUTION ONLY ACCOUNT": None,  # need sub-type from further in doc
}

SH_TYPE_MAP = {
    "NON-REG": "Non-reg",
    "SP RRSP": "RRSP",
    "TFSA": "TFSA",
    "RRSP": "RRSP",
}

# USD to CAD conversion rate (Jan 2026)
USD_CAD_RATE = 1.3562


def run_pdftotext(pdf_path: str) -> str:
    """Extract text from PDF using pdftotext -layout."""
    try:
        result = subprocess.run(
            ["pdftotext", "-layout", pdf_path, "-"],
            capture_output=True, text=True, check=True,
        )
        return result.stdout
    except FileNotFoundError:
        print("ERROR: pdftotext not found. Install poppler: brew install poppler")
        sys.exit(1)
    except subprocess.CalledProcessError as e:
        print(f"ERROR: pdftotext failed on {pdf_path}: {e.stderr}")
        return ""


def parse_wealthsimple_pdf(pdf_path: str) -> dict | None:
    """Parse a Wealthsimple monthly statement PDF and extract account data."""
    text = run_pdftotext(pdf_path)
    if not text:
        return None

    # Determine currency from header
    currency = "CAD"
    if "All figures in $USD" in text:
        currency = "USD"

    # Extract account number — on the line below "Account No." header
    # Account numbers are alphanumeric codes like WK6S90WK6CAD or HQ8G33Q05CAD
    # They contain both letters and digits, distinguishing them from English words
    acct_match = re.search(r"\b([A-Z]{2}[A-Z0-9]{6,}(?:CAD|USD))\b", text)
    if not acct_match:
        return None
    account_no = acct_match.group(1)

    # Extract owner from the same values line (after account number)
    owner_line_match = re.search(
        re.escape(account_no) + r"\s{2,}(.+?)\s{2,}\d{4}-\d{2}-\d{2}", text,
    )
    owner = owner_line_match.group(1).strip() if owner_line_match else "Unknown"

    # Extract account type — appears as a standalone centered line like "Managed TFSA Account"
    asset_type = None
    type_patterns = [
        (r"Managed TFSA Account", "TFSA"),
        (r"Managed RRSP Account", "RRSP"),
        (r"Managed RESP Account", "RESP"),
        (r"Managed Non-Registered Account", "Non-reg"),
        (r"USD Savings Account", "Cash"),
        (r"Chequing Account", "Cash"),
        (r"Non-Registered Cash Account", "Non-reg"),
    ]
    for pattern, mapped in type_patterns:
        if re.search(pattern, text, re.IGNORECASE):
            asset_type = mapped
            break

    if asset_type is None:
        asset_type = "Non-reg"

    # Extract total portfolio value
    total_match = re.search(r"Total Portfolio\s+\$([\d,]+\.\d{2})", text)
    if not total_match:
        return None
    total_value = float(total_match.group(1).replace(",", ""))

    # Convert USD to CAD if needed
    total_value_cad = total_value
    if currency == "USD":
        total_value_cad = total_value * USD_CAD_RATE

    # Extract holdings (symbols from Portfolio Assets table)
    # Symbols appear in a column between fund name and quantity, e.g.:
    #  BMO Aggregate Bond Index ETF   ZAG   428.7273 ...
    holdings = []
    SKIP_WORDS = {"Total", "Bought", "Sold", "Cash", "Date", "Symbol"}
    in_portfolio_assets = False
    for line in text.split("\n"):
        if "Portfolio Assets" in line:
            in_portfolio_assets = True
            continue
        if in_portfolio_assets and ("Activity" in line or "Portfolio Cash" in line):
            in_portfolio_assets = False
            continue
        if in_portfolio_assets:
            # Match lines with Symbol column: name  SYMBOL  quantity
            sym_match = re.match(r".*?\s{3,}(\w+(?:\.\w+)?)\s+[\d,]+\.\d{4}", line)
            if sym_match:
                symbol = sym_match.group(1)
                if (symbol not in holdings and symbol not in SKIP_WORDS
                        and len(symbol) <= 8 and symbol.upper() == symbol):
                    holdings.append(symbol)

    # Get account name from our mapping
    account_name = WS_ACCOUNT_NAMES.get(account_no, f"{asset_type} - {owner}")

    # For corporate accounts, check owner for Tall Tree
    if "Tall Tree" in owner:
        asset_type = "Corporate"

    # Extract account suffix from account number
    acct_suffix = account_no

    return {
        "account": account_name,
        "brokerage": "Wealthsimple",
        "asset_type": asset_type,
        "acct_suffix": acct_suffix,
        "total_value_cad": total_value_cad,
        "all_time_return": "TBD",
        "yield": "TBD",
        "holdings": ", ".join(holdings) if holdings else "",
        "currency": currency,
    }


def parse_steadyhand_pdf(pdf_path: str) -> list[dict]:
    """Parse a Steadyhand quarterly PDF and extract all account data."""
    text = run_pdftotext(pdf_path)
    if not text:
        return []

    accounts = []

    # Parse "Your Accounts" table on page 2
    # Format: Number  Owner  Type  Market Value
    acct_table = re.findall(
        r"^(\d{6,7})\s+(.+?)\s{2,}(NON-REG|SP RRSP|TFSA|RRSP)\s+([\d,]+\.\d{2})\s*$",
        text, re.MULTILINE,
    )

    # Parse per-account "Since Inception" return rates
    # These appear in Account Performance sections like:
    #   Account 1055904  RRSP
    #   ...
    #   Since Inception    6.9
    account_returns = {}
    # Find all account sections with their since-inception rates
    sections = re.split(r"Account\s+(\d{6,7})\s+", text)
    for i in range(1, len(sections), 2):
        acct_num = sections[i]
        section_text = sections[i + 1] if i + 1 < len(sections) else ""
        inception_match = re.search(
            r"Since Inception\s+([\d.]+)", section_text,
        )
        if inception_match:
            account_returns[acct_num] = float(inception_match.group(1))

    # Parse holdings per account
    account_holdings = {}
    for i in range(1, len(sections), 2):
        acct_num = sections[i]
        section_text = sections[i + 1] if i + 1 < len(sections) else ""
        holdings = []
        valid_funds = {"Savings", "Income", "Founders", "Builders"}
        for fund_match in re.finditer(r"(Steadyhand (\w+) Fund)", section_text):
            fund_name = fund_match.group(1)
            fund_type = fund_match.group(2)
            if fund_name not in holdings and fund_type in valid_funds:
                holdings.append(fund_name)
        if holdings:
            account_holdings[acct_num] = holdings

    for acct_num, owner, acct_type, market_value_str in acct_table:
        market_value = float(market_value_str.replace(",", ""))

        # Skip zero-value accounts
        if market_value <= 0:
            continue

        asset_type = SH_TYPE_MAP.get(acct_type, acct_type)
        since_inception = account_returns.get(acct_num, 0.0)
        account_name = SH_ACCOUNT_NAMES.get(acct_num, f"{asset_type} - {owner.strip()}")

        # Compute yield as total_value * since_inception_rate
        annual_yield = market_value * (since_inception / 100)

        # Get holdings for this account
        holdings = account_holdings.get(acct_num, [])

        # Use last 4 digits as suffix
        acct_suffix = acct_num

        accounts.append({
            "account": account_name,
            "brokerage": "Steadyhand",
            "asset_type": asset_type,
            "acct_suffix": acct_suffix,
            "total_value_cad": market_value,
            "all_time_return": f"{since_inception:.2f}%",
            "yield": f"${annual_yield:,.2f}",
            "holdings": ", ".join(holdings) if holdings else "",
        })

    return accounts


def load_overrides(folder: str) -> list[dict]:
    """Load manual overrides from investments/overrides.csv."""
    overrides_path = os.path.join(folder, "investments", "overrides.csv")
    if not os.path.exists(overrides_path):
        return []

    entries = []
    with open(overrides_path, newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        for row in reader:
            account = row.get("Account", "").strip()
            if not account:
                continue

            val_str = row.get("Total Value (CAD)", "").strip().replace("$", "").replace(",", "")
            try:
                total_value = float(val_str)
            except (ValueError, TypeError):
                total_value = 0.0

            entries.append({
                "account": account,
                "brokerage": row.get("Brokerage", "").strip(),
                "asset_type": row.get("Asset Type", "").strip(),
                "acct_suffix": row.get("Acct Suffix", "").strip(),
                "total_value_cad": total_value,
                "all_time_return": row.get("All Time Return", "").strip(),
                "yield": row.get("Yield", "").strip(),
                "holdings": row.get("Holdings", "").strip(),
            })

    return entries


def write_portfolio_csv(entries: list[dict], output_path: str):
    """Write portfolio.csv with the standard columns."""
    fieldnames = [
        "Account", "Brokerage", "Asset Type", "Acct Suffix",
        "Total Value (CAD)", "All Time Return", "Yield", "Holdings",
    ]

    # Sort by total value descending
    entries.sort(key=lambda e: e["total_value_cad"], reverse=True)

    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(fieldnames)

        for e in entries:
            val = e["total_value_cad"]
            val_formatted = f"${val:,.2f}"

            writer.writerow([
                e["account"],
                e["brokerage"],
                e["asset_type"],
                e["acct_suffix"],
                val_formatted,
                e["all_time_return"],
                e["yield"],
                e["holdings"],
            ])

    print(f"Wrote {len(entries)} accounts to {output_path}")


def main():
    parser = argparse.ArgumentParser(description="Generate portfolio.csv from statement PDFs")
    parser.add_argument("--path", default=".", help="Base folder (default: current directory)")
    parser.add_argument("--force", action="store_true", help="Overwrite existing portfolio.csv")
    args = parser.parse_args()

    folder = os.path.abspath(args.path)
    invest_dir = os.path.join(folder, "investments")
    output_path = os.path.join(invest_dir, "portfolio.csv")

    if os.path.exists(output_path) and not args.force:
        print(f"portfolio.csv already exists. Use --force to overwrite.")
        sys.exit(1)

    all_entries = []

    # ── Parse Steadyhand PDFs (use most recent = December 2025) ─────────
    sh_dir = os.path.join(invest_dir, "Steadyhand")
    if os.path.isdir(sh_dir):
        # Find the most recent quarterly PDF
        pdfs = sorted(
            [f for f in os.listdir(sh_dir) if f.endswith(".pdf") and not f.startswith(".")],
        )
        # Prefer December as most recent
        target_pdf = None
        for pdf in pdfs:
            if "December" in pdf or "Dec" in pdf:
                target_pdf = pdf
                break
        if not target_pdf and pdfs:
            target_pdf = pdfs[-1]  # fall back to last alphabetically

        if target_pdf:
            pdf_path = os.path.join(sh_dir, target_pdf)
            print(f"Parsing Steadyhand: {target_pdf}")
            sh_accounts = parse_steadyhand_pdf(pdf_path)
            print(f"  Found {len(sh_accounts)} active accounts")
            all_entries.extend(sh_accounts)
        else:
            print("No Steadyhand PDFs found")
    else:
        print(f"Steadyhand directory not found: {sh_dir}")

    # ── Parse Wealthsimple monthly statement PDFs ───────────────────────
    ws_dir = os.path.join(invest_dir, "Wealthsimple")
    if os.path.isdir(ws_dir):
        ws_pdfs = sorted([
            f for f in os.listdir(ws_dir)
            if f.endswith(".pdf") and "_2026-01_v_1" in f and not f.startswith(".")
        ])
        print(f"Parsing {len(ws_pdfs)} Wealthsimple monthly statements")
        for pdf_name in ws_pdfs:
            pdf_path = os.path.join(ws_dir, pdf_name)
            result = parse_wealthsimple_pdf(pdf_path)
            if result:
                # Skip zero-value accounts
                if result["total_value_cad"] <= 0:
                    print(f"  Skipping {result['account']} (zero value)")
                    continue
                print(f"  {result['account']}: ${result['total_value_cad']:,.2f}"
                      f" ({result.get('currency', 'CAD')})")
                all_entries.append(result)
    else:
        print(f"Wealthsimple directory not found: {ws_dir}")

    # ── Load manual overrides ───────────────────────────────────────────
    overrides = load_overrides(folder)
    if overrides:
        print(f"Loaded {len(overrides)} manual overrides")
        all_entries.extend(overrides)

    if not all_entries:
        print("No accounts found from any source!")
        sys.exit(1)

    # ── Write output ────────────────────────────────────────────────────
    write_portfolio_csv(all_entries, output_path)

    # Print summary
    total = sum(e["total_value_cad"] for e in all_entries)
    print(f"\nTotal portfolio: ${total:,.2f}")
    print(f"  Steadyhand: {sum(1 for e in all_entries if e['brokerage'] == 'Steadyhand')} accounts")
    print(f"  Wealthsimple: {sum(1 for e in all_entries if e['brokerage'] == 'Wealthsimple')} accounts")
    print(f"  Overrides: {len(overrides)} accounts")


if __name__ == "__main__":
    main()
