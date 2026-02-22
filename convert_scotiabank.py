#!/usr/bin/env python3
"""
ScotiaBank PDF e-statement → CSV converter.

Converts ScotiaBank PDF statements into CSV files matching Wealthsimple export
format so they can be consumed by dashboard.py.

Supports:
  - Personal VISA credit card  (credit card/Scotiabank VISA/)
  - Personal debit chequing    (debit card/Scotiabank - Chequing/)
  - Corporate VISA credit card (corporate/ScotiaBank/*VISA*)
  - Corporate debit card       (corporate/ScotiaBank/*DebitCard*)

Usage:
    python3 convert_scotiabank.py              # current directory
    python3 convert_scotiabank.py --path /dir  # explicit path
    python3 convert_scotiabank.py --force      # overwrite existing CSVs
"""

import argparse
import csv
import os
import re
import sys

try:
    import pdfplumber
except ImportError:
    print("Error: pdfplumber is required. Install with: pip install pdfplumber")
    sys.exit(1)


# ── Month helpers ────────────────────────────────────────────────────────────

MONTH_ABBR = {
    "Jan": 1, "Feb": 2, "Mar": 3, "Apr": 4, "May": 5, "Jun": 6,
    "Jul": 7, "Aug": 8, "Sep": 9, "Oct": 10, "Nov": 11, "Dec": 12,
}

MONTH_FULL = {
    "January": 1, "February": 2, "March": 3, "April": 4, "May": 5,
    "June": 6, "July": 7, "August": 8, "September": 9, "October": 10,
    "November": 11, "December": 12,
}


def parse_statement_period(text: str) -> tuple:
    """Extract (start_month, start_year, end_month, end_year) from statement header.

    Matches patterns like: "Dec 14, 2024 -Jan 13, 2025" or "Dec 14, 2024 - Jan 13, 2025"
    """
    m = re.search(
        r"StatementPeriod\s+(\w{3})\s+\d+,?\s*(\d{4})\s*-\s*(\w{3})\s+\d+,?\s*(\d{4})",
        text,
    )
    if m:
        return (
            MONTH_ABBR[m.group(1)],
            int(m.group(2)),
            MONTH_ABBR[m.group(3)],
            int(m.group(4)),
        )
    return None


def infer_year(month_abbr: str, start_month: int, start_year: int,
               end_month: int, end_year: int) -> int:
    """Assign correct year to a transaction month within a statement period."""
    m = MONTH_ABBR[month_abbr]
    if start_year == end_year:
        return start_year
    # Statement crosses year boundary (e.g. Dec 2024 - Jan 2025)
    if m >= start_month:
        return start_year
    return end_year


# ── Credit card parser ───────────────────────────────────────────────────────

# Matches transaction lines: REF# TRANS_DATE POST_DATE DETAILS AMOUNT
# Examples:
#   001 Dec 13 Dec 14 THE STADIUM MARKET VANCOUVER BC 8.98
#   018 Dec 19 Dec 20 PAYMENT FROM - *****10*5327 424.23-
VISA_TXN_RE = re.compile(
    r"^\s*(\d{3})\s+"           # REF#
    r"(\w{3})\s+(\d{1,2})\s+"  # TRANS DATE (Mon DD)
    r"(\w{3})\s+(\d{1,2})\s+"  # POST DATE (Mon DD)
    r"(.+?)\s+"                 # DETAILS (greedy but stops at amount)
    r"([\d,]+\.\d{2}-?)\s*$"   # AMOUNT (with optional trailing -)
)

# Lines to skip
SKIP_PATTERNS = [
    re.compile(r"^\s*AMT\s+[\d,.]+\s+USD"),      # FX detail line
    re.compile(r"^\s*SUB-TOTAL\s"),                # Sub-total lines
    re.compile(r"^\s*Continued\s*on"),             # Page continuation marker
    re.compile(r"^\s*StatementPeriod\s"),           # Page header
    re.compile(r"^\s*StatementDate\s"),
    re.compile(r"^\s*Account#\s"),
    re.compile(r"^\s*Page\s+\d+\s+of"),
    re.compile(r"^\s*Scotia\s+Momentum"),
    re.compile(r"^\s*Scotiabank\s+Passport"),
    re.compile(r"^\s*VISA\s+Infinite"),
    re.compile(r"^\s*Visa\s+Infinite"),
    re.compile(r"^\s*TRANS\.\s+POST"),
    re.compile(r"^\s*REF\.#\s+DATE"),
    re.compile(r"^\s*Transactions\s"),
    re.compile(r"^\s*MR[S]?\s+\w+.*-\s*\d{4}"),   # Cardholder section header
    re.compile(r"^\s*Interest\s+charges"),
    re.compile(r"^\s*Cash\s+advances"),
    re.compile(r"^\s*Specialrateoffers"),
    re.compile(r"^\s*Purchases\s+\$"),
]


def parse_visa_pdf(pdf_path: str) -> list[dict]:
    """Parse a ScotiaBank VISA credit card PDF into transaction dicts."""
    transactions = []
    period = None

    with pdfplumber.open(pdf_path) as pdf:
        for page in pdf.pages:
            text = page.extract_text()
            if not text:
                continue

            # Get statement period from first page
            if period is None:
                period = parse_statement_period(text)
                if period is None:
                    print(f"  WARNING: Could not parse statement period from {pdf_path}")
                    return []

            start_month, start_year, end_month, end_year = period

            for line in text.split("\n"):
                # Skip known non-transaction lines
                if any(p.search(line) for p in SKIP_PATTERNS):
                    continue

                m = VISA_TXN_RE.match(line)
                if not m:
                    continue

                trans_mon = m.group(2)
                trans_day = int(m.group(3))
                post_mon = m.group(4)
                post_day = int(m.group(5))
                details = m.group(6).strip()
                amount_str = m.group(7)

                # Parse amount — trailing '-' means credit/payment
                is_credit = amount_str.endswith("-")
                amount = float(amount_str.rstrip("-").replace(",", ""))
                if is_credit:
                    amount = -amount

                # Infer years
                trans_year = infer_year(trans_mon, start_month, start_year, end_month, end_year)
                post_year = infer_year(post_mon, start_month, start_year, end_month, end_year)

                trans_date = f"{trans_year}-{MONTH_ABBR[trans_mon]:02d}-{trans_day:02d}"
                post_date = f"{post_year}-{MONTH_ABBR[post_mon]:02d}-{post_day:02d}"

                txn_type = "Payment" if is_credit else "Purchase"

                transactions.append({
                    "transaction_date": trans_date,
                    "post_date": post_date,
                    "type": txn_type,
                    "details": details,
                    "amount": amount,
                    "currency": "CAD",
                })

    return transactions


def write_visa_csv(transactions: list[dict], out_path: str):
    """Write credit card transactions to CSV in Wealthsimple format."""
    with open(out_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f, quoting=csv.QUOTE_ALL)
        writer.writerow(["transaction_date", "post_date", "type", "details", "amount", "currency"])
        for t in sorted(transactions, key=lambda x: x["transaction_date"]):
            writer.writerow([
                t["transaction_date"],
                t["post_date"],
                t["type"],
                t["details"],
                t["amount"],
                t["currency"],
            ])


# ── Debit chequing parser ───────────────────────────────────────────────────

# Column x1 boundaries (right edge) for ScotiaBank personal chequing PDFs:
#   Withdrawn: x1 ≈ 306
#   Deposited: x1 ≈ 371
#   Balance:   x1 ≈ 436
CHEQUING_WITHDRAWN_X1_MAX = 330   # amounts with x1 < this are withdrawals
CHEQUING_DEPOSITED_X1_MAX = 390   # amounts with x1 < this are deposits
# amounts with x1 >= 390 are balance

# Transaction types to include (mapped to WS format)
INCLUDE_TYPES = {
    "Pointofsalepurchase": "SPEND",
    "Insurance": "AFT_OUT",       # only withdrawals; deposits are skipped
    "Mortgage": "AFT_OUT",
    "Loans": "AFT_OUT",
    "Autopayment": "AFT_OUT",
    "Billpayment": "OBP_OUT",
    "MB-Billpayment": "OBP_OUT",
    "PCBillpayment": "OBP_OUT",
    "Hydrobill": "OBP_OUT",
}

# Transaction types to skip entirely
SKIP_TYPES = {
    "OpeningBalance", "ClosingBalance",
    "Deposit", "Withdrawal",       # E-Transfers
    "MB-Transferto", "MB-Transferfrom",
    "Investment", "Mutualfunds", "Misc.payment",
    "Payrolldep.", "ABMwithdrawal", "SharedABMwithdrawal",
    "Servicecharge", "PCTransfer", "PCTransferto", "PCTransferfrom",
    "CreditCard/LOCpayment", "Debitmemo", "BRTransferfrom",
    "INTERACABMfee", "Errorcorrection",
}

# Description templates per WS transaction type
DESC_TEMPLATES = {
    "SPEND": lambda merchant: merchant,
    "AFT_OUT": lambda merchant: f"Pre-authorized Debit to {merchant}",
    "OBP_OUT": lambda merchant: f"Online bill payment for {merchant}",
}


def parse_chequing_pdf(pdf_path: str) -> list[dict]:
    """Parse a ScotiaBank personal chequing PDF into transaction dicts."""
    transactions = []

    with pdfplumber.open(pdf_path) as pdf:
        # Extract statement period from header text to determine year context.
        # Format: "OpeningBalanceonDecember18,2024" ... "ClosingBalanceonJanuary17,2025"
        first_text = pdf.pages[0].extract_text() or ""

        open_match = re.search(r"OpeningBalanceon(\w+?)(\d{1,2}),(\d{4})", first_text)
        close_match = re.search(r"ClosingBalanceon(\w+?)(\d{1,2}),(\d{4})", first_text)

        if not open_match or not close_match:
            print(f"  WARNING: Could not parse statement period from {pdf_path}")
            return []

        start_month_name = open_match.group(1)
        start_year = int(open_match.group(3))
        end_month_name = close_match.group(1)
        end_year = int(close_match.group(3))
        start_month = MONTH_FULL.get(start_month_name, 0)
        end_month = MONTH_FULL.get(end_month_name, 0)

        def year_for_month(mon_abbr: str) -> int:
            """Assign year based on statement period."""
            m = MONTH_ABBR.get(mon_abbr, 0)
            if start_year == end_year:
                return start_year
            # Statement crosses year boundary (e.g. Dec 2024 - Jan 2025)
            if m >= start_month:
                return start_year
            return end_year

        for page in pdf.pages:
            words = page.extract_words()

            # Filter out decorative/margin elements (x0 < 50)
            words = [w for w in words if w["x0"] >= 50]

            # Build amount lookup: y-position → (amount, column_type)
            import collections
            amount_rows = collections.defaultdict(list)
            for w in words:
                if re.match(r"^\d[\d,]*\.\d{2}$", w["text"]):
                    x1 = w["x1"]
                    if x1 < CHEQUING_WITHDRAWN_X1_MAX:
                        col = "withdrawn"
                    elif x1 < CHEQUING_DEPOSITED_X1_MAX:
                        col = "deposited"
                    else:
                        col = "balance"
                    y_key = round(w["top"])
                    amount_rows[y_key].append((col, float(w["text"].replace(",", ""))))

            # Parse using word positions to find date entries
            # Group words by y-position into lines
            from collections import defaultdict
            word_lines = defaultdict(list)
            for w in words:
                y_key = round(w["top"])
                word_lines[y_key].append(w)

            # Sort lines by y position
            sorted_y = sorted(word_lines.keys())

            # Find date entries: first word matches "MonDD" pattern (e.g. "Jan18")
            pending_txn = None
            date_re = re.compile(r"^(\w{3})(\d{1,2})$")

            for y_idx, y_key in enumerate(sorted_y):
                line_words = sorted(word_lines[y_key], key=lambda w: w["x0"])
                if not line_words:
                    continue

                first_word = line_words[0]["text"]
                dm = date_re.match(first_word)

                if dm:
                    # This is a date line — finalize any pending transaction first
                    if pending_txn:
                        _finalize_chequing_txn(pending_txn, transactions)
                        pending_txn = None

                    mon_abbr = dm.group(1)
                    day = int(dm.group(2))

                    if mon_abbr not in MONTH_ABBR:
                        continue

                    year = year_for_month(mon_abbr)
                    date_str = f"{year}-{MONTH_ABBR[mon_abbr]:02d}-{day:02d}"

                    # Transaction type is the second "word" (concatenated by pdfplumber)
                    txn_type = line_words[1]["text"] if len(line_words) > 1 else ""

                    # Get amounts from this line
                    withdrawn_amt = None
                    deposited_amt = None
                    balance_amt = None
                    for y_search in range(y_key - 2, y_key + 3):
                        for col, amt in amount_rows.get(y_search, []):
                            if col == "withdrawn":
                                withdrawn_amt = amt
                            elif col == "deposited":
                                deposited_amt = amt
                            elif col == "balance":
                                balance_amt = amt

                    is_withdrawal = withdrawn_amt is not None
                    is_deposit = deposited_amt is not None
                    amount = withdrawn_amt if is_withdrawal else deposited_amt

                    if amount is None:
                        continue

                    if txn_type in SKIP_TYPES:
                        continue

                    # Insurance deposits are benefit reimbursements — skip
                    if txn_type == "Insurance" and is_deposit:
                        continue

                    ws_type = INCLUDE_TYPES.get(txn_type)
                    if ws_type is None:
                        continue

                    pending_txn = {
                        "date": date_str,
                        "txn_type": txn_type,
                        "ws_type": ws_type,
                        "amount": amount,
                        "balance": balance_amt,
                        "is_withdrawal": is_withdrawal,
                        "merchant_detail": None,
                    }

                elif pending_txn:
                    # Non-date line after a pending transaction = merchant detail.
                    # Some entries have multiple detail lines (e.g. Insurance shows
                    # policy name on line 1, merchant on line 2). Keep updating
                    # merchant_detail — the last detail line is the actual merchant.
                    detail_text = " ".join(w["text"] for w in line_words)
                    # Skip header-like lines and page markers
                    if detail_text.startswith(("Amounts", "Date", "Here", "MR", "Your", "Page")):
                        continue
                    if "continuedonnextpage" in detail_text.lower():
                        continue
                    # Clean up — remove leading digits (e-transfer refs)
                    merchant = re.sub(r"^\d+", "", detail_text).strip()
                    if merchant:
                        pending_txn["merchant_detail"] = merchant

            # Finalize any remaining pending transaction
            if pending_txn:
                _finalize_chequing_txn(pending_txn, transactions)

    return transactions


def _finalize_chequing_txn(txn: dict, transactions: list):
    """Convert a parsed chequing transaction into WS debit CSV format."""
    merchant = txn.get("merchant_detail") or txn["txn_type"]

    # Clean up merchant name — remove ScotiaBank formatting artifacts
    # e.g. "CostcoWholesaleW552VancouverBCCA" → "CostcoWholesaleW552VancouverBCCA"
    # e.g. "FortisbcHoldingsInc." → "FortisbcHoldingsInc."
    # Leave as-is; normalize_merchant in dashboard.py handles cleanup

    ws_type = txn["ws_type"]
    description = DESC_TEMPLATES[ws_type](merchant)
    amount = -txn["amount"]  # WS debit format: withdrawals are negative

    transactions.append({
        "date": txn["date"],
        "transaction": ws_type,
        "description": description,
        "amount": amount,
        "balance": txn["balance"] if txn["balance"] is not None else "",
        "currency": "CAD",
    })


def write_chequing_csv(transactions: list[dict], out_path: str):
    """Write debit transactions to CSV in Wealthsimple format."""
    with open(out_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f, quoting=csv.QUOTE_ALL)
        writer.writerow(["date", "transaction", "description", "amount", "balance", "currency"])
        for t in sorted(transactions, key=lambda x: x["date"]):
            writer.writerow([
                t["date"],
                t["transaction"],
                t["description"],
                t["amount"],
                t["balance"],
                t["currency"],
            ])


# ── Corporate debit parser ──────────────────────────────────────────────────

# Corporate business account PDFs have a different format:
# - Dates as MM/DD/YYYY
# - Different transaction types: DEBIT MEMO, SERVICE CHARGE, TRANSFER FROM, etc.
# - Different column layout

CORP_DEBIT_WITHDRAWN_X1_MAX = 360
CORP_DEBIT_DEPOSITED_X1_MAX = 430


def parse_corporate_debit_pdf(pdf_path: str) -> list[dict]:
    """Parse a ScotiaBank corporate debit (business account) PDF."""
    transactions = []

    with pdfplumber.open(pdf_path) as pdf:
        for page in pdf.pages:
            text = page.extract_text(layout=True) or ""
            words = page.extract_words()

            # Build amount lookup by y-position
            import collections
            amount_rows = collections.defaultdict(list)
            for w in words:
                if re.match(r"^[\d,]+\.\d{2}$", w["text"]):
                    x1 = w["x1"]
                    if x1 < CORP_DEBIT_WITHDRAWN_X1_MAX:
                        col = "withdrawn"
                    elif x1 < CORP_DEBIT_DEPOSITED_X1_MAX:
                        col = "deposited"
                    else:
                        col = "balance"
                    y_key = round(w["top"])
                    amount_rows[y_key].append((col, float(w["text"].replace(",", ""))))

            lines = text.split("\n")
            i = 0
            while i < len(lines):
                line = lines[i].strip()

                # Match date line: MM/DD/YYYY DESCRIPTION ...
                date_match = re.match(r"(\d{2}/\d{2}/\d{4})\s+(.+)", line)
                if not date_match:
                    i += 1
                    continue

                date_raw = date_match.group(1)  # MM/DD/YYYY
                rest = date_match.group(2).strip()

                # Convert date
                parts = date_raw.split("/")
                date_str = f"{parts[2]}-{parts[0]}-{parts[1]}"

                # Skip BALANCE FORWARD
                if "BALANCE FORWARD" in rest:
                    i += 1
                    continue

                # Collect description lines (subsequent indented lines)
                desc_parts = [rest]
                j = i + 1
                while j < len(lines):
                    next_line = lines[j].strip()
                    if not next_line or re.match(r"\d{2}/\d{2}/\d{4}", next_line):
                        break
                    if next_line.startswith("No. of Debits") or next_line.startswith("Uncollected"):
                        break
                    desc_parts.append(next_line)
                    j += 1

                full_desc = " | ".join(desc_parts)

                # Determine amount and direction from word positions
                line_y = None
                for w in words:
                    if w["text"] == date_raw:
                        line_y = round(w["top"])
                        break

                withdrawn_amt = None
                deposited_amt = None
                balance_amt = None

                if line_y is not None:
                    for y_key in range(line_y - 3, line_y + 4):
                        for col, amt in amount_rows.get(y_key, []):
                            if col == "withdrawn":
                                withdrawn_amt = amt
                            elif col == "deposited":
                                deposited_amt = amt
                            elif col == "balance":
                                balance_amt = amt

                is_withdrawal = withdrawn_amt is not None
                amount = withdrawn_amt if is_withdrawal else deposited_amt

                if amount is None:
                    i = j
                    continue

                # Map to WS transaction types
                # Corporate account: DEBIT MEMO = e-transfer out, TRANSFER FROM = transfer in,
                # SERVICE CHARGE = fees
                if "DEBIT MEMO" in rest:
                    ws_type = "E_TRFOUT"
                    ws_amount = -amount
                elif "SERVICE CHARGE" in rest:
                    ws_type = "SPEND"
                    ws_amount = -amount
                elif "TRANSFER FROM" in rest:
                    ws_type = "TRFIN"
                    ws_amount = amount
                else:
                    # Unknown type — include as-is
                    ws_type = "SPEND" if is_withdrawal else "TRFIN"
                    ws_amount = -amount if is_withdrawal else amount

                transactions.append({
                    "date": date_str,
                    "transaction": ws_type,
                    "description": full_desc,
                    "amount": ws_amount,
                    "balance": balance_amt if balance_amt is not None else "",
                    "currency": "CAD",
                })

                i = j

    return transactions


# ── Output filename helpers ──────────────────────────────────────────────────

def output_path_for_visa(pdf_path: str) -> str:
    """Generate output CSV path from PDF path.

    Input:  .../Scotiabank VISA/January 2025 e-statement.pdf
    Output: .../Scotiabank VISA/scotiabank-visa-2025-01.csv
    """
    dirname = os.path.dirname(pdf_path)
    basename = os.path.basename(pdf_path)
    m = re.match(r"(\w+)\s+(\d{4})\s+e-statement\.pdf", basename)
    if not m:
        return None
    month_name, year = m.group(1), int(m.group(2))
    month_num = MONTH_FULL.get(month_name)
    if month_num is None:
        return None
    return os.path.join(dirname, f"scotiabank-visa-{year}-{month_num:02d}.csv")


def output_path_for_chequing(pdf_path: str) -> str:
    """Generate output CSV path from PDF path.

    Input:  .../Scotiabank - Chequing/January 2025 e-statement.pdf
    Output: .../Scotiabank - Chequing/scotiabank-chequing-2025-01.csv
    """
    dirname = os.path.dirname(pdf_path)
    basename = os.path.basename(pdf_path)
    m = re.match(r"(\w+)\s+(\d{4})\s+e-statement\.pdf", basename)
    if not m:
        return None
    month_name, year = m.group(1), int(m.group(2))
    month_num = MONTH_FULL.get(month_name)
    if month_num is None:
        return None
    return os.path.join(dirname, f"scotiabank-chequing-{year}-{month_num:02d}.csv")


def output_path_for_corporate(pdf_path: str, card_type: str) -> str:
    """Generate output CSV path for corporate PDFs.

    Input:  .../ScotiaBank/Tall Tree Technology - VISA - December 2025 e-statement.pdf
    Output: .../ScotiaBank/scotiabank-corp-visa-2025-12.csv
    """
    dirname = os.path.dirname(pdf_path)
    basename = os.path.basename(pdf_path)
    m = re.match(r".+?-\s*(\w+)\s+(\d{4})\s+e-statement\.pdf", basename)
    if not m:
        return None
    month_name, year = m.group(1), int(m.group(2))
    month_num = MONTH_FULL.get(month_name)
    if month_num is None:
        return None
    return os.path.join(dirname, f"scotiabank-corp-{card_type}-{year}-{month_num:02d}.csv")


# ── Main ─────────────────────────────────────────────────────────────────────

def convert_all(base_path: str, force: bool = False):
    """Find and convert all ScotiaBank PDFs under base_path."""
    converted = 0
    skipped = 0
    errors = 0

    # ── Personal VISA credit card ──
    visa_dir = os.path.join(base_path, "credit card", "Scotiabank VISA")
    if os.path.isdir(visa_dir):
        pdfs = sorted(f for f in os.listdir(visa_dir) if f.endswith(".pdf"))
        print(f"\n{'─'*60}")
        print(f"Personal VISA credit card: {len(pdfs)} PDFs in {visa_dir}")
        for pdf_name in pdfs:
            pdf_path = os.path.join(visa_dir, pdf_name)
            out_path = output_path_for_visa(pdf_path)
            if out_path is None:
                print(f"  SKIP (bad filename): {pdf_name}")
                skipped += 1
                continue
            if os.path.exists(out_path) and not force:
                print(f"  SKIP (exists): {os.path.basename(out_path)}")
                skipped += 1
                continue
            try:
                txns = parse_visa_pdf(pdf_path)
                write_visa_csv(txns, out_path)
                purchases = sum(1 for t in txns if t["type"] == "Purchase")
                payments = sum(1 for t in txns if t["type"] == "Payment")
                print(f"  OK: {os.path.basename(out_path)} — {purchases} purchases, {payments} payments")
                converted += 1
            except Exception as e:
                print(f"  ERROR: {pdf_name} — {e}")
                errors += 1

    # ── Personal debit chequing ──
    cheq_dir = os.path.join(base_path, "debit card", "Scotiabank - Chequing")
    if os.path.isdir(cheq_dir):
        pdfs = sorted(f for f in os.listdir(cheq_dir) if f.endswith(".pdf"))
        print(f"\n{'─'*60}")
        print(f"Personal debit chequing: {len(pdfs)} PDFs in {cheq_dir}")
        for pdf_name in pdfs:
            pdf_path = os.path.join(cheq_dir, pdf_name)
            out_path = output_path_for_chequing(pdf_path)
            if out_path is None:
                print(f"  SKIP (bad filename): {pdf_name}")
                skipped += 1
                continue
            if os.path.exists(out_path) and not force:
                print(f"  SKIP (exists): {os.path.basename(out_path)}")
                skipped += 1
                continue
            try:
                txns = parse_chequing_pdf(pdf_path)
                type_counts = {}
                for t in txns:
                    type_counts[t["transaction"]] = type_counts.get(t["transaction"], 0) + 1
                summary = ", ".join(f"{v} {k}" for k, v in sorted(type_counts.items()))
                print(f"  OK: {os.path.basename(out_path)} — {summary}")
                converted += 1
                write_chequing_csv(txns, out_path)
            except Exception as e:
                print(f"  ERROR: {pdf_name} — {e}")
                errors += 1

    # ── Corporate VISA ──
    corp_dir = os.path.join(base_path, "corporate", "ScotiaBank")
    if os.path.isdir(corp_dir):
        visa_pdfs = sorted(f for f in os.listdir(corp_dir)
                           if f.endswith(".pdf") and "VISA" in f)
        if visa_pdfs:
            print(f"\n{'─'*60}")
            print(f"Corporate VISA: {len(visa_pdfs)} PDFs in {corp_dir}")
            for pdf_name in visa_pdfs:
                pdf_path = os.path.join(corp_dir, pdf_name)
                out_path = output_path_for_corporate(pdf_path, "visa")
                if out_path is None:
                    print(f"  SKIP (bad filename): {pdf_name}")
                    skipped += 1
                    continue
                if os.path.exists(out_path) and not force:
                    print(f"  SKIP (exists): {os.path.basename(out_path)}")
                    skipped += 1
                    continue
                try:
                    txns = parse_visa_pdf(pdf_path)
                    write_visa_csv(txns, out_path)
                    purchases = sum(1 for t in txns if t["type"] == "Purchase")
                    payments = sum(1 for t in txns if t["type"] == "Payment")
                    print(f"  OK: {os.path.basename(out_path)} — {purchases} purchases, {payments} payments")
                    converted += 1
                except Exception as e:
                    print(f"  ERROR: {pdf_name} — {e}")
                    errors += 1

        # ── Corporate debit ──
        debit_pdfs = sorted(f for f in os.listdir(corp_dir)
                            if f.endswith(".pdf") and "DebitCard" in f)
        if debit_pdfs:
            print(f"\n{'─'*60}")
            print(f"Corporate debit: {len(debit_pdfs)} PDFs in {corp_dir}")
            for pdf_name in debit_pdfs:
                pdf_path = os.path.join(corp_dir, pdf_name)
                out_path = output_path_for_corporate(pdf_path, "debit")
                if out_path is None:
                    print(f"  SKIP (bad filename): {pdf_name}")
                    skipped += 1
                    continue
                if os.path.exists(out_path) and not force:
                    print(f"  SKIP (exists): {os.path.basename(out_path)}")
                    skipped += 1
                    continue
                try:
                    txns = parse_corporate_debit_pdf(pdf_path)
                    type_counts = {}
                    for t in txns:
                        type_counts[t["transaction"]] = type_counts.get(t["transaction"], 0) + 1
                    summary = ", ".join(f"{v} {k}" for k, v in sorted(type_counts.items()))
                    print(f"  OK: {os.path.basename(out_path)} — {summary or 'no transactions'}")
                    converted += 1
                    write_chequing_csv(txns, out_path)
                except Exception as e:
                    print(f"  ERROR: {pdf_name} — {e}")
                    errors += 1

    print(f"\n{'─'*60}")
    print(f"Done: {converted} converted, {skipped} skipped, {errors} errors")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Convert ScotiaBank PDF e-statements to CSV")
    parser.add_argument("--path", default=".", help="Base directory containing credit card/ and debit card/ folders")
    parser.add_argument("--force", action="store_true", help="Overwrite existing CSV files")
    args = parser.parse_args()

    if not os.path.isdir(args.path):
        print(f"Error: {args.path} is not a directory")
        sys.exit(1)

    convert_all(args.path, force=args.force)
