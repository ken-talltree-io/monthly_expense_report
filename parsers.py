"""Transaction CSV parsing and PDF statement balance extraction."""

import csv
import glob
import os
import re
import subprocess
import sys
from collections import defaultdict
from datetime import datetime

from config import (
    BUSINESS_MERCHANTS,
    CATEGORY_CONSOLIDATION,
    DEBT_PAYOFF_THRESHOLDS,
    FIXED_COST_MERCHANTS,
    categorize,
    normalize_merchant,
)


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
            "balance_history": [],
        }

        # Parse dividends + interest from ALL monthly statements for this account
        # Also extract balance history (balance, deposits, withdrawals) per month
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

            # Extract balance history entry for this month
            bh_m = re.search(
                r"Total Portfolio\s+\$([0-9,]+\.\d{2})\s+100\.00",
                pdf_text,
            )
            if bh_m:
                hist_bal = float(bh_m.group(1).replace(",", ""))
                # USD conversion using this statement's fx rate
                if is_usd:
                    hist_fx = re.search(r"\$1 USD = \$([0-9.]+) CAD", pdf_text)
                    if hist_fx:
                        hist_fx_rate = float(hist_fx.group(1))
                        hist_bal = round(hist_bal * hist_fx_rate, 2)
                    else:
                        continue  # Can't convert; skip this month

                # Parse deposits and withdrawals (first match = CAD column)
                dep_m = re.search(r"Deposits\s+\$([\d,]+\.\d{2})", pdf_text)
                wdr_m = re.search(r"Withdrawals\s+\$([\d,]+\.\d{2})", pdf_text)
                hist_dep = float(dep_m.group(1).replace(",", "")) if dep_m else 0.0
                hist_wdr = float(wdr_m.group(1).replace(",", "")) if wdr_m else 0.0

                # Parse statement end date
                hist_dm = re.search(
                    r"(\d{4}-\d{2}-\d{2})\s*-\s*(\d{4}-\d{2}-\d{2})", pdf_text
                )
                hist_date = hist_dm.group(2) if hist_dm else ds

                results[suffix]["balance_history"].append({
                    "date": hist_date,
                    "balance": hist_bal,
                    "deposits": hist_dep,
                    "withdrawals": hist_wdr,
                })

        if months_seen and total_income > 0:
            results[suffix]["dividends_annual"] = round(
                total_income / len(months_seen) * 12, 2
            )

        # Sort balance history chronologically
        results[suffix]["balance_history"].sort(key=lambda x: x["date"])

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

            # Collect balance history from ALL quarterly PDFs
            sh_balance_history: dict[str, list] = defaultdict(list)
            for sort_key, q_pdf in sh_pdfs:
                q_text = _pdf_text(q_pdf)
                if not q_text:
                    continue
                sk_year, sk_month = sort_key // 100, sort_key % 100
                iso_date = f"{sk_year}-{sk_month:02d}-28"
                for row_m in re.finditer(
                    r"^(\d{7})\s+.+?\s+([\d,]+\.\d{2})\s*$",
                    q_text, re.MULTILINE,
                ):
                    acct_num = row_m.group(1)
                    bal = float(row_m.group(2).replace(",", ""))
                    if bal > 0:
                        sh_balance_history[acct_num].append({
                            "date": iso_date,
                            "balance": bal,
                            "deposits": 0.0,
                            "withdrawals": 0.0,
                        })

            # Use latest PDF for current balance, returns, and dividends
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
                    history = sh_balance_history.get(acct_num, [])
                    history.sort(key=lambda x: x["date"])
                    results[acct_num] = {
                        "balance": balance,
                        "date": stmt_date,
                        "source": "Steadyhand statement",
                        "return_pct": None,
                        "dividends_annual": None,
                        "balance_history": history,
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
