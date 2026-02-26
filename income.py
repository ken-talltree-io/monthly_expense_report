"""Income extraction: passive income, corporate income, transfers, and bank interest."""

import calendar
import csv
import glob
import os
import re
from collections import defaultdict
from datetime import date, datetime, timedelta

from parsers import parse_statement_balances


# ── Passthrough Loading ──────────────────────────────────────────────────────

def load_passthrough(folder: str) -> list[dict]:
    """Load passthrough records from passthrough.csv.

    Returns list of {"account_suffix": str, "start_date": date, "end_date": date,
                      "principal": float, "description": str}.
    """
    path = os.path.join(folder, "passthrough.csv")
    if not os.path.exists(path):
        return []

    records = []
    with open(path, newline="", encoding="utf-8-sig") as f:
        for row in csv.DictReader(f):
            records.append({
                "account_suffix": row["account_suffix"].strip(),
                "start_date": datetime.strptime(row["start_date"].strip(), "%Y-%m-%d").date(),
                "end_date": datetime.strptime(row["end_date"].strip(), "%Y-%m-%d").date(),
                "principal": float(row["principal"].strip()),
                "description": row["description"].strip(),
            })
    return records


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
                "balance_history": stmt.get("balance_history", []) if stmt else [],
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


def compute_empirical_growth_rate(passive_income: dict) -> dict | None:
    """Compute weighted-average empirical monthly total return from account histories.

    Uses balance_history from statement data to compute actual observed growth,
    adjusting for deposits/withdrawals to isolate investment returns.
    Returns None if fewer than 3 data points are available.
    """
    all_accounts = passive_income.get("accounts", []) + passive_income.get("registered_accounts", [])
    per_account = []
    total_data_points = 0
    all_dates = []

    for acct in all_accounts:
        history = acct.get("balance_history", [])
        if len(history) < 2:
            continue

        monthly_returns = []
        balances = []
        for i in range(1, len(history)):
            t0 = history[i - 1]
            t1 = history[i]
            b0 = t0["balance"]
            b1 = t1["balance"]
            if b0 <= 0:
                continue

            net_flow = t1["deposits"] - t1["withdrawals"]
            investment_return = b1 - b0 - net_flow
            period_return = investment_return / b0

            # Determine months in period from date gap
            try:
                d0 = datetime.strptime(t0["date"][:10], "%Y-%m-%d")
                d1 = datetime.strptime(t1["date"][:10], "%Y-%m-%d")
                months_in_period = max(round((d1 - d0).days / 30.44), 1)
            except (ValueError, TypeError):
                months_in_period = 1

            # Convert period return to monthly return
            if period_return > -1:
                monthly_return = (1 + period_return) ** (1 / months_in_period) - 1
            else:
                monthly_return = -1.0  # total loss

            monthly_returns.append(monthly_return)
            balances.append((b0 + b1) / 2)
            all_dates.append(t0["date"][:10])
            all_dates.append(t1["date"][:10])

        if monthly_returns:
            avg_return = sum(monthly_returns) / len(monthly_returns)
            avg_balance = sum(balances) / len(balances)
            per_account.append({
                "account": acct["account"],
                "monthly_return": avg_return,
                "avg_balance": avg_balance,
                "data_points": len(monthly_returns),
            })
            total_data_points += len(monthly_returns)

    if total_data_points < 3:
        return None

    # Weighted average by balance
    weighted_sum = sum(pa["monthly_return"] * pa["avg_balance"] for pa in per_account)
    weight_total = sum(pa["avg_balance"] for pa in per_account)
    if weight_total <= 0:
        return None

    monthly_rate = weighted_sum / weight_total
    annualized = (1 + monthly_rate) ** 12 - 1

    all_dates.sort()
    return {
        "monthly_growth_rate": monthly_rate,
        "annualized_rate": annualized,
        "data_points": total_data_points,
        "date_range": (all_dates[0], all_dates[-1]) if all_dates else ("", ""),
        "per_account": per_account,
    }


def compute_net_worth_history(passive_income: dict, passthrough: list | None = None) -> list[dict] | None:
    """Build month-by-month net worth time series from account balance histories.

    Collects balance_history from all 4 account categories, forward-fills gaps
    (e.g. quarterly statements), backfills months before an account's first data
    point with its first known balance, and includes constant-value accounts
    (no history) at their current value for all months.

    When passthrough records are provided, their principal is subtracted from the
    "accessible" category for months where the passthrough was active, pro-rated
    for partial months.

    Returns list of {"month", "accessible", "registered", "corporate", "property", "total"}
    sorted chronologically, or None if < 2 months of data.
    """
    if not passive_income:
        return None

    CATEGORY_MAP = {
        "accounts": "accessible",
        "registered_accounts": "registered",
        "corporate_accounts": "corporate",
        "property_accounts": "property",
    }

    # Collect per-account time series and constant-value accounts
    account_series = []  # [(category, {month: balance})]
    constant_accounts = []  # [(category, value)]

    for key, category in CATEGORY_MAP.items():
        for acct in passive_income.get(key, []):
            history = acct.get("balance_history", [])
            if history:
                monthly = {}
                for entry in history:
                    date_str = entry["date"][:10]
                    try:
                        month = datetime.strptime(date_str, "%Y-%m-%d").strftime("%Y-%m")
                    except (ValueError, TypeError):
                        continue
                    monthly[month] = entry["balance"]
                if monthly:
                    account_series.append((category, monthly))
            else:
                # No history — use current value as constant
                val = acct.get("value", 0)
                if val > 0:
                    constant_accounts.append((category, val))

    # Determine all months from accounts with history
    all_months_set = set()
    for _, monthly in account_series:
        all_months_set.update(monthly.keys())

    if len(all_months_set) < 2:
        return None

    all_months = sorted(all_months_set)

    # Build per-category monthly totals
    category_totals = {cat: defaultdict(float) for cat in CATEGORY_MAP.values()}

    for category, monthly in account_series:
        # Forward-fill and backfill
        sorted_months_for_acct = sorted(monthly.keys())
        first_known = monthly[sorted_months_for_acct[0]]
        last_known = first_known

        for month in all_months:
            if month in monthly:
                last_known = monthly[month]
                category_totals[category][month] += last_known
            elif month < sorted_months_for_acct[0]:
                # Backfill: before first data point
                category_totals[category][month] += first_known
            else:
                # Forward-fill: carry last known balance
                category_totals[category][month] += last_known

    # Add constant accounts to every month
    for category, val in constant_accounts:
        for month in all_months:
            category_totals[category][month] += val

    # Subtract passthrough principals from accessible category.
    # Balance_history uses point-in-time end-of-month snapshots with forward/backfill,
    # so we: (a) only subtract when the money is present at month-end (before the
    # departure date), and (b) cap the subtraction so accessible never drops below
    # its pre-passthrough level (the balance data may not fully reflect the deposit
    # due to forward-fill gaps).
    for pt in (passthrough or []):
        # Find the pre-passthrough floor: highest accessible in months ending
        # before the passthrough start date
        floor = 0.0
        for month in all_months:
            year, mon = int(month[:4]), int(month[5:7])
            month_end = date(year, mon, calendar.monthrange(year, mon)[1])
            if month_end < pt["start_date"]:
                floor = max(floor, category_totals["accessible"][month])

        for month in all_months:
            year, mon = int(month[:4]), int(month[5:7])
            month_end = date(year, mon, calendar.monthrange(year, mon)[1])

            # Only subtract when money is present at month-end:
            # deposited on/before month_end AND hasn't left yet (end_date > month_end)
            if pt["start_date"] <= month_end < pt["end_date"]:
                max_sub = max(0, category_totals["accessible"][month] - floor)
                actual_sub = min(pt["principal"], max_sub)
                category_totals["accessible"][month] -= actual_sub

    # Assemble result
    result = []
    for month in all_months:
        row = {"month": month}
        total = 0.0
        for category in CATEGORY_MAP.values():
            val = round(category_totals[category][month], 2)
            row[category] = val
            total += val
        row["total"] = round(total, 2)
        result.append(row)

    return result


def extract_transfers(folder: str, passthrough: list | None = None) -> tuple[dict, list]:
    """Extract monthly transfer summary from debit card CSVs.

    Returns (aggregates, incoming_etransfers) where:
    - aggregates: dict of month -> {"in": float, "out": float}
    - incoming_etransfers: list of {"date": date, "amount": float} for E_TRFIN
    Covers TRFOUT, TRFIN, TRFINTF, E_TRFOUT, E_TRFIN, EFTOUT.

    If passthrough is provided, EFTOUT transactions whose description matches
    a passthrough description (substring) are excluded.
    """
    passthrough = passthrough or []
    pt_descriptions = [pt["description"] for pt in passthrough]

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

                # Skip EFTOUT transactions matching passthrough descriptions
                if txn_type == "EFTOUT" and pt_descriptions:
                    desc = row.get("description", "")
                    if any(ptd in desc for ptd in pt_descriptions):
                        continue

                amount = float(row["amount"])
                dt = datetime.strptime(row["date"], "%Y-%m-%d")
                month = dt.strftime("%Y-%m")

                if amount > 0:
                    transfers[month]["in"] += amount
                    if txn_type == "E_TRFIN":
                        incoming_etransfers.append({"date": dt.date(), "amount": amount})
                else:
                    transfers[month]["out"] += abs(amount)

    aggregates = {m: {"in": round(v["in"], 2), "out": round(v["out"], 2)}
                  for m, v in transfers.items()}
    incoming_etransfers.sort(key=lambda t: t["date"], reverse=True)
    return aggregates, incoming_etransfers


def extract_bank_interest(folder: str, passthrough: list | None = None) -> tuple[list, dict]:
    """Extract INT (interest) transactions from personal and corporate debit CSVs.

    Returns (interest_txns, passthrough_adj) where:
    - interest_txns: list of {"date": date, "amount": float, "account": str} sorted newest-first
    - passthrough_adj: dict of {description: total_amount_deducted} for each active passthrough

    When passthrough records are provided, interest from matching accounts during
    the passthrough period is reduced proportionally:
        sarah_pct = min(1.0, principal / (balance_after - interest_amount))
    INT posted on the 1st covers the prior month, so we check if the prior month
    overlaps with the passthrough period.
    """
    passthrough = passthrough or []
    interest_txns = []
    pt_adjustments = {}

    # Build suffix lookup for passthrough
    pt_by_suffix = {}
    for pt in passthrough:
        pt_by_suffix.setdefault(pt["account_suffix"], []).append(pt)

    for subdir in ["personal", "corporate"]:
        txn_dir = os.path.join(folder, "transactions", subdir)
        if not os.path.isdir(txn_dir):
            continue
        for fpath in sorted(glob.glob(os.path.join(txn_dir, "**", "*.csv"), recursive=True)):
            fname = os.path.basename(fpath)

            # Check if this file matches any passthrough account
            file_pts = []
            for suffix, pts in pt_by_suffix.items():
                if suffix in fname:
                    file_pts.extend(pts)

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
                    dt = datetime.strptime(row["date"], "%Y-%m-%d").date()

                    # Check for passthrough adjustment
                    adjustment = 0.0
                    if file_pts:
                        balance = float(row.get("balance", 0))
                        for pt in file_pts:
                            # INT on 1st covers prior month — check overlap
                            if dt.month == 1:
                                prior_start = date(dt.year - 1, 12, 1)
                                prior_end = date(dt.year - 1, 12, 31)
                            else:
                                prior_start = date(dt.year, dt.month - 1, 1)
                                prior_end = dt.replace(day=1) - timedelta(days=1)

                            if pt["start_date"] <= prior_end and pt["end_date"] >= prior_start:
                                balance_before = balance - amount
                                if balance_before > 0:
                                    sarah_pct = min(1.0, pt["principal"] / balance_before)
                                    adj = round(amount * sarah_pct, 2)
                                    adjustment += adj
                                    pt_adjustments[pt["description"]] = (
                                        pt_adjustments.get(pt["description"], 0.0) + adj
                                    )

                    adjusted_amount = round(amount - adjustment, 2)
                    if adjusted_amount > 0:
                        interest_txns.append({"date": dt, "amount": adjusted_amount, "account": account})

    interest_txns.sort(key=lambda t: t["date"], reverse=True)
    pt_adjustments = {k: round(v, 2) for k, v in pt_adjustments.items()}
    return interest_txns, pt_adjustments


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
