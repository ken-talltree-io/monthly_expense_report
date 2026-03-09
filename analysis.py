"""Core analysis (aggregation, subscription detection) and AI recommendations."""

import json
import os
import re
import ssl
import sys
from collections import defaultdict
from datetime import datetime
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

from config import (
    CATEGORY_CONSOLIDATION,
    CORPORATE_TAKE_HOME_RATE,
    DEBT_PAYOFF_THRESHOLDS,
    FIXED_COST_MERCHANTS,
    SUB_MIN_MONTHS,
    SUB_MIN_AMOUNT,
    SUB_RETAIL_CV,
    SUB_RETAIL_MIN_MONTHS,
    SUB_RETAIL_MAX_CHARGES,
    SUB_SERVICE_CV_TIGHT,
    SUB_SERVICE_MIN_MONTHS_TIGHT,
    SUB_SERVICE_MAX_CHARGES_TIGHT,
    SUB_SERVICE_CV_LOOSE,
    SUB_SERVICE_MIN_MONTHS_LOOSE,
    SUB_SERVICE_MAX_CHARGES_LOOSE,
    PRICE_MATCH_TOLERANCE,
    ALTERNATING_RANGE_LIMIT,
    SIGNIFICANT_VARIATION_RATIO,
    PRICE_CHANGE_THRESHOLD,
    ANOMALY_TXN_ZSCORE,
    ANOMALY_CATEGORY_SPIKE_STDEV,
    ANOMALY_NEW_MERCHANT_MIN,
    ANOMALY_MIN_HISTORY_MONTHS,
)


# ── Anomaly Detection ────────────────────────────────────────────────────────

def detect_anomalies(transactions, months, category_monthly, merchant_monthly):
    """Detect statistical outliers: large transactions, category spikes, new high-spend merchants.

    Returns a list of anomaly dicts sorted by severity then amount, capped at 15.
    """
    anomalies = []

    # Build per-merchant transaction lists for z-score analysis
    merchant_txns = defaultdict(list)
    for t in transactions:
        merchant_txns[t["merchant"]].append(t)

    # 1. Large transactions — per-merchant z-score > threshold
    for merchant, txns in merchant_txns.items():
        # Need >= 3 transactions across >= 3 months
        txn_months = {t["month"] for t in txns}
        if len(txns) < 3 or len(txn_months) < ANOMALY_MIN_HISTORY_MONTHS:
            continue
        amounts = [t["amount"] for t in txns]
        mean = sum(amounts) / len(amounts)
        variance = sum((a - mean) ** 2 for a in amounts) / len(amounts)
        std_dev = variance ** 0.5
        if std_dev == 0:
            continue
        for t in txns:
            z = (t["amount"] - mean) / std_dev
            if z > ANOMALY_TXN_ZSCORE:
                anomalies.append({
                    "type": "large_transaction",
                    "description": f"{merchant}: ${t['amount']:,.2f} is {z:.1f}σ above avg ${mean:,.2f}",
                    "amount": t["amount"],
                    "date": t["date"],
                    "severity": "alert" if z > 3.0 else "warning",
                    "category": t["category"],
                    "merchant": merchant,
                })

    # 2. Category spikes — monthly total > threshold std devs above category mean
    for category, by_month in category_monthly.items():
        month_vals = [by_month.get(m, 0) for m in months]
        if len(months) < ANOMALY_MIN_HISTORY_MONTHS:
            continue
        mean = sum(month_vals) / len(month_vals)
        variance = sum((v - mean) ** 2 for v in month_vals) / len(month_vals)
        std_dev = variance ** 0.5
        if std_dev == 0:
            continue
        for m, val in zip(months, month_vals):
            z = (val - mean) / std_dev
            if z > ANOMALY_CATEGORY_SPIKE_STDEV:
                anomalies.append({
                    "type": "category_spike",
                    "description": f"{category} in {m}: ${val:,.2f} is {z:.1f}σ above avg ${mean:,.2f}/mo",
                    "amount": val,
                    "date": None,
                    "severity": "alert" if z > 2.5 else "warning",
                    "category": category,
                    "merchant": None,
                })

    # 3. New high-spend merchants — first appearance in last 2 months, total >= threshold
    if len(months) >= 2:
        last_2 = set(months[-2:])
        earlier = set(months[:-2])
        for merchant, by_month in merchant_monthly.items():
            merchant_months = {m for m, v in by_month.items() if v > 0}
            # First appearance must be in last 2 months (no earlier history)
            if not merchant_months.issubset(last_2) or merchant_months & earlier:
                continue
            total_spend = sum(by_month.get(m, 0) for m in last_2)
            if total_spend >= ANOMALY_NEW_MERCHANT_MIN:
                # Find category from transactions
                cat = "Uncategorized"
                for t in transactions:
                    if t["merchant"] == merchant:
                        cat = t["category"]
                        break
                anomalies.append({
                    "type": "new_merchant",
                    "description": f"New merchant {merchant}: ${total_spend:,.2f} in first {'2 months' if len(merchant_months) == 2 else 'month'}",
                    "amount": total_spend,
                    "date": None,
                    "severity": "warning",
                    "category": cat,
                    "merchant": merchant,
                })

    # Sort by severity (alert first) then amount descending, cap at 15
    severity_order = {"alert": 0, "warning": 1}
    anomalies.sort(key=lambda a: (severity_order.get(a["severity"], 2), -a["amount"]))
    return anomalies[:15]


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
        if len(present_months) < SUB_MIN_MONTHS:
            continue
        amounts = [by_month[m] for m in present_months]
        avg_amount = sum(amounts) / len(amounts)
        if avg_amount < SUB_MIN_AMOUNT:
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
            if cv < SUB_RETAIL_CV and len(present_months) >= SUB_RETAIL_MIN_MONTHS and avg_charges <= SUB_RETAIL_MAX_CHARGES:
                is_subscription = True
        else:
            # For service-like categories (telecom, health, insurance, etc.)
            if cv < SUB_SERVICE_CV_TIGHT and len(present_months) >= SUB_SERVICE_MIN_MONTHS_TIGHT and avg_charges <= SUB_SERVICE_MAX_CHARGES_TIGHT:
                is_subscription = True
            elif cv < SUB_SERVICE_CV_LOOSE and len(present_months) >= SUB_SERVICE_MIN_MONTHS_LOOSE and avg_charges <= SUB_SERVICE_MAX_CHARGES_LOOSE:
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
                    if u > 0 and abs(a - u) / u < PRICE_MATCH_TOLERANCE:
                        matched = True
                        break
                if not matched:
                    unique_approx.add(a)
            if len(unique_approx) <= 2 and max_a / min_a < ALTERNATING_RANGE_LIMIT:
                # Alternating pattern — just note the range
                if max_a / min_a > SIGNIFICANT_VARIATION_RATIO:
                    alerts.append(f"Varies ${min_a:.2f} – ${max_a:.2f}")
                    status = "price_change"
            else:
                # True price changes — flag significant jumps
                for i in range(1, len(amounts)):
                    prev_a, curr_a = amounts[i-1], amounts[i]
                    if prev_a > 0 and abs(curr_a - prev_a) / prev_a > PRICE_CHANGE_THRESHOLD:
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

    anomalies = detect_anomalies(transactions, months_set, category_monthly, merchant_monthly)

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
        "anomalies": anomalies,
    }


# ── AI Recommendations ──────────────────────────────────────────────────────

def get_ai_recommendations(data: dict, passive_income: dict | None = None,
                           corporate_income: dict | None = None,
                           incoming_etransfers: list | None = None,
                           bank_interest: list | None = None,
                           notes: dict | None = None) -> str | None:
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

    # User-provided notes explaining known anomalies
    if notes:
        summary["user_notes"] = {merchant: note for merchant, note in notes.items()}

    # Passive investment income — per-account detail for portfolio-specific advice
    if passive_income:
        def _acct_summary(a):
            s = {"name": a["account"], "type": a["type"],
                 "balance": a["value"], "income_annual": a["income_annual"],
                 "growth_annual": a["growth_annual"], "return_pct": a["return_pct"],
                 "strategy": a.get("strategy", ""), "brokerage": a.get("brokerage", ""),
                 "start_date": str(a["start_date"]) if a.get("start_date") else ""}
            # Include recent dividend_history (last 6 months)
            dh = a.get("dividend_history", [])
            if dh:
                s["dividend_history"] = [{"month": d["month"], "amount": d["amount"]} for d in dh[-6:]]
            return s

        summary["passive_income"] = {
            "annual_income": passive_income["annual_income"],
            "annual_growth": passive_income.get("annual_growth", 0),
            "monthly_income": passive_income["monthly_income"],
            "accessible_balance": passive_income.get("accessible_balance", 0),
            "accounts": [_acct_summary(a) for a in passive_income["accounts"]],
            "registered_annual": passive_income.get("registered_annual", 0),
            "registered_growth": passive_income.get("registered_growth", 0),
            "registered_monthly": passive_income.get("registered_monthly", 0),
            "registered_balance": passive_income.get("registered_balance", 0),
            "registered_accounts": [_acct_summary(a) for a in passive_income.get("registered_accounts", [])],
            "net_worth": {
                "accessible": passive_income.get("accessible_balance", 0),
                "registered": passive_income.get("registered_balance", 0),
                "corporate": passive_income.get("corporate_balance", 0),
                "property": passive_income.get("property_balance", 0),
            },
        }

        # TWR (time-weighted return) per account
        twr = passive_income.get("twr")
        if twr and twr.get("per_account"):
            summary["passive_income"]["twr"] = {
                "annualized_rate_pct": round(twr["annualized_rate"] * 100, 2),
                "per_account": [
                    {"name": pa["account"],
                     "annualized_pct": round(((1 + pa["monthly_return"]) ** 12 - 1) * 100, 1),
                     "data_points": pa["data_points"]}
                    for pa in twr["per_account"]
                ],
            }

    # Corporate income
    if corporate_income:
        rev = corporate_income["revenue_monthly"]
        div = corporate_income["dividends_monthly"]
        all_corp_months = sorted(set(list(rev.keys()) + list(div.keys())))
        corp_trailing = all_corp_months[-6:]  # 6-month trailing (matches dashboard)
        corp_n = len(corp_trailing) or 1

        rev_avg = round(sum(rev.get(m, 0) for m in corp_trailing) / corp_n, 2)
        div_avg = round(sum(div.get(m, 0) for m in corp_trailing) / corp_n, 2)

        take_home_rate = CORPORATE_TAKE_HOME_RATE
        summary["corporate_income"] = {
            "revenue_monthly": rev,
            "dividends_monthly": div,
            "revenue_total": corporate_income["revenue_total"],
            "dividends_total": corporate_income["dividends_total"],
            "revenue_avg_trailing": rev_avg,
            "dividends_avg_trailing": div_avg,
            "trailing_months": corp_n,
            "take_home_rate": take_home_rate,
            "estimated_take_home_monthly": round(rev_avg * take_home_rate + div_avg, 2),
        }

        # Revenue trend: latest vs prior month
        rev_months = sorted(rev.keys())
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

    # Burn rate & coverage — exclude paid-off debt merchant payments, use 6-month trailing
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
    trailing_spend = spend_months[-6:] if len(spend_months) >= 6 else spend_months
    if trailing_spend:
        burn_rate = round(sum(adjusted[m] for m in trailing_spend) / len(trailing_spend), 2)
    else:
        burn_rate = 0

    # Combined monthly income (all streams)
    combined_monthly = 0.0
    passive_monthly = 0.0
    if passive_income:
        passive_monthly = passive_income["monthly_income"] + passive_income.get("registered_monthly", 0)
        combined_monthly += passive_monthly
    if corporate_income and "estimated_take_home_monthly" in summary.get("corporate_income", {}):
        combined_monthly += summary["corporate_income"]["estimated_take_home_monthly"]
    recent_months = set(spend_months[-6:])
    recent_n = len(recent_months) or 1
    etransfer_avg = round(sum(t["amount"] for t in (incoming_etransfers or []) if str(t["date"])[:7] in recent_months) / recent_n, 2)
    bi_avg = round(sum(t["amount"] for t in (bank_interest or []) if str(t["date"])[:7] in recent_months) / recent_n, 2)
    combined_monthly += etransfer_avg + bi_avg

    coverage_pct = round(combined_monthly / burn_rate * 100, 1) if burn_rate > 0 else 0
    passive_coverage = round(passive_monthly / burn_rate * 100, 1) if burn_rate > 0 else 0
    accessible_balance = passive_income.get("accessible_balance", 0) if passive_income else 0
    net_monthly_draw = max(burn_rate - combined_monthly, 0)
    runway_months = round(accessible_balance / net_monthly_draw, 1) if net_monthly_draw > 0 else None
    summary["burn_rate_coverage"] = {
        "burn_rate_monthly": burn_rate,
        "combined_monthly_income": round(combined_monthly, 2),
        "passive_monthly_income": round(passive_monthly, 2),
        "coverage_pct": coverage_pct,
        "passive_coverage_pct": passive_coverage,
        "monthly_surplus_or_gap": round(combined_monthly - burn_rate, 2),
        "accessible_savings": accessible_balance,
        "runway_months": runway_months,
    }

    # Income by month — actual per-month totals for trend analysis
    income_by_month = {}
    # Build passive_by_month from dividend_history (exclude Cash — already in bank interest)
    passive_by_month = defaultdict(float)
    if passive_income:
        for cat in ["accounts", "registered_accounts"]:
            for a in passive_income.get(cat, []):
                if a.get("type") == "Cash":
                    continue
                for dh in a.get("dividend_history", []):
                    passive_by_month[dh["month"]] += dh["amount"]
    etransfer_by_month = defaultdict(float)
    for t in (incoming_etransfers or []):
        etransfer_by_month[str(t["date"])[:7]] += t["amount"]
    bi_by_month_inc = defaultdict(float)
    for t in (bank_interest or []):
        bi_by_month_inc[str(t["date"])[:7]] += t["amount"]
    monthly_passive_flat = passive_monthly
    for m in spend_months:
        corp_rev = corporate_income["revenue_monthly"].get(m, 0) * CORPORATE_TAKE_HOME_RATE if corporate_income else 0
        corp_div = corporate_income["dividends_monthly"].get(m, 0) if corporate_income else 0
        passive_m = passive_by_month.get(m, monthly_passive_flat)
        income_by_month[m] = round(corp_rev + corp_div + passive_m + etransfer_by_month.get(m, 0) + bi_by_month_inc.get(m, 0), 2)
    summary["income_by_month"] = income_by_month

    # Savings rate by month
    savings_by_month = {}
    for m in spend_months:
        inc = income_by_month.get(m, 0)
        spend = adjusted.get(m, 0)
        savings_by_month[m] = round(((inc - spend) / inc * 100), 1) if inc > 0 else 0
    summary["savings_rate_by_month"] = savings_by_month

    # Anomalies — detected spending outliers
    anomalies = data.get("anomalies", [])
    if anomalies:
        summary["anomalies"] = [
            {"type": a["type"], "description": a["description"],
             "severity": a["severity"], "amount": a["amount"]}
            for a in anomalies[:10]
        ]

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

Context: Self-employed consultant pursuing financial sustainability (passive income >= burn rate). Three income streams: (1) passive portfolio yield (SUSTAINABLE), (2) corporate consulting revenue at ~{CORPORATE_TAKE_HOME_RATE*100:.0f}% take-home (ACTIVE bridge), (3) corporate dividends.

Key data sections:
- "burn_rate_coverage": 6-month trailing burn rate, passive_coverage_pct (passive-only), coverage_pct (all income). "runway_months" = how long accessible savings last if income stops.
- "income_by_month": actual total income per month. "savings_rate_by_month": % of income retained.
- "passive_income.accounts/registered_accounts": per-account balance, return_pct, dividend_history (actual monthly dividends). "twr": time-weighted returns per account (annualized).
- "anomalies": statistically detected spending outliers.
- "user_notes": confirmed explanations for known anomalies — do NOT re-flag these.
- "debts_paid_off": debts ALREADY eliminated (celebrate, not warn).
- Net worth split: accessible (spend without tax penalty), registered (RRSP/RESP), corporate, property.

DATA:
{json.dumps(summary, indent=2)}

Provide 5 to 10 recommendations. Each must be <= 80 words, specific, actionable, and reference actual numbers and merchant names from the data. Prioritize the most impactful insights from:
- Savings rate trend (is it improving or deteriorating month-over-month?)
- Sustainability gap (passive_coverage_pct vs 100% — what specifically would close it: higher yield, lower burn, reallocation?)
- Portfolio performance (TWR per account — which are underperforming? which are outperforming? rebalancing opportunities)
- Dividend income trends (per-account dividend_history — growing, shrinking, or flat?)
- Corporate bridge risk (revenue trend, client concentration)
- Spending anomalies (reference specific anomalies from the data)
- Category spending trends (use monthly data to identify categories trending up or down)
- Net worth composition (concentration risk, liquidity, growth vs income allocation)
- Fixed cost and subscription optimization (specific merchants, amounts)
- Corporate tax strategy (take-home rate, dividend timing)

Respond with ONLY a raw HTML <table> — no markdown, no code fences, no explanation before or after. Columns: #, Recommendation, Expected Impact. Rank recommendations by expected impact, highest first. Use <strong> for emphasis on merchant names and dollar amounts. Each recommendation cell must be <= 80 words. The Expected Impact column should quantify the benefit where possible (e.g. "$X/yr savings", "+$X/yr income", "reduce risk").

IMPORTANT: On each <tr> (except the header), include a data-sections attribute containing a comma-separated list of dashboard section IDs that the recommendation relates to. Use ONLY these IDs:
- subscriptions — Subscription Audit
- categories — Category Heatmap / spending categories
- fixed-discretionary — Fixed vs Discretionary costs
- corporate-income — Corporate Income
- passive-income — Investment Portfolio
- interac-transfers — Outgoing e-Transfers
- incoming-etransfers — Incoming e-Transfers
- bank-interest — Bank Interest
- debt-freedom — Debt Freedom

Example: <tr data-sections="subscriptions,categories"><td>1</td><td>Cancel Netflix...</td><td>$200/yr savings</td></tr>"""

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
            html = body["content"][0]["text"]
            # Strip markdown code fences / preamble the model may add
            if "<table" in html:
                html = html[html.index("<table"):]
            if "</table>" in html:
                html = html[:html.rindex("</table>") + len("</table>")]
            return html
    except HTTPError as e:
        error_body = e.read().decode("utf-8") if e.fp else ""
        print(f"API error ({e.code}): {error_body}")
        sys.exit(1)
    except URLError as e:
        print(f"Network error: {e.reason}")
        sys.exit(1)
