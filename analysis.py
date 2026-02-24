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
)


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

Context: This dashboard covers a self-employed consultant pursuing financial sustainability, defined as: passive income >= burn rate. Income comes from three streams: (1) passive portfolio yield from personal investments — this is the SUSTAINABLE income, (2) corporate consulting revenue (Tall Tree Technology) at ~60% take-home after tax/expenses — this is ACTIVE income that bridges the gap, and (3) corporate dividend income (Britton Holdings). The "burn_rate_coverage" section shows how much of the burn rate is covered by passive income alone — coverage_pct is passive-only. Corporate income bridges the remaining gap but is not considered sustainable. "accessible_savings" is the total balance in Non-registered, Cash, and TFSA accounts that can be drawn without tax penalty; "runway_months" shows how long savings last if all income stopped (null if passive income already covers expenses). Revenue trend shows month-over-month changes in consulting income. "debts_paid_off" lists debts that were fully eliminated during this period (with per-debt principal and payoff dates) — these are no longer owed and should be celebrated, not treated as outstanding obligations. "corporate_income.milestones" shows the corporate journey timeline (launch date, first revenue, first dividend). The spending data includes fixed costs (tuition, car payment, utilities) and discretionary spending across credit and debit cards. The "passive_income.accounts" array contains per-account detail (name, type, balance, annual_yield, return_pct) for accessible accounts and RRSP accounts — use this to identify underperforming or overconcentrated positions. The "passive_income.net_worth" object shows the full balance breakdown across accessible, RRSP, corporate, property, and RESP holdings. Each category includes a "monthly" object with per-month spending for the last 6 months — use this to spot categories trending up or down. "user_notes" contains the user's own explanations for known spending anomalies (e.g. renovations, billing errors) — treat these as confirmed facts and do NOT flag explained spikes as concerns.

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
