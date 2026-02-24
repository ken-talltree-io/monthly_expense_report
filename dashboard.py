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
import json
import os
from collections import defaultdict, OrderedDict
from datetime import datetime, timedelta

import config
from config import (
    CATEGORY_CONSOLIDATION,
    CORPORATE_TAKE_HOME_RATE,
    DEBT_PAYOFF_THRESHOLDS,
    load_budgets,
    load_notes,
    load_user_categories,
)
from parsers import parse_csvs
from income import (
    extract_passive_income,
    compute_empirical_growth_rate,
    extract_transfers,
    extract_bank_interest,
    extract_corporate_income,
)
from analysis import analyze, get_ai_recommendations


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
    # Collect all transactions across heatmap months for detail drill-down
    heatmap_txns_by_cat = defaultdict(list)
    for m in heatmap_months:
        for t in data["monthly_txns"].get(m, []):
            heatmap_txns_by_cat[t["category"]].append(t)

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

        # Build detail data: top 5 merchants and top 5 transactions
        cat_txns = heatmap_txns_by_cat.get(c, [])
        merchant_totals = defaultdict(float)
        for tx in cat_txns:
            merchant_totals[tx["merchant"]] += tx["amount"]
        top_merchants = sorted(merchant_totals.items(), key=lambda x: x[1], reverse=True)[:5]
        top_txns = sorted(cat_txns, key=lambda x: x["amount"], reverse=True)[:5]

        merchant_rows = ""
        for merch_name, merch_total in top_merchants:
            merchant_rows += f"<tr><td>{merch_name}</td><td style='text-align:right'>{money(merch_total)}</td></tr>"
        txn_rows = ""
        for tx in top_txns:
            txn_rows += f"<tr><td>{tx['merchant']}</td><td style='text-align:center'>{tx['date'].strftime('%b %d')}</td><td style='text-align:right'>{money(tx['amount'])}</td></tr>"

        detail_html = ""
        if cat_txns:
            detail_html = (
                f'<tr class="cat-detail" style="display:none">'
                f'<td colspan="9" style="padding:0">'
                f'<div style="padding:12px 20px;background:#f8f9fa;border-top:1px solid var(--border)">'
                f'<div style="display:grid;grid-template-columns:1fr 1fr;gap:20px">'
                f'<div>'
                f'<div style="font-weight:600;margin-bottom:6px;font-size:0.85em;color:var(--accent)">Top Merchants</div>'
                f'<table class="data-table"><thead><tr><th>Merchant</th><th style="text-align:right">Total</th></tr></thead>'
                f'<tbody>{merchant_rows}</tbody></table>'
                f'</div>'
                f'<div>'
                f'<div style="font-weight:600;margin-bottom:6px;font-size:0.85em;color:var(--accent)">Biggest Transactions</div>'
                f'<table class="data-table"><thead><tr><th>Merchant</th><th style="text-align:center">Date</th><th style="text-align:right">Amount</th></tr></thead>'
                f'<tbody>{txn_rows}</tbody></table>'
                f'</div>'
                f'</div></div></td></tr>'
            )

        cat_row = (
            f'<tr class="cat-row"><td><span class="cat-arrow">\u25b8</span> {c}</td>'
            f'{cells}{avg_cell}{total_cell}</tr>'
            f'{detail_html}'
        )
        heatmap_row_data.append((cat_total, cat_row))
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

    # ── Per-month income totals (for savings rate) ──
    income_by_month = {}
    # Bucket e-transfers by month
    etransfer_by_month = defaultdict(float)
    for t in (incoming_etransfers or []):
        m_key = str(t["date"])[:7]
        etransfer_by_month[m_key] += t["amount"]
    # Bucket bank interest by month
    bank_int_by_month = defaultdict(float)
    for t in (bank_interest or []):
        m_key = str(t["date"])[:7]
        bank_int_by_month[m_key] += t["amount"]
    for m in months:
        corp_rev = corporate_income["revenue_monthly"].get(m, 0) * CORPORATE_TAKE_HOME_RATE if corporate_income else 0
        corp_div = corporate_income["dividends_monthly"].get(m, 0) if corporate_income else 0
        passive_m = monthly_passive  # spread evenly
        etransfer_m = etransfer_by_month.get(m, 0)
        bank_int_m = bank_int_by_month.get(m, 0)
        income_by_month[m] = corp_rev + corp_div + passive_m + etransfer_m + bank_int_m

    # ── Savings rate per month ──
    savings_rate_by_month = {}
    savings_dollars_by_month = {}
    for m in months:
        inc = income_by_month[m]
        spend = adjusted_monthly[m]
        savings = inc - spend
        savings_dollars_by_month[m] = savings
        savings_rate_by_month[m] = round((savings / inc * 100), 1) if inc > 0 else 0

    savings_rates_list = [savings_rate_by_month[m] for m in months]
    savings_avg = round(sum(savings_rates_list) / len(savings_rates_list), 1) if savings_rates_list else 0
    trailing_3_rates = savings_rates_list[-3:]
    savings_3mo_avg = round(sum(trailing_3_rates) / len(trailing_3_rates), 1) if trailing_3_rates else 0
    savings_current = savings_rates_list[-1] if savings_rates_list else 0
    savings_best_rate = max(savings_rates_list) if savings_rates_list else 0
    savings_best_month = months[savings_rates_list.index(savings_best_rate)] if savings_rates_list else ""

    # ── Savings rate section HTML ──
    def _sr_color(rate):
        return "#27ae60" if rate >= 0 else "#e74c3c"

    savings_best_label = datetime.strptime(savings_best_month, "%Y-%m").strftime("%b %Y") if savings_best_month else ""
    savings_rate_section = ""
    if has_income:
        sr_chart_labels = json.dumps(month_labels)
        sr_chart_data = json.dumps(savings_rates_list)
        sr_income_data = json.dumps([round(income_by_month[m], 2) for m in months])
        sr_spending_data = json.dumps([round(adjusted_monthly[m], 2) for m in months])
        sr_savings_data = json.dumps([round(savings_dollars_by_month[m], 2) for m in months])

        savings_rate_section = f"""
    <div class="card" style="margin-bottom:20px">
        <h2>Savings Rate</h2>
        <p style="color:var(--muted);font-style:italic;margin-bottom:15px">Percentage of income retained each month. Savings rate = (income &minus; spending) &divide; income.</p>
        <div class="stats" style="margin-bottom:20px">
            <div class="stat">
                <div class="value" style="color:{_sr_color(savings_current)}">{savings_current:+.1f}%</div>
                <div class="label">Current Month</div>
            </div>
            <div class="stat">
                <div class="value" style="color:{_sr_color(savings_3mo_avg)}">{savings_3mo_avg:+.1f}%</div>
                <div class="label">3-Month Avg</div>
            </div>
            <div class="stat">
                <div class="value" style="color:{_sr_color(savings_avg)}">{savings_avg:+.1f}%</div>
                <div class="label">Overall Avg</div>
            </div>
            <div class="stat">
                <div class="value" style="color:#27ae60">{savings_best_rate:+.1f}%</div>
                <div class="label">Best ({savings_best_label})</div>
            </div>
        </div>
        <div class="chart-container">
            <canvas id="savingsRateChart" height="100"></canvas>
        </div>
    </div>"""

        sr_avg_line = json.dumps([savings_avg] * len(months))
        savings_rate_chart_js = f"""
    new Chart(document.getElementById('savingsRateChart'), {{
        type: 'line',
        data: {{
            labels: {sr_chart_labels},
            datasets: [{{
                label: 'Savings Rate',
                data: {sr_chart_data},
                borderColor: '#27ae60',
                backgroundColor: 'rgba(39, 174, 96, 0.1)',
                fill: true,
                tension: 0.3,
                pointRadius: 4,
                pointBackgroundColor: {sr_chart_data}.map(v => v >= 0 ? '#27ae60' : '#e74c3c'),
                pointBorderColor: {sr_chart_data}.map(v => v >= 0 ? '#27ae60' : '#e74c3c'),
                borderWidth: 2
            }}, {{
                label: 'Average ({savings_avg}%)',
                data: {sr_avg_line},
                borderColor: 'rgba(39, 174, 96, 0.5)',
                borderDash: [6, 4],
                fill: false,
                pointRadius: 0,
                borderWidth: 2
            }}]
        }},
        options: {{
            responsive: true,
            plugins: {{
                legend: {{ position: 'bottom' }},
                tooltip: {{
                    callbacks: {{
                        label: function(ctx) {{
                            if (ctx.datasetIndex === 1) return 'Average: ' + ctx.parsed.y.toFixed(1) + '%';
                            var idx = ctx.dataIndex;
                            var inc = {sr_income_data}[idx];
                            var spend = {sr_spending_data}[idx];
                            var sav = {sr_savings_data}[idx];
                            return [
                                'Savings Rate: ' + ctx.parsed.y.toFixed(1) + '%',
                                'Income: $' + inc.toLocaleString(undefined, {{minimumFractionDigits: 0, maximumFractionDigits: 0}}),
                                'Spending: $' + spend.toLocaleString(undefined, {{minimumFractionDigits: 0, maximumFractionDigits: 0}}),
                                'Saved: $' + sav.toLocaleString(undefined, {{minimumFractionDigits: 0, maximumFractionDigits: 0}})
                            ];
                        }}
                    }}
                }}
            }},
            scales: {{
                y: {{
                    ticks: {{
                        callback: function(v) {{ return v + '%'; }}
                    }}
                }}
            }}
        }}
    }});"""
    else:
        savings_rate_chart_js = ""

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

        # Use empirical growth rate if available (>= 3 data points), else fall back to CSV rates
        empirical = passive_income.get("empirical_growth")
        if empirical and empirical["data_points"] >= 3:
            monthly_total_return_rate = empirical["monthly_growth_rate"]
            empirical_annualized = empirical["annualized_rate"]
            using_empirical = True
        else:
            monthly_total_return_rate = annual_total_return_rate / 12
            empirical_annualized = None
            using_empirical = False
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

        if using_empirical:
            d0 = datetime.strptime(empirical["date_range"][0], "%Y-%m-%d").strftime("%b %Y")
            d1 = datetime.strptime(empirical["date_range"][1], "%Y-%m-%d").strftime("%b %Y")
            proj_desc = (
                f"Empirical total return: {empirical_annualized*100:.1f}%/yr "
                f"(observed {d0} \u2013 {d1}, {empirical['data_points']} data points), "
                f"{annual_yield_rate*100:.1f}% yield, ${burn_rate:,.0f}/mo burn rate."
            )
        else:
            proj_desc = f"Assuming {annual_yield_rate*100:.1f}% yield, {annual_total_return_rate*100:.1f}% total return, ${burn_rate:,.0f}/mo burn rate."

        sustainability_card = f"""
    <div class="card" style="margin-bottom:20px">
        <h2>Sustainability Projection</h2>
        <p style="color:var(--muted);font-style:italic;margin-bottom:10px">{proj_desc}</p>
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
        payoff_by_merchant = defaultdict(lambda: {"total": 0.0, "last_date": None})
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
    if milestones_section or sustainability_card:
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
.cat-row {{ cursor: pointer; }}
.cat-row:hover {{ background: #eef2f7 !important; }}
.cat-arrow {{ color: var(--muted); font-size: 0.8em; margin-right: 4px; display: inline-block; }}
.cat-detail .data-table {{ font-size: 0.85em; margin: 0; }}
.cat-detail .data-table tbody tr:hover {{ background: #eef2f7; }}
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
{net_worth_card}
<div class="stats">
    {overview_stats}
</div>
{savings_rate_section}

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
{'<div class="tab-panel" id="tab-milestones">' + sustainability_card + milestones_section + '</div>' if (milestones_section or sustainability_card) else ''}

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
    document.querySelectorAll('.cat-row').forEach(function(row) {{
        row.addEventListener('click', function() {{
            var detail = row.nextElementSibling;
            if (!detail || !detail.classList.contains('cat-detail')) return;
            var arrow = row.querySelector('.cat-arrow');
            if (detail.style.display === 'none') {{
                detail.style.display = '';
                arrow.textContent = '\u25be';
            }} else {{
                detail.style.display = 'none';
                arrow.textContent = '\u25b8';
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

    {savings_rate_chart_js}

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
    parser.add_argument("--no-ai", action="store_true", help="Skip AI recommendations even if cached")
    parser.add_argument("--source", choices=["csv", "statements"], default="csv",
                        help="Financial data source: csv (portfolio.csv) or statements (PDF statements)")
    args = parser.parse_args()

    folder = os.path.abspath(args.path)
    print(f"Reading CSVs from: {folder}")

    # Load user category overrides from categories.csv
    config._user_categories = load_user_categories(folder)

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
        empirical = compute_empirical_growth_rate(passive_income)
        if empirical:
            passive_income["empirical_growth"] = empirical
            print(f"Empirical monthly growth rate: {empirical['monthly_growth_rate']*100:.3f}% ({empirical['annualized_rate']*100:.1f}%/yr, {empirical['data_points']} data points, {empirical['date_range'][0]} to {empirical['date_range'][1]})")
            for pa in sorted(empirical["per_account"], key=lambda x: x["avg_balance"], reverse=True):
                ann = (1 + pa["monthly_return"]) ** 12 - 1
                print(f"  {pa['account']:40s}  {pa['monthly_return']*100:+.3f}%/mo  {ann*100:+.1f}%/yr  avg ${pa['avg_balance']:>12,.0f}  ({pa['data_points']} pts)")
        else:
            print("Empirical growth: insufficient data (< 3 data points), using CSV rates")

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

    ai_cache_path = os.path.join(folder, ".ai_cache.html")
    ai_html = None
    if args.no_ai:
        pass
    elif args.ai:
        ai_html = get_ai_recommendations(data, passive_income=passive_income,
                                          corporate_income=corporate_income,
                                          incoming_etransfers=incoming_etransfers,
                                          bank_interest=bank_interest,
                                          notes=user_notes)
        if ai_html:
            with open(ai_cache_path, "w", encoding="utf-8") as f:
                f.write(ai_html)
            print(f"AI recommendations cached to {ai_cache_path}")
    elif os.path.exists(ai_cache_path):
        with open(ai_cache_path, "r", encoding="utf-8") as f:
            ai_html = f.read()
        print("Loaded cached AI recommendations (use --no-ai to skip, --ai to refresh)")

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
