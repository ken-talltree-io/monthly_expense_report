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
from datetime import datetime

import config
from config import (
    CATEGORY_CONSOLIDATION,
    load_budgets,
    load_notes,
    load_user_categories,
)
from parsers import parse_csvs
from income import (
    extract_passive_income,
    compute_modified_dietz,
    compute_net_worth_history,
    extract_transfers,
    extract_bank_interest,
    extract_corporate_income,
    load_passthrough,
    load_liabilities,
)
from analysis import analyze, get_ai_recommendations
from metrics import compute_all_metrics


# ── HTML Generation ──────────────────────────────────────────────────────────

def generate_html(data: dict, ai_html: str | None = None,
                   notes: dict | None = None, budgets: dict | None = None,
                   passive_income: dict | None = None,
                   corporate_income: dict | None = None,
                   incoming_etransfers: list | None = None,
                   bank_interest: list | None = None,
                   passthrough_adj: dict | None = None,
                   folder: str = ".") -> str:
    notes = notes or {}
    budgets = budgets or {}

    met = compute_all_metrics(data, passive_income, corporate_income,
                              incoming_etransfers, bank_interest)
    months = met.months
    month_labels = met.month_labels

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

    def severity_badge(severity):
        if severity == "alert":
            return '<span style="background:#e15759;color:#fff;padding:2px 8px;border-radius:12px;font-size:0.8em;font-weight:600">Alert</span>'
        return '<span style="background:#f39c12;color:#fff;padding:2px 8px;border-radius:12px;font-size:0.8em;font-weight:600">Warning</span>'

    def type_label(atype):
        labels = {"large_transaction": "Large Txn", "category_spike": "Category Spike", "new_merchant": "New Merchant"}
        return labels.get(atype, atype)

    def month_grouped_rows(txns, render_row, colspan=2):
        by_month = {}
        for t in txns:
            m = str(t.date)[:7]
            by_month.setdefault(m, []).append(t)
        rows = ""
        for m in sorted(by_month, reverse=True):
            month_txns = by_month[m]
            label = datetime.strptime(m, "%Y-%m").strftime("%b %Y")
            total = sum(t.amount for t in month_txns)
            rows += f'<tr class="group-header"><td colspan="{colspan}">{label}</td><td style="text-align:right">{money(total)}</td></tr>'
            for t in sorted(month_txns, key=lambda x: x.date, reverse=True):
                rows += f'<tr>{render_row(t)}</tr>'
        return rows

    # ── Data preparation (from metrics) ──
    fixed_detail = data.get("fixed_cost_detail", [])
    fixed_total = data.get("fixed_total", 0)
    discretionary_total = data.get("discretionary_total", 0)
    fixed_pct = met.fixed_pct
    transfers = data.get("transfers", {})
    debt_payoffs = data.get("debt_payoffs", [])
    debt_payoff_total = met.debt_payoff_total
    annual_interest_saved = met.annual_interest_saved
    adjusted_monthly = met.adjusted_monthly
    cashback_monthly = met.cashback_monthly
    cashback_total = met.cashback_total
    adjusted_total = met.adjusted_total
    adjusted_avg = met.adjusted_avg
    recent_months = met.recent_months
    burn_rate = met.burn_rate

    # ── Build table rows ──

    # Sub months (last 6 only) and headers
    sub_months = met.sub_months
    sub_month_headers = "".join(f"<th class='sub-month-col' style='text-align:right'>{datetime.strptime(m, '%Y-%m').strftime('%b %Y')}</th>" for m in sub_months)

    # Subscription table rows — grouped by status
    sub_by_status = defaultdict(list)
    for s in data["subscriptions"]:
        sub_by_status[s.status].append(s)

    status_order = ["new", "price_change", "stopped", "stable"]
    status_labels = {"stable": "Stable", "price_change": "Price Change", "new": "New", "stopped": "Stopped"}

    # Compute visible (6-month) averages for subscriptions
    def _visible_avg(s):
        amounts = [s.history.get(m, 0) for m in sub_months if s.history.get(m, 0) > 0]
        return sum(amounts) / len(amounts) if amounts else s.avg

    sub_rows = ""
    total_monthly = sum(_visible_avg(s) for s in data["subscriptions"])
    for status in status_order:
        subs = sub_by_status.get(status, [])
        if not subs:
            continue
        group_total = sum(_visible_avg(s) for s in subs)
        num_cols = len(sub_months) + 2  # Service + Avg + months
        label = status_labels.get(status, status.title())
        sub_rows += f'<tr class="group-header"><td colspan="{num_cols}">{status_badge(status)} {label} — {money(group_total)}/mo ({len(subs)})</td></tr>'
        for s in subs:
            v_avg = _visible_avg(s)
            month_cells = ""
            for m in sub_months:
                val = s.history.get(m, 0)
                if val > 0:
                    month_cells += f"<td class='sub-month-col' style='text-align:right'>{money(val)}</td>"
                else:
                    month_cells += "<td class='sub-month-col' style='text-align:center;color:#ccc'>—</td>"
            alert_html = "<br>".join(f"<small style='color:#e74c3c'>{a}</small>" for a in s.alerts) if s.alerts else ""
            note = notes.get(s.merchant.lower(), "")
            note_html = f"<br><small style='color:#4e79a7;font-style:italic'>Note: {note}</small>" if note else ""
            sub_rows += f"""<tr>
            <td><strong>{s.merchant}</strong>{('<br>' + alert_html) if alert_html else ''}{note_html}</td>
            <td style="text-align:right">{money(v_avg)}</td>
            {month_cells}
        </tr>"""

    # Fixed costs breakdown rows (reuse sub_months for consistency)
    num_months = len(sub_months)
    fixed_rows = ""
    fixed_monthly_totals = {m: 0 for m in sub_months}
    for merchant, total, month_amounts in fixed_detail:
        visible_total = sum(month_amounts.get(m, 0) for m in sub_months)
        if visible_total == 0:
            continue
        active_months = sum(1 for m in sub_months if month_amounts.get(m, 0) > 0)
        avg_per_month = visible_total / active_months if active_months else 0
        month_cells = ""
        for m in sub_months:
            val = month_amounts.get(m, 0)
            if val > 0:
                fixed_monthly_totals[m] += val
                month_cells += f"<td class='sub-month-col' style='text-align:right'>{money(val)}</td>"
            else:
                month_cells += "<td class='sub-month-col' style='text-align:center;color:#ccc'>—</td>"
        fixed_rows += f"<tr><td><strong>{merchant}</strong></td><td style='text-align:right'>{money(avg_per_month)}</td>{month_cells}</tr>"
    fixed_footer_cells = "".join(f"<td class='sub-month-col' style='text-align:right'>{money(fixed_monthly_totals[m])}</td>" for m in sub_months)
    visible_fixed_total = sum(fixed_monthly_totals.values())
    fixed_avg_per_month = money(visible_fixed_total / num_months if num_months else 0)

    # Interac e-Transfer detail table (last 6 months, grouped by month, sorted by date)
    etransfer_txns = sorted(
        [t for txns in data["monthly_txns"].values() for t in txns if t.merchant == "Interac e-Transfer" and str(t.date)[:7] in set(sub_months)],
        key=lambda t: t.date, reverse=True
    )
    etransfer_total = sum(t.amount for t in etransfer_txns)
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
    def _etransfer_out_row(t):
        date_str = str(t.date)[:10]
        amt_str = f'{t.amount:.2f}'
        note = etransfer_notes.get((date_str, amt_str), "")
        note_html = f'<span style="color:var(--muted);font-style:italic">{note}</span>' if note else ""
        return f'<td>{date_str}</td><td>{note_html}</td><td style="text-align:right">{money(t.amount)}</td>'
    etransfer_rows = month_grouped_rows(etransfer_txns, _etransfer_out_row)

    # Category heatmap (last 6 months)
    heatmap_months = months[-6:]
    heatmap_month_headers = ""
    for i, m in enumerate(heatmap_months):
        label = datetime.strptime(m, '%Y-%m').strftime('%b')
        if i == len(heatmap_months) - 1:
            heatmap_month_headers += f"<th style='text-align:right;border-bottom:3px solid var(--accent);font-weight:700'>{label}</th>"
        else:
            heatmap_month_headers += f"<th style='text-align:right'>{label}</th>"
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
            heatmap_txns_by_cat[t.category].append(t)

    # ── Anomalies grouped by category (for heatmap integration) ──
    anomalies = data.get("anomalies", [])
    anomalies_by_cat = defaultdict(list)
    for a in anomalies:
        anomalies_by_cat[a["category"]].append(a)

    current_month = datetime.now().strftime("%Y-%m")

    col_count = 9
    last_col_idx = len(heatmap_months) - 1

    heatmap_row_data = []
    for c, t, a, n in data["categories"]:
        monthly_vals = [data["category_monthly"].get(c, {}).get(m, 0) for m in heatmap_months]
        cat_total = sum(monthly_vals)
        cat_avg = cat_total / len(monthly_vals) if monthly_vals else 0
        cells = ""
        for idx, val in enumerate(monthly_vals):
            intensity = (val / heatmap_global_max) if heatmap_global_max > 0 else 0
            bg = f"rgba(78, 121, 167, {intensity:.2f})"
            text_color = "#fff" if intensity > 0.5 else "var(--text)"
            cell_text = money(val) if val > 0 else '<span style="color:#ccc">\u2014</span>'
            if idx == last_col_idx:
                cells += f"<td style='text-align:right;background:{bg};color:{text_color};border-left:2px solid rgba(78,121,167,0.3);border-right:2px solid rgba(78,121,167,0.3)'>{cell_text}</td>"
            else:
                cells += f"<td style='text-align:right;background:{bg};color:{text_color}'>{cell_text}</td>"
        avg_cell = f"<td style='text-align:right;font-weight:600'>{money(cat_avg)}</td>"
        total_cell = f"<td style='text-align:right;font-weight:600'>{money(cat_total)}</td>"

        # Build detail data: top 5 merchants and top 5 transactions
        cat_txns = heatmap_txns_by_cat.get(c, [])
        merchant_totals = defaultdict(float)
        for tx in cat_txns:
            merchant_totals[tx.merchant] += tx.amount
        top_merchants = sorted(merchant_totals.items(), key=lambda x: x[1], reverse=True)[:5]
        top_txns = sorted(cat_txns, key=lambda x: x.amount, reverse=True)[:5]

        merchant_rows = ""
        for merch_name, merch_total in top_merchants:
            merch_label = merch_name
            if merch_name == "Interac e-Transfer":
                et_notes_list = []
                for tx in cat_txns:
                    if tx.merchant == "Interac e-Transfer":
                        et_n = etransfer_notes.get((str(tx.date)[:10], f'{tx.amount:.2f}'), "")
                        if et_n and et_n not in et_notes_list:
                            et_notes_list.append(et_n)
                if et_notes_list:
                    notes_html = ", ".join(et_notes_list)
                    merch_label += f'<br><span style="color:var(--muted);font-style:italic;font-size:0.85em">{notes_html}</span>'
            merchant_rows += f"<tr><td>{merch_label}</td><td style='text-align:right'>{money(merch_total)}</td></tr>"
        txn_rows = ""
        for tx in top_txns:
            tx_label = tx.merchant
            if tx.merchant == "Interac e-Transfer":
                et_note = etransfer_notes.get((str(tx.date)[:10], f'{tx.amount:.2f}'), "")
                if et_note:
                    tx_label += f'<br><span style="color:var(--muted);font-style:italic;font-size:0.85em">{et_note}</span>'
            txn_rows += f"<tr><td>{tx_label}</td><td style='text-align:center'>{tx.date.strftime('%b %d')}</td><td style='text-align:right'>{money(tx.amount)}</td></tr>"

        # Anomaly badges for this category
        cat_anomalies = anomalies_by_cat.get(c, [])
        alert_count = sum(1 for a in cat_anomalies if a["severity"] == "alert")
        warn_count = len(cat_anomalies) - alert_count
        anom_badge = ""
        if alert_count:
            anom_badge += f' <span style="background:#e15759;color:#fff;padding:1px 5px;border-radius:8px;font-size:0.7em">{alert_count}</span>'
        if warn_count:
            anom_badge += f' <span style="background:#f39c12;color:#fff;padding:1px 5px;border-radius:8px;font-size:0.7em">{warn_count}</span>'

        # Anomaly detail rows for drill-down
        anom_detail = ""
        if cat_anomalies:
            anom_detail_rows = ""
            for a in cat_anomalies:
                anom_detail_rows += f"<tr><td>{severity_badge(a['severity'])}</td><td>{type_label(a['type'])}</td><td>{a['description']}</td><td style='text-align:right'>{money(a['amount'])}</td></tr>"
            anom_detail = (
                f'<div style="margin-top:12px">'
                f'<div style="font-weight:600;margin-bottom:6px;font-size:0.85em;color:#e15759">Anomalies</div>'
                f'<div class="table-scroll"><table class="data-table"><thead><tr><th>Severity</th><th>Type</th><th>Description</th><th style="text-align:right">Amount</th></tr></thead>'
                f'<tbody>{anom_detail_rows}</tbody></table></div></div>'
            )

        detail_html = ""
        if cat_txns:
            detail_html = (
                f'<tr class="cat-detail" style="display:none">'
                f'<td colspan="{col_count}" style="padding:0">'
                f'<div style="padding:12px 20px;background:#f8f9fa;border-top:1px solid var(--border)">'
                f'<div class="detail-grid">'
                f'<div>'
                f'<div style="font-weight:600;margin-bottom:6px;font-size:0.85em;color:var(--accent)">Top Merchants</div>'
                f'<div class="table-scroll"><table class="data-table"><thead><tr><th>Merchant</th><th style="text-align:right">Total</th></tr></thead>'
                f'<tbody>{merchant_rows}</tbody></table></div>'
                f'</div>'
                f'<div>'
                f'<div style="font-weight:600;margin-bottom:6px;font-size:0.85em;color:var(--accent)">Biggest Transactions</div>'
                f'<div class="table-scroll"><table class="data-table"><thead><tr><th>Merchant</th><th style="text-align:center">Date</th><th style="text-align:right">Amount</th></tr></thead>'
                f'<tbody>{txn_rows}</tbody></table></div>'
                f'</div>'
                f'</div>'
                f'{anom_detail}'
                f'</div></td></tr>'
            )

        cat_row = (
            f'<tr class="cat-row"><td><span class="cat-arrow">\u25b8</span> {c}{anom_badge}</td>'
            f'{cells}{avg_cell}{total_cell}</tr>'
            f'{detail_html}'
        )
        heatmap_row_data.append((cat_total, cat_row))
    heatmap_row_data.sort(key=lambda x: x[0], reverse=True)
    heatmap_rows = "".join(row for _, row in heatmap_row_data)

    # Heatmap totals row
    hm_month_totals = [sum(data["category_monthly"].get(c, {}).get(m, 0) for c, _, _, _ in data["categories"]) for m in heatmap_months]
    hm_grand_total = sum(hm_month_totals)
    hm_grand_avg = hm_grand_total / len(heatmap_months) if heatmap_months else 0
    hm_total_cells = "".join(f"<td style='text-align:right'>{money(v)}</td>" for v in hm_month_totals)
    heatmap_tfoot = (
        f'<tfoot><tr style="font-weight:700"><td>Total</td>{hm_total_cells}'
        f'<td style="text-align:right">{money(hm_grand_avg)}</td>'
        f'<td style="text-align:right">{money(hm_grand_total)}</td></tr>'
        f'<tr style="color:var(--muted);font-size:0.85em"><td colspan="{len(heatmap_months) + 3}" style="text-align:right;padding-top:2px">Avg/Total over last 6 months (unadjusted)</td></tr>'
        f'</tfoot>'
    )

    # (Anomalies are now integrated into the Category Heatmap drill-down)

    # Trend indicator
    trend_arrow = "\u2191" if data["mom_change"] > 0 else "\u2193" if data["mom_change"] < 0 else "\u2192"
    trend_color = "#e74c3c" if data["mom_change"] > 5 else "#27ae60" if data["mom_change"] < -5 else "#f39c12"

    num_months = len(months)

    # Fixed vs discretionary per-month data (last 6 months) for stacked bar chart
    fixed_disc_labels = json.dumps(met.fixed_disc_labels)
    fixed_per_month = met.fixed_per_month
    disc_per_month = met.disc_per_month
    fixed_per_month_json = json.dumps(fixed_per_month)
    disc_per_month_json = json.dumps(disc_per_month)
    fixed_avg = met.fixed_avg
    disc_avg = met.disc_avg

    # AI section
    ai_section = ""
    if ai_html:
        ai_section = f"""
        <section id="recommendations" class="card">
            <h2>AI-Powered Recommendations</h2>
            <div class="ai-recommendations">{ai_html}</div>
        </section>"""

    # ── Income & savings (from metrics) ──
    registered_monthly = met.registered_monthly
    monthly_passive = met.monthly_passive
    annual_passive = met.annual_passive
    passive_by_month = met.passive_by_month
    corp_revenue_avg = met.corp_revenue_avg
    corp_div_avg = met.corp_div_avg
    corp_revenue_takehome = met.corp_revenue_takehome
    corp_monthly_takehome = met.corp_monthly_takehome
    corp_trailing_n = met.corp_trailing_n
    etransfer_in_monthly_avg = met.etransfer_in_monthly_avg
    bank_interest_monthly_avg = met.bank_interest_monthly_avg
    other_income_monthly = met.other_income_monthly
    combined_monthly = met.combined_monthly
    has_income = met.has_income
    income_by_month = met.income_by_month
    savings_rate_by_month = met.savings_rate_by_month
    savings_dollars_by_month = met.savings_dollars_by_month
    savings_rates_list = met.savings_rates_list
    savings_3mo_avg = met.savings_3mo_avg
    savings_current = met.savings_current

    # ── Savings rate section HTML ──
    def _sr_color(rate):
        return "#27ae60" if rate >= 0 else "#e74c3c"
    savings_rate_section = ""
    if has_income:
        sr_chart_labels = json.dumps(month_labels)
        sr_chart_data = json.dumps(savings_rates_list)
        sr_income_data = json.dumps([round(income_by_month[m], 2) for m in months])
        sr_spending_data = json.dumps([round(adjusted_monthly[m], 2) for m in months])
        sr_savings_data = json.dumps([round(savings_dollars_by_month[m], 2) for m in months])

        savings_rate_section = f"""
    <div class="card">
        <h2>Savings Rate</h2>
        <p class="section-desc">Percentage of income retained each month. Savings rate = (income &minus; spending) &divide; income.</p>
        <div class="stats">
            <div class="stat">
                <div class="value" style="color:{_sr_color(savings_current)}">{savings_current:+.1f}%</div>
                <div class="label">Current Month</div>
            </div>
            <div class="stat">
                <div class="value" style="color:{_sr_color(savings_3mo_avg)}">{savings_3mo_avg:+.1f}%</div>
                <div class="label">3-Month Avg</div>
            </div>
        </div>
        <div class="chart-container">
            <canvas id="savingsRateChart" height="100"></canvas>
        </div>
    </div>"""

        sr_avg_line = json.dumps([savings_3mo_avg] * len(months))
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
                label: '3-Mo Avg ({savings_3mo_avg}%)',
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
                x: {{
                    ticks: {{ maxRotation: 45, maxTicksLimit: 8 }}
                }},
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

    # Sustainability (from metrics)
    coverage_pct = met.coverage_pct
    coverage_color = met.coverage_color
    coverage_label = met.coverage_label
    accessible_balance = met.accessible_balance

    # Hero card: passive income vs burn rate
    hero_card = ""
    if has_income:
        bar_fill = min(coverage_pct, 100)
        # Savings runway line
        savings_line = ""
        if accessible_balance > 0 and burn_rate > 0:
            net_draw = max(burn_rate - combined_monthly, 0)
            if net_draw > 0:
                runway = accessible_balance / net_draw
                savings_line = f'<div class="metric-sub" style="margin-top:4px">Accessible savings: {money(accessible_balance)} &middot; {runway:.0f} months runway</div>'
            else:
                savings_line = f'<div class="metric-sub" style="margin-top:4px">Accessible savings: {money(accessible_balance)}</div>'
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
            <div class="hero-sep">+</div>
            <div class="hero-block text-center">
                <div class="metric-label">Other Income</div>
                <div class="metric-value text-positive">{money(other_income_monthly)}<span class="unit">/mo</span></div>
                <div class="metric-sub">{other_subtitle}</div>
            </div>"""
        hero_card = f"""
    <div class="card">
        <div class="hero-layout">
            <div class="hero-block">
                <div class="metric-label">Income</div>
                <div class="metric-value text-positive">{money(corp_monthly_takehome)}<span class="unit">/mo</span></div>
                <div class="metric-sub">corporate take-home</div>
            </div>
            <div class="hero-sep">+</div>
            <div class="hero-block text-center">
                <div class="metric-label">Passive Income</div>
                <div class="metric-value text-positive">{money(monthly_passive)}<span class="unit">/mo</span></div>
                <div class="metric-sub">portfolio yield</div>
            </div>
            {other_income_block}
            <div class="hero-sep">vs</div>
            <div class="hero-block text-right">
                <div class="metric-label">Burn Rate</div>
                <div class="metric-value text-negative">{money(burn_rate)}<span class="unit">/mo</span></div>
                <div class="metric-sub">6-month trailing avg (net of 2% cash-back)</div>
            </div>
        </div>
        <div style="margin-top:20px">
            <div style="display:flex;justify-content:space-between;margin-bottom:6px">
                <span style="font-size:0.85em;font-weight:600;color:{coverage_color}">Coverage: {coverage_pct:.0f}%</span>
                <span style="font-size:0.85em;color:{coverage_color}">{coverage_label}</span>
            </div>
            <div class="progress-bar">
                <div class="progress-fill" style="width:{bar_fill:.0f}%;background:{coverage_color}"></div>
            </div>
            {savings_line}
        </div>
    </div>"""

    # ── Sustainability Projection (from metrics) ──
    sustainability_card = ""
    sustainability_chart_js = ""
    proj = met.projection
    if proj:
        proj_desc = proj["proj_desc"]
        summary_text = proj["summary_text"]
        if summary_text == "already_sustainable":
            summary_html = '<div style="font-size:1.1em;font-weight:600;color:#27ae60;margin:10px 0">You\'re already sustainable! Passive income covers your burn rate.</div>'
        elif summary_text == "not_projected":
            summary_html = '<div style="font-size:1.1em;font-weight:600;color:#e74c3c;margin:10px 0">Not projected within 10 years at current rates</div>'
        else:
            summary_html = f'<div style="font-size:1.1em;font-weight:600;color:#27ae60;margin:10px 0">Sustainability projected in {summary_text}</div>'

        proj_labels_json = json.dumps(proj["proj_labels"])
        proj_passive_json = json.dumps(proj["proj_passive"])
        proj_burn_json = json.dumps(proj["proj_burn"])
        point_radius_json = json.dumps(proj["point_radius"])
        point_bg_json = json.dumps(proj["point_bg"])

        sustainability_card = f"""
    <div class="card">
        <h2>Sustainability Projection</h2>
        <p class="section-desc" style="margin-bottom:10px">{proj_desc}</p>
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
        nw_hist = passive_income.get("net_worth_history")
        if nw_hist:
            latest = nw_hist[-1]
            nw_accessible = latest["accessible"]
            nw_registered = latest["registered"]
            nw_property = latest["property"]
            nw_corporate = latest["corporate"]
        else:
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
            <div class="nw-item">
                <div class="nw-label">Accessible</div>
                <div class="nw-value">{fmt_compact(nw_accessible)}</div>
            </div>
            <div class="nw-item">
                <div class="nw-label">Registered</div>
                <div class="nw-value">{fmt_compact(nw_registered)}</div>
            </div>"""
        if nw_property > 0:
            nw_metrics += f"""
            <div class="nw-item">
                <div class="nw-label">Property</div>
                <div class="nw-value">{fmt_compact(nw_property)}</div>
            </div>"""
        if nw_corporate > 0:
            nw_metrics += f"""
            <div class="nw-item">
                <div class="nw-label">Corporate</div>
                <div class="nw-value">{fmt_compact(nw_corporate)}</div>
            </div>"""
        nw_metrics += f"""
            <div class="nw-item">
                <div class="nw-label" style="color:var(--accent);font-weight:600">Total</div>
                <div class="nw-value" style="font-size:1.6em;font-weight:700;color:var(--accent)">{fmt_compact(nw_total)}</div>
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
                    f'<span class="metric-sub" style="font-size:0.8em">{label}</span></span>'
                )
        nw_legend = "".join(nw_legend_items)

        net_worth_card = f"""
    <div class="card">
        <h2>Net Worth</h2>
        <div class="nw-metrics">
            {nw_metrics}
        </div>
        <div class="nw-bar">
            {nw_bar_html}
        </div>
        <div class="nw-legend">{nw_legend}</div>
    </div>"""

    # ── Net Worth History chart ──
    net_worth_history_card = ""
    net_worth_history_chart_js = ""
    nw_history = passive_income.get("net_worth_history") if passive_income else None
    if nw_history and len(nw_history) >= 2:
        nwh_labels = json.dumps([datetime.strptime(r["month"], "%Y-%m").strftime("%b %Y") for r in nw_history])
        nwh_accessible = json.dumps([r["accessible"] for r in nw_history])
        nwh_registered = json.dumps([r["registered"] for r in nw_history])
        nwh_corporate = json.dumps([r["corporate"] for r in nw_history])
        nwh_property = json.dumps([r["property"] for r in nw_history])
        nwh_liabilities = json.dumps([r.get("liabilities", 0) for r in nw_history])
        nwh_total = json.dumps([r["total"] for r in nw_history])
        # Summary: total change over period
        first_total = nw_history[0]["total"]
        last_total = nw_history[-1]["total"]
        nwh_change = last_total - first_total
        nwh_change_pct = (nwh_change / first_total * 100) if first_total > 0 else 0
        nwh_color = "#27ae60" if nwh_change >= 0 else "#e15759"
        nwh_arrow = "+" if nwh_change >= 0 else ""
        if abs(nwh_change) >= 1_000_000:
            nwh_change_str = f"{nwh_arrow}${nwh_change/1_000_000:,.2f}M"
        elif abs(nwh_change) >= 1_000:
            nwh_change_str = f"{nwh_arrow}${nwh_change/1_000:,.0f}K"
        else:
            nwh_change_str = f"{nwh_arrow}{money(nwh_change)}"

        net_worth_history_card = f"""
    <div class="card">
        <h2>Net Worth Over Time</h2>
        <div style="display:flex;align-items:center;gap:18px;margin-bottom:10px;flex-wrap:wrap">
            <span style="font-size:1.1em;font-weight:600;color:{nwh_color}">{nwh_change_str} ({nwh_arrow}{nwh_change_pct:.1f}%)</span>
            <span class="metric-sub">{nw_history[0]['month']} to {nw_history[-1]['month']}</span>
            <label style="margin-left:auto;font-size:0.85em;cursor:pointer;display:flex;align-items:center;gap:4px">
                <input type="checkbox" id="nwhExclProperty" style="cursor:pointer"> Excl. Property
            </label>
        </div>
        <div class="chart-container">
            <canvas id="netWorthHistoryChart" height="120"></canvas>
        </div>
    </div>"""

        net_worth_history_chart_js = f"""
    var nwhChart = new Chart(document.getElementById('netWorthHistoryChart'), {{
        type: 'line',
        data: {{
            labels: {nwh_labels},
            datasets: [
                {{
                    label: 'Accessible',
                    data: {nwh_accessible},
                    borderColor: '#4e79a7',
                    backgroundColor: 'rgba(78, 121, 167, 0.35)',
                    fill: true,
                    tension: 0.3,
                    pointRadius: 2,
                    borderWidth: 2,
                    order: 4
                }},
                {{
                    label: 'Registered',
                    data: {nwh_registered},
                    borderColor: '#76b7b2',
                    backgroundColor: 'rgba(118, 183, 178, 0.35)',
                    fill: true,
                    tension: 0.3,
                    pointRadius: 2,
                    borderWidth: 2,
                    order: 3
                }},
                {{
                    label: 'Corporate',
                    data: {nwh_corporate},
                    borderColor: '#59a14f',
                    backgroundColor: 'rgba(89, 161, 79, 0.35)',
                    fill: true,
                    tension: 0.3,
                    pointRadius: 2,
                    borderWidth: 2,
                    order: 2
                }},
                {{
                    label: 'Property',
                    data: {nwh_property},
                    borderColor: '#f28e2b',
                    backgroundColor: 'rgba(242, 142, 43, 0.35)',
                    fill: true,
                    tension: 0.3,
                    pointRadius: 2,
                    borderWidth: 2,
                    order: 1
                }},
                {{
                    label: 'Liabilities',
                    data: {nwh_liabilities},
                    borderColor: '#e15759',
                    backgroundColor: 'rgba(225, 87, 89, 0.25)',
                    fill: 'origin',
                    borderDash: [6, 3],
                    tension: 0.3,
                    pointRadius: 0,
                    borderWidth: 2,
                    stack: 'liabilities'
                }},
                {{
                    label: 'Total Net Worth',
                    data: {nwh_total},
                    borderColor: '#333',
                    backgroundColor: 'transparent',
                    fill: false,
                    tension: 0.3,
                    pointRadius: 3,
                    borderWidth: 2.5,
                    stack: 'total',
                    order: 0
                }}
            ]
        }},
        options: {{
            responsive: true,
            interaction: {{ mode: 'index', intersect: false }},
            plugins: {{
                legend: {{ position: 'bottom' }},
                tooltip: {{
                    mode: 'index',
                    intersect: false,
                    callbacks: {{
                        label: function(ctx) {{
                            var v = ctx.parsed.y;
                            var abs = Math.abs(v);
                            var fmt = abs >= 1000000 ? '$' + (abs/1000000).toFixed(2) + 'M' : '$' + (abs/1000).toFixed(0) + 'k';
                            if (v < 0) fmt = '-' + fmt;
                            return ctx.dataset.label + ': ' + fmt;
                        }}
                    }}
                }}
            }},
            scales: {{
                x: {{ stacked: true, ticks: {{ maxRotation: 45, maxTicksLimit: 8 }} }},
                y: {{
                    stacked: true,
                    ticks: {{
                        callback: function(v) {{
                            var abs = Math.abs(v);
                            var fmt = abs >= 1000000 ? '$' + (abs/1000000).toFixed(1) + 'M' : '$' + (abs/1000).toFixed(0) + 'k';
                            return v < 0 ? '-' + fmt : fmt;
                        }}
                    }}
                }},
            }}
        }}
    }});
    // Excl. Property checkbox
    var nwhProperty = {nwh_property};
    var nwhTotalOrig = {nwh_total};
    var nwhCb = document.getElementById('nwhExclProperty');
    if (nwhCb) {{
        nwhCb.addEventListener('change', function() {{
            // Property is dataset index 3, Total Net Worth is index 5
            nwhChart.data.datasets[3].hidden = this.checked;
            var totalDs = nwhChart.data.datasets[5];
            if (this.checked) {{
                totalDs.data = nwhTotalOrig.map(function(v, i) {{ return v - nwhProperty[i]; }});
            }} else {{
                totalDs.data = nwhTotalOrig.slice();
            }}
            nwhChart.update();
        }});
    }}"""

    # ── Total income & milestones (from metrics) ──
    total_income_actual = met.total_income_actual
    timeline_events = met.timeline_events

    def _to_date(d):
        return d.date() if isinstance(d, datetime) else d

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
            summary_html = '<p class="section-desc">' + ". ".join(summary_parts) + ".</p>"

        milestones_section = f"""
<section class="card">
    <h2>Timeline</h2>
    {summary_html}
    <table style="width:100%;border-collapse:separate;border-spacing:0">
        <tbody>{timeline_rows}</tbody>
    </table>
</section>"""

    # (Fixed vs Discretionary is now integrated into the Subscription Audit card)

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

    # ── Incoming e-Transfers data prep ──
    incoming_etransfers = incoming_etransfers or []
    etransfer_in_rows = ""
    etransfer_in_total = 0
    if incoming_etransfers:
        etransfer_in_notes = {}
        in_notes_path = os.path.join(folder, "etransfer-notes-in.csv")
        if os.path.exists(in_notes_path):
            with open(in_notes_path, newline="") as f:
                for row in csv.DictReader(f):
                    amt = row["amount"].replace("$", "").replace(",", "")
                    key = (row["date"], amt)
                    if row.get("note", "").strip():
                        etransfer_in_notes[key] = row["note"].strip()
        etransfer_in_total = sum(t.amount for t in incoming_etransfers)
        def _etransfer_in_row(t):
            date_str = str(t.date)[:10]
            amt_str = f'{t.amount:.2f}'
            note = etransfer_in_notes.get((date_str, amt_str), "")
            note_html = f'<span style="color:var(--muted);font-style:italic">{note}</span>' if note else ""
            return f'<td>{date_str}</td><td>{note_html}</td><td style="text-align:right">{money(t.amount)}</td>'
        etransfer_in_rows = month_grouped_rows(incoming_etransfers, _etransfer_in_row)

    # ── Bank Interest data prep ──
    bank_interest = bank_interest or []
    bi_rows = ""
    bi_total = 0
    if bank_interest:
        bi_total = sum(t.amount for t in bank_interest)
        def _bi_row(t):
            date_str = str(t.date)[:10]
            return f'<td>{date_str}</td><td>{t.account}</td><td style="text-align:right">{money(t.amount)}</td>'
        bi_rows = month_grouped_rows(bank_interest, _bi_row)

    # ── Income tab top-level stats (trailing 6 months, matching burn rate) ──
    income_tab_stats = ""
    if has_income:
        income_trailing = months[-6:] if len(months) >= 6 else months
        income_trailing_n = len(income_trailing) or 1
        income_trailing_total = sum(income_by_month.get(m, 0) for m in income_trailing)
        income_trailing_avg = income_trailing_total / income_trailing_n
        income_tab_stats = f"""
<div class="stats">
    <div class="stat"><div class="value" style="color:#27ae60">{money(income_trailing_total)}</div><div class="label">Total Income ({income_trailing_n} months)</div></div>
    <div class="stat"><div class="value" style="color:#27ae60">{money(income_trailing_avg)}</div><div class="label">6-Mo Avg Income</div></div>"""
        if cashback_total > 0:
            income_tab_stats += f"""
    <div class="stat"><div class="value" style="color:#27ae60">{money(cashback_total)}</div><div class="label">VISA Cash-Back ({len(months)} months)</div></div>"""
        income_tab_stats += """
</div>"""

    # ── Corporate Income section ──
    corporate_section = ""
    if corporate_income:
        corporate_section = f"""
<section id="corporate-income" class="card">
    <div style="display:flex;justify-content:space-between;align-items:baseline;flex-wrap:wrap">
        <h2 style="margin-bottom:0">Corporate Income</h2>
        <div style="font-size:1.3em;font-weight:700;color:#27ae60">{money(corp_monthly_takehome)}<span style="font-size:0.55em;font-weight:400;color:var(--muted)">/mo take-home</span></div>
    </div>
    <p class="section-desc">Revenue from Tall Tree Technology (client payments) and dividends from Britton Holdings Growth (investment portfolio)</p>
    {corp_revenue_warning}
    <div class="table-scroll"><table class="data-table table-narrow">
        <thead><tr><th>Month</th><th style="text-align:right">Revenue (Tall Tree)</th><th style="text-align:right">Dividends (BH Growth)</th><th style="text-align:right">Total</th></tr></thead>
        <tbody>{corp_rows}</tbody>
        <tfoot>
            <tr style="font-weight:700"><td>Trailing {corp_trailing_n}-Mo Avg</td><td style="text-align:right">{money(corp_revenue_avg)}</td><td style="text-align:right">{money(corp_div_avg)}</td><td style="text-align:right">{money(corp_trailing_total_avg)}</td></tr>
        </tfoot>
    </table></div>
</section>"""

    # ── Other Income section (e-transfers + bank interest, last 6 months, columnar) ──
    other_income_section = ""
    sub_months_set = set(sub_months)
    if incoming_etransfers or bank_interest or passive_income:
        # Build source-level monthly totals for columnar display
        other_sources = {}  # key -> {month: total}
        other_total = 0
        other_count = 0

        # Seed rows for all Cash accounts so they always appear
        if passive_income:
            for a in passive_income.get("cash_accounts", []):
                other_sources[f"Interest: {a.account}"] = {}

        for t in incoming_etransfers:
            m = str(t.date)[:7]
            if m not in sub_months_set:
                continue
            date_str = str(t.date)[:10]
            amt_str = f'{t.amount:.2f}'
            note = etransfer_in_notes.get((date_str, amt_str), "") if incoming_etransfers else ""
            key = f"e-Transfer: {note}" if note else "e-Transfer"
            other_sources.setdefault(key, {})
            other_sources[key][m] = other_sources[key].get(m, 0) + t.amount
            other_total += t.amount
            other_count += 1
        for t in bank_interest:
            m = str(t.date)[:7]
            if m not in sub_months_set:
                continue
            key = f"Interest: {t.account}"
            other_sources.setdefault(key, {})
            other_sources[key][m] = other_sources[key].get(m, 0) + t.amount
            other_total += t.amount
            other_count += 1

        sorted_sources = sorted(other_sources.items(), key=lambda x: sum(x[1].values()), reverse=True)
        oi_month_headers = "".join(
            f"<th class='sub-month-col' style='text-align:right'>{datetime.strptime(m, '%Y-%m').strftime('%b %Y')}</th>"
            for m in sub_months
        )
        # Compute global max for heatmap scale
        oi_global_max = max(
            (v for _, mm in sorted_sources for v in mm.values()), default=0
        )
        other_rows = ""
        oi_month_totals = [0.0] * len(sub_months)
        for key, month_map in sorted_sources:
            cells = ""
            row_total = 0.0
            active_months = 0
            for i, m in enumerate(sub_months):
                val = month_map.get(m, 0)
                if val > 0:
                    row_total += val
                    oi_month_totals[i] += val
                    active_months += 1
                    intensity = (val / oi_global_max) if oi_global_max > 0 else 0
                    bg = f"rgba(39, 174, 96, {intensity:.2f})"
                    text_color = "#fff" if intensity > 0.5 else "var(--text)"
                    cells += f"<td class='sub-month-col' style='text-align:right;background:{bg};color:{text_color}'>{money(val)}</td>"
                else:
                    cells += "<td class='sub-month-col' style='text-align:center;color:#ccc'>\u2014</td>"
            avg = row_total / active_months if active_months else 0
            other_rows += f"<tr><td>{key}</td><td style='text-align:right'>{money(avg)}</td>{cells}<td style='text-align:right;font-weight:600'>{money(row_total)}</td></tr>"

        oi_grand_total = sum(oi_month_totals)
        oi_avg = oi_grand_total / len(sub_months) if sub_months else 0
        oi_footer_cells = "".join(f"<td class='sub-month-col' style='text-align:right'>{money(t)}</td>" for t in oi_month_totals)

        other_income_section = f"""
<section id="other-income" class="card">
    <div style="display:flex;justify-content:space-between;align-items:baseline;flex-wrap:wrap">
        <h2 style="margin-bottom:0">Other Income</h2>
        <div style="font-size:1.3em;font-weight:700;color:#27ae60">{money(other_income_monthly)}<span style="font-size:0.55em;font-weight:400;color:var(--muted)">/mo avg</span></div>
    </div>
    <p class="section-desc">e-Transfer reimbursements and bank interest (last 6 months) &mdash; {other_count} transactions totalling {money(other_total)}</p>
    {''.join(f'<p style="font-size:0.85em;color:var(--muted);margin:0.3em 0"><em>Adjusted for {money(adj)} pass-through ({desc}): &minus;{money(adj)} in interest excluded</em></p>' for desc, adj in (passthrough_adj or {}).items())}
    <div class="table-scroll"><table class="data-table" style="max-width:100%">
        <thead><tr><th>Source</th><th style="text-align:right">Avg</th>{oi_month_headers}<th style="text-align:right">Total</th></tr></thead>
        <tbody>{other_rows}</tbody>
        <tfoot>
            <tr style="font-weight:700"><td>Total</td><td style="text-align:right">{money(oi_avg)}</td>{oi_footer_cells}<td style="text-align:right">{money(oi_grand_total)}</td></tr>
        </tfoot>
    </table></div>
</section>"""

    # ── Passive Income section ──
    def balance_cell(a) -> str:
        """Render a balance <td> with source annotation."""
        src = a.balance_source
        dt = a.statement_date
        val = money(a.value)
        if src and src != "portfolio.csv":
            note = dt if dt else src
            return (f"<td style='text-align:right'>{val}"
                    f"<br><span style='font-size:0.75em;color:var(--muted)'>{note}</span></td>")
        else:
            return (f"<td style='text-align:right;font-style:italic'>{val}"
                    f"<br><span style='font-size:0.75em;color:#e67e22'>csv</span></td>")

    def return_cell(a, has_twr: bool = False) -> str:
        """Render a return % <td> with source annotation.

        When has_twr is True and the source is 'estimated', suppress the
        estimated return in favour of the TWR column.
        """
        pct = a.return_pct or 0
        src = a.return_source
        if has_twr and src == "estimated":
            return "<td style='text-align:right;color:var(--muted)'>—</td>"
        if not src and pct == 0:
            return "<td style='text-align:right;color:var(--muted)'>—</td>"
        if src == "csv":
            return (f"<td style='text-align:right;font-style:italic'>{pct:.1f}%"
                    f"<br><span style='font-size:0.75em;color:#e67e22'>csv</span></td>")
        note = src.replace(" statement", "").replace(" report", "")
        return (f"<td style='text-align:right'>{pct:.1f}%"
                f"<br><span style='font-size:0.75em;color:var(--muted)'>{note}</span></td>")

    def income_cell(a) -> str:
        """Render Income/yr <td> with source annotation."""
        val = a.income_annual
        src = a.income_source
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

    def growth_cell(a) -> str:
        """Render Growth/yr <td>."""
        val = a.growth_annual
        if val == 0:
            return "<td style='text-align:right;color:var(--muted)'>—</td>"
        color = "#27ae60" if val > 0 else "#e74c3c"
        return f"<td style='text-align:right;color:{color}'>{money(val)}</td>"

    passive_section = ""
    if passive_income:
        # Build TWR lookup by account name
        twr_by_account = {}
        eg = passive_income.get("twr")
        if eg and eg.get("per_account"):
            for pa in eg["per_account"]:
                twr_by_account[pa["account"]] = pa

        def twr_cell(acct_name: str) -> str:
            pa = twr_by_account.get(acct_name)
            if not pa:
                return "<td style='text-align:right;color:var(--muted)'>—</td>"
            ann = (1 + pa["monthly_return"]) ** 12 - 1
            color = "#27ae60" if ann >= 0 else "#e74c3c"
            return f"<td style='text-align:right;color:{color}'>{ann*100:.1f}%/yr<br><span style='font-size:0.8em;color:var(--muted)'>{pa['data_points']} pts</span></td>"

        # Combined portfolio table — all accounts sorted by TWR desc
        acc_total_balance = passive_income["accessible_balance"]
        acc_total_income = passive_income["annual_income"]
        acc_total_growth = passive_income.get("annual_growth", 0)
        acc_monthly = passive_income["monthly_income"]

        def _sort_twr(a):
            pa = twr_by_account.get(a.account)
            if pa:
                return ((1 + pa["monthly_return"]) ** 12 - 1) * 100
            return a.return_pct

        all_accounts = list(passive_income["accounts"])
        reg_accounts = passive_income.get("registered_accounts", [])
        if reg_accounts:
            all_accounts.extend(reg_accounts)

        acc_sorted = sorted(all_accounts, key=_sort_twr, reverse=True)

        # Tag each account so we can identify accessible vs registered
        reg_names = {a.account for a in reg_accounts} if reg_accounts else set()

        portfolio_rows = ""
        total_balance = acc_total_balance + passive_income.get("registered_balance", 0)
        total_growth = acc_total_growth + passive_income.get("registered_growth", 0)
        for a in acc_sorted:
            has_twr = a.account in twr_by_account
            acct_type = a.type
            if a.account in reg_names:
                acct_type += " <span style='font-size:0.75em;color:var(--muted)'>(reg)</span>"
            portfolio_rows += (
                f"<tr><td>{a.account}</td><td>{a.brokerage}</td><td>{acct_type}</td>"
                f"{balance_cell(a)}"
                f"{return_cell(a, has_twr=has_twr)}"
                f"{growth_cell(a)}"
                f"{twr_cell(a.account)}</tr>"
            )

        portfolio_monthly = acc_monthly + registered_monthly

        # Build monthly dividend breakdown table (accounts as rows, months as columns)
        # Exclude Cash accounts — their interest is in Other Income (bank interest)
        all_div_accounts = []  # [(name, {month: amount})]
        for a in acc_sorted:
            if a.type == "Cash":
                continue
            dh = a.dividend_history
            monthly_map = {entry.month: entry.amount for entry in dh}
            has_any_income = any(entry.amount > 0 for entry in dh)
            is_ws_investment = a.balance_source == "Wealthsimple statement" and a.type == "Non-reg"
            if has_any_income or a.income_annual > 0 or is_ws_investment:
                all_div_accounts.append((a.account, monthly_map))

        dividend_breakdown_html = ""
        div_avg = 0
        if all_div_accounts:
            # Sort by 6-month total descending
            all_div_accounts.sort(
                key=lambda x: sum(x[1].get(m, 0) for m in sub_months), reverse=True
            )
            div_month_headers = "".join(
                f"<th class='sub-month-col' style='text-align:right'>{datetime.strptime(m, '%Y-%m').strftime('%b %Y')}</th>"
                for m in sub_months
            )
            # Compute global max across all cells for heatmap scale
            div_global_max = max(
                (mm.get(m, 0) for _, mm in all_div_accounts for m in sub_months), default=0
            )
            div_rows = ""
            div_month_totals = [0.0] * len(sub_months)
            for name, mm in all_div_accounts:
                cells = ""
                row_total = 0.0
                active_months = 0
                for i, m in enumerate(sub_months):
                    val = mm.get(m, 0)
                    if val > 0:
                        row_total += val
                        div_month_totals[i] += val
                        active_months += 1
                        intensity = (val / div_global_max) if div_global_max > 0 else 0
                        bg = f"rgba(39, 174, 96, {intensity:.2f})"
                        text_color = "#fff" if intensity > 0.5 else "var(--text)"
                        cells += f"<td class='sub-month-col' style='text-align:right;background:{bg};color:{text_color}'>{money(val)}</td>"
                    elif m in mm:
                        cells += "<td class='sub-month-col' style='text-align:right;color:var(--muted)'>$0</td>"
                    else:
                        cells += "<td class='sub-month-col' style='text-align:center;color:#ccc'>\u2014</td>"
                avg = row_total / active_months if active_months > 0 else 0
                div_rows += f"<tr><td>{name}</td>{cells}<td style='text-align:right'>{money(avg)}</td><td style='text-align:right;font-weight:600'>{money(row_total)}</td></tr>"

            div_grand_total = sum(div_month_totals)
            div_avg = div_grand_total / len(sub_months) if sub_months else 0
            div_footer_cells = "".join(f"<td class='sub-month-col' style='text-align:right'>{money(t)}</td>" for t in div_month_totals)
            dividend_breakdown_html = f"""
    <h3 style="margin-top:30px">Monthly Dividends</h3>
    <div class="table-scroll"><table class="data-table" style="max-width:100%">
        <thead><tr><th>Account</th>{div_month_headers}<th style="text-align:right">Avg</th><th style="text-align:right">Total</th></tr></thead>
        <tbody>{div_rows}</tbody>
        <tfoot>
            <tr style="font-weight:700"><td>Total</td>{div_footer_cells}<td style="text-align:right">{money(div_avg)}</td><td style="text-align:right">{money(div_grand_total)}</td></tr>
        </tfoot>
    </table></div>"""

        passive_section = f"""
<section id="passive-income" class="card">
    <div style="display:flex;justify-content:space-between;align-items:baseline;flex-wrap:wrap">
        <h2 style="margin-bottom:0">Investment Portfolio</h2>
        <div style="font-size:1.3em;font-weight:700;color:#27ae60">{money(div_avg)}<span style="font-size:0.55em;font-weight:400;color:var(--muted)">/mo income</span></div>
    </div>
    <p class="section-desc">Yield and growth from personal investment accounts — sorted by TWR. Registered accounts marked <span style="color:var(--muted)">(reg)</span>.</p>
    <div class="table-scroll"><table class="data-table" style="max-width:100%">
        <thead><tr><th>Account</th><th>Brokerage</th><th>Type</th><th style="text-align:right">Balance</th><th style="text-align:right" title="All-time return reported by brokerage (performance report or statement)">Return</th><th style="text-align:right" title="Annual capital appreciation (return minus income)">Growth/yr</th><th style="text-align:right" title="Modified Dietz return annualized from statement balance history">TWR</th></tr></thead>
        <tbody>{portfolio_rows}</tbody>
        <tfoot>
            <tr style="font-weight:700"><td colspan="3">Total Portfolio</td><td style="text-align:right">{money(total_balance)}</td><td style="text-align:right"></td><td style="text-align:right">{money(total_growth)}</td><td></td></tr>
        </tfoot>
    </table></div>
    {dividend_breakdown_html}
</section>"""

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
                x: {{ stacked: true, ticks: {{ maxRotation: 45, maxTicksLimit: 8 }} }},
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
@media (max-width: 768px) {{
    .chart-row {{ grid-template-columns: 1fr; }}
    .hero-layout {{ gap: 10px; }}
    .hero-block {{ min-width: 120px; }}
    .hero-sep {{ flex: 0 0 30px; font-size: 1.4em; }}
    .metric-value {{ font-size: 1.6em; }}
    .nw-metrics {{ gap: 6px; }}
    .nw-item .nw-value {{ font-size: 1.2em; }}
    .stats {{ grid-template-columns: repeat(auto-fit, minmax(150px, 1fr)); gap: 10px; }}
    .stat {{ padding: 14px; }}
    .stat .value {{ font-size: 1.4em; }}
    .tab-nav {{ padding: 10px 12px; gap: 6px; }}
    .tab-nav button {{ padding: 10px 16px; font-size: 0.82em; min-height: 44px; }}
    .card {{ padding: 18px; }}
    body {{ padding: 12px; }}
}}
@media (max-width: 480px) {{
    .hero-layout {{ flex-direction: column; align-items: stretch; text-align: center; }}
    .hero-block {{ min-width: auto; text-align: center !important; }}
    .hero-sep {{ flex: 0 0 auto; font-size: 1.2em; }}
    .metric-value {{ font-size: 1.4em; }}
    .metric-value .unit {{ font-size: 0.5em; }}
    .nw-metrics {{ flex-direction: column; }}
    .nw-item {{ min-width: auto; }}
    .stats {{ grid-template-columns: 1fr 1fr; }}
    h1 {{ font-size: 1.4em; }}
    h2 {{ font-size: 1.1em; }}
    .data-table {{ font-size: 0.8em; }}
    .data-table th, .data-table td {{ padding: 6px 8px; }}
    .detail-grid {{ grid-template-columns: 1fr; }}
    .tab-nav button {{ padding: 8px 12px; min-height: 44px; }}
    .sub-month-col {{ display: none; }}
}}
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
.ai-recommendations table {{ width: 100%; border-collapse: collapse; }}
.ai-recommendations th {{ text-align: left; padding: 10px 14px; border-bottom: 2px solid var(--border); font-size: 0.85em; color: var(--muted); }}
.ai-recommendations td {{ padding: 12px 14px; border-bottom: 1px solid var(--border); vertical-align: top; }}
.ai-recommendations td:first-child {{ width: 2em; text-align: center; font-weight: 700; color: var(--accent); }}
.ai-recommendations td:last-child {{ width: 20%; font-size: 0.9em; }}
.ai-badge {{ font-size: 0.65em; background: #f39c12; color: #fff; padding: 2px 8px; border-radius: 10px; margin-left: 8px; cursor: pointer; vertical-align: middle; text-decoration: none; }}
.ai-badge:hover {{ background: #e67e22; }}
canvas {{ max-width: 100%; }}
.noscript-table {{ margin-top: 10px; }}
.tab-nav {{ display: flex; flex-wrap: wrap; gap: 8px; background: var(--card); border-radius: 12px; padding: 15px 25px; margin-bottom: 20px; box-shadow: 0 1px 3px rgba(0,0,0,0.08); }}
.tab-nav button {{ border: none; cursor: pointer; font-family: inherit; color: var(--accent); background: var(--bg); padding: 7px 18px; border-radius: 20px; font-size: 0.88em; font-weight: 500; transition: background 0.15s, color 0.15s; min-height: 44px; }}
.tab-nav button:hover {{ background: var(--accent); color: #fff; }}
.tab-nav button.active {{ background: var(--accent); color: #fff; }}
.tab-panel {{ display: none; }}
.tab-panel.active {{ display: block; }}
/* Utility classes */
.text-right {{ text-align: right; }}
.text-center {{ text-align: center; }}
.text-muted {{ color: var(--muted); }}
.text-dim {{ color: #ccc; }}
.text-positive {{ color: #27ae60; }}
.text-negative {{ color: #e15759; }}
.fw-bold {{ font-weight: 600; }}
.section-desc {{ color: var(--muted); font-style: italic; margin-bottom: 15px; }}
.metric-label {{ font-size: 0.85em; color: var(--muted); text-transform: uppercase; letter-spacing: 0.5px; margin-bottom: 4px; }}
.metric-value {{ font-size: 2.2em; font-weight: 700; }}
.metric-value .unit {{ font-size: 0.4em; font-weight: 400; color: var(--muted); }}
.metric-sub {{ font-size: 0.85em; color: var(--muted); }}
.hero-layout {{ display: flex; align-items: center; justify-content: space-between; flex-wrap: wrap; gap: 20px; }}
.hero-block {{ flex: 1; min-width: 160px; }}
.hero-sep {{ flex: 0 0 40px; text-align: center; font-size: 1.8em; color: var(--muted); }}
.nw-metrics {{ display: flex; align-items: center; justify-content: space-around; flex-wrap: wrap; gap: 10px; margin-bottom: 18px; }}
.nw-item {{ flex: 1; min-width: 120px; text-align: center; }}
.nw-item .nw-label {{ font-size: 0.78em; color: var(--muted); text-transform: uppercase; letter-spacing: 0.5px; }}
.nw-item .nw-value {{ font-size: 1.4em; font-weight: 600; }}
.nw-bar {{ background: #eee; border-radius: 6px; height: 18px; overflow: hidden; font-size: 0; line-height: 0; white-space: nowrap; }}
.nw-legend {{ margin-top: 8px; text-align: center; }}
.progress-bar {{ background: #eee; border-radius: 6px; height: 12px; overflow: hidden; }}
.progress-fill {{ height: 100%; border-radius: 6px; transition: width 0.3s; }}
.group-header {{ background: var(--bg); font-weight: 600; }}
.table-narrow {{ max-width: 600px; }}
.table-scroll {{ overflow-x: auto; }}
.detail-grid {{ display: grid; grid-template-columns: 1fr 1fr; gap: 20px; }}
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
{net_worth_history_card}
{savings_rate_section}

</div>

<!-- ═══ INCOME ═══ -->
{'<div class="tab-panel" id="tab-income">' + income_tab_stats + corporate_section + passive_section + other_income_section + '</div>' if (corporate_income or passive_income or incoming_etransfers or bank_interest) else ''}

<!-- ═══ SPENDING ANALYSIS ═══ -->
<div class="tab-panel" id="tab-spending">
<div class="stats">
    <div class="stat"><div class="value">{money(adjusted_total)}</div><div class="label">Total Spend ({len(months)} months)</div></div>
    <div class="stat"><div class="value">{money(adjusted_avg)}</div><div class="label">Monthly Avg ({len(months)}mo, adjusted)</div></div>
    <div class="stat"><div class="value" style="color:{trend_color}">{trend_arrow} {abs(data['mom_change']):.0f}%</div><div class="label">3-Mo Avg vs Prior 3-Mo</div></div>
</div>

<section id="categories" class="card">
    <h2>Category Heatmap</h2>
    <p class="section-desc">Spending intensity by category over the last 6 months, sorted by total. Darker cells = higher spend.</p>
    <div class="table-scroll">
    <table class="data-table">
        <thead><tr><th>Category</th>{heatmap_month_headers}<th style="text-align:right">Avg</th><th style="text-align:right">6m Total</th></tr></thead>
        <tbody>{heatmap_rows}</tbody>
        {heatmap_tfoot}
    </table>
    </div>
</section>

<section id="subscriptions" class="card">
    <h2>Subscription Audit</h2>
    <p class="section-desc">Recurring charges detected across your statements, grouped by status. {fixed_pct}% of total spending is fixed.</p>
    <div class="stats">
        <div class="stat"><div class="value">{fixed_pct}%</div><div class="label">Fixed Costs</div></div>
        <div class="stat"><div class="value">{money(fixed_avg)}</div><div class="label">Fixed / Month</div></div>
        <div class="stat"><div class="value">{money(disc_avg)}</div><div class="label">Discretionary / Month</div></div>
    </div>
    <div class="chart-container" style="max-height:200px"><canvas id="fixedDiscChart"></canvas></div>
    <div class="table-scroll">
    <table class="data-table">
        <thead><tr><th>Service</th><th style="text-align:right">Avg/Mo</th>{sub_month_headers}</tr></thead>
        <tbody>{sub_rows}</tbody>
        <tfoot><tr style="font-weight:700"><td>Total Subscriptions</td><td style="text-align:right">{money(total_monthly)}/mo</td><td class="sub-month-col" colspan="{len(sub_months)}"></td></tr></tfoot>
    </table>
    </div>
</section>

{'<section id="fixed-costs" class="card"><h2>Fixed Costs Breakdown</h2><p class="section-desc">All fixed-cost merchants (utilities, insurance, etc.) with monthly amounts over the last 6 months.</p><div class="table-scroll"><table class="data-table"><thead><tr><th>Merchant</th><th style="text-align:right">Avg/Mo</th>' + sub_month_headers + '</tr></thead><tbody>' + fixed_rows + '</tbody><tfoot><tr style="font-weight:700"><td>Total Fixed Costs</td><td style="text-align:right">' + fixed_avg_per_month + '/mo</td>' + fixed_footer_cells + '</tr></tfoot></table></div></section>' if fixed_detail else ''}

{'<section id="interac-transfers" class="card"><h2>Interac e-Transfer Details</h2><p class="section-desc">All outgoing e-Transfers &mdash; ' + str(len(etransfer_txns)) + ' transactions totalling ' + money(etransfer_total) + '</p><div class="table-scroll"><table class="data-table"><thead><tr><th>Date</th><th>Note</th><th style="text-align:right">Amount</th></tr></thead><tbody>' + etransfer_rows + '</tbody></table></div></section>' if etransfer_txns else ''}
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
    var items = document.querySelectorAll('.ai-recommendations tr[data-sections]');
    items.forEach(function(tr, idx) {{
        var tipNum = idx + 1;
        var sections = tr.getAttribute('data-sections').split(',');
        tr.id = 'ai-tip-' + tipNum;
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
                    tr.scrollIntoView({{ behavior: 'smooth', block: 'center' }});
                    tr.style.outline = '2px solid #f39c12';
                    setTimeout(function() {{ tr.style.outline = ''; }}, 2000);
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

    {net_worth_history_chart_js}

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
    args = parser.parse_args()

    folder = os.path.abspath(args.path)
    print(f"Reading CSVs from: {folder}")

    # Load user category overrides from categories.csv
    config._user_categories = load_user_categories(folder)

    # Load notes, budgets, and passthrough records
    user_notes = load_notes(folder)
    user_budgets = load_budgets(folder)
    passthrough = load_passthrough(folder)
    if passthrough:
        print(f"Loaded {len(passthrough)} passthrough record(s): {', '.join(pt.description for pt in passthrough)}")
    liabilities = load_liabilities(folder)
    if liabilities:
        total_liab = sum(l.amount for l in liabilities)
        print(f"Loaded {len(liabilities)} liability record(s): {', '.join(l.description for l in liabilities)} (${total_liab:,.2f} total)")

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
                if t.merchant == "Interac e-Transfer":
                    key = (str(t.date)[:10], f'{t.amount:.2f}')
                    if key in override_map:
                        t.category = CATEGORY_CONSOLIDATION.get(override_map[key], override_map[key])
                        count += 1
            if count:
                print(f"Applied {count} e-transfer category overrides")

    # Extract transfer data from debit card CSVs
    transfers, incoming_etransfers = extract_transfers(folder, passthrough=passthrough)
    if transfers:
        print(f"Found transfer data across {len(transfers)} months")
    if incoming_etransfers:
        print(f"Found {len(incoming_etransfers)} incoming e-transfers")

    # Extract bank interest from personal + corporate debit CSVs
    bank_interest, passthrough_adj = extract_bank_interest(folder, passthrough=passthrough)
    if bank_interest:
        bi_total = sum(t.amount for t in bank_interest)
        print(f"Found {len(bank_interest)} bank interest payments totalling ${bi_total:,.2f}")
    if passthrough_adj:
        for desc, adj in passthrough_adj.items():
            print(f"Passthrough adjustment ({desc}): −${adj:,.2f} in interest excluded")

    # Extract passive income from investment portfolio
    passive_income = extract_passive_income(folder)
    if passive_income:
        print(f"Portfolio passive income: ${passive_income['annual_income']:,.2f}/year (${passive_income['monthly_income']:,.2f}/month) from {len(passive_income['accounts'])} accounts")
        twr_result = compute_modified_dietz(passive_income, passthrough=passthrough)
        if twr_result:
            passive_income["twr"] = twr_result
            print(f"TWR: {twr_result['monthly_growth_rate']*100:.3f}%/mo ({twr_result['annualized_rate']*100:.1f}%/yr, {twr_result['data_points']} data points, {twr_result['date_range'][0]} to {twr_result['date_range'][1]})")
            for pa in sorted(twr_result["per_account"], key=lambda x: x["avg_balance"], reverse=True):
                ann = (1 + pa["monthly_return"]) ** 12 - 1
                print(f"  {pa['account']:40s}  {pa['monthly_return']*100:+.3f}%/mo  {ann*100:+.1f}%/yr  avg ${pa['avg_balance']:>12,.0f}  ({pa['data_points']} pts)")
        else:
            print("TWR: insufficient data (< 3 data points), using CSV rates")

        nw_history = compute_net_worth_history(passive_income, passthrough=passthrough, liabilities=liabilities)
        if nw_history:
            passive_income["net_worth_history"] = nw_history
            print(f"Net worth history: {len(nw_history)} months ({nw_history[0]['month']} to {nw_history[-1]['month']})")
        else:
            print("Net worth history: insufficient data (< 2 months)")

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
                         passthrough_adj=passthrough_adj,
                         folder=folder)
    output_path = os.path.join(folder, "dashboard.html")
    with open(output_path, "w", encoding="utf-8") as f:
        f.write(html)

    print(f"\nDashboard written to: {output_path}")
    print("Open it in your browser to view the report.")


if __name__ == "__main__":
    main()
