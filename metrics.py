"""Dashboard metric calculations — pure functions, independently testable.

All functions take raw data and return computed values. No HTML generation,
no I/O, no side effects.
"""

from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timedelta

from config import CASHBACK_RATE, CORPORATE_TAKE_HOME_RATE, DEBT_PAYOFF_THRESHOLDS, INTEREST_RATES, SUSTAINABILITY_PROJECTION_MONTHS


@dataclass
class DashboardMetrics:
    """All computed metrics needed by the HTML renderer."""
    # Display window
    months: list[str]
    month_labels: list[str]
    sub_months: list[str]              # last 6 months for tables
    recent_months: list[str]           # last 6 months for trailing averages

    # Adjusted spending
    adjusted_monthly: dict[str, float]
    cashback_monthly: dict[str, float]
    cashback_total: float
    adjusted_total: float
    adjusted_avg: float
    burn_rate: float

    # Debt
    debt_payoff_total: float
    annual_interest_saved: float

    # Income components
    passive_by_month: dict[str, float]
    monthly_passive: float
    annual_passive: float
    registered_monthly: float
    corp_revenue_avg: float
    corp_div_avg: float
    corp_revenue_takehome: float
    corp_monthly_takehome: float
    corp_trailing: list[str]
    corp_trailing_n: int
    etransfer_in_monthly_avg: float
    bank_interest_monthly_avg: float
    other_income_monthly: float
    combined_monthly: float
    has_income: bool
    income_by_month: dict[str, float]
    total_income_actual: float

    # Savings rate
    savings_rate_by_month: dict[str, float]
    savings_dollars_by_month: dict[str, float]
    savings_rates_list: list[float]
    savings_3mo_avg: float
    savings_current: float

    # Coverage / sustainability
    coverage_pct: float
    coverage_color: str
    coverage_label: str
    sustainability_gap: float
    accessible_balance: float

    # Fixed vs discretionary
    fixed_pct: float
    fixed_disc_labels: list[str]
    fixed_per_month: list[float]
    disc_per_month: list[float]
    fixed_avg: float
    disc_avg: float

    # Sustainability projection
    projection: dict | None = None     # projection data if available

    # Timeline events
    timeline_events: list[tuple] = field(default_factory=list)


def compute_all_metrics(data: dict,
                        passive_income: dict | None = None,
                        corporate_income: dict | None = None,
                        incoming_etransfers: list | None = None,
                        bank_interest: list | None = None) -> DashboardMetrics:
    """Compute all dashboard metrics from raw data.

    This is the single entry point. It calls individual computation functions
    in dependency order and returns a DashboardMetrics object.
    """
    months = data["months"][-12:]
    month_labels = [datetime.strptime(m, "%Y-%m").strftime("%b %Y") for m in months]
    sub_months = months[-6:]
    recent_months = months[-6:]

    # ── Debt payoffs ──
    debt_payoffs = data.get("debt_payoffs", [])
    debt_payoff_total = sum(d.amount for d in debt_payoffs)
    annual_interest_saved = sum(
        d.amount * INTEREST_RATES.get(d.merchant, 0) for d in debt_payoffs
    )

    # ── Adjusted spending ──
    source_breakdown = data.get("source_breakdown", {})
    adjusted_monthly, cashback_monthly, cashback_total = _compute_adjusted_monthly(
        months, data, source_breakdown, debt_payoffs)
    adjusted_total = sum(adjusted_monthly.values())
    adjusted_avg = adjusted_total / len(months) if months else 0
    burn_rate = _compute_burn_rate(adjusted_monthly, recent_months)

    # ── Fixed vs discretionary ──
    fixed_pct = round(data.get("fixed_total", 0) / data["total"] * 100, 1) if data["total"] > 0 else 0
    fixed_disc_labels, fixed_per_month, disc_per_month = _compute_fixed_disc(months, data)
    fixed_avg = sum(fixed_per_month) / len(fixed_per_month) if fixed_per_month else 0
    disc_avg = sum(disc_per_month) / len(disc_per_month) if disc_per_month else 0

    # ── Income components ──
    registered_monthly = passive_income["registered_monthly"] if passive_income else 0
    annual_passive = (
        (passive_income["annual_income"] if passive_income else 0) +
        (passive_income["registered_annual"] if passive_income else 0)
    )

    passive_by_month = _compute_passive_by_month(passive_income)
    monthly_passive = (
        sum(passive_by_month.get(m, 0) for m in sub_months) / len(sub_months)
        if sub_months else 0
    )

    (corp_revenue_avg, corp_div_avg, corp_revenue_takehome,
     corp_monthly_takehome, corp_trailing, corp_trailing_n) = _compute_corporate_averages(
        corporate_income)

    recent_months_set = set(recent_months)
    etransfer_in_monthly_avg, bank_interest_monthly_avg, other_income_monthly = (
        _compute_other_income_monthly(
            incoming_etransfers, bank_interest, recent_months_set, len(recent_months)))

    combined_monthly = monthly_passive + corp_monthly_takehome + other_income_monthly
    has_income = bool(passive_income or corporate_income or incoming_etransfers or bank_interest)

    income_by_month = _compute_income_by_month(
        months, passive_by_month, monthly_passive,
        corporate_income, incoming_etransfers, bank_interest)

    total_income_actual = _compute_total_income_actual(
        corporate_income, incoming_etransfers, bank_interest)

    # ── Savings rate ──
    (savings_rate_by_month, savings_dollars_by_month, savings_rates_list,
     savings_3mo_avg, savings_current) = _compute_savings_rate(
        months, income_by_month, adjusted_monthly)

    # ── Sustainability ──
    coverage_pct, coverage_color, coverage_label, sustainability_gap = (
        _compute_sustainability(combined_monthly, burn_rate))

    accessible_balance = passive_income.get("accessible_balance", 0) if passive_income else 0

    # ── Sustainability projection ──
    projection = _compute_projection(
        passive_income, accessible_balance, burn_rate,
        corp_monthly_takehome, other_income_monthly, monthly_passive)

    # ── Timeline events ──
    timeline_events = _compute_timeline_events(
        debt_payoffs, corporate_income, passive_income,
        monthly_passive, annual_passive, burn_rate, combined_monthly)

    return DashboardMetrics(
        months=months,
        month_labels=month_labels,
        sub_months=sub_months,
        recent_months=recent_months,
        adjusted_monthly=adjusted_monthly,
        cashback_monthly=cashback_monthly,
        cashback_total=cashback_total,
        adjusted_total=adjusted_total,
        adjusted_avg=adjusted_avg,
        burn_rate=burn_rate,
        debt_payoff_total=debt_payoff_total,
        annual_interest_saved=annual_interest_saved,
        passive_by_month=passive_by_month,
        monthly_passive=monthly_passive,
        annual_passive=annual_passive,
        registered_monthly=registered_monthly,
        corp_revenue_avg=corp_revenue_avg,
        corp_div_avg=corp_div_avg,
        corp_revenue_takehome=corp_revenue_takehome,
        corp_monthly_takehome=corp_monthly_takehome,
        corp_trailing=corp_trailing,
        corp_trailing_n=corp_trailing_n,
        etransfer_in_monthly_avg=etransfer_in_monthly_avg,
        bank_interest_monthly_avg=bank_interest_monthly_avg,
        other_income_monthly=other_income_monthly,
        combined_monthly=combined_monthly,
        has_income=has_income,
        income_by_month=income_by_month,
        total_income_actual=total_income_actual,
        savings_rate_by_month=savings_rate_by_month,
        savings_dollars_by_month=savings_dollars_by_month,
        savings_rates_list=savings_rates_list,
        savings_3mo_avg=savings_3mo_avg,
        savings_current=savings_current,
        coverage_pct=coverage_pct,
        coverage_color=coverage_color,
        coverage_label=coverage_label,
        sustainability_gap=sustainability_gap,
        accessible_balance=accessible_balance,
        fixed_pct=fixed_pct,
        fixed_disc_labels=fixed_disc_labels,
        fixed_per_month=fixed_per_month,
        disc_per_month=disc_per_month,
        fixed_avg=fixed_avg,
        disc_avg=disc_avg,
        projection=projection,
        timeline_events=timeline_events,
    )


# ── Individual computation functions ────────────────────────────────────────


def _compute_adjusted_monthly(months, data, source_breakdown, debt_payoffs):
    """Compute adjusted monthly spending: subtract debt payments and cash-back."""
    debt_merchants = set(DEBT_PAYOFF_THRESHOLDS.keys()) if debt_payoffs else set()
    adjusted_monthly = {}
    for m in months:
        m_total = data["monthly_totals"].get(m, 0)
        if debt_merchants:
            debt_in_month = sum(t.amount for t in data["monthly_txns"].get(m, [])
                                if t.merchant in debt_merchants)
            m_total -= debt_in_month
        adjusted_monthly[m] = m_total

    # Apply VISA cash-back reduction to credit card spend
    credit_by_month = source_breakdown.get("credit", {})
    cashback_monthly = {m: round(credit_by_month.get(m, 0) * CASHBACK_RATE, 2) for m in months}
    cashback_total = sum(cashback_monthly.values())
    for m in months:
        adjusted_monthly[m] -= cashback_monthly[m]

    return adjusted_monthly, cashback_monthly, cashback_total


def _compute_burn_rate(adjusted_monthly, recent_months):
    """6-month trailing average of adjusted spending."""
    if not recent_months:
        return 0
    return sum(adjusted_monthly[m] for m in recent_months) / len(recent_months)


def _compute_fixed_disc(months, data):
    """Per-month fixed vs discretionary breakdown (last 6 months)."""
    fixed_disc_months = months[-6:]
    labels = [datetime.strptime(m, "%Y-%m").strftime("%b") for m in fixed_disc_months]
    fixed_per_month = []
    disc_per_month = []
    for m in fixed_disc_months:
        fixed_m = data.get("fixed_costs", {}).get(m, 0)
        total_m = data["monthly_totals"].get(m, 0)
        disc_m = total_m - fixed_m
        fixed_per_month.append(round(fixed_m, 2))
        disc_per_month.append(round(max(disc_m, 0), 2))
    return labels, fixed_per_month, disc_per_month


def _compute_passive_by_month(passive_income):
    """Build actual monthly passive income from dividend_history, excluding Cash accounts."""
    passive_by_month: dict[str, float] = defaultdict(float)
    if passive_income:
        for cat in ["accounts", "registered_accounts"]:
            for a in passive_income.get(cat, []):
                if a.type == "Cash":
                    continue
                for dh in a.dividend_history:
                    passive_by_month[dh.month] += dh.amount
    return dict(passive_by_month)


def _compute_corporate_averages(corporate_income):
    """Compute trailing 6-month averages for corporate revenue and dividends."""
    if corporate_income:
        corp_months_all = sorted(set(
            list(corporate_income["revenue_monthly"].keys()) +
            list(corporate_income["dividends_monthly"].keys())
        ))
        corp_trailing = corp_months_all[-6:]
        corp_trailing_n = len(corp_trailing)
        corp_revenue_avg = round(
            sum(corporate_income["revenue_monthly"].get(m, 0) for m in corp_trailing) / corp_trailing_n, 2
        ) if corp_trailing_n else 0
        corp_div_avg = round(
            sum(corporate_income["dividends_monthly"].get(m, 0) for m in corp_trailing) / corp_trailing_n, 2
        ) if corp_trailing_n else 0
    else:
        corp_trailing = []
        corp_trailing_n = 0
        corp_revenue_avg = 0
        corp_div_avg = 0

    corp_revenue_takehome = round(corp_revenue_avg * CORPORATE_TAKE_HOME_RATE, 2)
    corp_monthly_takehome = corp_revenue_takehome + corp_div_avg
    return (corp_revenue_avg, corp_div_avg, corp_revenue_takehome,
            corp_monthly_takehome, corp_trailing, corp_trailing_n)


def _compute_other_income_monthly(incoming_etransfers, bank_interest, recent_months_set, n_recent):
    """Monthly averages for e-transfer and bank interest income."""
    if not n_recent:
        return 0.0, 0.0, 0.0
    etransfer_avg = round(
        sum(t.amount for t in (incoming_etransfers or []) if str(t.date)[:7] in recent_months_set) / n_recent, 2)
    bi_avg = round(
        sum(t.amount for t in (bank_interest or []) if str(t.date)[:7] in recent_months_set) / n_recent, 2)
    return etransfer_avg, bi_avg, etransfer_avg + bi_avg


def _compute_income_by_month(months, passive_by_month, monthly_passive,
                              corporate_income, incoming_etransfers, bank_interest):
    """Assemble total income per month from all sources."""
    etransfer_by_month = defaultdict(float)
    for t in (incoming_etransfers or []):
        etransfer_by_month[str(t.date)[:7]] += t.amount
    bank_int_by_month = defaultdict(float)
    for t in (bank_interest or []):
        bank_int_by_month[str(t.date)[:7]] += t.amount

    income_by_month = {}
    for m in months:
        corp_rev = corporate_income["revenue_monthly"].get(m, 0) * CORPORATE_TAKE_HOME_RATE if corporate_income else 0
        corp_div = corporate_income["dividends_monthly"].get(m, 0) if corporate_income else 0
        passive_m = passive_by_month.get(m, monthly_passive)
        etransfer_m = etransfer_by_month.get(m, 0)
        bank_int_m = bank_int_by_month.get(m, 0)
        income_by_month[m] = corp_rev + corp_div + passive_m + etransfer_m + bank_int_m
    return income_by_month


def _compute_total_income_actual(corporate_income, incoming_etransfers, bank_interest):
    """Total actual income across all sources."""
    total = 0.0
    if corporate_income:
        total += corporate_income["total_income"]
    if incoming_etransfers:
        total += sum(t.amount for t in incoming_etransfers)
    if bank_interest:
        total += sum(t.amount for t in bank_interest)
    return total


def _compute_savings_rate(months, income_by_month, adjusted_monthly):
    """Per-month savings rate, clamped to [-300%, +100%]."""
    SR_FLOOR, SR_CEIL = -300.0, 100.0
    savings_rate_by_month = {}
    savings_dollars_by_month = {}
    for m in months:
        inc = income_by_month[m]
        spend = adjusted_monthly[m]
        savings = inc - spend
        savings_dollars_by_month[m] = savings
        raw = round((savings / inc * 100), 1) if inc > 0 else 0
        savings_rate_by_month[m] = max(SR_FLOOR, min(SR_CEIL, raw))

    savings_rates_list = [savings_rate_by_month[m] for m in months]
    trailing_3 = savings_rates_list[-3:]
    savings_3mo_avg = round(sum(trailing_3) / len(trailing_3), 1) if trailing_3 else 0
    savings_current = savings_rates_list[-1] if savings_rates_list else 0
    return (savings_rate_by_month, savings_dollars_by_month, savings_rates_list,
            savings_3mo_avg, savings_current)


def _compute_sustainability(combined_monthly, burn_rate):
    """Coverage percentage, color, label, and gap."""
    if combined_monthly > 0 and burn_rate > 0:
        coverage_pct = combined_monthly / burn_rate * 100
        gap = combined_monthly - burn_rate
        if coverage_pct >= 100:
            color = "#27ae60"
            label = f"Surplus: ${gap:,.2f}/mo"
        elif coverage_pct >= 50:
            color = "#f39c12"
            label = f"Gap: ${abs(gap):,.2f}/mo to sustainability"
        else:
            color = "#e74c3c"
            label = f"Gap: ${abs(gap):,.2f}/mo to sustainability"
    else:
        coverage_pct = 0
        color = "#95a5a6"
        label = ""
        gap = 0

    return coverage_pct, color, label, gap


def _compute_projection(passive_income, accessible_balance, burn_rate,
                         corp_monthly_takehome, other_income_monthly, monthly_passive):
    """Compute sustainability projection data for chart rendering."""
    if not passive_income or accessible_balance <= 0 or burn_rate <= 0:
        return None

    annual_income_proj = passive_income["annual_income"]
    annual_growth_proj = passive_income.get("annual_growth", 0)
    annual_yield_rate = annual_income_proj / accessible_balance if accessible_balance else 0
    annual_total_return_rate = (annual_income_proj + annual_growth_proj) / accessible_balance if accessible_balance else 0
    monthly_yield_rate = annual_yield_rate / 12

    # Use TWR if available
    twr = passive_income.get("twr")
    if twr and twr["data_points"] >= 3:
        monthly_total_return_rate = twr["monthly_growth_rate"]
        twr_annualized = twr["annualized_rate"]
        using_twr = True
    else:
        monthly_total_return_rate = annual_total_return_rate / 12
        twr_annualized = None
        using_twr = False

    proj_balance = accessible_balance
    proj_labels = []
    proj_passive = []
    proj_burn = []
    crossover_month = None
    already_sustainable = (monthly_passive >= burn_rate)
    now = datetime.now()
    max_months = SUSTAINABILITY_PROJECTION_MONTHS
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

    # Build summary text
    if already_sustainable:
        summary_text = "already_sustainable"
    elif crossover_month is not None:
        cross_date = datetime(now.year, now.month, 1) + timedelta(days=32 * crossover_month)
        cross_date = cross_date.replace(day=1)
        years = crossover_month // 12
        mos = crossover_month % 12
        time_str = ""
        if years > 0:
            time_str += f"{years}y "
        time_str += f"{mos}m"
        summary_text = f"{time_str} ({cross_date.strftime('%b %Y')})"
    else:
        summary_text = "not_projected"

    # Description line
    if using_twr:
        d0 = datetime.strptime(twr["date_range"][0], "%Y-%m-%d").strftime("%b %Y")
        d1 = datetime.strptime(twr["date_range"][1], "%Y-%m-%d").strftime("%b %Y")
        proj_desc = (
            f"TWR: {twr_annualized*100:.1f}%/yr "
            f"(observed {d0} \u2013 {d1}, {twr['data_points']} data points), "
            f"{annual_yield_rate*100:.1f}% yield, ${burn_rate:,.0f}/mo burn rate."
        )
    else:
        proj_desc = f"Assuming {annual_yield_rate*100:.1f}% yield, {annual_total_return_rate*100:.1f}% total return, ${burn_rate:,.0f}/mo burn rate."

    # Chart styling
    point_radius = [0] * len(proj_passive)
    point_bg = ["#27ae60"] * len(proj_passive)
    if crossover_month is not None and crossover_month < len(point_radius):
        point_radius[crossover_month] = 8

    return {
        "proj_labels": proj_labels,
        "proj_passive": proj_passive,
        "proj_burn": proj_burn,
        "crossover_month": crossover_month,
        "already_sustainable": already_sustainable,
        "summary_text": summary_text,
        "proj_desc": proj_desc,
        "using_twr": using_twr,
        "point_radius": point_radius,
        "point_bg": point_bg,
    }


def _compute_timeline_events(debt_payoffs, corporate_income, passive_income,
                              monthly_passive, annual_passive, burn_rate, combined_monthly):
    """Build milestone timeline events from various sources."""
    timeline_events = []

    # Debt payoff events
    if debt_payoffs:
        payoff_by_merchant = defaultdict(lambda: {"total": 0.0, "last_date": None})
        for d in debt_payoffs:
            payoff_by_merchant[d.merchant]["total"] += d.amount
            dt = d.date
            prev = payoff_by_merchant[d.merchant]["last_date"]
            if prev is None or dt > prev:
                payoff_by_merchant[d.merchant]["last_date"] = dt
        for merchant, info in payoff_by_merchant.items():
            rate = INTEREST_RATES.get(merchant, 0)
            annual_saved = info["total"] * rate
            detail = f"${info['total']:,.2f} principal eliminated"
            if annual_saved > 0:
                detail += f" &mdash; saving ${annual_saved:,.2f}/yr in interest"
            timeline_events.append((info["last_date"], "\u2705", f"{merchant} Paid Off", detail, "#27ae60"))

    # Corporate milestones
    if corporate_income:
        earliest = corporate_income.get("earliest_txn_date")
        first_rev = corporate_income.get("first_revenue")
        first_div = corporate_income.get("first_dividend")
        if earliest:
            timeline_events.append((earliest, "\U0001f3e2", "Corporate Ventures Launch", "Tall Tree Technology &amp; Britton Holdings accounts opened", "#4e79a7"))
        if first_rev:
            timeline_events.append((first_rev["date"], "\U0001f4b5", "First Tall Tree Revenue", f"First client payment received &mdash; ${first_rev['amount']:,.2f}", "#27ae60"))
        if first_div:
            timeline_events.append((first_div["date"], "\U0001f4c8", "First Corporate Dividend", f"First investment dividend from Britton Holdings &mdash; ${first_div['amount']:,.2f}", "#4e79a7"))

    # Passive income milestone
    if passive_income and monthly_passive > 0:
        timeline_events.append((datetime.now().date(), "\U0001f33f", "Portfolio Yielding Passive Income", f"${monthly_passive:,.2f}/mo from {passive_income.get('account_count', 0)} accounts (${annual_passive:,.2f}/yr)", "#27ae60"))

    # Sustainability milestone
    if passive_income and burn_rate > 0 and combined_monthly >= burn_rate:
        timeline_events.append((datetime.now().date(), "\u2b50", "Sustainability Achieved", f"Combined income (${combined_monthly:,.2f}/mo) covers burn rate (${burn_rate:,.2f}/mo)", "#f28e2b"))

    def _to_date(d):
        return d.date() if isinstance(d, datetime) else d
    timeline_events.sort(key=lambda e: _to_date(e[0]))

    return timeline_events
