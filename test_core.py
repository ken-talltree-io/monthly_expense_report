"""Behavioural tests for the financial dashboard pipeline.

Tests what the system *does* — normalize merchants, categorize spending,
detect subscriptions, aggregate totals, and compute financial metrics —
rather than checking internal constants or data structures.
"""

import json
import os
import pytest
from datetime import date
from collections import defaultdict
from io import BytesIO
from unittest.mock import patch, MagicMock
from urllib.error import HTTPError, URLError

import config
from config import (
    normalize_merchant,
    categorize,
    CATEGORY_CONSOLIDATION,
    CASHBACK_RATE,
    CORPORATE_TAKE_HOME_RATE,
    ANOMALY_TXN_ZSCORE,
    ANOMALY_NEW_MERCHANT_MIN,
)
from analysis import analyze, detect_anomalies, get_ai_recommendations
from income import (
    compute_net_worth_history, compute_modified_dietz, extract_passive_income,
    load_passthrough, load_liabilities, extract_bank_interest, extract_transfers,
)
from parsers import parse_csvs, parse_statement_balances


# ── Helpers ─────────────────────────────────────────────────────────────────


def _txn(merchant, amount, month, category="Uncategorized", source="credit",
         fixed_cost=False):
    """Build a minimal transaction dict."""
    return {
        "merchant": merchant,
        "amount": amount,
        "month": month,
        "date": date(int(month[:4]), int(month[5:7]), 15),
        "category": category,
        "source": source,
        "fixed_cost": fixed_cost,
    }


def _months(n, start="2025-07"):
    """Generate n consecutive month strings starting from start."""
    y, m = int(start[:4]), int(start[5:7])
    result = []
    for _ in range(n):
        result.append(f"{y}-{m:02d}")
        m += 1
        if m > 12:
            m = 1
            y += 1
    return result


# ── Merchant normalization: raw bank text → clean display name ──────────────


class TestMerchantNormalization:
    """Verifies that ugly bank text gets cleaned into human-readable names."""

    def test_credit_card_amazon(self):
        assert normalize_merchant("AMAZON.CA MONTREAL QC") == "Amazon"

    def test_amazon_prime_is_distinct_from_amazon(self):
        assert normalize_merchant("AMAZON.CA PRIME") == "Amazon Prime"
        assert normalize_merchant("AMAZON.CA SOMETHING") == "Amazon"

    def test_scotiabank_no_spaces_format(self):
        """Scotiabank debit removes all spaces from merchant names."""
        assert normalize_merchant("COSTCOWHOLESALE") == "Costco"
        assert normalize_merchant("REALCDNSUPERSTORE") == "Real Canadian Superstore"

    def test_square_pos_prefix(self):
        assert normalize_merchant("SQ *MRPETS VANCOUVER BC") == "MrPets"

    def test_toast_pos_prefix(self):
        assert normalize_merchant("TST-TACOFINO MAIN ST") == "Tacofino"

    def test_debit_card_bill_payment(self):
        """Debit cards use short codes for pre-authorized payments."""
        assert normalize_merchant("FN") == "Mortgage (First National)"

    def test_unknown_merchant_gets_title_cased(self):
        result = normalize_merchant("BRAND NEW RESTAURANT 12345")
        assert result == "Brand New Restaurant"
        assert result[0].isupper()

    def test_case_insensitive(self):
        assert normalize_merchant("netflix") == "Netflix"
        assert normalize_merchant("NETFLIX") == "Netflix"

    def test_bc_ferries_substring_match(self):
        """BCF - LANGDALE should match the 'BCF - ' prefix alias."""
        assert normalize_merchant("BCF - LANGDALE") == "BC Ferries"
        assert normalize_merchant("BCF-NANAIMO") == "BC Ferries"


# ── Categorization: merchant name → spending category ───────────────────────


class TestCategorization:
    """Verifies merchants end up in the correct spending category."""

    def test_streaming_services(self):
        assert categorize("Netflix") == "Streaming & Subscriptions"
        assert categorize("Disney+") == "Streaming & Subscriptions"

    def test_groceries(self):
        assert categorize("Costco") == "Groceries"
        assert categorize("Real Canadian Superstore") == "Groceries"

    def test_restaurants(self):
        assert categorize("La Mezcaleria") == "Restaurants & Dining"

    def test_telecom(self):
        assert categorize("Telus Mobility") == "Telecom"
        assert categorize("Fido Mobile") == "Telecom"

    def test_unknown_merchant_is_uncategorized(self):
        assert categorize("Some Brand New Store") == "Uncategorized"

    def test_user_override_takes_precedence(self):
        """If the user adds a merchant→category in categories.csv, it overrides rules."""
        config._user_categories["custom test store"] = "Travel"
        try:
            assert categorize("Custom Test Store") == "Travel"
        finally:
            del config._user_categories["custom test store"]

    def test_category_consolidation_collapses_fine_grained(self):
        """Restaurants, Cafes, and Groceries all collapse into 'Food & Dining'."""
        assert CATEGORY_CONSOLIDATION["Restaurants & Dining"] == "Food & Dining"
        assert CATEGORY_CONSOLIDATION["Cafes & Treats"] == "Food & Dining"
        assert CATEGORY_CONSOLIDATION["Groceries"] == "Food & Dining"
        assert CATEGORY_CONSOLIDATION["Liquor & Alcohol"] == "Food & Dining"

    def test_normalization_to_categorization_pipeline(self):
        """Raw bank text → normalized → categorized should work end-to-end."""
        raw = "AMAZON.CA PRIME MONTHLY"
        normalized = normalize_merchant(raw)
        category = categorize(normalized)
        assert category == "Streaming & Subscriptions"


# ── Subscription detection ──────────────────────────────────────────────────


class TestSubscriptionDetection:
    """Verifies the subscription detection algorithm's behavioural rules."""

    def test_steady_monthly_charge_is_subscription(self):
        months = _months(6)
        txns = [_txn("Test Streaming", 15.99, m, "Subscriptions & Telecom") for m in months]
        subs = {s["merchant"] for s in analyze(txns)["subscriptions"]}
        assert "Test Streaming" in subs

    def test_one_off_purchase_is_not_subscription(self):
        txns = [_txn("Big Purchase Store", 500.00, "2025-10", "Shopping")]
        subs = {s["merchant"] for s in analyze(txns)["subscriptions"]}
        assert "Big Purchase Store" not in subs

    def test_cheap_recurring_charge_ignored(self):
        """Recurring charges under $5/mo are noise, not subscriptions."""
        months = _months(6)
        txns = [_txn("Micro Charge", 1.50, m, "Subscriptions & Telecom") for m in months]
        subs = {s["merchant"] for s in analyze(txns)["subscriptions"]}
        assert "Micro Charge" not in subs

    def test_wildly_variable_amounts_not_subscription(self):
        """A store with unpredictable amounts isn't a subscription."""
        months = _months(5)
        amounts = [10, 85, 20, 150, 40]
        txns = [_txn("Random Shop", a, m, "Shopping") for a, m in zip(amounts, months)]
        subs = {s["merchant"] for s in analyze(txns)["subscriptions"]}
        assert "Random Shop" not in subs

    def test_retail_needs_more_evidence_than_services(self):
        """Grocery/retail categories need 4+ months; service categories need 3."""
        # 3 months of grocery visits — should NOT be flagged
        txns_3 = [_txn("Weekly Haircut", 30.00, m, "Food & Dining") for m in _months(3)]
        subs_3 = {s["merchant"] for s in analyze(txns_3)["subscriptions"]}
        assert "Weekly Haircut" not in subs_3

        # Same merchant at 5 months — now it qualifies
        txns_5 = [_txn("Weekly Haircut", 30.00, m, "Food & Dining") for m in _months(5)]
        subs_5 = {s["merchant"] for s in analyze(txns_5)["subscriptions"]}
        assert "Weekly Haircut" in subs_5

    def test_price_increase_flagged(self):
        """A subscription that raises its price should be flagged as 'price_change'."""
        months = _months(6)
        amounts = [15.99, 15.99, 15.99, 15.99, 19.99, 19.99]
        txns = [_txn("Price Hike Svc", a, m, "Subscriptions & Telecom")
                for a, m in zip(amounts, months)]
        subs = {s["merchant"]: s for s in analyze(txns)["subscriptions"]}
        assert "Price Hike Svc" in subs
        assert subs["Price Hike Svc"]["status"] == "price_change"

    def test_known_telecom_always_detected(self):
        """Known subscription keywords like 'telus' bypass the category filter."""
        months = _months(3)
        txns = [_txn("Telus Mobility", 160.00, m, "Subscriptions & Telecom")
                for m in months]
        subs = {s["merchant"] for s in analyze(txns)["subscriptions"]}
        assert "Telus Mobility" in subs

    def test_excluded_merchant_never_detected(self):
        """Merchants in the exclusion list (e.g. Amazon) are never subscriptions."""
        months = _months(6)
        txns = [_txn("Amazon Vancouver", 15.00, m, "Shopping") for m in months]
        subs = {s["merchant"] for s in analyze(txns)["subscriptions"]}
        assert "Amazon Vancouver" not in subs


# ── Spending aggregation ────────────────────────────────────────────────────


class TestSpendingAggregation:
    """Verifies that transactions are correctly totalled and grouped."""

    @pytest.fixture
    def grocery_and_dining(self):
        return [
            _txn("Costco", 200, "2025-10", "Food & Dining"),
            _txn("Costco", 150, "2025-11", "Food & Dining"),
            _txn("Restaurant X", 80, "2025-10", "Food & Dining"),
            _txn("Gas Station", 60, "2025-10", "Auto"),
            _txn("Gas Station", 55, "2025-11", "Auto"),
        ]

    def test_total_across_all_months(self, grocery_and_dining):
        result = analyze(grocery_and_dining)
        assert result["total"] == 545.00

    def test_monthly_breakdown(self, grocery_and_dining):
        result = analyze(grocery_and_dining)
        assert result["monthly_totals"]["2025-10"] == 340.00
        assert result["monthly_totals"]["2025-11"] == 205.00

    def test_category_totals(self, grocery_and_dining):
        result = analyze(grocery_and_dining)
        cat_dict = {c: t for c, t, _, _ in result["categories"]}
        assert cat_dict["Food & Dining"] == 430.00
        assert cat_dict["Auto"] == 115.00

    def test_categories_ranked_by_spend(self, grocery_and_dining):
        result = analyze(grocery_and_dining)
        totals = [t for _, t, _, _ in result["categories"]]
        assert totals == sorted(totals, reverse=True)

    def test_monthly_avg(self, grocery_and_dining):
        result = analyze(grocery_and_dining)
        assert result["monthly_avg"] == 272.50

    def test_per_category_per_month(self, grocery_and_dining):
        result = analyze(grocery_and_dining)
        assert result["category_monthly"]["Food & Dining"]["2025-10"] == 280.00
        assert result["category_monthly"]["Food & Dining"]["2025-11"] == 150.00


# ── Fixed vs discretionary spending ─────────────────────────────────────────


class TestFixedVsDiscretionary:
    """Verifies that fixed costs (insurance, utilities) are separated from
    discretionary spending (restaurants, shopping)."""

    def test_fixed_cost_merchant_counted_as_fixed(self):
        txns = [
            _txn("Wawanesa Insurance", 240, "2025-10", "Insurance", fixed_cost=True),
            _txn("Costco", 200, "2025-10", "Food & Dining"),
        ]
        result = analyze(txns)
        assert result["fixed_total"] == 240.00
        assert result["discretionary_total"] == 200.00

    def test_fixed_costs_tracked_per_month(self):
        txns = [
            _txn("BC Hydro", 80, "2025-10", "Utilities", "debit", fixed_cost=True),
            _txn("BC Hydro", 95, "2025-11", "Utilities", "debit", fixed_cost=True),
            _txn("Store", 50, "2025-10", "Shopping"),
        ]
        result = analyze(txns)
        assert result["fixed_costs"]["2025-10"] == 80.00
        assert result["fixed_costs"]["2025-11"] == 95.00


# ── Debt payoff handling ────────────────────────────────────────────────────


class TestDebtPayoffs:
    """Verifies that large debt payoffs are tracked separately from spending."""

    def test_debt_payoffs_passed_through(self):
        txns = [_txn("Store", 100, "2025-10", "Shopping")]
        payoffs = [
            {"merchant": "Mortgage (First National)", "amount": 10000,
             "date": date(2025, 10, 1)},
        ]
        result = analyze(txns, debt_payoffs=payoffs)
        assert len(result["debt_payoffs"]) == 1
        assert result["debt_payoffs"][0]["amount"] == 10000

    def test_debt_payoffs_not_in_spending_total(self):
        """Debt payoffs should be excluded from the regular spending total."""
        txns = [_txn("Store", 100, "2025-10", "Shopping")]
        payoffs = [
            {"merchant": "Mortgage (First National)", "amount": 8000,
             "date": date(2025, 10, 1)},
        ]
        result = analyze(txns, debt_payoffs=payoffs)
        # Only the store transaction should count
        assert result["total"] == 100.00


# ── End-to-end pipeline: raw text → category → aggregation ─────────────────


class TestEndToEndPipeline:
    """Tests the full merchant→category→aggregation pipeline."""

    def test_raw_bank_text_ends_up_in_correct_category_total(self):
        """Simulates what happens when a CSV row flows through the pipeline."""
        raw_merchant = "AMAZON.CA PRIME MONTHLY"
        normalized = normalize_merchant(raw_merchant)
        fine_category = categorize(normalized)
        consolidated = CATEGORY_CONSOLIDATION.get(fine_category, fine_category)

        # Amazon Prime should end up in Subscriptions & Telecom
        assert consolidated == "Subscriptions & Telecom"

    def test_grocery_run_flows_to_food_and_dining(self):
        raw = "COSTCO WHOLESALE #123"
        normalized = normalize_merchant(raw)
        fine = categorize(normalized)
        consolidated = CATEGORY_CONSOLIDATION.get(fine, fine)
        assert consolidated == "Food & Dining"

    def test_analyze_with_mixed_categories(self):
        """Multiple categories aggregate correctly through analyze()."""
        txns = [
            _txn("Netflix", 16.99, "2025-10", "Subscriptions & Telecom"),
            _txn("Netflix", 16.99, "2025-11", "Subscriptions & Telecom"),
            _txn("Costco", 250, "2025-10", "Food & Dining"),
            _txn("Costco", 180, "2025-11", "Food & Dining"),
            _txn("BC Ferries", 185, "2025-10", "Transportation"),
        ]
        result = analyze(txns)

        # Totals
        assert result["total"] == pytest.approx(648.98)

        # Netflix should be detected as subscription
        sub_merchants = {s["merchant"] for s in result["subscriptions"]}
        assert "Netflix" in sub_merchants

        # Categories should be ranked by total
        cats = [c for c, _, _, _ in result["categories"]]
        assert cats[0] == "Food & Dining"  # $430 > $185 > $33.98

    def test_mom_trend_direction(self):
        """Month-over-month trend should reflect spending direction."""
        # Spending going UP: first 3 months low, last 3 months high
        months = _months(6)
        txns = []
        for i, m in enumerate(months):
            amount = 500 if i < 3 else 1000
            txns.append(_txn("Store", amount, m, "Shopping"))

        result = analyze(txns)
        # Recent 3-month avg ($1000) > prior 3-month avg ($500) → positive change
        assert result["mom_change"] > 0

    def test_mom_trend_stable(self):
        """Flat spending should show ~0% MoM change."""
        months = _months(6)
        txns = [_txn("Store", 500, m, "Shopping") for m in months]
        result = analyze(txns)
        assert abs(result["mom_change"]) < 1  # approximately 0%

    def test_mom_trend_with_few_months(self):
        """MoM trend with 2-3 months uses available data."""
        txns = [
            _txn("Store", 400, "2025-10", "Shopping"),
            _txn("Store", 600, "2025-11", "Shopping"),
        ]
        result = analyze(txns)
        # 2 months: recent avg = 600, prior = 400, change = +50%
        assert result["mom_change"] > 0

    def test_single_month_has_zero_mom(self):
        """A single month of data can't compute a trend."""
        txns = [_txn("Store", 500, "2025-10", "Shopping")]
        result = analyze(txns)
        assert result["mom_change"] == 0


# ── Subscription status detection ──────────────────────────────────────────


class TestSubscriptionStatus:
    """Verifies subscription status: new, stopped, price_change variants."""

    def test_loose_service_subscription_detected(self):
        """Service-category merchant with moderate variability still detected
        via the looser CV threshold (0.20 < cv < 0.40)."""
        months = _months(5)
        # avg=50, std≈10.95, cv≈0.219 — between tight (0.20) and loose (0.40)
        amounts = [35, 55, 40, 65, 55]
        txns = [_txn("Moderate Svc", a, m, "Subscriptions & Telecom")
                for a, m in zip(amounts, months)]
        subs = {s["merchant"] for s in analyze(txns)["subscriptions"]}
        assert "Moderate Svc" in subs

    def test_alternating_price_pattern_flagged(self):
        """Two alternating amounts flag 'Varies' alert when ratio > 1.20."""
        months = _months(6)
        # Alternate $80/$100: ratio 1.25 > 1.20, < 1.50
        amounts = [80, 100, 80, 100, 80, 100]
        txns = [_txn("Alternating Svc", a, m, "Subscriptions & Telecom")
                for a, m in zip(amounts, months)]
        subs = {s["merchant"]: s for s in analyze(txns)["subscriptions"]}
        assert "Alternating Svc" in subs
        assert subs["Alternating Svc"]["status"] == "price_change"
        assert any("Varies" in a for a in subs["Alternating Svc"]["alerts"])

    def test_true_multi_price_change_shows_direction(self):
        """Multiple distinct price levels produce directional alerts."""
        months = _months(6)
        # 3 distinct prices with >20% jumps between levels
        amounts = [50, 50, 70, 70, 100, 100]
        txns = [_txn("Multi Price Svc", a, m, "Subscriptions & Telecom")
                for a, m in zip(amounts, months)]
        subs = {s["merchant"]: s for s in analyze(txns)["subscriptions"]}
        assert "Multi Price Svc" in subs
        assert subs["Multi Price Svc"]["status"] == "price_change"
        assert any("increased" in a for a in subs["Multi Price Svc"]["alerts"])

    def test_price_decrease_shows_decreased_direction(self):
        """A price drop should produce a 'decreased' alert."""
        months = _months(6)
        # 3 distinct price levels with >20% drops; cv≈0.27 (within loose threshold)
        amounts = [100, 100, 75, 75, 50, 50]
        txns = [_txn("Shrinking Svc", a, m, "Subscriptions & Telecom")
                for a, m in zip(amounts, months)]
        subs = {s["merchant"]: s for s in analyze(txns)["subscriptions"]}
        assert "Shrinking Svc" in subs
        assert any("decreased" in a for a in subs["Shrinking Svc"]["alerts"])

    def test_new_subscription_detected(self):
        """A subscription first appearing in the last 2 months gets 'new' status."""
        months = _months(6)
        # Background transactions span all months to establish the dataset
        txns = [_txn("Background Store", 100, m, "Shopping") for m in months]
        # Known-sub keyword merchant appearing only in the last 2 months
        txns.append(_txn("Netflix New", 16.99, months[-2], "Subscriptions & Telecom"))
        txns.append(_txn("Netflix New", 16.99, months[-1], "Subscriptions & Telecom"))
        subs = {s["merchant"]: s for s in analyze(txns)["subscriptions"]}
        assert "Netflix New" in subs
        assert subs["Netflix New"]["status"] == "new"
        assert any("New recurring" in a for a in subs["Netflix New"]["alerts"])

    def test_stopped_subscription_detected(self):
        """A subscription absent from recent months gets 'stopped' status."""
        months = _months(6)
        # Present in first 4 months, absent in last 2
        txns = [_txn("Stopped Svc", 25.00, m, "Subscriptions & Telecom")
                for m in months[:4]]
        # Background txns to span all 6 months
        txns.extend([_txn("Background", 100, m, "Shopping") for m in months])
        subs = {s["merchant"]: s for s in analyze(txns)["subscriptions"]}
        assert "Stopped Svc" in subs
        assert subs["Stopped Svc"]["status"] == "stopped"
        assert any("Last charge" in a for a in subs["Stopped Svc"]["alerts"])


# ── Source breakdown tracking ──────────────────────────────────────────────


class TestSourceBreakdown:
    """Verifies credit vs debit spending is tracked separately by month."""

    def test_source_breakdown_separates_credit_and_debit(self):
        txns = [
            _txn("Store A", 100, "2025-10", "Shopping", source="credit"),
            _txn("Store B", 200, "2025-10", "Shopping", source="debit"),
            _txn("Store C", 150, "2025-11", "Shopping", source="credit"),
        ]
        result = analyze(txns)
        assert result["source_breakdown"]["credit"]["2025-10"] == 100.00
        assert result["source_breakdown"]["debit"]["2025-10"] == 200.00
        assert result["source_breakdown"]["credit"]["2025-11"] == 150.00


# ── CSV loader functions ──────────────────────────────────────────────────


class TestCSVLoaders:
    """Verifies that user CSV files (categories, notes, budgets) are loaded."""

    def test_load_user_categories(self, tmp_path):
        (tmp_path / "categories.csv").write_text(
            "merchant,category\nTest Store,Travel\nAnother,Shopping\n"
        )
        result = config.load_user_categories(str(tmp_path))
        assert result["test store"] == "Travel"
        assert result["another"] == "Shopping"

    def test_load_user_categories_missing_file(self, tmp_path):
        result = config.load_user_categories(str(tmp_path))
        assert result == {}

    def test_load_user_categories_skips_comments(self, tmp_path):
        (tmp_path / "categories.csv").write_text(
            "merchant,category\n# Comment,Travel\nReal Store,Shopping\n"
        )
        result = config.load_user_categories(str(tmp_path))
        assert "# comment" not in result
        assert result["real store"] == "Shopping"

    def test_load_user_categories_skips_empty_rows(self, tmp_path):
        (tmp_path / "categories.csv").write_text(
            "merchant,category\n,\nValid Store,Travel\n"
        )
        result = config.load_user_categories(str(tmp_path))
        assert len(result) == 1
        assert result["valid store"] == "Travel"

    def test_load_notes(self, tmp_path):
        (tmp_path / "notes.csv").write_text(
            "merchant,note\nCostco,Bulk holiday supplies\n"
        )
        result = config.load_notes(str(tmp_path))
        assert result["costco"] == "Bulk holiday supplies"

    def test_load_notes_missing_file(self, tmp_path):
        result = config.load_notes(str(tmp_path))
        assert result == {}

    def test_load_notes_skips_comments(self, tmp_path):
        (tmp_path / "notes.csv").write_text(
            "merchant,note\n# Ignore,This note\nReal,Actual note\n"
        )
        result = config.load_notes(str(tmp_path))
        assert "# ignore" not in result
        assert result["real"] == "Actual note"

    def test_load_budgets(self, tmp_path):
        (tmp_path / "budgets.csv").write_text(
            "category,monthly_target\nFood & Dining,$800\nShopping,\"$1,200\"\n"
        )
        result = config.load_budgets(str(tmp_path))
        assert result["Food & Dining"] == 800.0
        assert result["Shopping"] == 1200.0

    def test_load_budgets_missing_file(self, tmp_path):
        result = config.load_budgets(str(tmp_path))
        assert result == {}

    def test_load_budgets_invalid_amount_skipped(self, tmp_path):
        (tmp_path / "budgets.csv").write_text(
            "category,monthly_target\nFood,invalid\nShopping,500\n"
        )
        result = config.load_budgets(str(tmp_path))
        assert "Food" not in result
        assert result["Shopping"] == 500.0

    def test_load_budgets_skips_comments(self, tmp_path):
        (tmp_path / "budgets.csv").write_text(
            "category,monthly_target\n# Draft,999\nReal Category,300\n"
        )
        result = config.load_budgets(str(tmp_path))
        assert "# Draft" not in result
        assert result["Real Category"] == 300.0


# ── Anomaly detection ────────────────────────────────────────────────────


class TestAnomalyDetection:
    """Verifies statistical outlier detection for transactions and categories."""

    def test_large_transaction_flagged(self):
        """A transaction with z-score > 2.0 should be flagged."""
        months = _months(6)
        # 5 normal transactions + 1 outlier across 6 months
        txns = [
            _txn("Store A", 50, months[0], "Shopping"),
            _txn("Store A", 50, months[1], "Shopping"),
            _txn("Store A", 50, months[2], "Shopping"),
            _txn("Store A", 50, months[3], "Shopping"),
            _txn("Store A", 50, months[4], "Shopping"),
            _txn("Store A", 500, months[5], "Shopping"),  # outlier
        ]
        result = analyze(txns)
        large = [a for a in result["anomalies"] if a["type"] == "large_transaction"]
        assert len(large) >= 1
        assert large[0]["amount"] == 500
        assert large[0]["merchant"] == "Store A"

    def test_normal_transactions_not_flagged(self):
        """Consistent amounts should produce no large transaction anomalies."""
        months = _months(6)
        txns = [_txn("Store A", 50, m, "Shopping") for m in months]
        result = analyze(txns)
        large = [a for a in result["anomalies"] if a["type"] == "large_transaction"]
        assert len(large) == 0

    def test_category_spike_detected(self):
        """A month with spending > 1.5 std devs above mean flags a category spike."""
        months = _months(4)
        txns = [
            _txn("Store A", 100, months[0], "Shopping"),
            _txn("Store A", 110, months[1], "Shopping"),
            _txn("Store A", 90, months[2], "Shopping"),
            _txn("Store A", 500, months[3], "Shopping"),  # spike
        ]
        result = analyze(txns)
        spikes = [a for a in result["anomalies"] if a["type"] == "category_spike"]
        assert len(spikes) >= 1
        assert spikes[0]["category"] == "Shopping"

    def test_category_with_few_months_skipped(self):
        """Categories with < 3 months of history should not generate spikes."""
        months = _months(2)
        txns = [
            _txn("Store A", 100, months[0], "Shopping"),
            _txn("Store A", 500, months[1], "Shopping"),  # big but only 2 months
        ]
        result = analyze(txns)
        spikes = [a for a in result["anomalies"] if a["type"] == "category_spike"]
        assert len(spikes) == 0

    def test_new_high_spend_merchant_flagged(self):
        """A merchant appearing only in last 2 months with >= $200 total is flagged."""
        months = _months(4)
        txns = [_txn("Old Store", 100, m, "Shopping") for m in months]
        # New merchant only in last 2 months with $250 total
        txns.append(_txn("New Expensive", 150, months[-2], "Shopping"))
        txns.append(_txn("New Expensive", 100, months[-1], "Shopping"))
        result = analyze(txns)
        new_merch = [a for a in result["anomalies"] if a["type"] == "new_merchant"]
        assert len(new_merch) >= 1
        assert new_merch[0]["merchant"] == "New Expensive"
        assert new_merch[0]["amount"] == 250

    def test_new_cheap_merchant_not_flagged(self):
        """A new merchant spending < $200 should not be flagged."""
        months = _months(4)
        txns = [_txn("Old Store", 100, m, "Shopping") for m in months]
        txns.append(_txn("New Cheap", 50, months[-1], "Shopping"))
        result = analyze(txns)
        new_merch = [a for a in result["anomalies"] if a["type"] == "new_merchant"]
        new_cheap = [a for a in new_merch if a["merchant"] == "New Cheap"]
        assert len(new_cheap) == 0

    def test_anomalies_key_always_present(self):
        """The 'anomalies' key should always be in the analyze() result."""
        txns = [_txn("Store", 100, "2025-10", "Shopping")]
        result = analyze(txns)
        assert "anomalies" in result
        assert isinstance(result["anomalies"], list)

    def test_severity_levels_correct(self):
        """Extreme outliers (z > 3.0) should be 'alert', others 'warning'."""
        months = _months(12)
        # 11 normal + 1 extreme gives z ≈ √11 ≈ 3.3 → alert
        txns = [_txn("Store A", 50, m, "Shopping") for m in months[:11]]
        txns.append(_txn("Store A", 5000, months[11], "Shopping"))
        result = analyze(txns)
        large = [a for a in result["anomalies"] if a["type"] == "large_transaction"]
        assert len(large) >= 1
        assert large[0]["severity"] == "alert"

        # Moderate outlier with 6 data points — z ≈ √5 ≈ 2.24 → warning
        months6 = _months(6)
        txns2 = [_txn("Store B", 50, m, "Shopping") for m in months6[:5]]
        txns2.append(_txn("Store B", 5000, months6[5], "Shopping"))
        result2 = analyze(txns2)
        large2 = [a for a in result2["anomalies"] if a["type"] == "large_transaction"]
        assert len(large2) >= 1
        assert large2[0]["severity"] == "warning"


# ── AI recommendations (mocked API) ──────────────────────────────────────


def _analysis_data():
    """Build a minimal analyze() result for testing get_ai_recommendations."""
    return {
        "total": 1000.0,
        "months": ["2025-09", "2025-10", "2025-11"],
        "monthly_avg": 333.33,
        "mom_change": 5.0,
        "monthly_totals": {"2025-09": 300, "2025-10": 350, "2025-11": 350},
        "categories": [("Food & Dining", 700, 233.33, 10),
                        ("Shopping", 300, 100, 5)],
        "category_monthly": {
            "Food & Dining": {"2025-09": 250, "2025-10": 250, "2025-11": 200},
            "Shopping": {"2025-09": 50, "2025-10": 100, "2025-11": 150},
        },
        "subscriptions": [
            {"merchant": "Netflix", "avg": 16.99, "status": "stable",
             "alerts": [], "history": {"2025-10": 16.99, "2025-11": 16.99}},
        ],
        "fixed_cost_detail": [("BC Hydro", 180, {"2025-10": 90, "2025-11": 90})],
        "fixed_total": 180,
        "discretionary_total": 820,
        "monthly_txns": {
            "2025-09": [_txn("Store", 300, "2025-09", "Shopping")],
            "2025-10": [_txn("Store", 350, "2025-10", "Shopping")],
            "2025-11": [_txn("Store", 350, "2025-11", "Shopping")],
        },
        "debt_payoffs": [],
    }


def _mock_api_response(text="<ol><li>Test recommendation</li></ol>"):
    """Create a mock urlopen response."""
    body = json.dumps({"content": [{"text": text}]}).encode()
    resp = MagicMock()
    resp.read.return_value = body
    resp.__enter__ = lambda s: s
    resp.__exit__ = MagicMock(return_value=False)
    return resp


class TestAIRecommendations:
    """Verifies the AI recommendations pipeline builds correct summaries
    and handles API success/failure gracefully."""

    @patch.dict(os.environ, {}, clear=True)
    def test_missing_api_key_exits(self):
        with pytest.raises(SystemExit):
            get_ai_recommendations(_analysis_data())

    @patch("analysis.urlopen", return_value=_mock_api_response())
    @patch.dict(os.environ, {"ANTHROPIC_API_KEY": "test-key"})
    def test_successful_api_call_returns_html(self, mock_urlopen):
        result = get_ai_recommendations(_analysis_data())
        assert "Test recommendation" in result
        mock_urlopen.assert_called_once()

    @patch("analysis.urlopen")
    @patch.dict(os.environ, {"ANTHROPIC_API_KEY": "test-key"})
    def test_http_error_exits(self, mock_urlopen):
        mock_urlopen.side_effect = HTTPError(
            "https://api.anthropic.com", 400, "Bad Request", {},
            BytesIO(b"error details"))
        with pytest.raises(SystemExit):
            get_ai_recommendations(_analysis_data())

    @patch("analysis.urlopen")
    @patch.dict(os.environ, {"ANTHROPIC_API_KEY": "test-key"})
    def test_network_error_exits(self, mock_urlopen):
        mock_urlopen.side_effect = URLError("Connection refused")
        with pytest.raises(SystemExit):
            get_ai_recommendations(_analysis_data())

    @patch("analysis.urlopen", return_value=_mock_api_response())
    @patch.dict(os.environ, {"ANTHROPIC_API_KEY": "test-key"})
    def test_with_notes(self, mock_urlopen):
        """User notes are included in the API summary."""
        result = get_ai_recommendations(
            _analysis_data(), notes={"costco": "Holiday party"})
        assert result is not None

    @patch("analysis.urlopen", return_value=_mock_api_response())
    @patch.dict(os.environ, {"ANTHROPIC_API_KEY": "test-key"})
    def test_with_passive_income(self, mock_urlopen):
        """Passive income data is included in the summary."""
        passive = {
            "annual_income": 12000, "monthly_income": 1000,
            "accessible_balance": 50000, "annual_growth": 5000,
            "accounts": [
                {"account": "TFSA", "type": "TFSA", "value": 50000,
                 "income_annual": 2000, "growth_annual": 3000,
                 "return_pct": 10.0, "strategy": "growth",
                 "brokerage": "Questrade", "start_date": date(2020, 1, 1)},
            ],
            "registered_annual": 5000, "registered_growth": 2000,
            "registered_monthly": 416, "registered_balance": 100000,
            "registered_accounts": [
                {"account": "RRSP", "type": "RRSP", "value": 100000,
                 "income_annual": 5000, "growth_annual": 2000,
                 "return_pct": 7.0},
            ],
            "corporate_balance": 20000, "property_balance": 300000,
        }
        result = get_ai_recommendations(_analysis_data(), passive_income=passive)
        assert result is not None

    @patch("analysis.urlopen", return_value=_mock_api_response())
    @patch.dict(os.environ, {"ANTHROPIC_API_KEY": "test-key"})
    def test_with_corporate_income(self, mock_urlopen):
        """Corporate income + milestones included in summary."""
        corporate = {
            "revenue_monthly": {"2025-09": 9000, "2025-10": 8000, "2025-11": 7000},
            "dividends_monthly": {"2025-09": 2000, "2025-10": 2000, "2025-11": 2000},
            "revenue_total": 24000, "dividends_total": 6000,
            "earliest_txn_date": date(2024, 6, 1),
            "first_revenue": {"date": date(2024, 7, 1), "amount": 5000},
            "first_dividend": {"date": date(2024, 12, 1), "amount": 1000},
        }
        result = get_ai_recommendations(_analysis_data(), corporate_income=corporate)
        assert result is not None

    @patch("analysis.urlopen", return_value=_mock_api_response())
    @patch.dict(os.environ, {"ANTHROPIC_API_KEY": "test-key"})
    def test_with_etransfers_and_bank_interest(self, mock_urlopen):
        """Incoming e-transfers and bank interest are included."""
        etransfers = [
            {"date": date(2025, 10, 5), "amount": 500},
            {"date": date(2025, 11, 3), "amount": 300},
        ]
        bank_interest = [
            {"date": date(2025, 10, 31), "amount": 50},
            {"date": date(2025, 11, 30), "amount": 45},
        ]
        result = get_ai_recommendations(
            _analysis_data(),
            incoming_etransfers=etransfers, bank_interest=bank_interest)
        assert result is not None

    @patch("analysis.urlopen", return_value=_mock_api_response())
    @patch.dict(os.environ, {"ANTHROPIC_API_KEY": "test-key"})
    def test_with_debt_payoffs(self, mock_urlopen):
        """Debt payoffs are summarised in the API payload."""
        data = _analysis_data()
        data["debt_payoffs"] = [
            {"merchant": "Car Loan", "amount": 5000, "date": date(2025, 10, 15)},
            {"merchant": "Car Loan", "amount": 3000, "date": date(2025, 11, 1)},
        ]
        result = get_ai_recommendations(data)
        assert result is not None

    @patch("analysis.urlopen", return_value=_mock_api_response())
    @patch.dict(os.environ, {"ANTHROPIC_API_KEY": "test-key"})
    def test_full_summary_all_income_sources(self, mock_urlopen):
        """All optional parameters together exercise every summary branch."""
        data = _analysis_data()
        data["debt_payoffs"] = [
            {"merchant": "Mortgage (First National)", "amount": 10000,
             "date": date(2025, 10, 1)},
        ]
        # Add a mortgage transaction to monthly_txns so debt adjustment works
        data["monthly_txns"]["2025-10"].append(
            _txn("Mortgage (First National)", 2000, "2025-10", "Housing & Utilities"))

        passive = {
            "annual_income": 12000, "monthly_income": 1000,
            "accessible_balance": 50000, "annual_growth": 5000,
            "accounts": [
                {"account": "TFSA", "type": "TFSA", "value": 50000,
                 "income_annual": 2000, "growth_annual": 3000,
                 "return_pct": 10.0},
            ],
            "registered_annual": 5000, "registered_growth": 2000,
            "registered_monthly": 416, "registered_balance": 100000,
            "registered_accounts": [],
            "corporate_balance": 20000, "property_balance": 300000,
        }
        corporate = {
            "revenue_monthly": {"2025-09": 9000, "2025-10": 8000, "2025-11": 7000},
            "dividends_monthly": {"2025-09": 2000, "2025-10": 2000, "2025-11": 2000},
            "revenue_total": 24000, "dividends_total": 6000,
            "earliest_txn_date": date(2024, 6, 1),
            "first_revenue": {"date": date(2024, 7, 1), "amount": 5000},
            "first_dividend": {"date": date(2024, 12, 1), "amount": 1000},
        }
        etransfers = [{"date": date(2025, 10, 5), "amount": 500}]
        bank_interest = [{"date": date(2025, 10, 31), "amount": 50}]
        notes = {"costco": "Holiday party"}

        result = get_ai_recommendations(
            data, passive_income=passive, corporate_income=corporate,
            incoming_etransfers=etransfers, bank_interest=bank_interest,
            notes=notes)
        assert result is not None


# ── Net worth history computation ─────────────────────────────────────────


def _make_passive(accounts=None, registered=None, corporate=None, property_=None):
    """Build a minimal passive_income dict for net worth history tests."""
    return {
        "accounts": accounts or [],
        "registered_accounts": registered or [],
        "corporate_accounts": corporate or [],
        "property_accounts": property_ or [],
        "accessible_balance": sum(a.get("value", 0) for a in (accounts or [])),
        "registered_balance": sum(a.get("value", 0) for a in (registered or [])),
        "corporate_balance": sum(a.get("value", 0) for a in (corporate or [])),
        "property_balance": sum(a.get("value", 0) for a in (property_ or [])),
    }


def _acct(name, value, history=None):
    """Build a minimal account dict with optional balance_history."""
    return {"account": name, "value": value, "balance_history": history or []}


def _hist(date_str, balance):
    """Build a balance_history entry (deposits/withdrawals not needed here)."""
    return {"date": date_str, "balance": balance, "deposits": 0, "withdrawals": 0}


class TestNetWorthHistory:
    """Verifies month-by-month net worth time series computation."""

    def test_returns_none_when_no_balance_history(self):
        """Accounts with no balance_history produce None."""
        pi = _make_passive(accounts=[_acct("TFSA", 50000)])
        assert compute_net_worth_history(pi) is None

    def test_returns_none_with_only_one_month(self):
        """A single month of data is insufficient."""
        pi = _make_passive(accounts=[
            _acct("TFSA", 50000, [_hist("2025-01-31", 50000)])
        ])
        assert compute_net_worth_history(pi) is None

    def test_basic_two_month_history(self):
        """Two months of data should produce a 2-entry time series."""
        pi = _make_passive(accounts=[
            _acct("TFSA", 52000, [
                _hist("2025-01-31", 50000),
                _hist("2025-02-28", 52000),
            ])
        ])
        result = compute_net_worth_history(pi)
        assert result is not None
        assert len(result) == 2
        assert result[0]["month"] == "2025-01"
        assert result[0]["accessible"] == 50000.0
        assert result[1]["accessible"] == 52000.0
        assert result[1]["total"] == 52000.0

    def test_forward_fill_with_multiple_accounts(self):
        """When another account provides monthly data, gaps are forward-filled."""
        pi = _make_passive(accounts=[
            _acct("Monthly", 55000, [
                _hist("2025-01-31", 50000),
                _hist("2025-02-28", 52000),
                _hist("2025-03-31", 55000),
            ]),
            _acct("Quarterly", 110000, [
                _hist("2025-01-31", 100000),
                _hist("2025-03-31", 110000),
            ]),
        ])
        result = compute_net_worth_history(pi)
        feb = next(r for r in result if r["month"] == "2025-02")
        # Quarterly has no Feb data -> forward-filled from Jan (100000)
        assert feb["accessible"] == 52000 + 100000

    def test_property_constant_uses_current_value(self):
        """Accounts without balance_history use their current value for all months."""
        pi = _make_passive(
            accounts=[
                _acct("TFSA", 52000, [
                    _hist("2025-01-31", 50000),
                    _hist("2025-02-28", 52000),
                ])
            ],
            property_=[_acct("House", 1900000)],
        )
        result = compute_net_worth_history(pi)
        assert result[0]["property"] == 1900000.0
        assert result[1]["property"] == 1900000.0
        assert result[0]["total"] == 50000 + 1900000
        assert result[1]["total"] == 52000 + 1900000

    def test_multiple_categories_aggregate(self):
        """Each category aggregates independently per month."""
        pi = _make_passive(
            accounts=[_acct("TFSA", 50000, [
                _hist("2025-01-31", 48000),
                _hist("2025-02-28", 50000),
            ])],
            registered=[_acct("RRSP", 100000, [
                _hist("2025-01-31", 95000),
                _hist("2025-02-28", 100000),
            ])],
            corporate=[_acct("Corp", 30000, [
                _hist("2025-01-31", 28000),
                _hist("2025-02-28", 30000),
            ])],
        )
        result = compute_net_worth_history(pi)
        jan = result[0]
        assert jan["accessible"] == 48000
        assert jan["registered"] == 95000
        assert jan["corporate"] == 28000
        assert jan["total"] == 48000 + 95000 + 28000

    def test_multiple_accounts_same_category_sum(self):
        """Multiple accounts within one category sum their balances."""
        pi = _make_passive(accounts=[
            _acct("TFSA", 30000, [
                _hist("2025-01-31", 25000),
                _hist("2025-02-28", 30000),
            ]),
            _acct("Cash", 10000, [
                _hist("2025-01-31", 8000),
                _hist("2025-02-28", 10000),
            ]),
        ])
        result = compute_net_worth_history(pi)
        assert result[0]["accessible"] == 33000  # 25000 + 8000
        assert result[1]["accessible"] == 40000  # 30000 + 10000

    def test_staggered_start_dates_backfilled(self):
        """An account starting later gets backfilled with its first known balance."""
        pi = _make_passive(accounts=[
            _acct("Early", 60000, [
                _hist("2025-01-31", 50000),
                _hist("2025-02-28", 55000),
                _hist("2025-03-31", 60000),
            ]),
            _acct("Late", 20000, [
                _hist("2025-03-31", 20000),
            ]),
        ])
        result = compute_net_worth_history(pi)
        jan = next(r for r in result if r["month"] == "2025-01")
        # Late account backfilled with first known (20000)
        assert jan["accessible"] == 50000 + 20000

    def test_empty_passive_income_returns_none(self):
        """Empty dict returns None."""
        assert compute_net_worth_history({}) is None

    def test_months_sorted_chronologically(self):
        """Output months are in ascending order."""
        pi = _make_passive(accounts=[
            _acct("TFSA", 50000, [
                _hist("2025-03-31", 50000),
                _hist("2025-01-31", 45000),
                _hist("2025-02-28", 48000),
            ])
        ])
        result = compute_net_worth_history(pi)
        months = [r["month"] for r in result]
        assert months == sorted(months)

    def test_values_rounded_to_two_decimals(self):
        """All monetary values are rounded to 2 decimal places."""
        pi = _make_passive(accounts=[
            _acct("TFSA", 33333.336, [
                _hist("2025-01-31", 33333.333),
                _hist("2025-02-28", 33333.336),
            ])
        ])
        result = compute_net_worth_history(pi)
        for row in result:
            for key in ("accessible", "registered", "corporate", "property", "total"):
                assert row[key] == round(row[key], 2)


# ── Passthrough Tests ────────────────────────────────────────────────────────


class TestLoadPassthrough:
    """Verifies passthrough.csv loading."""

    def test_load_from_csv(self, tmp_path):
        """Reads passthrough.csv and returns structured records."""
        csv_file = tmp_path / "passthrough.csv"
        csv_file.write_text(
            "account_suffix,start_date,end_date,principal,description\n"
            "WK4ZQ2P35CAD,2025-07-10,2026-01-22,619500.00,Sarah inheritance\n"
        )
        result = load_passthrough(str(tmp_path))
        assert len(result) == 1
        assert result[0]["account_suffix"] == "WK4ZQ2P35CAD"
        assert result[0]["start_date"] == date(2025, 7, 10)
        assert result[0]["end_date"] == date(2026, 1, 22)
        assert result[0]["principal"] == 619500.00
        assert result[0]["description"] == "Sarah inheritance"

    def test_missing_file_returns_empty(self, tmp_path):
        """No passthrough.csv means no passthrough records."""
        assert load_passthrough(str(tmp_path)) == []

    def test_multiple_records(self, tmp_path):
        """Multiple rows produce multiple records."""
        csv_file = tmp_path / "passthrough.csv"
        csv_file.write_text(
            "account_suffix,start_date,end_date,principal,description\n"
            "ACCT1,2025-01-01,2025-06-30,10000.00,First\n"
            "ACCT2,2025-03-01,2025-09-30,20000.00,Second\n"
        )
        result = load_passthrough(str(tmp_path))
        assert len(result) == 2


class TestPassthroughBankInterest:
    """Verifies interest adjustment for passthrough accounts."""

    def _make_csvs(self, tmp_path, rows):
        """Create a minimal debit CSV with given rows in a Wealthsimple-style path."""
        txn_dir = tmp_path / "transactions" / "personal" / "Wealthsimple Chequing"
        txn_dir.mkdir(parents=True)
        for fname, csv_rows in rows.items():
            fpath = txn_dir / fname
            fpath.write_text(
                "date,transaction,description,amount,balance,currency\n"
                + "\n".join(csv_rows) + "\n"
            )

    def test_interest_reduced_during_passthrough(self, tmp_path):
        """INT during passthrough period is reduced by sarah_pct."""
        self._make_csvs(tmp_path, {
            "Chequing-WK4ZQ2P35CAD-2025-09-01.csv": [
                # INT on Sep 1 covers August — passthrough active (started Jul 10)
                # balance_before = 700000 - 1000 = 699000
                # sarah_pct = min(1.0, 619500 / 699000) = 0.8862
                # adjustment = 1000 * 0.8862 = 886.2 → adjusted = 113.8
                "2025-09-01,INT,Interest earned,1000.00,700000.00,CAD",
            ],
        })
        pt = [{
            "account_suffix": "WK4ZQ2P35CAD",
            "start_date": date(2025, 7, 10),
            "end_date": date(2026, 1, 22),
            "principal": 619500.00,
            "description": "Sarah inheritance",
        }]
        txns, adj = extract_bank_interest(str(tmp_path), passthrough=pt)
        assert len(txns) == 1
        assert txns[0]["amount"] < 1000.00  # reduced
        expected_adj = round(1000 * min(1.0, 619500 / 699000), 2)
        assert adj["Sarah inheritance"] == expected_adj
        assert txns[0]["amount"] == round(1000 - expected_adj, 2)

    def test_interest_before_passthrough_not_adjusted(self, tmp_path):
        """INT covering a month before passthrough start is untouched."""
        self._make_csvs(tmp_path, {
            "Chequing-WK4ZQ2P35CAD-2025-07-01.csv": [
                # INT on Jul 1 covers June — passthrough starts Jul 10
                "2025-07-01,INT,Interest earned,125.00,37500.00,CAD",
            ],
        })
        pt = [{
            "account_suffix": "WK4ZQ2P35CAD",
            "start_date": date(2025, 7, 10),
            "end_date": date(2026, 1, 22),
            "principal": 619500.00,
            "description": "Sarah inheritance",
        }]
        txns, adj = extract_bank_interest(str(tmp_path), passthrough=pt)
        assert len(txns) == 1
        assert txns[0]["amount"] == 125.00
        assert adj == {}

    def test_full_deduction_when_principal_exceeds_balance(self, tmp_path):
        """When principal >= balance_before, all interest is deducted."""
        self._make_csvs(tmp_path, {
            "Chequing-WK4ZQ2P35CAD-2025-11-01.csv": [
                # balance_before = 300000 - 500 = 299500
                # sarah_pct = min(1.0, 619500 / 299500) = 1.0
                "2025-11-01,INT,Interest earned,500.00,300000.00,CAD",
            ],
        })
        pt = [{
            "account_suffix": "WK4ZQ2P35CAD",
            "start_date": date(2025, 7, 10),
            "end_date": date(2026, 1, 22),
            "principal": 619500.00,
            "description": "Sarah inheritance",
        }]
        txns, adj = extract_bank_interest(str(tmp_path), passthrough=pt)
        # All interest deducted, so no transaction emitted
        assert len(txns) == 0
        assert adj["Sarah inheritance"] == 500.00

    def test_no_passthrough_returns_original(self, tmp_path):
        """Without passthrough, function returns all interest unchanged."""
        self._make_csvs(tmp_path, {
            "Chequing-WK4ZQ2P35CAD-2025-09-01.csv": [
                "2025-09-01,INT,Interest earned,1000.00,700000.00,CAD",
            ],
        })
        txns, adj = extract_bank_interest(str(tmp_path))
        assert len(txns) == 1
        assert txns[0]["amount"] == 1000.00
        assert adj == {}


class TestPassthroughTransfers:
    """Verifies EFTOUT exclusion for passthrough accounts."""

    def test_eftout_matching_passthrough_excluded(self, tmp_path):
        """EFTOUT with matching description is skipped."""
        txn_dir = tmp_path / "transactions" / "personal" / "Wealthsimple Chequing"
        txn_dir.mkdir(parents=True)
        (txn_dir / "chequing.csv").write_text(
            "date,transaction,description,amount,balance,currency\n"
            "2026-01-22,EFTOUT,Withdrawal: Sarah inheritance,-629025.64,97095.22,CAD\n"
            "2026-01-05,E_TRFOUT,Regular transfer,-500.00,200000.00,CAD\n"
        )
        pt = [{
            "account_suffix": "WK4ZQ2P35CAD",
            "start_date": date(2025, 7, 10),
            "end_date": date(2026, 1, 22),
            "principal": 619500.00,
            "description": "Sarah inheritance",
        }]
        agg, _ = extract_transfers(str(tmp_path), passthrough=pt)
        # The $629K EFTOUT should be excluded, only the $500 E_TRFOUT remains
        jan = agg.get("2026-01", {})
        assert jan.get("out", 0) == 500.00

    def test_eftout_without_passthrough_included(self, tmp_path):
        """Without passthrough, all EFTOUTs are counted."""
        txn_dir = tmp_path / "transactions" / "personal" / "Wealthsimple Chequing"
        txn_dir.mkdir(parents=True)
        (txn_dir / "chequing.csv").write_text(
            "date,transaction,description,amount,balance,currency\n"
            "2026-01-22,EFTOUT,Withdrawal: Sarah inheritance,-629025.64,97095.22,CAD\n"
        )
        agg, _ = extract_transfers(str(tmp_path))
        assert agg["2026-01"]["out"] == 629025.64


class TestPassthroughNetWorth:
    """Verifies net worth adjustment for passthrough accounts."""

    def test_subtraction_capped_at_excess_over_floor(self):
        """Subtraction is capped so accessible never drops below pre-passthrough floor."""
        pi = _make_passive(accounts=[
            _acct("Chequing", 720000, [
                _hist("2025-06-30", 100000),   # pre-passthrough floor
                _hist("2025-08-31", 720000),   # includes $619.5K deposit
                _hist("2025-09-30", 720000),
            ])
        ])
        pt = [{
            "account_suffix": "WK4ZQ2P35CAD",
            "start_date": date(2025, 7, 10),
            "end_date": date(2026, 1, 22),
            "principal": 619500.00,
            "description": "Sarah inheritance",
        }]
        result = compute_net_worth_history(pi, passthrough=pt)
        aug = next(r for r in result if r["month"] == "2025-08")
        sep = next(r for r in result if r["month"] == "2025-09")
        # Excess over floor: 720000 - 100000 = 620000 > principal 619500
        # So full principal subtracted
        assert aug["accessible"] == round(720000 - 619500, 2)
        assert sep["accessible"] == round(720000 - 619500, 2)

    def test_departure_month_not_subtracted(self):
        """Month where passthrough ends is not adjusted (balance already reflects departure)."""
        pi = _make_passive(accounts=[
            _acct("Chequing", 200000, [
                _hist("2025-06-30", 100000),
                _hist("2025-12-31", 720000),
                _hist("2026-01-31", 200000),  # after $629K EFTOUT
            ])
        ])
        pt = [{
            "account_suffix": "WK4ZQ2P35CAD",
            "start_date": date(2025, 7, 10),
            "end_date": date(2026, 1, 22),
            "principal": 619500.00,
            "description": "Sarah inheritance",
        }]
        result = compute_net_worth_history(pi, passthrough=pt)
        jan = next(r for r in result if r["month"] == "2026-01")
        # January: end_date (Jan 22) <= month_end (Jan 31), so no subtraction
        assert jan["accessible"] == 200000.0

    def test_forward_filled_month_not_over_subtracted(self):
        """When balance is forward-filled (doesn't include deposit), subtraction is minimal."""
        pi = _make_passive(accounts=[
            _acct("Chequing", 700000, [
                _hist("2025-06-30", 100000),   # pre-passthrough
                _hist("2025-07-31", 100000),   # forward-filled, same as June
                _hist("2025-08-31", 700000),   # actual balance with deposit
            ])
        ])
        pt = [{
            "account_suffix": "WK4ZQ2P35CAD",
            "start_date": date(2025, 7, 10),
            "end_date": date(2026, 1, 22),
            "principal": 619500.00,
            "description": "Sarah inheritance",
        }]
        result = compute_net_worth_history(pi, passthrough=pt)
        jul = next(r for r in result if r["month"] == "2025-07")
        # July balance == floor, so no subtraction (excess = 0)
        assert jul["accessible"] == 100000.0

    def test_no_passthrough_unchanged(self):
        """Without passthrough, net worth is unmodified."""
        pi = _make_passive(accounts=[
            _acct("Chequing", 700000, [
                _hist("2025-08-31", 700000),
                _hist("2025-09-30", 700000),
            ])
        ])
        result = compute_net_worth_history(pi)
        for row in result:
            assert row["accessible"] == 700000.0

    def test_month_outside_passthrough_unaffected(self):
        """Months outside the passthrough period are not adjusted."""
        pi = _make_passive(accounts=[
            _acct("Chequing", 200000, [
                _hist("2025-05-31", 200000),  # before passthrough
                _hist("2025-06-30", 200000),  # before passthrough
            ])
        ])
        pt = [{
            "account_suffix": "WK4ZQ2P35CAD",
            "start_date": date(2025, 7, 10),
            "end_date": date(2026, 1, 22),
            "principal": 619500.00,
            "description": "Sarah inheritance",
        }]
        result = compute_net_worth_history(pi, passthrough=pt)
        for row in result:
            assert row["accessible"] == 200000.0


# ── Liability Tests ─────────────────────────────────────────────────────────


class TestLoadLiabilities:
    """Verifies liabilities.csv loading."""

    def test_load_from_csv(self, tmp_path):
        """Reads liabilities.csv and returns structured records."""
        csv_file = tmp_path / "liabilities.csv"
        csv_file.write_text(
            "description,start_date,end_date,amount\n"
            "KIA auto loan,2023-01-01,2025-06-27,39993.32\n"
        )
        result = load_liabilities(str(tmp_path))
        assert len(result) == 1
        assert result[0]["description"] == "KIA auto loan"
        assert result[0]["start_date"] == date(2023, 1, 1)
        assert result[0]["end_date"] == date(2025, 6, 27)
        assert result[0]["amount"] == 39993.32

    def test_missing_file_returns_empty(self, tmp_path):
        """No liabilities.csv means no liabilities."""
        assert load_liabilities(str(tmp_path)) == []

    def test_multiple_records(self, tmp_path):
        """Multiple rows produce multiple records."""
        csv_file = tmp_path / "liabilities.csv"
        csv_file.write_text(
            "description,start_date,end_date,amount\n"
            "KIA auto loan,2023-01-01,2025-06-27,39993.32\n"
            "First National mortgage,2010-06-14,2025-07-29,296942.47\n"
        )
        result = load_liabilities(str(tmp_path))
        assert len(result) == 2
        assert result[0]["amount"] == 39993.32
        assert result[1]["amount"] == 296942.47


class TestLiabilitiesNetWorth:
    """Verifies net worth adjustment for liabilities."""

    def test_active_liability_subtracted_from_total(self):
        """Active liabilities reduce the total but not individual categories."""
        pi = _make_passive(accounts=[
            _acct("TFSA", 500000, [
                _hist("2025-01-31", 500000),
                _hist("2025-02-28", 500000),
            ])
        ])
        liabilities = [{
            "description": "Mortgage",
            "start_date": date(2020, 1, 1),
            "end_date": date(2025, 7, 29),
            "amount": 296942.47,
        }]
        result = compute_net_worth_history(pi, liabilities=liabilities)
        jan = result[0]
        # Category value unaffected
        assert jan["accessible"] == 500000.0
        # Liabilities shown as negative
        assert jan["liabilities"] == -296942.47
        # Total reflects subtraction
        assert jan["total"] == round(500000 - 296942.47, 2)

    def test_liability_disappears_after_end_date(self):
        """Once end_date passes, the liability no longer reduces the total."""
        pi = _make_passive(accounts=[
            _acct("TFSA", 500000, [
                _hist("2025-06-30", 500000),
                _hist("2025-07-31", 500000),
                _hist("2025-08-31", 500000),
            ])
        ])
        liabilities = [{
            "description": "KIA auto loan",
            "start_date": date(2023, 1, 1),
            "end_date": date(2025, 6, 27),
            "amount": 39993.32,
        }]
        result = compute_net_worth_history(pi, liabilities=liabilities)
        jun = next(r for r in result if r["month"] == "2025-06")
        jul = next(r for r in result if r["month"] == "2025-07")
        # June: end_date is Jun 27, month_end is Jun 30 → 30 >= 27 → NOT active
        assert jun["liabilities"] == 0.0
        assert jun["total"] == 500000.0
        # July: also not active
        assert jul["liabilities"] == 0.0
        assert jul["total"] == 500000.0

    def test_liability_active_before_end_date_month(self):
        """Liability is active for months ending before end_date."""
        pi = _make_passive(accounts=[
            _acct("TFSA", 500000, [
                _hist("2025-05-31", 500000),
                _hist("2025-06-30", 500000),
            ])
        ])
        liabilities = [{
            "description": "Mortgage",
            "start_date": date(2020, 1, 1),
            "end_date": date(2025, 7, 29),
            "amount": 296942.47,
        }]
        result = compute_net_worth_history(pi, liabilities=liabilities)
        may = next(r for r in result if r["month"] == "2025-05")
        jun = next(r for r in result if r["month"] == "2025-06")
        # May 31 < Jul 29 → active
        assert may["liabilities"] == -296942.47
        assert may["total"] == round(500000 - 296942.47, 2)
        # Jun 30 < Jul 29 → active
        assert jun["liabilities"] == -296942.47

    def test_multiple_liabilities_sum(self):
        """Multiple active liabilities are summed."""
        pi = _make_passive(accounts=[
            _acct("TFSA", 500000, [
                _hist("2025-05-31", 500000),
                _hist("2025-06-30", 500000),
            ])
        ])
        liabilities = [
            {
                "description": "KIA auto loan",
                "start_date": date(2023, 1, 1),
                "end_date": date(2025, 6, 27),
                "amount": 39993.32,
            },
            {
                "description": "Mortgage",
                "start_date": date(2020, 1, 1),
                "end_date": date(2025, 7, 29),
                "amount": 296942.47,
            },
        ]
        result = compute_net_worth_history(pi, liabilities=liabilities)
        may = next(r for r in result if r["month"] == "2025-05")
        jun = next(r for r in result if r["month"] == "2025-06")
        # May: both active (May 31 < Jun 27 and May 31 < Jul 29)
        assert may["liabilities"] == round(-(39993.32 + 296942.47), 2)
        assert may["total"] == round(500000 - 39993.32 - 296942.47, 2)
        # June: only mortgage active (Jun 30 >= Jun 27, but Jun 30 < Jul 29)
        assert jun["liabilities"] == -296942.47
        assert jun["total"] == round(500000 - 296942.47, 2)

    def test_no_liabilities_has_zero_field(self):
        """Without liabilities, the liabilities field is 0."""
        pi = _make_passive(accounts=[
            _acct("TFSA", 50000, [
                _hist("2025-01-31", 50000),
                _hist("2025-02-28", 50000),
            ])
        ])
        result = compute_net_worth_history(pi)
        for row in result:
            assert row["liabilities"] == 0.0
            assert row["total"] == 50000.0

    def test_liability_before_start_date_not_subtracted(self):
        """Months before the liability's start_date are unaffected."""
        pi = _make_passive(accounts=[
            _acct("TFSA", 500000, [
                _hist("2022-11-30", 500000),
                _hist("2022-12-31", 500000),
            ])
        ])
        liabilities = [{
            "description": "KIA auto loan",
            "start_date": date(2023, 1, 1),
            "end_date": date(2025, 6, 27),
            "amount": 39993.32,
        }]
        result = compute_net_worth_history(pi, liabilities=liabilities)
        for row in result:
            assert row["liabilities"] == 0.0
            assert row["total"] == 500000.0


# ── CSV Parsing (parsers.parse_csvs) ─────────────────────────────────────────


class TestParseCsvs:
    """Tests for parsers.parse_csvs — credit + debit CSV reading."""

    def _write_credit_csv(self, tmp_path, rows, subdir="Visa"):
        """Write a credit card CSV and return folder path."""
        d = tmp_path / "transactions" / "personal" / subdir
        d.mkdir(parents=True)
        csv_path = d / "credit.csv"
        lines = ["transaction_date,details,amount,type\n"]
        for r in rows:
            lines.append(f"{r[0]},{r[1]},{r[2]},{r[3]}\n")
        csv_path.write_text("".join(lines))
        return str(tmp_path)

    def _write_debit_csv(self, tmp_path, rows, subdir="Chequing"):
        """Write a debit card CSV and return folder path."""
        d = tmp_path / "transactions" / "personal" / subdir
        d.mkdir(parents=True)
        csv_path = d / "debit.csv"
        lines = ["date,transaction,description,amount,balance,currency\n"]
        for r in rows:
            lines.append(f"{r[0]},{r[1]},{r[2]},{r[3]},1000.00,CAD\n")
        csv_path.write_text("".join(lines))
        return str(tmp_path)

    def test_credit_card_purchase(self, tmp_path):
        """Credit card purchases are parsed with amount, merchant, and date."""
        folder = self._write_credit_csv(tmp_path, [
            ("2025-07-15", "GROCERY STORE", "42.50", "Purchase"),
        ])
        txns, payoffs = parse_csvs(folder)
        assert len(txns) == 1
        assert txns[0]["amount"] == 42.50
        assert txns[0]["source"] == "credit"
        assert txns[0]["month"] == "2025-07"

    def test_credit_card_payment_excluded(self, tmp_path):
        """Credit card payments (negative or Payment type) are excluded."""
        folder = self._write_credit_csv(tmp_path, [
            ("2025-07-15", "GROCERY STORE", "-100.00", "Purchase"),
            ("2025-07-20", "PAYMENT", "500.00", "Payment"),
            ("2025-07-25", "RESTAURANT", "30.00", "Purchase"),
        ])
        txns, _ = parse_csvs(folder)
        assert len(txns) == 1
        assert txns[0]["amount"] == 30.00

    def test_debit_spend(self, tmp_path):
        """Debit SPEND transactions are parsed."""
        folder = self._write_debit_csv(tmp_path, [
            ("2025-08-01", "SPEND", "Coffee Shop Purchase", "-5.50"),
        ])
        txns, _ = parse_csvs(folder)
        assert len(txns) == 1
        assert txns[0]["amount"] == 5.50
        assert txns[0]["source"] == "debit"

    def test_debit_aft_out(self, tmp_path):
        """Debit AFT_OUT (pre-authorized debits) are parsed as fixed costs."""
        folder = self._write_debit_csv(tmp_path, [
            ("2025-08-01", "AFT_OUT", "Pre-authorized Debit to INSURANCE CO", "-150.00"),
        ])
        txns, _ = parse_csvs(folder)
        assert len(txns) == 1
        assert txns[0]["amount"] == 150.00
        assert txns[0]["fixed_cost"] is True

    def test_debit_obp_out(self, tmp_path):
        """Debit OBP_OUT (online bill payments) are parsed as fixed costs."""
        folder = self._write_debit_csv(tmp_path, [
            ("2025-08-01", "OBP_OUT", "Online bill payment for HYDRO", "-200.00"),
        ])
        txns, _ = parse_csvs(folder)
        assert len(txns) == 1
        assert txns[0]["amount"] == 200.00
        assert txns[0]["fixed_cost"] is True

    def test_debit_etransfer(self, tmp_path):
        """Debit E_TRFOUT (e-Transfers) are parsed."""
        folder = self._write_debit_csv(tmp_path, [
            ("2025-08-01", "E_TRFOUT", "Sent to John", "-50.00"),
        ])
        txns, _ = parse_csvs(folder)
        assert len(txns) == 1
        assert txns[0]["merchant"] == "Interac e-Transfer"

    def test_debt_payoff_excluded(self, tmp_path):
        """AFT_OUT exceeding debt threshold is excluded and tracked as payoff."""
        from config import DEBT_PAYOFF_THRESHOLDS
        if not DEBT_PAYOFF_THRESHOLDS:
            pytest.skip("No DEBT_PAYOFF_THRESHOLDS configured")
        merchant = next(iter(DEBT_PAYOFF_THRESHOLDS))
        threshold = DEBT_PAYOFF_THRESHOLDS[merchant]
        folder = self._write_debit_csv(tmp_path, [
            ("2025-08-01", "AFT_OUT", f"Pre-authorized Debit to {merchant}", f"-{threshold + 1000}"),
        ])
        txns, payoffs = parse_csvs(folder)
        assert len(txns) == 0
        assert len(payoffs) == 1
        assert payoffs[0]["amount"] == threshold + 1000

    def test_business_merchants_excluded(self, tmp_path):
        """Transactions for business merchants are excluded from personal spending."""
        from config import BUSINESS_MERCHANTS
        if not BUSINESS_MERCHANTS:
            pytest.skip("No BUSINESS_MERCHANTS configured")
        biz = next(iter(BUSINESS_MERCHANTS))
        folder = self._write_credit_csv(tmp_path, [
            ("2025-07-15", biz, "100.00", "Purchase"),
        ])
        txns, _ = parse_csvs(folder)
        assert len(txns) == 0


# ── Statement Balance Parsing (parsers.parse_statement_balances) ─────────────


class TestParseStatementBalances:
    """Tests for parsers.parse_statement_balances — PDF statement extraction."""

    def test_no_statements_dir(self, tmp_path):
        """Returns empty dict when statements/ directory doesn't exist."""
        result = parse_statement_balances(str(tmp_path))
        assert result == {}

    @patch("parsers.subprocess.run")
    def test_pdftotext_not_available(self, mock_run, tmp_path):
        """Returns empty dict when pdftotext is not installed."""
        (tmp_path / "statements").mkdir()
        mock_run.side_effect = FileNotFoundError
        result = parse_statement_balances(str(tmp_path))
        assert result == {}

    @patch("parsers.subprocess.run")
    def test_wealthsimple_statement(self, mock_run, tmp_path):
        """Parses Wealthsimple monthly statement for balance and book cost."""
        ws_dir = tmp_path / "statements" / "personal" / "Wealthsimple"
        ws_dir.mkdir(parents=True)
        (ws_dir / "HQ1234CAD_person-abc_2025-07_v_0.pdf").touch()

        pdf_text = (
            "2025-07-01 - 2025-07-31\n"
            "Total Portfolio  $10,500.00  100.00  $10,000.00  100.00\n"
        )

        def fake_run(cmd, **kwargs):
            m = MagicMock()
            if cmd[0] == "pdftotext" and cmd[1] == "-layout":
                m.stdout = pdf_text
                m.returncode = 0
            elif cmd[0] == "pdftotext" and cmd[1] == "-v":
                m.returncode = 0
            return m

        mock_run.side_effect = fake_run
        result = parse_statement_balances(str(tmp_path))
        assert "HQ1234CAD" in result
        assert result["HQ1234CAD"]["balance"] == 10500.00
        assert result["HQ1234CAD"]["source"] == "Wealthsimple statement"
        assert result["HQ1234CAD"]["return_pct"] == pytest.approx(5.0, abs=0.01)
        assert result["HQ1234CAD"]["return_source"] == "estimated"

    @patch("parsers.subprocess.run")
    def test_wealthsimple_performance_pdf(self, mock_run, tmp_path):
        """Performance PDF overrides return rate with 1-year money-weighted return."""
        ws_dir = tmp_path / "statements" / "personal" / "Wealthsimple"
        ws_dir.mkdir(parents=True)
        (ws_dir / "WK9999CAD_person-abc_2025-07_v_0.pdf").touch()
        (ws_dir / "Performance_WK9999CAD_person-abc_2025-07_v_0.pdf").touch()

        stmt_text = (
            "2025-07-01 - 2025-07-31\n"
            "Total Portfolio  $10,500.00  100.00  $10,000.00  100.00\n"
        )
        perf_text = (
            "Money-weighted Return Rates\n"
            "    Current period    1 year    3 years    5 years    10 years    Since inception\n"
            "        3.90%        12.78%     14.39%      0.00%       0.00%         8.48%\n"
        )

        def fake_run(cmd, **kwargs):
            m = MagicMock()
            if cmd[0] == "pdftotext" and cmd[1] == "-v":
                m.returncode = 0
                return m
            m.returncode = 0
            path = cmd[2] if len(cmd) > 2 else ""
            m.stdout = perf_text if "Performance_" in str(path) else stmt_text
            return m

        mock_run.side_effect = fake_run
        result = parse_statement_balances(str(tmp_path))
        assert result["WK9999CAD"]["return_pct"] == 12.78
        assert result["WK9999CAD"]["return_source"] == "performance report"

    @patch("parsers.subprocess.run")
    def test_wealthsimple_performance_fallback_since_inception(self, mock_run, tmp_path):
        """Falls back to since-inception when 1-year is 0.00%."""
        ws_dir = tmp_path / "statements" / "personal" / "Wealthsimple"
        ws_dir.mkdir(parents=True)
        (ws_dir / "WK8888CAD_person-abc_2025-07_v_0.pdf").touch()
        (ws_dir / "Performance_WK8888CAD_person-abc_2025-07_v_0.pdf").touch()

        stmt_text = (
            "2025-07-01 - 2025-07-31\n"
            "Total Portfolio  $10,500.00  100.00  $10,000.00  100.00\n"
        )
        perf_text = (
            "Money-weighted Return Rates\n"
            "    Current period    1 year    3 years    5 years    10 years    Since inception\n"
            "        3.90%        0.00%     0.00%      0.00%       0.00%         8.48%\n"
        )

        def fake_run(cmd, **kwargs):
            m = MagicMock()
            if cmd[0] == "pdftotext" and cmd[1] == "-v":
                m.returncode = 0
                return m
            m.returncode = 0
            path = cmd[2] if len(cmd) > 2 else ""
            m.stdout = perf_text if "Performance_" in str(path) else stmt_text
            return m

        mock_run.side_effect = fake_run
        result = parse_statement_balances(str(tmp_path))
        assert result["WK8888CAD"]["return_pct"] == 8.48
        assert result["WK8888CAD"]["return_source"] == "performance report"

    @patch("parsers.subprocess.run")
    def test_wealthsimple_dividends(self, mock_run, tmp_path):
        """Parses dividends and interest from monthly statements."""
        ws_dir = tmp_path / "statements" / "personal" / "Wealthsimple"
        ws_dir.mkdir(parents=True)
        # Two months of statements
        for mo in ["2025-06", "2025-07"]:
            (ws_dir / f"HQ5678CAD_person-abc_{mo}_v_0.pdf").touch()

        pdf_text = (
            "2025-07-01 - 2025-07-31\n"
            "Total Portfolio  $50,000.00  100.00  $48,000.00  100.00\n"
            "Dividends  $150.00\n"
            "Interest Earned  $25.00\n"
        )

        def fake_run(cmd, **kwargs):
            m = MagicMock()
            if cmd[0] == "pdftotext" and cmd[1] == "-v":
                m.returncode = 0
                return m
            m.returncode = 0
            m.stdout = pdf_text
            return m

        mock_run.side_effect = fake_run
        result = parse_statement_balances(str(tmp_path))
        assert "HQ5678CAD" in result
        # 2 months of $175/mo → annualized = $350/2*12 = $2100
        assert result["HQ5678CAD"]["dividends_annual"] == pytest.approx(2100.0)

    @patch("parsers.subprocess.run")
    def test_wealthsimple_usd_conversion(self, mock_run, tmp_path):
        """USD accounts are converted to CAD using statement exchange rate."""
        ws_dir = tmp_path / "statements" / "personal" / "Wealthsimple"
        ws_dir.mkdir(parents=True)
        (ws_dir / "WK1234USD_person-abc_2025-07_v_0.pdf").touch()

        pdf_text = (
            "2025-07-01 - 2025-07-31\n"
            "Total Portfolio  $1,000.00  100.00  $900.00  100.00\n"
            "$1 USD = $1.35 CAD\n"
        )

        def fake_run(cmd, **kwargs):
            m = MagicMock()
            if cmd[0] == "pdftotext" and cmd[1] == "-v":
                m.returncode = 0
                return m
            m.returncode = 0
            m.stdout = pdf_text
            return m

        mock_run.side_effect = fake_run
        result = parse_statement_balances(str(tmp_path))
        assert "WK1234USD" in result
        assert result["WK1234USD"]["balance"] == pytest.approx(1350.0)

    @patch("parsers.subprocess.run")
    def test_wealthsimple_crm2_skipped(self, mock_run, tmp_path):
        """CRM2 annual reports are skipped."""
        ws_dir = tmp_path / "statements" / "personal" / "Wealthsimple"
        ws_dir.mkdir(parents=True)
        (ws_dir / "HQ1234CAD_CRM2_2025_report.pdf").touch()

        def fake_run(cmd, **kwargs):
            m = MagicMock()
            m.returncode = 0
            m.stdout = ""
            return m

        mock_run.side_effect = fake_run
        result = parse_statement_balances(str(tmp_path))
        assert "HQ1234CAD" not in result

    @patch("parsers.subprocess.run")
    def test_scotiabank_personal(self, mock_run, tmp_path):
        """Parses Scotiabank personal chequing statement."""
        sc_dir = tmp_path / "statements" / "personal" / "Scotiabank Chequing"
        sc_dir.mkdir(parents=True)
        (sc_dir / "February 2026 e-statement.pdf").touch()

        pdf_text = (
            "Your account number:\n"
            "11080 00070 21\n"
            "Closing Balance on February 17, 2026  $2,382.71\n"
        )

        def fake_run(cmd, **kwargs):
            m = MagicMock()
            if cmd[0] == "pdftotext" and cmd[1] == "-v":
                m.returncode = 0
                return m
            m.returncode = 0
            m.stdout = pdf_text
            return m

        mock_run.side_effect = fake_run
        result = parse_statement_balances(str(tmp_path))
        assert "110800007021" in result
        assert result["110800007021"]["balance"] == 2382.71
        assert result["110800007021"]["source"] == "Scotiabank statement"

    @patch("parsers.subprocess.run")
    def test_scotiabank_ken_personal(self, mock_run, tmp_path):
        """Parses Scotiabank Ken Personal statements with abbreviated month names."""
        sc_dir = tmp_path / "statements" / "personal" / "Scotiabank Ken Personal"
        sc_dir.mkdir(parents=True)
        (sc_dir / "Scotiabank - Ken Personal - Jul 2025.pdf").touch()

        pdf_text = (
            "Your account number:\n"
            "76018 10653 27\n"
            "Closing Balance on July 31, 2025  $0.00\n"
        )

        def fake_run(cmd, **kwargs):
            m = MagicMock()
            if cmd[0] == "pdftotext" and cmd[1] == "-v":
                m.returncode = 0
                return m
            m.returncode = 0
            m.stdout = pdf_text
            return m

        mock_run.side_effect = fake_run
        result = parse_statement_balances(str(tmp_path))
        assert "760181065327" in result
        assert result["760181065327"]["balance"] == 0.0

    @patch("parsers.subprocess.run")
    def test_scotiabank_corporate(self, mock_run, tmp_path):
        """Parses Scotiabank corporate statement from transaction lines."""
        sc_dir = tmp_path / "statements" / "corporate" / "Scotiabank Chequing"
        sc_dir.mkdir(parents=True)
        (sc_dir / "Tall Tree Technology - DebitCard - January 2026 e-statement.pdf").touch()

        pdf_text = (
            "Business Account  40360 01202 19  Dec 31 2025  Jan 30 2026\n"
            "01/15/2026  DEPOSIT  1,000.00  0.00  8,274.25\n"
        )

        def fake_run(cmd, **kwargs):
            m = MagicMock()
            if cmd[0] == "pdftotext" and cmd[1] == "-v":
                m.returncode = 0
                return m
            m.returncode = 0
            m.stdout = pdf_text
            return m

        mock_run.side_effect = fake_run
        result = parse_statement_balances(str(tmp_path))
        assert "403600120219" in result
        assert result["403600120219"]["balance"] == 8274.25
        assert result["403600120219"]["date"] == "Jan 30 2026"

    @patch("parsers.subprocess.run")
    def test_bc_property_assessment(self, mock_run, tmp_path):
        """Parses BC property assessments from sidecar CSV."""
        bc_dir = tmp_path / "statements" / "personal" / "British Columbia"
        bc_dir.mkdir(parents=True)
        csv_path = bc_dir / "property_assessments.csv"
        csv_path.write_text(
            "Address,Suffix,Assessed Value,Change,Year\n"
            '1829 East 2nd,6113,"$1,904,900.00",-5.00%,2026\n'
            '1829 East 2nd,6113,"$2,005,158.00",-4.00%,2025\n'
        )

        def fake_run(cmd, **kwargs):
            m = MagicMock()
            m.returncode = 0
            m.stdout = ""
            return m

        mock_run.side_effect = fake_run
        result = parse_statement_balances(str(tmp_path))
        assert "6113" in result
        assert result["6113"]["balance"] == 1904900.0
        assert result["6113"]["return_pct"] == -5.0
        assert result["6113"]["return_source"] == "BC Assessment"

    @patch("parsers.subprocess.run")
    def test_steadyhand_quarterly(self, mock_run, tmp_path):
        """Parses Steadyhand quarterly PDF for balance and returns."""
        sh_dir = tmp_path / "statements" / "personal" / "Steadyhand"
        sh_dir.mkdir(parents=True)
        (sh_dir / "December 2025.pdf").touch()

        pdf_text = (
            "As of December 31, 2025\n"
            "1055896  TFSA  Ken Britton  70,647.20\n\n"
            "Account 1055896  TFSA\n"
            "KEN BRITTON\n"
            "Beginning Value                 69,197.82\n"
            "Contributions                        0.00\n"
            "Redemptions                          0.00\n"
            "Gain/Loss                        1,449.38\n"
            "Ending Value                  $ 70,647.20\n\n"
            "Account Performance\n"
            "Performance Period                Rate of return (%)\n"
            "3 Months                                              2.1\n"
            "1 Year                                                7.4\n\n"
            "Transactions Throughout the Period\n"
            "Distribution - Reinvested  12/15  250.00  15.00  16.67  1234.56\n"
        )

        def fake_run(cmd, **kwargs):
            m = MagicMock()
            if cmd[0] == "pdftotext" and cmd[1] == "-v":
                m.returncode = 0
                return m
            m.returncode = 0
            m.stdout = pdf_text
            return m

        mock_run.side_effect = fake_run
        result = parse_statement_balances(str(tmp_path))
        assert "1055896" in result
        assert result["1055896"]["balance"] == 70647.20
        assert result["1055896"]["return_pct"] == 7.4
        assert result["1055896"]["dividends_annual"] == pytest.approx(1000.0)  # 250 * 4
        assert result["1055896"]["source"] == "Steadyhand statement"

    @patch("parsers.subprocess.run")
    def test_steadyhand_beginning_value(self, mock_run, tmp_path):
        """Beginning value from oldest PDF creates an extra balance history point."""
        sh_dir = tmp_path / "statements" / "personal" / "Steadyhand"
        sh_dir.mkdir(parents=True)
        (sh_dir / "March 2025.pdf").touch()
        (sh_dir / "June 2025.pdf").touch()

        q1_text = (
            "As of March 31, 2025\n"
            "1055896  TFSA  Ken Britton  64,764.64\n\n"
            "Account 1055896  TFSA\n"
            "Beginning Value                 62,049.94\n"
            "Contributions                    2,249.99\n"
            "Redemptions                          0.00\n"
        )
        q2_text = (
            "As of June 30, 2025\n"
            "1055896  TFSA  Ken Britton  67,102.08\n\n"
            "Account 1055896  TFSA\n"
            "Beginning Value                 64,764.64\n"
            "Contributions                    1,500.00\n"
            "Redemptions                          0.00\n"
        )

        def fake_run(cmd, **kwargs):
            m = MagicMock()
            if cmd[0] == "pdftotext" and cmd[1] == "-v":
                m.returncode = 0
                return m
            m.returncode = 0
            path = str(cmd[2]) if len(cmd) > 2 else ""
            m.stdout = q1_text if "March" in path else q2_text
            return m

        mock_run.side_effect = fake_run
        result = parse_statement_balances(str(tmp_path))
        hist = result["1055896"]["balance_history"]
        # 3 points: beginning value (Dec 2024) + Q1 + Q2
        assert len(hist) == 3
        assert hist[0]["balance"] == 62049.94
        assert hist[0]["deposits"] == 0.0  # beginning value has no flows
        assert hist[1]["deposits"] == 2249.99
        assert hist[2]["deposits"] == 1500.00


# ── Modified Dietz Return Calculation ────────────────────────────────────────


class TestModifiedDietz:
    """Tests for income.compute_modified_dietz."""

    def _make_passive(self, accounts):
        return {"accounts": accounts, "registered_accounts": []}

    def _acct(self, name, history):
        return {"account": name, "balance_history": history}

    def _hist(self, date, balance, deposits=0.0, withdrawals=0.0):
        return {"date": date, "balance": balance,
                "deposits": deposits, "withdrawals": withdrawals}

    def test_simple_growth_no_flows(self):
        """Pure growth with no deposits/withdrawals."""
        acct = self._acct("Test", [
            self._hist("2025-01-31", 10000),
            self._hist("2025-02-28", 10100),
            self._hist("2025-03-31", 10201),
            self._hist("2025-04-30", 10303),
        ])
        result = compute_modified_dietz(self._make_passive([acct]))
        assert result is not None
        pa = result["per_account"][0]
        ann = (1 + pa["monthly_return"]) ** 12 - 1
        assert ann == pytest.approx(0.1268, abs=0.01)  # ~1%/mo compounded

    def test_large_inflow_handled(self):
        """Large deposit relative to starting balance doesn't distort return."""
        acct = self._acct("Funded", [
            self._hist("2025-01-31", 5000),
            self._hist("2025-04-30", 255000, deposits=250000),
            self._hist("2025-07-31", 257000),
            self._hist("2025-10-31", 259000),
        ])
        result = compute_modified_dietz(self._make_passive([acct]))
        pa = result["per_account"][0]
        ann = (1 + pa["monthly_return"]) ** 12 - 1
        # Should be a modest return, not inflated by the large deposit
        assert ann < 0.10  # less than 10%

    def test_withdrawal_handled(self):
        """Withdrawals don't distort return calculation."""
        acct = self._acct("Withdrawing", [
            self._hist("2025-01-31", 100000),
            self._hist("2025-04-30", 52000, withdrawals=50000),
            self._hist("2025-07-31", 53000),
            self._hist("2025-10-31", 54000),
        ])
        result = compute_modified_dietz(self._make_passive([acct]))
        pa = result["per_account"][0]
        ann = (1 + pa["monthly_return"]) ** 12 - 1
        assert ann > 0  # positive return despite balance declining

    def test_insufficient_data_returns_none(self):
        """Returns None with fewer than 3 data points across all accounts."""
        acct = self._acct("Short", [
            self._hist("2025-01-31", 10000),
            self._hist("2025-02-28", 10100),
        ])
        result = compute_modified_dietz(self._make_passive([acct]))
        assert result is None

    def test_weighted_average_across_accounts(self):
        """Portfolio return is balance-weighted across accounts."""
        big = self._acct("Big", [
            self._hist("2025-01-31", 100000),
            self._hist("2025-02-28", 101000),
            self._hist("2025-03-31", 102000),
            self._hist("2025-04-30", 103000),
        ])
        small = self._acct("Small", [
            self._hist("2025-01-31", 1000),
            self._hist("2025-02-28", 990),
            self._hist("2025-03-31", 980),
            self._hist("2025-04-30", 970),
        ])
        result = compute_modified_dietz(self._make_passive([big, small]))
        # Portfolio should be positive (dominated by Big account)
        assert result["annualized_rate"] > 0

    def test_zero_balance_period_skipped(self):
        """Periods where starting balance is zero are skipped."""
        acct = self._acct("ZeroStart", [
            self._hist("2025-01-31", 0),
            self._hist("2025-02-28", 10000, deposits=10000),
            self._hist("2025-03-31", 10100),
            self._hist("2025-04-30", 10200),
            self._hist("2025-05-31", 10300),
        ])
        result = compute_modified_dietz(self._make_passive([acct]))
        assert result is not None
        assert result["data_points"] == 3  # first period skipped

    def test_date_range_reported(self):
        """Result includes correct date range."""
        acct = self._acct("Dated", [
            self._hist("2025-01-31", 10000),
            self._hist("2025-02-28", 10100),
            self._hist("2025-03-31", 10200),
            self._hist("2025-04-30", 10300),
        ])
        result = compute_modified_dietz(self._make_passive([acct]))
        assert result["date_range"][0] == "2025-01-31"
        assert result["date_range"][1] == "2025-04-30"


# ── Extract Passive Income ───────────────────────────────────────────────────


class TestExtractPassiveIncome:
    """Tests for income.extract_passive_income with mocked statements."""

    def _write_portfolio_csv(self, tmp_path, rows):
        import csv as csv_mod
        with open(tmp_path / "portfolio.csv", "w", newline="") as f:
            w = csv_mod.writer(f)
            w.writerow(["Account", "Brokerage", "Asset Type", "Acct Suffix", "Investment start date"])
            for r in rows:
                w.writerow(r)

    @patch("income.parse_statement_balances")
    def test_statement_mode_uses_stmt_balance(self, mock_stmts, tmp_path):
        """Statement mode uses balance from parsed statements, not CSV."""
        mock_stmts.return_value = {
            "WK1234CAD": {
                "balance": 55000.0,
                "date": "2025-07-31",
                "source": "Wealthsimple statement",
                "return_pct": 8.5,
                "return_source": "performance report",
                "dividends_annual": 1200.0,
                "balance_history": [],
            }
        }
        self._write_portfolio_csv(tmp_path, [
            ("Ken TFSA", "WS", "TFSA", "WK1234CAD", ""),
        ])
        result = extract_passive_income(str(tmp_path))
        assert result is not None
        acct = result["accounts"][0]
        assert acct["value"] == 55000.0  # from statement, not CSV
        assert acct["return_pct"] == 8.5
        assert acct["return_source"] == "performance report"
        assert acct["income_annual"] == 1200.0
        assert acct["income_source"] == "dividends"
        assert acct["brokerage"] == "WS"

    @patch("income.parse_statement_balances")
    def test_statement_mode_growth_suppressed_for_estimated(self, mock_stmts, tmp_path):
        """Growth is suppressed when return_source is 'estimated'."""
        mock_stmts.return_value = {
            "HQ1234CAD": {
                "balance": 100000.0,
                "date": "2025-07-31",
                "source": "Wealthsimple statement",
                "return_pct": 2.0,
                "return_source": "estimated",
                "dividends_annual": 5000.0,
                "balance_history": [],
            }
        }
        self._write_portfolio_csv(tmp_path, [
            ("ETF Income", "WS", "Non-reg", "HQ1234CAD", ""),
        ])
        result = extract_passive_income(str(tmp_path))
        acct = result["accounts"][0]
        assert acct["growth_annual"] == 0.0  # suppressed for estimated

    @patch("income.parse_statement_balances")
    def test_statement_mode_growth_computed_for_performance_report(self, mock_stmts, tmp_path):
        """Growth is computed when return_source is authoritative."""
        mock_stmts.return_value = {
            "WK5678CAD": {
                "balance": 100000.0,
                "date": "2025-07-31",
                "source": "Wealthsimple statement",
                "return_pct": 10.0,
                "return_source": "performance report",
                "dividends_annual": 3000.0,
                "balance_history": [],
            }
        }
        self._write_portfolio_csv(tmp_path, [
            ("Managed", "WS", "TFSA", "WK5678CAD", ""),
        ])
        result = extract_passive_income(str(tmp_path))
        acct = result["accounts"][0]
        # total_return = 100k * 10% = 10k; growth = 10k - 3k = 7k
        assert acct["growth_annual"] == pytest.approx(7000.0)

    @patch("income.parse_statement_balances")
    def test_no_portfolio_csv_returns_none(self, mock_stmts, tmp_path):
        """Returns None when portfolio.csv doesn't exist."""
        mock_stmts.return_value = {}
        result = extract_passive_income(str(tmp_path))
        assert result is None

    @patch("income.parse_statement_balances")
    def test_suffix_substring_matching(self, mock_stmts, tmp_path):
        """Statement keys ending with CSV suffix are matched."""
        mock_stmts.return_value = {
            "HQ8KF6905CAD": {
                "balance": 20000.0,
                "date": "2025-07-31",
                "source": "Wealthsimple statement",
                "return_pct": 3.0,
                "return_source": "estimated",
                "dividends_annual": 500.0,
                "balance_history": [],
            }
        }
        self._write_portfolio_csv(tmp_path, [
            ("Corp Savings", "WS", "Non-reg", "6905CAD", ""),
        ])
        result = extract_passive_income(str(tmp_path))
        assert result is not None
        assert result["accounts"][0]["value"] == 20000.0

    @patch("income.parse_statement_balances")
    def test_corporate_and_property_accounts(self, mock_stmts, tmp_path):
        """Corporate and Property accounts are routed correctly."""
        mock_stmts.return_value = {
            "WK61NR": {
                "balance": 30000.0,
                "date": "2025-07-31",
                "source": "Wealthsimple statement",
                "return_pct": 2.0,
                "return_source": "estimated",
                "dividends_annual": None,
                "balance_history": [],
            },
            "6113": {
                "balance": 1900000.0,
                "date": "2025-07-31",
                "source": "property assessment",
                "return_pct": -5.0,
                "return_source": "estimated",
                "dividends_annual": None,
                "balance_history": [],
            },
        }
        self._write_portfolio_csv(tmp_path, [
            ("Tall Tree", "WS", "Corporate", "WK61NR", ""),
            ("1829 East 2nd", "Vancouver", "Property", "6113", ""),
        ])
        result = extract_passive_income(str(tmp_path))
        assert result is not None
        assert result["corporate_balance"] == 30000.0
        assert result["property_balance"] == 1900000.0
