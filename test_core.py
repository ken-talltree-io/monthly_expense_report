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
from income import compute_net_worth_history, load_passthrough, extract_bank_interest, extract_transfers


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

    def test_full_month_subtracted(self):
        """Passthrough principal fully subtracted for months entirely within period."""
        pi = _make_passive(accounts=[
            _acct("Chequing", 700000, [
                _hist("2025-08-31", 700000),
                _hist("2025-09-30", 700000),
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
        assert result is not None
        # Both months are fully within the passthrough period
        for row in result:
            assert row["accessible"] == round(700000 - 619500, 2)

    def test_partial_month_prorated(self):
        """Passthrough principal pro-rated for partial months."""
        pi = _make_passive(accounts=[
            _acct("Chequing", 700000, [
                _hist("2025-07-31", 700000),  # Jul: passthrough starts Jul 10 = 22/31
                _hist("2025-08-31", 700000),  # Aug: full month
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
        aug = next(r for r in result if r["month"] == "2025-08")
        # July: 22 days active out of 31
        expected_jul = round(700000 - 619500 * (22 / 31), 2)
        assert jul["accessible"] == expected_jul
        # August: full month
        assert aug["accessible"] == round(700000 - 619500, 2)

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
