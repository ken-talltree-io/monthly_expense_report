"""Behavioural tests for the financial dashboard pipeline.

Tests what the system *does* — normalize merchants, categorize spending,
detect subscriptions, aggregate totals, and compute financial metrics —
rather than checking internal constants or data structures.
"""

import pytest
from datetime import date
from collections import defaultdict

import config
from config import (
    normalize_merchant,
    categorize,
    CATEGORY_CONSOLIDATION,
    CASHBACK_RATE,
    CORPORATE_TAKE_HOME_RATE,
)
from analysis import analyze


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
