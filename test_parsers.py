"""Behavioural tests for CSV transaction parsing.

Tests parse_csvs() — the data ingestion layer that reads credit card and
debit card CSVs, normalizes merchants, and returns a unified transaction list.
"""

import os
import pytest
from datetime import datetime

from parsers import parse_csvs


# ── Helpers ─────────────────────────────────────────────────────────────────


def _write_credit_csv(path, rows):
    """Write a credit card CSV with the standard header."""
    lines = ["transaction_date,details,amount,type"]
    for row in rows:
        lines.append(",".join(str(v) for v in row))
    path.write_text("\n".join(lines))


def _write_debit_csv(path, rows):
    """Write a debit card CSV with the standard header."""
    import csv as _csv
    import io
    buf = io.StringIO()
    writer = _csv.writer(buf)
    writer.writerow(["date", "transaction", "description", "amount"])
    for row in rows:
        writer.writerow(row)
    path.write_text(buf.getvalue())


def _setup_personal_dir(tmp_path):
    """Create the transactions/personal/ directory structure."""
    d = tmp_path / "transactions" / "personal"
    d.mkdir(parents=True)
    return d


# ── Credit card parsing ─────────────────────────────────────────────────────


class TestCreditCardParsing:
    """Credit card CSVs: transaction_date, details, amount, type columns."""

    def test_valid_purchase(self, tmp_path):
        d = _setup_personal_dir(tmp_path)
        _write_credit_csv(d / "cc.csv", [
            ("2025-10-15", "NETFLIX.COM", "16.99", "Purchase"),
        ])
        txns, _ = parse_csvs(str(tmp_path))
        assert len(txns) == 1
        t = txns[0]
        assert t.merchant == "Netflix"
        assert t.category == "Subscriptions & Telecom"
        assert t.amount == 16.99
        assert t.date == datetime(2025, 10, 15)
        assert t.month == "2025-10"
        assert t.source == "credit"

    def test_negative_amount_refund_skipped(self, tmp_path):
        d = _setup_personal_dir(tmp_path)
        _write_credit_csv(d / "cc.csv", [
            ("2025-10-15", "AMAZON.CA REFUND", "-25.00", "Purchase"),
        ])
        txns, _ = parse_csvs(str(tmp_path))
        assert len(txns) == 0

    def test_payment_row_skipped(self, tmp_path):
        d = _setup_personal_dir(tmp_path)
        _write_credit_csv(d / "cc.csv", [
            ("2025-10-01", "PAYMENT THANK YOU", "500.00", "Payment"),
        ])
        txns, _ = parse_csvs(str(tmp_path))
        assert len(txns) == 0

    def test_fixed_cost_merchant(self, tmp_path):
        d = _setup_personal_dir(tmp_path)
        _write_credit_csv(d / "cc.csv", [
            ("2025-10-15", "WAWANESA INSURANCE", "240.00", "Purchase"),
        ])
        txns, _ = parse_csvs(str(tmp_path))
        assert len(txns) == 1
        assert txns[0].fixed_cost is True

    def test_non_fixed_cost_has_no_flag(self, tmp_path):
        d = _setup_personal_dir(tmp_path)
        _write_credit_csv(d / "cc.csv", [
            ("2025-10-15", "COSTCO WHOLESALE #123", "200.00", "Purchase"),
        ])
        txns, _ = parse_csvs(str(tmp_path))
        assert txns[0].fixed_cost is False

    def test_business_merchant_excluded(self, tmp_path):
        d = _setup_personal_dir(tmp_path)
        _write_credit_csv(d / "cc.csv", [
            ("2025-10-15", "ZENSURANCE", "150.00", "Purchase"),
            ("2025-10-16", "NETFLIX.COM", "16.99", "Purchase"),
        ])
        txns, _ = parse_csvs(str(tmp_path))
        assert len(txns) == 1
        assert txns[0].merchant == "Netflix"

    def test_category_consolidation_applied(self, tmp_path):
        """Fine-grained category gets consolidated (e.g. Groceries → Food & Dining)."""
        d = _setup_personal_dir(tmp_path)
        _write_credit_csv(d / "cc.csv", [
            ("2025-10-15", "COSTCO WHOLESALE #123", "200.00", "Purchase"),
        ])
        txns, _ = parse_csvs(str(tmp_path))
        assert txns[0].category == "Food & Dining"


# ── Debit card SPEND ─────────────────────────────────────────────────────────


class TestDebitSpend:
    """Debit card SPEND transactions (point-of-sale purchases)."""

    def test_valid_spend(self, tmp_path):
        d = _setup_personal_dir(tmp_path)
        _write_debit_csv(d / "debit.csv", [
            ("2025-10-20", "SPEND", "COSTCOWHOLESALE", "-150.00"),
        ])
        txns, _ = parse_csvs(str(tmp_path))
        assert len(txns) == 1
        t = txns[0]
        assert t.merchant == "Costco"
        assert t.amount == 150.00  # abs() applied
        assert t.source == "debit"
        assert t.date == datetime(2025, 10, 20)

    def test_empty_amount_skipped(self, tmp_path):
        d = _setup_personal_dir(tmp_path)
        _write_debit_csv(d / "debit.csv", [
            ("2025-10-20", "SPEND", "SOME STORE", ""),
        ])
        txns, _ = parse_csvs(str(tmp_path))
        assert len(txns) == 0

    def test_business_merchant_excluded(self, tmp_path):
        d = _setup_personal_dir(tmp_path)
        _write_debit_csv(d / "debit.csv", [
            ("2025-10-20", "SPEND", "FRESHBOOKS", "-50.00"),
            ("2025-10-21", "SPEND", "COSTCOWHOLESALE", "-100.00"),
        ])
        txns, _ = parse_csvs(str(tmp_path))
        assert len(txns) == 1
        assert txns[0].merchant == "Costco"

    def test_fixed_cost_merchant_flagged(self, tmp_path):
        d = _setup_personal_dir(tmp_path)
        _write_debit_csv(d / "debit.csv", [
            ("2025-10-20", "SPEND", "ICBC", "-280.00"),
        ])
        txns, _ = parse_csvs(str(tmp_path))
        assert txns[0].fixed_cost is True


# ── Debit card AFT_OUT (pre-authorized debits) ──────────────────────────────


class TestDebitAftOut:
    """AFT_OUT: pre-authorized debits — extracts merchant, handles debt payoffs."""

    def test_normal_aft_out(self, tmp_path):
        d = _setup_personal_dir(tmp_path)
        _write_debit_csv(d / "debit.csv", [
            ("2025-10-15", "AFT_OUT", "Pre-authorized Debit to B.C. HYDRO", "-95.00"),
        ])
        txns, debt_payoffs = parse_csvs(str(tmp_path))
        assert len(txns) == 1
        t = txns[0]
        assert t.merchant == "BC Hydro"
        assert t.amount == 95.00
        assert t.fixed_cost is True
        assert t.source == "debit"
        assert len(debt_payoffs) == 0

    def test_debt_payoff_above_threshold_excluded(self, tmp_path):
        """Mortgage payment above $5000 threshold → debt_payoffs, not transactions."""
        d = _setup_personal_dir(tmp_path)
        _write_debit_csv(d / "debit.csv", [
            ("2025-10-01", "AFT_OUT", "Pre-authorized Debit to FN", "-10000.00"),
        ])
        txns, debt_payoffs = parse_csvs(str(tmp_path))
        assert len(txns) == 0
        assert len(debt_payoffs) == 1
        assert debt_payoffs[0].merchant == "Mortgage (First National)"
        assert debt_payoffs[0].amount == 10000.00
        assert debt_payoffs[0].date == datetime(2025, 10, 1)

    def test_below_threshold_included_as_fixed_cost(self, tmp_path):
        """Mortgage payment at or below threshold → regular fixed-cost transaction."""
        d = _setup_personal_dir(tmp_path)
        _write_debit_csv(d / "debit.csv", [
            ("2025-10-01", "AFT_OUT", "Pre-authorized Debit to FN", "-2000.00"),
        ])
        txns, debt_payoffs = parse_csvs(str(tmp_path))
        assert len(txns) == 1
        assert txns[0].merchant == "Mortgage (First National)"
        assert txns[0].amount == 2000.00
        assert txns[0].fixed_cost is True
        assert len(debt_payoffs) == 0

    def test_hyundai_above_threshold_excluded(self, tmp_path):
        """Hyundai payment above $5000 → debt_payoffs."""
        d = _setup_personal_dir(tmp_path)
        _write_debit_csv(d / "debit.csv", [
            ("2025-11-01", "AFT_OUT", "Pre-authorized Debit to HYUNDAI PMNT", "-6000.00"),
        ])
        txns, debt_payoffs = parse_csvs(str(tmp_path))
        assert len(txns) == 0
        assert len(debt_payoffs) == 1
        assert debt_payoffs[0].merchant == "Hyundai Car Payment"


# ── Debit card OBP_OUT (online bill payments) ───────────────────────────────


class TestDebitObpOut:
    """OBP_OUT: online bill payments — extracts merchant before comma."""

    def test_obp_out_extracts_merchant(self, tmp_path):
        d = _setup_personal_dir(tmp_path)
        _write_debit_csv(d / "debit.csv", [
            ("2025-10-15", "OBP_OUT",
             "Online bill payment for VANCOUVERPROPERTYTAXES, account 12345", "-500.00"),
        ])
        txns, _ = parse_csvs(str(tmp_path))
        assert len(txns) == 1
        t = txns[0]
        assert t.merchant == "Vancouver Property Taxes"
        assert t.amount == 500.00
        assert t.fixed_cost is True
        assert t.source == "debit"

    def test_obp_out_without_standard_prefix(self, tmp_path):
        """If the description doesn't match the expected format, use raw description."""
        d = _setup_personal_dir(tmp_path)
        _write_debit_csv(d / "debit.csv", [
            ("2025-10-15", "OBP_OUT", "Some other bill", "-100.00"),
        ])
        txns, _ = parse_csvs(str(tmp_path))
        assert len(txns) == 1
        assert txns[0].fixed_cost is True


# ── Debit card E_TRFOUT (e-transfers) ───────────────────────────────────────


class TestDebitETransfer:
    """E_TRFOUT: outgoing Interac e-Transfers."""

    def test_etransfer_merchant_hardcoded(self, tmp_path):
        d = _setup_personal_dir(tmp_path)
        _write_debit_csv(d / "debit.csv", [
            ("2025-10-20", "E_TRFOUT", "Interac e-Transfer to John Doe", "-200.00"),
        ])
        txns, _ = parse_csvs(str(tmp_path))
        assert len(txns) == 1
        t = txns[0]
        assert t.merchant == "Interac e-Transfer"
        assert t.amount == 200.00
        assert t.source == "debit"

    def test_etransfer_has_no_fixed_cost_flag(self, tmp_path):
        d = _setup_personal_dir(tmp_path)
        _write_debit_csv(d / "debit.csv", [
            ("2025-10-20", "E_TRFOUT", "Interac e-Transfer to Jane", "-50.00"),
        ])
        txns, _ = parse_csvs(str(tmp_path))
        assert txns[0].fixed_cost is False


# ── Output & sorting ────────────────────────────────────────────────────────


class TestOutputSorting:
    """Verify output shape and date ordering."""

    def test_transactions_sorted_by_date(self, tmp_path):
        d = _setup_personal_dir(tmp_path)
        _write_credit_csv(d / "cc.csv", [
            ("2025-11-01", "NETFLIX.COM", "16.99", "Purchase"),
            ("2025-09-15", "STARBUCKS MAIN ST", "6.50", "Purchase"),
            ("2025-10-20", "COSTCO WHOLESALE #123", "200.00", "Purchase"),
        ])
        txns, _ = parse_csvs(str(tmp_path))
        dates = [t.date for t in txns]
        assert dates == sorted(dates)

    def test_returns_tuple_of_transactions_and_debt_payoffs(self, tmp_path):
        d = _setup_personal_dir(tmp_path)
        _write_credit_csv(d / "cc.csv", [
            ("2025-10-15", "NETFLIX.COM", "16.99", "Purchase"),
        ])
        result = parse_csvs(str(tmp_path))
        assert isinstance(result, tuple)
        assert len(result) == 2
        txns, debt_payoffs = result
        assert isinstance(txns, list)
        assert isinstance(debt_payoffs, list)

    def test_mixed_credit_and_debit_sorted_together(self, tmp_path):
        d = _setup_personal_dir(tmp_path)
        _write_credit_csv(d / "credit.csv", [
            ("2025-10-20", "NETFLIX.COM", "16.99", "Purchase"),
        ])
        _write_debit_csv(d / "debit.csv", [
            ("2025-10-10", "SPEND", "COSTCOWHOLESALE", "-100.00"),
            ("2025-10-25", "E_TRFOUT", "Interac e-Transfer to X", "-50.00"),
        ])
        txns, _ = parse_csvs(str(tmp_path))
        assert len(txns) == 3
        dates = [t.date for t in txns]
        assert dates == sorted(dates)


# ── CSV discovery strategies ─────────────────────────────────────────────────


class TestCsvDiscovery:
    """Verify the 4 CSV discovery strategies and the no-CSVs error."""

    def test_finds_files_in_transactions_personal(self, tmp_path):
        """Primary strategy: transactions/personal/ directory."""
        d = _setup_personal_dir(tmp_path)
        _write_credit_csv(d / "cc.csv", [
            ("2025-10-15", "NETFLIX.COM", "16.99", "Purchase"),
        ])
        txns, _ = parse_csvs(str(tmp_path))
        assert len(txns) == 1

    def test_finds_files_in_nested_subdirs(self, tmp_path):
        """transactions/personal/ with nested subdirectories."""
        d = tmp_path / "transactions" / "personal" / "2025"
        d.mkdir(parents=True)
        _write_credit_csv(d / "cc.csv", [
            ("2025-10-15", "NETFLIX.COM", "16.99", "Purchase"),
        ])
        txns, _ = parse_csvs(str(tmp_path))
        assert len(txns) == 1

    def test_fallback_to_old_directory_structure(self, tmp_path):
        """Backward compat: credit card/ and debit card/ subdirectories."""
        cc_dir = tmp_path / "credit card"
        cc_dir.mkdir()
        _write_credit_csv(cc_dir / "cc.csv", [
            ("2025-10-15", "NETFLIX.COM", "16.99", "Purchase"),
        ])
        txns, _ = parse_csvs(str(tmp_path))
        assert len(txns) == 1

    def test_fallback_to_root_credit_card_csvs(self, tmp_path):
        """Fallback: credit-card-*.csv in root folder."""
        _write_credit_csv(tmp_path / "credit-card-2025.csv", [
            ("2025-10-15", "NETFLIX.COM", "16.99", "Purchase"),
        ])
        txns, _ = parse_csvs(str(tmp_path))
        assert len(txns) == 1

    def test_fallback_to_any_root_csv(self, tmp_path):
        """Last resort: any *.csv in root (excluding categories/notes/budgets)."""
        _write_credit_csv(tmp_path / "data.csv", [
            ("2025-10-15", "NETFLIX.COM", "16.99", "Purchase"),
        ])
        txns, _ = parse_csvs(str(tmp_path))
        assert len(txns) == 1

    def test_root_fallback_skips_reserved_names(self, tmp_path):
        """categories.csv, notes.csv, budgets.csv are skipped in root fallback."""
        # Only a categories.csv — should trigger no-CSVs error
        (tmp_path / "categories.csv").write_text("merchant,category\nTest,Travel\n")
        with pytest.raises(SystemExit):
            parse_csvs(str(tmp_path))

    def test_exits_when_no_csvs_found(self, tmp_path):
        """Empty directory → sys.exit(1)."""
        with pytest.raises(SystemExit):
            parse_csvs(str(tmp_path))
