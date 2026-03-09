"""Data models shared across the dashboard pipeline."""

from dataclasses import dataclass, field
from datetime import date
from typing import TypedDict


@dataclass
class Transaction:
    """A single spending transaction (credit card, debit, or e-transfer)."""
    date: date
    month: str           # YYYY-MM
    raw_merchant: str    # original description from CSV
    merchant: str        # normalized merchant name
    category: str        # spending category (post-consolidation)
    amount: float
    source: str          # "credit" or "debit"
    fixed_cost: bool = False

    def __post_init__(self):
        if self.amount < 0:
            raise ValueError(f"Transaction amount must be >= 0, got {self.amount}")
        if self.source not in ("credit", "debit"):
            raise ValueError(f"Transaction source must be 'credit' or 'debit', got {self.source!r}")
        if not self.merchant:
            raise ValueError("Transaction merchant must not be empty")


@dataclass
class DebtPayoff:
    """A large debt payment excluded from regular spending (mortgage, auto)."""
    merchant: str
    amount: float
    date: date

    def __post_init__(self):
        if self.amount <= 0:
            raise ValueError(f"DebtPayoff amount must be > 0, got {self.amount}")
        if not self.merchant:
            raise ValueError("DebtPayoff merchant must not be empty")


@dataclass
class Subscription:
    """A detected recurring charge with history and status tracking."""
    merchant: str
    avg: float
    history: dict        # month -> amount
    status: str          # "stable", "price_change", "new", "stopped"
    alerts: list         # alert message strings
    months_active: int
    category: str

    _VALID_STATUSES = frozenset({"stable", "price_change", "new", "stopped"})

    def __post_init__(self):
        if self.status not in self._VALID_STATUSES:
            raise ValueError(f"Subscription status must be one of {self._VALID_STATUSES}, got {self.status!r}")
        if self.avg < 0:
            raise ValueError(f"Subscription avg must be >= 0, got {self.avg}")


@dataclass
class BalanceHistoryEntry:
    """Monthly snapshot of an investment account's balance and cash flows."""
    date: str            # YYYY-MM-DD or YYYY-MM
    balance: float
    deposits: float
    withdrawals: float

    def __post_init__(self):
        if self.deposits < 0:
            raise ValueError(f"BalanceHistoryEntry deposits must be >= 0, got {self.deposits}")
        if self.withdrawals < 0:
            raise ValueError(f"BalanceHistoryEntry withdrawals must be >= 0, got {self.withdrawals}")


@dataclass
class DividendHistoryEntry:
    """Monthly dividend/interest income from an investment account."""
    month: str           # YYYY-MM
    amount: float


@dataclass
class StatementBalance:
    """Parsed balance and metadata from a brokerage/bank statement PDF."""
    balance: float
    date: str
    source: str
    return_pct: float | None = None
    return_source: str = ""
    dividends_annual: float | None = None
    balance_history: list[BalanceHistoryEntry] = field(default_factory=list)
    dividend_history: list[DividendHistoryEntry] = field(default_factory=list)


@dataclass
class Passthrough:
    """A passthrough deposit (external money parked in an account temporarily)."""
    account_suffix: str
    start_date: date
    end_date: date
    principal: float
    description: str

    def __post_init__(self):
        if self.principal <= 0:
            raise ValueError(f"Passthrough principal must be > 0, got {self.principal}")
        if not self.account_suffix:
            raise ValueError("Passthrough account_suffix must not be empty")
        if self.end_date <= self.start_date:
            raise ValueError(f"Passthrough end_date must be after start_date, got {self.start_date} to {self.end_date}")


@dataclass
class Liability:
    """A liability (loan, debt) tracked for net worth calculations."""
    description: str
    start_date: date
    end_date: date
    amount: float

    def __post_init__(self):
        if self.amount <= 0:
            raise ValueError(f"Liability amount must be > 0, got {self.amount}")
        if not self.description:
            raise ValueError("Liability description must not be empty")


@dataclass
class BankInterest:
    """An interest payment from a bank account."""
    date: date
    amount: float
    account: str

    def __post_init__(self):
        if self.amount <= 0:
            raise ValueError(f"BankInterest amount must be > 0, got {self.amount}")


@dataclass
class ETransfer:
    """An incoming e-transfer payment."""
    date: date
    amount: float

    def __post_init__(self):
        if self.amount <= 0:
            raise ValueError(f"ETransfer amount must be > 0, got {self.amount}")


@dataclass
class AccountEntry:
    """An investment/property account with balance, returns, and history."""
    account: str
    brokerage: str
    type: str            # "Non-reg", "TFSA", "RRSP", "RESP", "Corporate", "Property", "Cash"
    suffix: str
    value: float
    income_annual: float
    growth_annual: float
    return_pct: float
    return_source: str
    income_source: str
    strategy: str
    start_date: date | None
    balance_source: str
    statement_date: str
    balance_history: list[BalanceHistoryEntry] = field(default_factory=list)
    dividend_history: list[DividendHistoryEntry] = field(default_factory=list)


# ── TypedDicts for function return types ────────────────────────────────────


class Anomaly(TypedDict):
    """A detected spending anomaly (large transaction, category spike, or new merchant)."""
    type: str               # "large_transaction", "category_spike", "new_merchant"
    description: str
    amount: float
    date: date | None
    severity: str           # "alert" or "warning"
    category: str
    merchant: str | None


class AnalysisResult(TypedDict):
    """Return type of analyze() — full spending analysis."""
    months: list[str]
    total: float
    monthly_avg: float
    mom_change: float
    monthly_totals: dict[str, float]
    categories: list[tuple]                     # [(name, total, monthly_avg, count)]
    category_monthly: dict[str, dict[str, float]]
    subscriptions: list[Subscription]
    monthly_txns: dict[str, list[Transaction]]
    transfers: dict
    fixed_costs: dict[str, float]
    fixed_cost_detail: list[tuple]              # [(merchant, total, {month: amount})]
    fixed_total: float
    discretionary_total: float
    source_breakdown: dict[str, dict[str, float]]
    debt_payoffs: list[DebtPayoff]
    anomalies: list[Anomaly]


class PassiveIncomeResult(TypedDict):
    """Return type of extract_passive_income() — investment portfolio summary."""
    annual_income: float
    monthly_income: float
    annual_growth: float
    accounts: list[AccountEntry]
    accessible_balance: float
    registered_annual: float
    registered_monthly: float
    registered_growth: float
    registered_accounts: list[AccountEntry]
    registered_balance: float
    corporate_accounts: list[AccountEntry]
    corporate_balance: float
    property_accounts: list[AccountEntry]
    property_balance: float
    cash_accounts: list[AccountEntry]
    cash_balance: float
