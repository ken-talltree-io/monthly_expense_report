# Dashboard Feature Ideas

Brainstormed during early development sessions. Items are checked off as they're implemented.

## Implemented

- [x] Sustainability projection chart
- [x] Section explainers (1-line descriptions for each section)
- [x] 6-month total column on Category Heatmap
- [x] Milestones grouped by time period
- [x] Corporate milestones section
- [x] Data-driven milestone discovery (first revenue, first dividend, etc.)
- [x] Interac e-Transfer detail table on Spending tab
- [x] Incoming e-Transfers section on Income tab
- [x] Coverage milestones on Milestones tab (removed — projections not realistic)
- [x] git-crypt for sensitive financial data

## Not Yet Implemented

- [x] **Savings rate tracking** — Month-over-month savings rate (income minus spending / income). Single most predictive metric for financial independence.
- [ ] **Budget targets with progress bars** — Surface `budgets.csv` visually with red/green indicators per category.
- [ ] **"What-if" scenarios** — Interactive sliders for spending/yield changes showing impact on runway and time-to-sustainability.
- [ ] **Year-over-year comparisons** — Same month last year vs this year to account for seasonality (holiday spending, annual insurance, etc.).
- [ ] **Anomaly detection** — Baked-in visual alerts for transactions or category totals that are statistical outliers.
- [x] **Net worth over time** — Track net worth snapshots across dashboard runs and chart the trajectory.
- [ ] **Recurring transaction detection** — Auto-flag new recurring charges that aren't in the subscription list (merchant appearing 3+ months in a row).
- [ ] **Subscription price increase alerts** — Compare current subscription costs to previous periods and flag increases.
- [ ] **Cash flow calendar** — When big bills hit each month relative to income deposits.
- [ ] **Debt payoff projections** — For active debts, show projected payoff dates at current payment rates.
- [ ] **PDF/email export** — Auto-generate a monthly summary email or PDF snapshot for archival.
- [ ] **Multi-period dashboard** — Compare any two time ranges side-by-side.
- [ ] **Dark mode** — Alternate CSS palette + toggle (CSS variables already in place).
- [ ] **Cross-reference anomalies vs savings rate** — Visual link between anomaly alerts and their impact on the savings rate chart.
