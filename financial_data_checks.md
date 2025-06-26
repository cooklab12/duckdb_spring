# Financial Data Profiling Checks by Domain

## Common Checks (All Financial Data)

| Check Category | Check Name | Description |
|---|---|---|
| **Data Quality** | Null/Missing Value Analysis | Percentage and pattern of missing values across critical fields |
| **Data Quality** | Duplicate Record Detection | Exact and fuzzy duplicate identification with business key matching |
| **Data Quality** | Data Type Consistency | Verify expected data types (numeric, date, string) match actual |
| **Data Quality** | Format Standardization | Consistent formatting for amounts, dates, codes, identifiers |
| **Data Quality** | Character Set Validation | Detect invalid characters, encoding issues, special symbols |
| **Data Quality** | Field Length Validation | Min/max length constraints for identifiers, codes, descriptions |
| **Numerical** | Negative Value Detection | Identify negative values in fields that should be positive only |
| **Numerical** | Zero Value Analysis | Unusual concentration of zero values indicating data issues |
| **Numerical** | Decimal Precision Check | Currency amounts have proper decimal places (typically 2-4) |
| **Numerical** | Outlier Detection | Statistical outliers using Z-score, IQR, or domain-specific thresholds |
| **Numerical** | Range Validation | Values within expected business ranges (e.g., percentages 0-100) |
| **Temporal** | Date Format Consistency | Standardized date formats across all date fields |
| **Temporal** | Business Day Validation | Transactions only on valid business days (exclude weekends/holidays) |
| **Temporal** | Date Sequence Logic | Logical date ordering (trade ≤ settlement ≤ maturity) |
| **Temporal** | Data Freshness Check | Time since last update, stale data identification |
| **Temporal** | Historical Completeness | Missing time periods in time series data |
| **Regulatory** | PII Detection | Personally identifiable information in wrong fields |
| **Regulatory** | Data Retention Compliance | Records outside regulatory retention periods |
| **Regulatory** | Audit Trail Completeness | Required fields for regulatory reporting present |
| **Security** | Sensitive Data Masking | Proper masking of account numbers, SSNs, card numbers |
| **Referential** | Cross-Field Validation | Related field consistency (currency matches geography) |
| **Statistical** | Benford's Law Analysis | First digit distribution for fraud detection |

## Investment Data Checks

| Check Category | Check Name | Description |
|---|---|---|
| **Securities** | ISIN/CUSIP Validation | Valid security identifier format and check digits |
| **Securities** | Ticker Symbol Validation | Valid ticker format and exchange consistency |
| **Securities** | Security Type Consistency | Security classification matches instrument characteristics |
| **Securities** | Sector/Industry Mapping | Valid sector codes and consistent industry classification |
| **Pricing** | Bid-Ask Spread Validation | Bid ≤ Mid ≤ Ask price relationships |
| **Pricing** | Price Movement Limits | Daily price changes within exchange circuit breaker limits |
| **Pricing** | Volume-Price Correlation | Unusual volume spikes with minimal price movement |
| **Pricing** | Market Hours Validation | Trades only during market operating hours |
| **Portfolio** | Position Limit Validation | Holdings within regulatory and internal limits |
| **Portfolio** | Concentration Risk Check | Excessive concentration in single security/sector |
| **Portfolio** | Asset Allocation Consistency | Portfolio weights sum to 100%, match target allocations |
| **Portfolio** | Long/Short Position Logic | Correct sign conventions for long/short positions |
| **Performance** | Return Calculation Validation | Performance metrics calculated correctly |
| **Performance** | Benchmark Consistency | Performance compared against appropriate benchmarks |
| **Risk** | VaR Model Validation | Value-at-Risk calculations within expected ranges |
| **Risk** | Volatility Analysis | Price volatility patterns and outlier identification |
| **Corporate Actions** | Ex-Date Logic | Corporate action dates follow market conventions |
| **Corporate Actions** | Adjustment Factor Validation | Price/quantity adjustments applied correctly |

## Lending Data Checks

| Check Category | Check Name | Description |
|---|---|---|
| **Loan Basics** | Loan Number Format | Valid loan identifier format and uniqueness |
| **Loan Basics** | Product Type Validation | Loan product codes match approved product catalog |
| **Loan Basics** | Interest Rate Reasonableness | Rates within market and regulatory bounds |
| **Loan Basics** | Term Length Validation | Loan terms within product guidelines |
| **Balances** | Principal Balance Logic | Outstanding balance ≤ original principal |
| **Balances** | Payment Application Order | Payments applied per regulatory requirements (interest, principal, fees) |
| **Balances** | Escrow Balance Reconciliation | Escrow accounts balanced and properly funded |
| **Balances** | Amortization Schedule Validation | Payment schedule mathematically correct |
| **Payment History** | Payment Frequency Consistency | Payment intervals match loan terms |
| **Payment History** | Delinquency Calculation | Days past due calculated correctly |
| **Payment History** | Late Fee Assessment | Late fees applied per contract terms |
| **Payment History** | Payment Reversal Logic | Proper handling of returned/reversed payments |
| **Credit Risk** | Credit Score Validation | Scores within valid ranges for scoring model used |
| **Credit Risk** | Debt-to-Income Ratio | DTI calculations accurate and within policy limits |
| **Credit Risk** | Loan-to-Value Check | LTV ratios calculated correctly with current valuations |
| **Credit Risk** | Risk Rating Consistency | Internal risk ratings align with credit metrics |
| **Collateral** | Property Valuation Currency | Collateral values in correct currency and recent |
| **Collateral** | Insurance Coverage Adequacy | Required insurance coverage in place and current |
| **Regulatory** | HMDA Reporting Fields | Required fields for regulatory reporting complete |
| **Regulatory** | Fair Lending Compliance | Pricing and terms consistent across similar risk profiles |

## Deposit Data Checks

| Check Category | Check Name | Description |
|---|---|---|
| **Account Basics** | Account Number Format | Valid account number format with proper check digits |
| **Account Basics** | Account Type Consistency | Account classification matches product features |
| **Account Basics** | Routing Number Validation | Valid ABA routing numbers with check digit verification |
| **Account Basics** | Account Status Logic | Status transitions follow business rules |
| **Balances** | Balance Reconciliation | Ledger balance = available balance + holds |
| **Balances** | Overdraft Logic | Negative balances only for accounts with overdraft privileges |
| **Balances** | Interest Calculation | Interest accrual calculations accurate |
| **Balances** | Minimum Balance Requirements | Account balances meet minimum requirements |
| **Transactions** | Transaction Code Validation | Valid transaction type codes from approved list |
| **Transactions** | Daily Transaction Limits | Transaction amounts within daily/monthly limits |
| **Transactions** | Cut-off Time Compliance | Transactions processed according to cut-off schedules |
| **Transactions** | Debit/Credit Balance Logic | Debits/credits applied correctly based on account type |
| **Fees** | Fee Assessment Logic | Fees charged according to fee schedule |
| **Fees** | Fee Waiver Validation | Fee waivers applied correctly per customer agreements |
| **Fees** | Monthly Maintenance Fees | Recurring fees calculated and applied correctly |
| **Interest** | Interest Rate Tiers | Tiered interest rates applied correctly by balance ranges |
| **Interest** | Compounding Frequency | Interest compounded per account terms |
| **Interest** | Rate Change Documentation | Interest rate changes properly documented and notified |
| **Regulatory** | Reserve Requirement Calculation | Deposits classified correctly for reserve requirements |
| **Regulatory** | FDIC Insurance Coverage | Deposit amounts within insurance coverage limits |
| **Regulatory** | CTR Reporting Thresholds | Large cash transactions flagged for reporting |

## Reference Data Checks

| Check Category | Check Name | Description |
|---|---|---|
| **Master Data** | Unique Identifier Validation | Primary keys unique across all records |
| **Master Data** | Reference Code Consistency | Codes follow established naming conventions |
| **Master Data** | Hierarchical Relationship Integrity | Parent-child relationships maintained correctly |
| **Master Data** | Effective Date Logic | Start dates ≤ end dates, no gaps in effective periods |
| **Geographic** | Country Code Validation | Valid ISO country codes (2-letter, 3-letter) |
| **Geographic** | Currency Code Validation | Valid ISO 4217 currency codes |
| **Geographic** | Time Zone Consistency | Geographic locations match appropriate time zones |
| **Geographic** | Address Format Validation | Addresses follow country-specific postal formats |
| **Financial Instruments** | Classification Code Validation | CFI codes, asset class codes follow standards |
| **Financial Instruments** | Market Identifier Codes | Valid MIC codes for trading venues |
| **Financial Instruments** | Holiday Calendar Accuracy | Market holidays accurate for each exchange/country |
| **Financial Instruments** | Trading Hours Validation | Market operating hours current and accurate |
| **Organizational** | Legal Entity Identifier | Valid LEI format and registration status |
| **Organizational** | Regulatory License Numbers | Valid license numbers for regulated entities |
| **Organizational** | Tax Identification Numbers | Valid format for various tax ID types |
| **Organizational** | SWIFT/BIC Code Validation | Valid SWIFT codes with proper check digits |
| **Rates & Curves** | Interest Rate Curve Consistency | Rate curves monotonic and arbitrage-free |
| **Rates & Curves** | FX Rate Cross-Validation | Currency cross-rates mathematically consistent |
| **Rates & Curves** | Benchmark Rate Validation | Reference rates within historical ranges |
| **Data Lineage** | Source System Identification | Clear identification of data source systems |
| **Data Lineage** | Update Frequency Validation | Data refresh frequencies meet SLA requirements |
| **Data Lineage** | Version Control | Proper versioning of reference data changes |