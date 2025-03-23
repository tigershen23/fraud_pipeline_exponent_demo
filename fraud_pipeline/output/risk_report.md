# Financial Transaction Risk & Fraud Analysis Report

Report generated at: 2025-03-23 20:12:33

## Summary Statistics

* Total Transactions: 1000
* Known Fraudulent Transactions: 50 (5.00%)

## Risk Assessment Summary

| Risk Level | Count | Percentage |
|------------|-------|------------|
| HIGH | 6 | 0.60% |
| MEDIUM | 23 | 2.30% |
| LOW | 971 | 97.10% |

## Fraud Detection Performance

| Detection Result | Count |
|------------------|-------|
| True Positive | 6 |
| True Negative | 950 |
| False Negative | 44 |

### Detection Metrics

* Precision: 1.00 (ability to avoid false positives)
* Recall: 0.12 (ability to find all fraudulent transactions)
* F1 Score: 0.21 (balance between precision and recall)

## High Risk Transactions

| Transaction ID | Timestamp | Account | Type | Amount | Risk Score | Risk Factors | Known Fraud |
|----------------|-----------|---------|------|--------|------------|--------------|-------------|
| 0756e4ba... | 2025-01-30 02:31:27.867541 | ACCT-54502091 | PAYMENT | $2672.35 | 65 | Odd Hours, Suspicious Merchant | Yes |
| c799c18b... | 2024-12-28 01:28:58.867541 | ACCT-41000625 | TRANSFER | $74335.19 | 55 | Large Amount, Odd Hours | Yes |
| 2f5c3142... | 2025-01-01 01:14:53.867541 | ACCT-20130109 | TRANSFER | $80209.08 | 55 | Large Amount, Odd Hours | Yes |
| 98a97623... | 2025-02-09 03:53:10.867541 | ACCT-59747222 | TRANSFER | $15231.78 | 55 | Large Amount, Odd Hours | Yes |
| 92afb1b2... | 2025-02-10 03:15:33.867541 | ACCT-18048668 | WITHDRAWAL | $31570.87 | 55 | Large Amount, Odd Hours | Yes |
| 94a84bcb... | 2025-03-11 02:24:23.867541 | ACCT-43247742 | TRANSFER | $34508.48 | 55 | Large Amount, Odd Hours | Yes |

## Conclusion

This report presents the results of a heuristic-based risk assessment on financial transaction data. The model uses threshold-based rules to identify potentially fraudulent transactions. The model demonstrates high precision, suggesting it effectively avoids false alarms. However, its recall is relatively low, indicating that many fraudulent transactions are not being caught. Further refinement of the risk rules and thresholds could improve overall detection performance.
