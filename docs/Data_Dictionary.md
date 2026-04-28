# Data Dictionary

## Accounts Table (`Accounts.csv`)

| Column | Type | Format / Example | Allowed Values | Notes |
|--------|------|------------------|----------------|-------|
| Account_ID | String | `ACC00001` | `ACC` + 5 digits | Primary Key, unique, no nulls |
| Account_Open_Date | Date | `2024-08-15` | YYYY-MM-DD | Used to derive Account_Age_Days |
| Account_Age_Days | Integer | 58 | 7 - 3650 | Days from open date to simulation end (2026-04-27) |
| Region | String | `Bangkok` | Bangkok, Central, North, Northeast, East, West, South | Geographic region of account holder |
| Avg_Monthly_Balance_THB | Float | 12500.50 | > 0 | 30-day rolling mean balance |
| Is_Mule | Binary (Int) | 0 | 0 = Normal, 1 = Mule | **Target Variable**, ~4% prevalence |

## Transactions Table (`Cleaned_Mule_Transactions.csv`)

| Column | Type | Format / Example | Allowed Values | Notes |
|--------|------|------------------|----------------|-------|
| Transaction_ID | String | `TXN0001234` | `TXN` + 7 digits | Primary Key, unique, no nulls |
| Sender_Account_ID | String | `ACC00123` | `ACC` + 5 digits | Foreign Key → Accounts.Account_ID |
| Receiver_Account_ID | String | `ACC00456` | `ACC` + 5 digits | Foreign Key → Accounts.Account_ID |
| Amount_THB | Float | 20000.00 | > 0, < 5,000,000 | Transaction amount in Thai Baht |
| Timestamp | Datetime | `2026-04-15 02:34:12` | YYYY-MM-DD HH:MM:SS | Transaction execution time |
| Channel | String | `Mobile` | Mobile, Internet Banking, ATM, Branch | Origination channel |
| Is_Scam_Inflow | Binary (Int) | 0 | 0 / 1 | Victim → Mule transfer (ground truth) |
| Is_Mule_Outflow | Binary (Int) | 0 | 0 / 1 | Mule → Cashout transfer (ground truth) |
| Sender_Is_Mule | Binary (Int) | 0 | 0 / 1 | Joined from Accounts (sender side) |
| Receiver_Is_Mule | Binary (Int) | 0 | 0 / 1 | Joined from Accounts (receiver side) |

## Engineered Features (computed in EDA, not stored)

| Feature | Formula | Granularity |
|---------|---------|-------------|
| Outflow_Ratio_24h | Σ(outbound within 24h of inflow) / Σ(inflow) | Per account, rolling |
| Counterparty_Diversity | Unique senders / Total inbound transactions | Per account, 30-day window |
| Off_Peak_Frequency | Count(txns in 23:00-03:00) / Total | Per account, 30-day window |
| Round_Amount_Ratio | Count(amount % 1000 == 0) / Total | Per account, 30-day window |
| Time_To_First_Outflow | (First outflow timestamp - Inflow timestamp) in hours | Per inflow event |

## Data Quality Rules

| Column | Validation Rule | Handling Strategy |
|--------|-----------------|-------------------|
| Transaction_ID | Must be unique | Drop duplicates (keep first) |
| Sender/Receiver_Account_ID | Must start with `ACC` | Drop row if invalid |
| Amount_THB | > 0 AND < 5,000,000 | Negative → abs(); zero/huge → drop |
| Timestamp | Valid datetime | Parse error → drop |
| Channel | One of 4 allowed values | Null → impute with mode (`Mobile`) |
| Is_Mule | Strictly 0 or 1 | Drop other values |

## Outlier Handling Stance

**We do NOT cap outliers using IQR or any percentile method.** In fraud detection the largest amounts and most extreme velocity values are precisely the fraud signal. Standard cleansing logic that drops the top 1% of amounts would delete the entire mule class. We only remove structurally invalid records (negative, zero, or above 5M THB single-transfer ceiling), and rely on log-scale visualisation rather than data trimming to handle the heavy tail.
