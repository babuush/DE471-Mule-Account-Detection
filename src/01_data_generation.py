"""
Mule Account Detection - Data Generation
========================================
Generates two linked tables (Accounts, Transactions) with realistic mule
fraud patterns. Replaces the single-table midterm code.

Behavioural injections:
  - Mule accounts are young (<90 days) with low average balance.
  - Scam inflows are large round amounts from a small set of recurring victims.
  - Mule outflows happen 0.25-24h after inflow, often split across 1-3 transfers.
  - Both legs concentrate in 23:00-03:00 (off-peak).
  - Class imbalance is preserved (~4% mule accounts, ~3-5% mule-touched txns).

Outputs (in working dir):
  Accounts.csv
  Raw_Mule_Transactions.csv          (with injected noise)
  Cleaned_Mule_Transactions.csv      (post-cleaning, full dataset)
  Small_Data_Sample.csv              (1,000-row sample for repo)
"""
from __future__ import annotations

import random
from datetime import datetime, timedelta

import numpy as np
import pandas as pd
from faker import Faker

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
SEED = 42
NUM_ACCOUNTS = 2_000
TARGET_TRANSACTIONS = 12_000
MULE_ACCOUNT_RATE = 0.04
SIM_END = datetime(2026, 4, 27, 23, 59, 0)

REGIONS = ["Bangkok", "Central", "North", "Northeast", "East", "West", "South"]
REGION_WEIGHTS_NORMAL = [0.40, 0.15, 0.10, 0.15, 0.08, 0.05, 0.07]
# Mules cluster in tourism / migrant-worker regions (realistic for Thai APP fraud)
REGION_WEIGHTS_MULE = [0.55, 0.05, 0.05, 0.20, 0.05, 0.02, 0.08]

CHANNELS = ["Mobile", "Internet Banking", "ATM", "Branch"]
ROUND_MULE_AMOUNTS = [5_000, 10_000, 15_000, 20_000, 30_000, 50_000, 80_000, 100_000]

fake = Faker("th_TH")
Faker.seed(SEED)
np.random.seed(SEED)
random.seed(SEED)


# ---------------------------------------------------------------------------
# Hour-of-day distributions
# ---------------------------------------------------------------------------
def daytime_hour_weights() -> np.ndarray:
    """Normal accounts: peak 09:00-21:00, low overnight."""
    w = np.array([
        0.5, 0.3, 0.2, 0.2, 0.3, 0.5, 1.5, 3.0, 5.0, 6.0, 6.0, 6.0,
        7.0, 6.0, 6.0, 6.0, 7.0, 6.5, 6.0, 5.0, 4.0, 3.0, 2.0, 1.0,
    ])
    return w / w.sum()


def offpeak_hour_weights() -> np.ndarray:
    """Mules: heavy concentration 23:00-03:00 with a thin daytime tail."""
    w = np.full(24, 0.5)
    w[23] = 5.0
    w[0] = 6.0
    w[1] = 7.0
    w[2] = 6.0
    w[3] = 4.0
    return w / w.sum()


# ---------------------------------------------------------------------------
# Account generation
# ---------------------------------------------------------------------------
def generate_accounts() -> pd.DataFrame:
    n_mules = int(NUM_ACCOUNTS * MULE_ACCOUNT_RATE)
    rows = []
    for i in range(NUM_ACCOUNTS):
        is_mule = 1 if i < n_mules else 0
        if is_mule:
            # 80% young mules (recruited), 20% compromised older accounts
            if random.random() < 0.80:
                age_days = int(np.random.randint(7, 90))
            else:
                age_days = int(np.random.randint(90, 720))
            avg_bal = float(np.random.uniform(500, 5_000))
            region = random.choices(REGIONS, weights=REGION_WEIGHTS_MULE)[0]
        else:
            age_days = int(np.random.randint(30, 3_650))  # some young legit accounts too
            avg_bal = float(np.random.lognormal(mean=10, sigma=1.2))
            region = random.choices(REGIONS, weights=REGION_WEIGHTS_NORMAL)[0]
        rows.append({
            "Account_ID": None,
            "Account_Open_Date": (SIM_END - timedelta(days=age_days)).date(),
            "Account_Age_Days": age_days,
            "Region": region,
            "Avg_Monthly_Balance_THB": round(avg_bal, 2),
            "Is_Mule": is_mule,
        })
    random.shuffle(rows)
    for i, r in enumerate(rows):
        r["Account_ID"] = f"ACC{str(i + 1).zfill(5)}"
    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# Transaction generation
# ---------------------------------------------------------------------------
def generate_transactions(accounts_df: pd.DataFrame) -> pd.DataFrame:
    mule_ids = accounts_df.loc[accounts_df["Is_Mule"] == 1, "Account_ID"].tolist()
    normal_ids = accounts_df.loc[accounts_df["Is_Mule"] == 0, "Account_ID"].tolist()

    txns: list[dict] = []
    counter = [1]
    daytime_w = daytime_hour_weights()
    offpeak_w = offpeak_hour_weights()

    def add(sender, receiver, amount, ts, channel, scam_in, mule_out):
        txns.append({
            "Transaction_ID": f"TXN{str(counter[0]).zfill(7)}",
            "Sender_Account_ID": sender,
            "Receiver_Account_ID": receiver,
            "Amount_THB": round(float(amount), 2),
            "Timestamp": ts,
            "Channel": channel,
            "Is_Scam_Inflow": scam_in,
            "Is_Mule_Outflow": mule_out,
        })
        counter[0] += 1

    # Mule episodes: each mule has a small set of recurring "victims"
    for mule_id in mule_ids:
        n_episodes = max(1, int(np.random.poisson(3)))
        n_victims = random.choice([1, 1, 2, 2, 3])
        recurring_victims = random.sample(normal_ids, n_victims)

        for _ in range(n_episodes):
            victim = random.choice(recurring_victims)
            # 75% round amounts, 25% slightly off-round (victim sent partial balance)
            if random.random() < 0.75:
                amount = float(random.choice(ROUND_MULE_AMOUNTS))
            else:
                base = random.choice(ROUND_MULE_AMOUNTS)
                amount = float(base) - random.choice([100, 250, 500, 750, 1234])
            day_back = int(np.random.randint(1, 29))
            # 70% off-peak, 30% daytime cover (urgency-driven scams during work hours)
            if random.random() < 0.70:
                hour = int(np.random.choice(range(24), p=offpeak_w))
            else:
                hour = int(np.random.choice(range(24), p=daytime_w))
            minute = int(np.random.randint(0, 60))
            inflow_ts = (SIM_END.replace(hour=hour, minute=minute, second=0)
                         - timedelta(days=day_back))

            add(victim, mule_id, amount, inflow_ts,
                random.choices(["Mobile", "Internet Banking"], weights=[0.7, 0.3])[0],
                1, 0)

            # Outflow leg: 1-3 splits within 0.25-48h (90% of mules drain quickly,
            # 10% hold funds longer to evade simple velocity rules)
            n_out = random.choice([1, 1, 2, 2, 3])
            splits = np.random.dirichlet(np.ones(n_out)) * amount
            max_delay_h = 24 if random.random() < 0.90 else 48
            for s in splits:
                s_amt = max(100.0, round(s / 100) * 100)
                delay_h = float(np.random.uniform(0.25, max_delay_h))
                out_ts = inflow_ts + timedelta(hours=delay_h)
                if out_ts > SIM_END:
                    continue
                cashout = random.choice(normal_ids + mule_ids)
                if cashout == mule_id:
                    cashout = random.choice(normal_ids)
                add(mule_id, cashout, s_amt, out_ts,
                    random.choices(["Mobile", "ATM"], weights=[0.5, 0.5])[0],
                    0, 1)

    # Fill remainder with normal traffic
    while len(txns) < TARGET_TRANSACTIONS:
        sender = random.choice(normal_ids)
        receiver = random.choice(normal_ids)
        if sender == receiver:
            continue
        # 7% of normal transfers are large round amounts (rent, salary, big purchases)
        if random.random() < 0.07:
            amount = float(random.choice([5_000, 10_000, 15_000, 20_000, 25_000, 30_000]))
        else:
            amount = float(np.random.exponential(scale=800.0) + 50.0)
        day_back = int(np.random.randint(0, 30))
        hour = int(np.random.choice(range(24), p=daytime_w))
        minute = int(np.random.randint(0, 60))
        ts = (SIM_END.replace(hour=hour, minute=minute, second=0)
              - timedelta(days=day_back))
        channel = random.choices(CHANNELS, weights=[0.60, 0.20, 0.15, 0.05])[0]
        add(sender, receiver, amount, ts, channel, 0, 0)

    return pd.DataFrame(txns)


# ---------------------------------------------------------------------------
# Noise injection (small, controlled, realistic)
# ---------------------------------------------------------------------------
def inject_noise(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    # 1. Negative amounts (system sign flip)
    err_idx = df.sample(n=30, random_state=SEED).index
    df.loc[err_idx, "Amount_THB"] = df.loc[err_idx, "Amount_THB"] * -1
    # 2. Null channels
    null_idx = df.sample(n=60, random_state=SEED + 1).index
    df.loc[null_idx, "Channel"] = np.nan
    # 3. Duplicate rows (network retry)
    dups = df.sample(n=20, random_state=SEED + 2)
    df = pd.concat([df, dups], ignore_index=True)
    # 4. Invalid sender format
    inv_idx = df.sample(n=15, random_state=SEED + 3).index
    df.loc[inv_idx, "Sender_Account_ID"] = "INVALID"
    return df.sample(frac=1, random_state=SEED).reset_index(drop=True)


# ---------------------------------------------------------------------------
# Cleaning (preserves fraud outliers; NO IQR capping)
# ---------------------------------------------------------------------------
def clean_transactions(df: pd.DataFrame) -> pd.DataFrame:
    df = df.drop_duplicates(subset=["Transaction_ID"]).copy()
    # Negative amount → absolute (assume sign error, not magnitude error)
    df["Amount_THB"] = df["Amount_THB"].abs()
    # Drop zero / implausibly huge values (>5M THB single transfer)
    df = df[(df["Amount_THB"] > 0) & (df["Amount_THB"] < 5_000_000)]
    # Impute null channel with mode
    mode_ch = df["Channel"].mode()[0]
    df["Channel"] = df["Channel"].fillna(mode_ch)
    # Drop invalid account formats
    df = df[df["Sender_Account_ID"].astype(str).str.startswith("ACC")]
    df = df[df["Receiver_Account_ID"].astype(str).str.startswith("ACC")]
    df["Amount_THB"] = df["Amount_THB"].round(2)
    return df.reset_index(drop=True)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main() -> None:
    print("Generating accounts...")
    accounts_df = generate_accounts()
    n_mule = int(accounts_df["Is_Mule"].sum())
    print(f"  {len(accounts_df)} accounts | {n_mule} mules ({accounts_df['Is_Mule'].mean():.2%})")

    print("Generating transactions...")
    txn_df = generate_transactions(accounts_df)
    print(f"  {len(txn_df)} raw transactions")

    print("Injecting noise...")
    raw = inject_noise(txn_df)
    raw.to_csv("Raw_Mule_Transactions.csv", index=False)

    print("Cleaning (no IQR capping; fraud IS the outlier)...")
    clean = clean_transactions(raw)

    # Join Is_Mule labels for downstream EDA convenience
    a_lbl = accounts_df[["Account_ID", "Is_Mule"]]
    clean = (clean
             .merge(a_lbl.rename(columns={"Account_ID": "Sender_Account_ID",
                                          "Is_Mule": "Sender_Is_Mule"}),
                    on="Sender_Account_ID", how="left")
             .merge(a_lbl.rename(columns={"Account_ID": "Receiver_Account_ID",
                                          "Is_Mule": "Receiver_Is_Mule"}),
                    on="Receiver_Account_ID", how="left"))

    accounts_df.to_csv("Accounts.csv", index=False)
    clean.to_csv("Cleaned_Mule_Transactions.csv", index=False)
    clean.sample(n=min(1_000, len(clean)), random_state=SEED).to_csv(
        "Small_Data_Sample.csv", index=False
    )

    print()
    print(f"Final cleaned transactions: {len(clean)}")
    print(f"  Scam inflow rate:  {clean['Is_Scam_Inflow'].mean():.2%}")
    print(f"  Mule outflow rate: {clean['Is_Mule_Outflow'].mean():.2%}")
    print(f"  Mule-touched rate: {((clean['Is_Scam_Inflow']==1) | (clean['Is_Mule_Outflow']==1)).mean():.2%}")


if __name__ == "__main__":
    main()
