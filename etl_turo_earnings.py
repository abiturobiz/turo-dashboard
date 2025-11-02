# etl_turo_earnings.py — tailored to your CSV headers
import argparse, re, duckdb, pandas as pd
from pathlib import Path

DB_DEFAULT = "turo.duckdb"

# Columns we expect from your sample (will skip ones not present)
RAW_MONEY_COLS = [
    "Trip price","Boost price","3-day discount","1-week discount","2-week discount","3-week discount",
    "1-month discount","2-month discount","3-month discount","Non-refundable discount","Early bird discount",
    "Host promotional credit","Delivery","Excess distance","Extras","Cancellation fee","Additional usage",
    "Late fee","Improper return fee","Airport operations fee","Tolls & tickets","On-trip EV charging",
    "Post-trip EV charging","Smoking","Cleaning","Fines (paid to host)","Gas reimbursement","Gas fee",
    "Other fees","Sales tax","Total earnings"
]

# Normalize header to a tidy snake_case category name
def norm(name: str) -> str:
    name = name.strip().lower()
    name = name.replace("&", "and")
    name = re.sub(r"[^a-z0-9]+", "_", name)
    name = re.sub(r"_+", "_", name).strip("_")
    return name

# Convert money strings like "$1,234.56" or "($10.00)" to float
def to_money(s):
    if pd.isna(s):
        return 0.0
    if isinstance(s, (int, float)):
        return float(s)
    s = str(s).strip()
    if not s:
        return 0.0
    neg = s.startswith("(") and s.endswith(")")
    s = s.replace("(", "").replace(")", "")
    s = s.replace("$", "").replace(",", "").strip()
    try:
        val = float(s) if s else 0.0
    except ValueError:
        val = 0.0
    return -val if neg else val

def extract(csv_dir: Path) -> pd.DataFrame:
    files = sorted(csv_dir.glob("*.csv"))
    if not files:
        raise SystemExit(f"No CSV files found in {csv_dir}")
    dfs = []
    for p in files:
        d = pd.read_csv(p)
        d["__source_file"] = p.name
        dfs.append(d)
    raw = pd.concat(dfs, ignore_index=True)
    return raw

def transform(raw: pd.DataFrame):
    from datetime import datetime, timezone

    df = raw.copy()

    # --- Keys ---
    df["trip_id"] = df.get("Reservation ID", df.get("__source_file", "file")) \
                      .astype(str) + ":" + df.index.astype(str) if "Reservation ID" not in df.columns else df["Reservation ID"]
    df["vehicle"] = df["Vehicle name"] if "Vehicle name" in df.columns else df.get("Vehicle", "Unknown vehicle")

    # --- Dates ---
    df["trip_start"] = pd.to_datetime(df.get("Trip start"), errors="coerce")
    df["trip_end_raw"] = pd.to_datetime(df.get("Trip end"), errors="coerce")

    # Now = script run time (naive local is fine for bucketing; change if you prefer tz-aware)
    now = pd.Timestamp.now()

    # Rule:
    # 1) If trip_end exists -> use it
    # 2) Else if (now - trip_start) > 30 days -> treat end as now
    # 3) Else -> leave NaT (not ended yet; exclude from aggregation)
    long_running_mask = df["trip_end_raw"].isna() & df["trip_start"].notna() & ((now - df["trip_start"]).dt.days > 30)
    df["trip_end_effective"] = df["trip_end_raw"]
    df.loc[long_running_mask, "trip_end_effective"] = now

    # Only aggregate trips that have an effective end date
    df = df[df["trip_end_effective"].notna()].copy()

    # Month bucket comes from the EFFECTIVE END date
    df["month"] = df["trip_end_effective"].dt.to_period("M").astype(str)

    # --- Identify money columns present in your file ---
    money_cols_present = [c for c in RAW_MONEY_COLS if c in df.columns]

    # Convert money strings to numeric
    for c in money_cols_present:
        df[c] = df[c].apply(to_money)

    # Normalize names (snake_case) for melt
    rename_map = {c: norm(c) for c in money_cols_present}
    value_cols_norm = list(rename_map.values())
    df_norm = df.rename(columns=rename_map)

    # --- Long table (per-category line items) ---
    id_vars = ["trip_id", "vehicle", "month"]
    long = df_norm.melt(
        id_vars=id_vars,
        value_vars=value_cols_norm,
        var_name="category",
        value_name="amount"
    ).fillna(0.0)

    # --- Per-trip net ---
    if "total_earnings" in df_norm.columns:
        per_trip = df_norm[id_vars + ["total_earnings"]].rename(columns={"total_earnings": "trip_net"})
    else:
        per_trip = long.groupby(id_vars, as_index=False)["amount"].sum().rename(columns={"amount": "trip_net"})

    return long, per_trip


def load(db_path: Path, long: pd.DataFrame, per_trip: pd.DataFrame):
    con = duckdb.connect(str(db_path))

    con.execute("""
        CREATE TABLE IF NOT EXISTS fact_line_items(
            trip_id VARCHAR, vehicle VARCHAR, month VARCHAR,
            category VARCHAR, amount DOUBLE
        );
    """)
    con.execute("""
        CREATE TABLE IF NOT EXISTS fact_trip_net(
            trip_id VARCHAR, vehicle VARCHAR, month VARCHAR, trip_net DOUBLE
        );
    """)

    con.execute("DELETE FROM fact_line_items;")
    con.execute("DELETE FROM fact_trip_net;")
    con.register("long_df", long)
    con.register("trip_df", per_trip)
    con.execute("INSERT INTO fact_line_items SELECT * FROM long_df;")
    con.execute("INSERT INTO fact_trip_net SELECT * FROM trip_df;")

    # Monthly rollups
    con.execute("""
        CREATE OR REPLACE TABLE rpt_month_vehicle_category AS
        SELECT month, vehicle, category, SUM(amount) AS amount
        FROM fact_line_items
        GROUP BY 1,2,3
        ORDER BY 1,2,3;
    """)
    con.execute("""
        CREATE OR REPLACE TABLE rpt_month_vehicle_total AS
        SELECT month, vehicle, SUM(trip_net) AS net_total
        FROM fact_trip_net
        GROUP BY 1,2
        ORDER BY 1,2;
    """)

    con.close()

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--csv_dir", required=True)
    ap.add_argument("--db", default=DB_DEFAULT)
    args = ap.parse_args()

    raw = extract(Path(args.csv_dir))
    long, per_trip = transform(raw)
    load(Path(args.db), long, per_trip)

    # Export handy CSV
    con = duckdb.connect(args.db)
    con.sql("""
        COPY (SELECT month, vehicle, category, amount
              FROM rpt_month_vehicle_category
              ORDER BY month, vehicle, category)
        TO 'out/monthly_breakdown.csv' WITH (HEADER, DELIMITER ',');
    """)
    con.close()
    print("Wrote out/monthly_breakdown.csv")

if __name__ == "__main__":
    main()
