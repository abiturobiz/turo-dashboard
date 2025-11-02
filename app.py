# app.py – Turo Earnings Dashboard (Streamlit)
import duckdb, pandas as pd, numpy as np
import streamlit as st
import plotly.express as px
from pathlib import Path

DB_PATH = Path("turo.duckdb")  # change if needed

st.set_page_config(page_title="Turo Earnings Dashboard", layout="wide")

@st.cache_data(show_spinner=False)
def load_data(db_path: Path):
    if not db_path.exists():
        return None, None, "Database not found at: " + str(db_path)
    con = duckdb.connect(str(db_path))
    try:
        rpt_cat = con.execute("""
            SELECT month, vehicle, category, amount
            FROM rpt_month_vehicle_category
            ORDER BY month, vehicle, category
        """).df()
        rpt_total = con.execute("""
            SELECT month, vehicle, net_total
            FROM rpt_month_vehicle_total
            ORDER BY month, vehicle
        """).df()
    except Exception as e:
        return None, None, f"Missing tables. Run ETL first. Error: {e}"
    finally:
        con.close()
    # Coerce month to period-like ordering
    if not rpt_cat.empty:
        rpt_cat["month"] = pd.PeriodIndex(rpt_cat["month"], freq="M").astype(str)
    if not rpt_total.empty:
        rpt_total["month"] = pd.PeriodIndex(rpt_total["month"], freq="M").astype(str)
    return rpt_cat, rpt_total, None

rpt_cat, rpt_total, err = load_data(DB_PATH)

st.title("🚗 Turo Earnings Dashboard")

if err:
    st.error(err)
    st.stop()

if rpt_cat.empty or rpt_total.empty:
    st.warning("No data found. Download a CSV and run the ETL first.")
    st.stop()

# ----- Sidebar Filters -----
with st.sidebar:
    st.header("Filters")
    db_in = st.text_input("DuckDB path", str(DB_PATH))
    # If path changed, reload
    if db_in != str(DB_PATH):
        DB_PATH = Path(db_in)
        rpt_cat, rpt_total, err = load_data(DB_PATH)
        if err:
            st.error(err)
            st.stop()

    all_months = sorted(rpt_total["month"].unique())
    m1, m2 = (all_months[0], all_months[-1]) if all_months else ("", "")
    month_range = st.select_slider(
        "Month range",
        options=all_months,
        value=(m1, m2),
        disabled=not all_months
    )

    # Default list of vehicles to show initially
    DEFAULT_VEHICLES = [
        "Nissan Versa Note 2015",
        "Ford Fusion 2018",
        "Nissan Versa 2017",
        "Nissan Versa 2023",
        "Nissan Versa Note 2016",
        "Nissan Versa Note 2017",
        "Toyota Corolla 2017",
        "Volkswagen Atlas 2018",
        "Volkswagen Jetta 2016",
        "Volkswagen Jetta 2017",
        "Volkswagen Jetta 2019",
        "Volkswagen Taos 2022",
        "Volkswagen Tiguan 2016",
    ]

    vehicles = sorted(rpt_total["vehicle"].unique())

    # Keep only defaults that exist in the dataset
    default_existing = [v for v in DEFAULT_VEHICLES if v in vehicles]

    sel_vehicles = st.multiselect(
        "Vehicles",
        vehicles,
        default=default_existing if default_existing else vehicles,
    )


    # Default categories to show
    DEFAULT_CATEGORIES = [
        "additional_usage",
        "boost_price",
        "cancellation_fee",
        "cleaning",
        "delivery",
        "excess_distance",
        "extras",
        "fines_paid_to_host",
        "gas_reimbursement",
        "host_promotional_credit",
        "improper_return_fee",
        "late_fee",
        "other_fees",
        "smoking",
        "tolls_and_tickets",
        "trip_price",
    ]

    all_categories = sorted(rpt_cat["category"].unique())

    # Keep only defaults that actually exist in this dataset
    default_existing_cats = [c for c in DEFAULT_CATEGORIES if c in all_categories]

    sel_categories = st.multiselect(
        "Categories",
        all_categories,
        default=default_existing_cats if default_existing_cats else all_categories,
    )


# ----- Apply filters -----
m_start, m_end = month_range
mask_month = (rpt_total["month"] >= m_start) & (rpt_total["month"] <= m_end)
mask_vehicle = rpt_total["vehicle"].isin(sel_vehicles)
total_f = rpt_total[mask_month & mask_vehicle].copy()

mask_month_c = (rpt_cat["month"] >= m_start) & (rpt_cat["month"] <= m_end)
mask_vehicle_c = rpt_cat["vehicle"].isin(sel_vehicles)
mask_cat_c = rpt_cat["category"].isin(sel_categories)
cat_f = rpt_cat[mask_month_c & mask_vehicle_c & mask_cat_c].copy()

# ----- KPIs -----
fleet_net = float(total_f["net_total"].sum()) if not total_f.empty else 0.0
trip_count_est = (
    cat_f.groupby(["month", "vehicle"]).size().groupby(level=[0,1]).sum().sum()
    if not cat_f.empty else 0
)
# Better trip count: estimate by counting distinct (month, vehicle) in fact_trip_net if available
# For this simple dashboard, we’ll approximate by distinct (month, vehicle) from totals
trip_count = int(total_f.shape[0])  # per vehicle per month rows
avg_per_vehicle_month = fleet_net / max(total_f.shape[0], 1)

c1, c2, c3 = st.columns(3)
c1.metric("Fleet Net (selected range)", f"${fleet_net:,.2f}")
c2.metric("Vehicle-Month Rows", f"{trip_count}")
c3.metric("Avg Net / Vehicle-Month", f"${avg_per_vehicle_month:,.2f}")

st.markdown("---")

# ----- Charts -----
# 1) Stacked bar: monthly by category (sum over vehicles)
if not cat_f.empty:
    monthly_by_cat = (cat_f.groupby(["month", "category"], as_index=False)["amount"].sum())
    fig1 = px.bar(
        monthly_by_cat,
        x="month", y="amount", color="category",
        title="Monthly Earnings by Category (stacked)",
        barmode="relative"
    )
    st.plotly_chart(fig1, use_container_width=True)
else:
    st.info("No category data for the selected filters.")

# 2) Line: net by month (sum over vehicles)
if not total_f.empty:
    net_by_month = total_f.groupby("month", as_index=False)["net_total"].sum()
    fig2 = px.line(net_by_month, x="month", y="net_total", markers=True, title="Fleet Net by Month")
    st.plotly_chart(fig2, use_container_width=True)
else:
    st.info("No net totals for the selected filters.")

st.markdown("---")

# ----- Category Trends (toggle) -----
st.markdown("### Category Trends")

trend_mode = st.radio(
    "View",
    ["By category (sum over vehicles)", "By vehicle (one category at a time)"],
    horizontal=True,
)

if trend_mode == "By category (sum over vehicles)":
    # Choose which categories to plot (defaults to whatever is selected in the sidebar)
    cats_for_trend = st.multiselect(
        "Categories to plot",
        sorted(rpt_cat["category"].unique()),
        default=sel_categories,
        key="cats_trend_multi",
    )

    df_trend = (
        cat_f[cat_f["category"].isin(cats_for_trend)]
        .groupby(["month", "category"], as_index=False)["amount"].sum()
        .sort_values(["month", "category"])
    )

    if df_trend.empty:
        st.info("No data for the selected categories.")
    else:
        fig_trend = px.line(
            df_trend, x="month", y="amount", color="category",
            markers=True, title="Trends by Category (sum over selected vehicles)"
        )
        st.plotly_chart(fig_trend, use_container_width=True)

else:  # By vehicle (one category at a time)
    # Pick one category, then see separate lines per vehicle
    cat_single = st.selectbox(
        "Category to plot",
        sorted(rpt_cat["category"].unique()),
        index=sorted(rpt_cat["category"].unique()).index(sel_categories[0]) if sel_categories else 0,
        key="cat_trend_single",
    )

    df_trend = (
        cat_f[cat_f["category"] == cat_single]
        .groupby(["month", "vehicle"], as_index=False)["amount"].sum()
        .sort_values(["month", "vehicle"])
    )

    if df_trend.empty:
        st.info("No data for the selected category.")
    else:
        fig_trend = px.line(
            df_trend, x="month", y="amount", color="vehicle",
            markers=True, title=f"Trends for '{cat_single}' by Vehicle"
        )
        st.plotly_chart(fig_trend, use_container_width=True)

st.caption("Tip: click items in the legend to toggle lines; double-click isolates a single series.")


# ----- Tables + Downloads -----
colA, colB = st.columns(2)

with colA:
    st.subheader("Per-vehicle monthly totals")
    vt = total_f.sort_values(["month","vehicle"]).reset_index(drop=True)
    st.dataframe(vt, use_container_width=True, hide_index=True)
    st.download_button(
        "Download per-vehicle monthly totals (CSV)",
        vt.to_csv(index=False).encode("utf-8"),
        file_name="per_vehicle_monthly_totals.csv",
        mime="text/csv"
    )

with colB:
    st.subheader("Category breakdown (filtered)")
    ct = cat_f.sort_values(["month","vehicle","category"]).reset_index(drop=True)
    st.dataframe(ct, use_container_width=True, hide_index=True)
    st.download_button(
        "Download category breakdown (CSV)",
        ct.to_csv(index=False).encode("utf-8"),
        file_name="category_breakdown.csv",
        mime="text/csv"
    )

st.caption("Data source: turo.duckdb → rpt_month_vehicle_total & rpt_month_vehicle_category")
