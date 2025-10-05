import streamlit as st
import pandas as pd
import pyarrow.parquet as pq
import os
import glob

st.set_page_config(page_title="Ride Sharing Analytics Dashboard", layout="wide")
st.title("Ride Sharing Analytics Dashboard ")

# -----------------------------
# Function to load latest parquet files
# -----------------------------
def load_parquet(folder_path, pattern="*.parquet"):
    files = glob.glob(os.path.join(folder_path, pattern))
    if not files:
        return pd.DataFrame()
    df_list = [pq.read_table(f).to_pandas() for f in files]
    return pd.concat(df_list, ignore_index=True)

# -----------------------------
# BATCH METRICS
# -----------------------------
st.header("Batch Metrics")

batch_hourly = load_parquet("data/processed/batch_hourly")
if not batch_hourly.empty:
    st.subheader("Hourly Trips (Batch)")
    st.dataframe(batch_hourly)
    st.line_chart(batch_hourly.set_index("hour")["count"])
else:
    st.info("No batch hourly trips data found.")

batch_pickup = load_parquet("data/processed/batch_pickup")
if not batch_pickup.empty:
    st.subheader("Top Pickup Zones (Batch)")
    st.dataframe(batch_pickup)
else:
    st.info("No batch pickup zone data found.")

batch_dropoff = load_parquet("data/processed/batch_dropoff")
if not batch_dropoff.empty:
    st.subheader("Top Dropoff Zones (Batch)")
    st.dataframe(batch_dropoff)
else:
    st.info("No batch dropoff zone data found.")

batch_avg = load_parquet("data/processed/batch_avg")
if not batch_avg.empty:
    st.subheader("Average Fare & Distance (Batch)")
    st.dataframe(batch_avg)
else:
    st.info("No batch avg metrics data found.")

# -----------------------------
# STREAMING METRICS
# -----------------------------
st.header("⚡ Streaming Metrics")

stream_monthly_trips = load_parquet("data/processed/stream_monthly_trips")
if not stream_monthly_trips.empty:
    st.subheader("Monthly Trips & MoM Growth (Streaming)")
    st.dataframe(stream_monthly_trips)
    st.line_chart(stream_monthly_trips.set_index("month")["count"])
else:
    st.info("No streaming monthly trips data found.")

stream_monthly_revenue = load_parquet("data/processed/stream_monthly_revenue")
if not stream_monthly_revenue.empty:
    st.subheader("Monthly Revenue & Growth (Streaming)")
    st.dataframe(stream_monthly_revenue)
    st.line_chart(stream_monthly_revenue.set_index("month")["total_revenue"])
else:
    st.info("No streaming revenue data found.")

stream_top_pickups = load_parquet("data/processed/stream_top_pickups")
if not stream_top_pickups.empty:
    st.subheader("Top Pickup Zones per Month (Streaming)")
    st.dataframe(stream_top_pickups)
else:
    st.info("No streaming top pickup zones data found.")

stream_cum_distance = load_parquet("data/processed/stream_cum_distance")
if not stream_cum_distance.empty:
    st.subheader("Cumulative Distance Traveled (Streaming)")
    st.dataframe(stream_cum_distance[["year","month","pickup_datetime","trip_distance","cumulative_distance"]].head(10))
else:
    st.info("No streaming cumulative distance data found.")

# -----------------------------
# High-level KPIs combining Batch & Streaming
# -----------------------------
st.header("📈 Key Metrics Summary")

total_trips_batch = batch_hourly["count"].sum() if not batch_hourly.empty else 0
total_trips_stream = stream_monthly_trips["count"].sum() if not stream_monthly_trips.empty else 0
total_revenue_stream = stream_monthly_revenue["total_revenue"].sum() if not stream_monthly_revenue.empty else 0
avg_distance_batch = batch_avg["avg_distance"].mean() if not batch_avg.empty else 0

col1, col2, col3, col4 = st.columns(4)
col1.metric("Total Trips (Batch)", f"{total_trips_batch:,}")
col2.metric("Total Trips (Streaming)", f"{total_trips_stream:,}")
col3.metric("Total Revenue (Streaming $)", f"{total_revenue_stream:,.2f}")
col4.metric("Avg Trip Distance (Batch miles)", f"{avg_distance_batch:.2f}")