# RideSharingAnalytics 🚖

**RideSharingAnalytics** is a PySpark-based data engineering project for analyzing NYC taxi trip records. It supports **both batch and streaming pipelines** to provide insights into trip patterns, revenue trends, top pickup zones, and cumulative distances.

---

## Table of Contents
- [Project Highlights](#project-highlights)
- [Installation](#installation)
- [Metrics & Insights](#metrics--insights)
- [Project Structure](#project-structure)


---

## Project Highlights
- Analyze large NYC taxi datasets efficiently using **PySpark**.
- **Batch pipeline**: processes historical data to generate metrics.
- **Streaming pipeline**: simulates real-time ingestion and transformations.
- Metrics include:
  - Monthly trips & month-over-month (MoM) growth.
  - Monthly revenue & MoM growth.
  - Top 5 pickup zones per month.
  - Cumulative distance per month.
- Dashboard-ready outputs for visualization of trends.

---

## Installation

**Installation**

Clone the repository and navigate into it: git clone https://github.com/madhuri-bv/FakeNewsDetection.git cd FakeNewsDetection

Create and activate a virtual environment: python3 -m venv venv source venv/bin/activate (macOS/Linux) OR .\venv\Scripts\activate (Windows)

Install the dependencies: pip install -r requirements.txt

## Project Structure

- **batch_ingestion.py** – Loads raw taxi trip parquet data into Spark DataFrame for batch processing.  
- **transform.py** – Cleans and transforms batch data (adds datetime features, prepares metrics).  
- **aggregate.py** – Generates batch-level aggregates like hourly trips, fare averages, and trip counts.  
- **stream_ingest.py** – Simulates real-time ingestion and streaming analytics using PySpark Structured Streaming.  
- **dashboard/dashboard.py** – Streamlit app visualizing batch and streaming metrics interactively.  
- **requirements.txt** – Python dependencies required to run the project.  
- **README.md** – Project overview and instructions.  

## metrics--insights

| Metric              | Description                               |
| ------------------- | ----------------------------------------- |
| Monthly Trips       | Total trips per month                     |
| MoM Trip Growth     | Month-over-month % change in trips        |
| Monthly Revenue     | Total revenue per month                   |
| MoM Revenue Growth  | Month-over-month % change in revenue      |
| Top Pickup Zones    | Top 5 pickup locations per month          |
| Cumulative Distance | Running total of trip distances per month |
