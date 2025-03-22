### Project Description: **News Data Pipeline with Airflow, Google Cloud Storage, and Snowflake**

This project automates the extraction, storage, and analysis of news articles using **NewsAPI**, **Apache Airflow**, **Google Cloud Storage (GCS)**, and **Snowflake**. The pipeline retrieves news data, stores it in GCS, loads it into Snowflake, and performs analytical transformations.

---

### **Components & Workflow**
#### 1. **Fetching News Data (`fetch_news.py`)**
- **Uses NewsAPI** to fetch recent news articles based on keywords (e.g., "apple").
- Extracts **title, timestamp, source, author, URL, and content**.
- Stores data in a **Parquet file** for efficient storage.
- Uploads the file to **Google Cloud Storage (GCS)** under `news_data_analysis/parquet_files/`.
- Deletes the local Parquet file after upload.

#### 2. **Apache Airflow DAG (`news_api_airflow_job.py`)**
- **Schedules and automates** the news data pipeline daily.
- **Tasks in DAG:**
  1. `fetch_news_data_task`: Fetches news and uploads it to GCS.
  2. `snowflake_create_table`: Creates a Snowflake table from Parquet files in GCS.
  3. `snowflake_copy`: Copies data from GCS to Snowflake.
  4. `news_summary_task`: Generates a summary table aggregating article counts per source.
  5. `author_activity_task`: Analyzes author activity across different news sources.

#### 3. **Snowflake SQL (`snowflake_commands.sql`)**
- Defines **SQL queries** for creating, transforming, and summarizing news data in Snowflake.

---

### **Technologies Used**
- **Python, Pandas, Requests** (for data extraction & processing)
- **Google Cloud Storage (GCS)** (for storing Parquet files)
- **Apache Airflow** (for workflow automation)
- **Snowflake** (for data warehousing & analytics)
- **SQL** (for data transformations & aggregations)

---

