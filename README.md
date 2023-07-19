# Airflow ETLs

### Dags

- [x] [ETL to fetch, process and store RSS feed in MongoDB](/dags/rss_feed_etl.py) (Change MONGO_URI inside store_currated_in_db function)

### Installation guide

1. Clone the repository
2. Create `.env` file with `AIRFLOW_UID` followed by any value
3. Run `docker compose up`
