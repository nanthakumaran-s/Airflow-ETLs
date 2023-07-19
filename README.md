# Airflow ETLs

### Dags

- [x] [ETL to fetch, process and store RSS feed in MongoDB](/dags/rss_feed_etl.py) (Change MONGO_URI inside store_currated_in_db function)

  ![image](https://github.com/nanthakumaran-s/Airflow-ETLs/assets/59391441/ac278e37-2836-4b21-961e-4bf6e5dca113)   
  ![image](https://github.com/nanthakumaran-s/Airflow-ETLs/assets/59391441/668e993e-da76-4cf7-b4bc-ec79a0515869)



### Installation guide

1. Clone the repository
2. Create `.env` file with `AIRFLOW_UID` followed by any value
3. Run `docker compose up`
