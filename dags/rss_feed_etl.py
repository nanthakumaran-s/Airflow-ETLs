from datetime import datetime, timedelta
import requests
import os
import csv
from xml.etree import ElementTree as ET
from pymongo import MongoClient

from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator

if not os.path.exists("raw"):
    os.mkdir("raw")
if not os.path.exists("currated"):
    os.mkdir("currated")

d = datetime.now().strftime("%Y-%m-%d %H:%M:%S")


default_args = {
    'owner': 'airflow',
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
}


def fetch_data_and_store(ti):
    resp = requests.get(
        "https://timesofindia.indiatimes.com/rssfeedstopstories.cms")
    file = f"raw/raw_rss_feed_{d}.xml"
    with open(file, "w") as f:
        f.write(resp.text)
        ti.xcom_push(key="filepath", value=file)


def process_raw_rss_feed(ti):
    filepath = ti.xcom_pull(task_ids="fetch_data_and_store", key="filepath")
    tree = ET.parse(filepath)
    file = f"currated/currated_{d}.csv"
    curratedCSV = open(file, "w")

    csvwriter = csv.writer(curratedCSV)

    col = ["title", "description", "link", "publish date"]
    csvwriter.writerow(col)

    root = tree.getroot()

    for item in root.findall(".//item"):
        item_data = []

        item_title = item.find("title").text
        item_data.append(item_title)

        item_description = item.find("description").text
        item_data.append(item_description)

        item_link = item.find("link").text
        item_data.append(item_link)

        item_publish_date = item.find("pubDate").text
        item_data.append(item_publish_date)

        print(item_data)
        csvwriter.writerow(item_data)

    ti.xcom_push(key="filepath", value=file)


def store_currated_in_db(ti):
    filepath = ti.xcom_pull(task_ids="process_raw_rss_feed", key="filepath")
    mongoClient = MongoClient(
        "<connection_string>")
    db = mongoClient['rss_data']
    collection = db['rss_feed']

    header = ["title", "description", "link", "publish date"]
    csvFile = open(filepath, 'r')
    reader = csv.DictReader(csvFile)

    for each in reader:
        row = {}
        for field in header:
            row[field] = each[field]
        collection.insert_one(row)


with DAG(
    default_args=default_args,
    dag_id='RSS_Feed_ETL',
    description='ETL pipeline to extract data from RSS feed, transforms into CSV and loads them into database',
    start_date=datetime(2023, 7, 17),
    schedule_interval='@daily'
) as dag:
    start = DummyOperator(task_id="start")

    task1 = PythonOperator(
        task_id="fetch_data_and_store",
        python_callable=fetch_data_and_store
    )

    task2 = PythonOperator(
        task_id="process_raw_rss_feed",
        python_callable=process_raw_rss_feed
    )

    task3 = PythonOperator(
        task_id="store_currated_in_db",
        python_callable=store_currated_in_db
    )

    end = DummyOperator(task_id="end")

    start >> task1 >> task2 >> task3 >> end
