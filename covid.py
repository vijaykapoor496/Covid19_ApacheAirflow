import datetime
import json
import csv
from datetime import timedelta
import os

import requests
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from google.cloud import bigquery
from airflow.utils.dates import days_ago

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/home/nineleaps/PycharmProjects/Apache_Airflow_Covid/covid19-dd56394bb734.json"
# export GOOGLE_APPLICATION_CREDENTIALS="/home/nineleaps/PycharmProjects/Apache_Airflow_Covid/covid19-dd56394bb734.json"
dataset_id = 'covid_data'
table_id = 'state_data'
client = bigquery.Client()

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    # 'end_date': datetime(2020, 6, 2),
    'depends_on_past': False,
    'email': ['vijay.kapoor@nineleaps.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}
dag = DAG(dag_id='Covid',
          default_args=default_args,
          description="Covid19 DAG",
          schedule_interval=timedelta(days=1),
          )
def fetch_covid19_data():
    req = requests.get('https://api.covidindiatracker.com/state_data.json')
    url_data = req.text
    data = json.loads(url_data)
    covid_data = [['date', 'state', 'number_of_cases']]
    date = datetime.datetime.today().strftime('%Y-%m-%d')
    csv_rows_count = 0
    for state in data:
        covid_data.append([date, state.get('state'), state.get('aChanges')])
        csv_rows_count += 1
    with open("covid_data_{}.csv".format(date), "w") as f:
        writer = csv.writer(f)
        writer.writerows(covid_data)
    return csv_rows_count


def upload_data_to_big_query():
    dataset_ref = client.dataset(dataset_id)
    table_ref = dataset_ref.table(table_id)
    job_config = bigquery.LoadJobConfig()
    job_config.source_format = bigquery.SourceFormat.CSV
    job_config.skip_leading_rows = 1
    job_config.autodetect = True
    date = datetime.datetime.today().strftime('%Y-%m-%d')

    with open("covid_data_{}.csv".format(date), "rb") as source_file:
        job = client.load_table_from_file(source_file, table_ref, job_config=job_config)

    job.result()  # Waits for table load to complete.

    print("Loaded {} rows into {}:{}.".format(job.output_rows, dataset_id, table_id))
    return job.output_rows

def percent_upload(**kwargs):
    rows_affected = kwargs['ti'].xcom_pull(task_ids=['upload_data'])
    csv_rows_count = kwargs['ti'].xcom_pull(task_ids=['fetch_data'])
    print("Percentage upload of data: {}".format((rows_affected[0] / csv_rows_count[0]) * 100))

t1 = PythonOperator(task_id='fetch_data', python_callable=fetch_covid19_data, dag=dag)
t2 = PythonOperator(task_id='upload_data', python_callable=upload_data_to_big_query, dag=dag)
t3 = PythonOperator(task_id='percent_upload', python_callable=percent_upload, provide_context=True, dag=dag)

t1 >> t2 >> t3



