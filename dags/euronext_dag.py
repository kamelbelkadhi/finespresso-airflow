from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import requests
from bs4 import BeautifulSoup
import logging
from utils.db_utils import *
from utils.oslobors_euronext import get_news_details
from utils.openai_utils import *
from pytz import timezone
from utils.dag_utils import *

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Define constants
EURONEXT_URL = "https://live.euronext.com/en/products/equities/company-news"
TIMEZONE = "GMT"
URL_PREFIX = 'https://live.euronext.com'

# Function to scrape the Euronext news
def fetch_euronext_news(**kwargs):    
    rows = get_news_details()
    data = []
    for row in rows:               
        pub_date = datetime.strptime(row['publishedTime'], '%Y-%m-%dT%H:%M:%S.%fZ')

        pub_date_gmt_str = pub_date.strftime('%Y-%m-%d %H:%M:%S')
        data.append({
            'published_date': pub_date_gmt_str,
            'published_date_gmt': pub_date_gmt_str,
            'company': row['issuerName'],
            'title': row['title'],
            'link': row['link'],
            'industry': '',
            'publisher_topic': '',
            'publisher': row['publisher'],
            'content': row['body'],
            'ticker': row['issuerSign'],
            'ai_summary': '',
            'status': 'raw',
            'timezone': TIMEZONE,
            'publisher_summary': '',
        })
    
    # Insert data into the database
    if data:
        logger.info(f"Inserting {len(data)} news items into the database")
        insert_news_data(data)
    else:
        logger.info("No news items to insert")

def process_news_content(**kwargs):
    logger.info("Starting process_news_content task...")
    new_rows = check_for_empty_fields(fields=['ai_summary', 'publisher_topic'], publisher_filter='Baltics')
    logger.info(f"Found {len(new_rows)} rows to process for tags and summaries")
    
    for row in new_rows:
        fields_to_update = process_content_and_tags(row)
        
        if fields_to_update:
            logger.info(f"Updating fields {fields_to_update.keys()} for title: {row['title']}")
            update_news_row(row, fields_to_update)

# Define the default_args for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime.now().replace(minute=0, second=0, microsecond=0),
    'email': ['kamelbelkadhi2@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
    'catchup':False
}

# Define the DAG
dag = DAG(
    'euronext_dag',
    default_args=default_args,
    description='A DAG to fetch and save Euronext company news data',
    schedule_interval='@hourly',  # Run every hour
)

# Define the tasks
fetch_news = PythonOperator(
    task_id='fetch_euronext_news',
    python_callable=fetch_euronext_news,
    provide_context=True,
    dag=dag,
)

process_news = PythonOperator(
    task_id='process_news_content',
    python_callable=process_news_content,
    provide_context=True,
    dag=dag,
)

# Set task dependencies
fetch_news >> process_news
