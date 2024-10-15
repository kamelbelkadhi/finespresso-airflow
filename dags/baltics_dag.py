from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import requests
import xml.etree.ElementTree as ET
from pytz import timezone
from utils.web_utils import get_content
from utils.db_utils import insert_news_data

# Define constants
RSS_FEED_URL = "https://nasdaqbaltic.com/statistics/en/news?rss=1&num=100"
TIMEZONE = 'EET'  # Estonian timezone

# Function to parse the RSS feed and extract data for today only
def fetch_nasdaqbaltic_rss(**kwargs):
    # Fetch the current date in Estonian timezone
    est_tz = timezone(TIMEZONE)
    today = datetime.now(est_tz).date()
    
    # Fetch RSS feed
    response = requests.get(RSS_FEED_URL)
    root = ET.fromstring(response.content)

    data = []
    for item in root.findall('./channel/item'):
        title = item.find('title').text
        link = item.find('link').text
        company = item.find('issuer').text
        pub_date_str = item.find('pubDate').text
        pub_date = datetime.strptime(pub_date_str, '%a, %d %b %Y %H:%M:%S %z')
        
        # Convert to Estonian timezone and check if it is today
        pub_date_est = pub_date.astimezone(est_tz)
        if pub_date_est.date() == today:
            pub_date_gmt_str = pub_date.strftime('%Y-%m-%d %H:%M:%S')
            pub_date_est_str = pub_date_est.strftime('%Y-%m-%d %H:%M:%S')
            
            data.append({
                'title': title,
                'link': link,
                'company': company,
                'published_date': pub_date_est_str,
                'published_date_gmt': pub_date_gmt_str,
                'publisher': 'baltics',
                'industry': '',
                'content': '',
                'ticker': '',
                'ai_summary': '',
                'publisher_topic': '',
                'status': 'raw',
                'timezone': TIMEZONE,
                'publisher_summary': ''
            })
    insert_news_data(news_data, DATABASE_URL)
    return

def get_news_content(**kwargs):
    return 


def process_news_content(**kwargs):
    return 


# Define the default_args for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 10, 15),
    'email': ['kamelbelkadhi2@gmail.com'],  # Add your email here
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    'nasdaqbaltic_dag',
    default_args=default_args,
    description='A DAG to fetch and save Nasdaq Baltic RSS feed data',
    schedule_interval='@daily',  # Schedule to run daily
)

# Define the task
fetch_news = PythonOperator(
    task_id='fetch_nasdaqbaltic_rss',
    python_callable=fetch_nasdaqbaltic_rss,
    provide_context=True,
    dag=dag,
)

collect_news = PythonOperator(
    task_id='get_news_content',
    python_callable=get_news_content,
    provide_context=True,
    dag=dag,
)

process_news = PythonOperator(
    task_id='process_news_content',
    python_callable=process_news_content,
    provide_context=True,
    dag=dag,
)

# Set task dependencies (optional here as there is only one task)
fetch_news >> collect_news >> process_news
