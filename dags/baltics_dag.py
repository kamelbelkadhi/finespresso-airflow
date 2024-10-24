from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import requests
import xml.etree.ElementTree as ET
from pytz import timezone
import logging
from utils.web_utils import get_content
from utils.db_utils import *
from utils.openai_utils import *
from utils.dag_utils import *

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Define constants
RSS_FEED_URL = "https://nasdaqbaltic.com/statistics/en/news?rss=1&num=100"
TIMEZONE = 'EET'  # Estonian timezone

# Function to parse the RSS feed and extract data for today only
def fetch_nasdaqbaltic_rss(**kwargs):
    logger.info("Starting fetch_nasdaqbaltic_rss task...")
    est_tz = timezone(TIMEZONE)
    today = datetime.now(est_tz).date()
    
    try:
        response = requests.get(RSS_FEED_URL)
        response.raise_for_status()  # Raise an exception for bad responses
        logger.info("Fetched RSS feed successfully")
    except requests.exceptions.RequestException as e:
        logger.error(f"Error fetching RSS feed: {e}")
        return

    root = ET.fromstring(response.content)
    logger.info("Parsing RSS feed XML")

    data = []
    for item in root.findall('./channel/item'):
        title = item.find('title').text
        link = item.find('link').text
        company = item.find('issuer').text
        pub_date_str = item.find('pubDate').text
        pub_date = datetime.strptime(pub_date_str, '%a, %d %b %Y %H:%M:%S %z')
        
        pub_date_est = pub_date.astimezone(est_tz)
        if pub_date_est.date() == today and not check_existing_news(link):
            pub_date_gmt_str = pub_date.strftime('%Y-%m-%d %H:%M:%S')
            pub_date_est_str = pub_date_est.strftime('%Y-%m-%d %H:%M:%S')
            
            logger.info(f"Adding news item: {title}, {link}, {company}")
            data.append({
                'title': title,
                'link': link,
                'company': company,
                'published_date': pub_date_est_str,
                'published_date_gmt': pub_date_gmt_str,
                'publisher': 'Baltics',
                'industry': '',
                'content': '',
                'ticker': '',
                'ai_summary': '',
                'publisher_topic': '',
                'status': 'raw',
                'timezone': TIMEZONE,
                'publisher_summary': ''
            })
    
    if data:
        logger.info(f"Inserting {len(data)} news items into the database")
        insert_news_data(data)
    else:
        logger.info("No new news items to insert")

# Function to fetch and update content, ticker, and other fields dynamically
def get_news_content(**kwargs):
    logger.info("Starting get_news_content task...")
    # Fetch rows where 'content' is empty
    new_rows = check_for_empty_fields(fields=['content'], publisher_filter='Baltics')
    logger.info(f"Found {len(new_rows)} rows with empty content")
    
    for row in new_rows:
        link = row['link']
        logger.info(f"Fetching content for link: {link}")
        
        # Fetch content from the URL
        content = get_content(link)
        fields_to_update = {}
        
        if content:
            fields_to_update['content'] = content
        else:
            logger.warning(f"Failed to fetch content for link: {link}")
        
        if fields_to_update:
            logger.info(f"Updating fields {fields_to_update.keys()} for link: {link}")
            update_news_row(row, fields_to_update)

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
    'email': ['kamelbelkadhi2@gmail.com'],  # Add your email here
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
    'catchup':False,
    'execution_timeout':timedelta(minutes=15)
}

# Define the DAG
dag = DAG(
    'nasdaqbaltic_dag',
    default_args=default_args,
    description='A DAG to fetch and save Nasdaq Baltic RSS feed data',
    schedule_interval='@hourly',  # Schedule to run every hour
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

# Set task dependencies
fetch_news >> collect_news >> process_news
