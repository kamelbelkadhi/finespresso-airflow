from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import asyncio
from playwright.async_api import async_playwright
import pandas as pd
import logging
from utils.news_db_util import map_to_db, add_news_items
from datetime import datetime, timedelta
import pytz

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
URL_PREFIX = 'https://live.euronext.com'
DEFAULT_URL = "https://live.euronext.com/en/products/equities/company-news"
DEFAULT_BROWSER = "firefox"
TIMEZONE = "CET"
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def scrape_euronext():
    with async_playwright() as p:
        logging.info(f"Launching {DEFAULT_BROWSER} browser")
        browser =  p.firefox.launch(headless=True)
        context =  browser.new_context(ignore_https_errors=True)
        page =  context.new_page()

        logging.info(f"Navigating to {DEFAULT_URL}")
        page.goto(DEFAULT_URL)

        logging.info("Waiting for the news table to load")
        page.wait_for_selector('table.table')

        logging.info("Extracting news data")
        news_data = []
        rows =  page.query_selector_all('table.table tbody tr')
        
        for row in rows:
            columns =  row.query_selector_all('td')
            if len(columns) >= 5:
                date =  columns[0].inner_text()
                company =  columns[1].inner_text()
                title_link =  columns[2].query_selector('a')
                title =  title_link.inner_text() if title_link else "N/A"
                link =  title_link.get_attribute('href') if title_link else "N/A"
                industry =  columns[3].inner_text()
                topic =  columns[4].inner_text()
                
                # Extract timezone and convert published_date to GMT
                try:
                    date_parts = date.split('\n')
                    if len(date_parts) == 2:
                        date_str, time_str = date_parts
                        time_parts = time_str.split()
                        if len(time_parts) == 2:
                            time, extracted_timezone = time_parts
                            date_str = f"{date_str} {time}"
                            local_dt = datetime.strptime(date_str, "%d %b %Y %H:%M")
                            timezone = extracted_timezone  # Use extracted timezone
                        else:
                            raise ValueError("Unexpected time format")
                    else:
                        raise ValueError("Unexpected date format")
                except ValueError as e:
                    logging.error(f"Unable to parse date: {date}. Error: {str(e)}")
                    continue

                local_tz = pytz.timezone(timezone)
                local_dt = local_tz.localize(local_dt)
                gmt_dt = local_dt.astimezone(pytz.UTC)
                
                # Adjust GMT date if it's a future date
                current_time = datetime.now(pytz.UTC)
                if gmt_dt > current_time:
                    gmt_dt -= timedelta(days=1)
                
                news_data.append({
                    'published_date': local_dt.strftime("%Y-%m-%d %H:%M:%S"),
                    'published_date_gmt': gmt_dt.strftime("%Y-%m-%d %H:%M:%S"),
                    'company': company,
                    'title': title,
                    'link': URL_PREFIX + link,
                    'industry': industry,
                    'publisher_topic': topic,
                    'publisher': 'euronext',
                    'content': '',
                    'ticker': '',
                    'ai_summary': '',
                    'status': 'raw',
                    'timezone': timezone,
                    'publisher_summary': '',
                })

        browser.close()
        
        df = pd.DataFrame(news_data)
        logging.info(f"Scraped {len(df)} news items")
        return df

def main():
    try:
        df =  scrape_euronext()
        logging.info(f"Got {len(df)} rows from Euronext")
        logging.info(f"Sample data:\n{df.head()}")
        
        # Map dataframe to News objects
        news_items = map_to_db(df, 'euronext')

        # Store news in the database
        logging.info(f"Adding {len(news_items)} news items to the database")
        add_news_items(news_items)
        logging.info("Euronext: added news items to the database")
    except Exception as e:
        logging.error(f"Euronext: An error occurred: {str(e)}")


with DAG('euronext_dag', default_args=default_args, schedule_interval='@daily', catchup=False) as dag:
    run_euronext = PythonOperator(
        task_id='run_euronext',
        python_callable=main
    )
