import psycopg2
from psycopg2 import sql
from datetime import datetime, timedelta
import logging
from psycopg2.extras import RealDictCursor

# Set up logging
logging.basicConfig(level=logging.INFO, 
                    format='%(asctime)s - %(levelname)s - %(message)s', 
                    filename='news_data.log', 
                    filemode='a')  # 'a' for append mode

db_url = 'postgresql://finespresso_user:7RkY1Sj27bzcoChbP7BlFLzOa9w77CxZ@dpg-cro7gpi3esus73bv7uvg-a.frankfurt-postgres.render.com/finespresso'

def connect_to_db():
    conn = psycopg2.connect(db_url)
    logging.info("Database connection established.")
    return conn

def insert_news_data(data):
    """
    Inserts news data into the PostgreSQL database.
    
    :param data: A list of dictionaries containing the news data to be inserted.
    """
    insert_query = """
    INSERT INTO public.news_ (
        title, link, company, published_date, published_date_gmt,
        publisher, industry, content, ticker, ai_summary, publisher_topic,
        status, timezone, publisher_summary, insert_date, update_date
    ) VALUES (
        %(title)s, %(link)s, %(company)s, %(published_date)s, %(published_date_gmt)s,
        %(publisher)s, %(industry)s, %(content)s, %(ticker)s, %(ai_summary)s, %(publisher_topic)s,
        %(status)s, %(timezone)s, %(publisher_summary)s, %(insert_date)s, %(update_date)s
    );
    """
    
    conn = None
    cursor = None
    try:
        # Connect to the PostgreSQL database
        conn = connect_to_db()
        cursor = conn.cursor()

        # Insert each row of data
        for row in data:
            row['insert_date'] = datetime.now()  # Add current timestamp for insert_date
            row['update_date'] = datetime.now()  # Add current timestamp for update_date (same as insert date)
            cursor.execute(insert_query, row)
        
        # Commit the transaction
        conn.commit()
        logging.info(f"{len(data)} records inserted successfully.")

    except Exception as e:
        logging.error(f"An error occurred while inserting data: {e}")
        if conn:
            conn.rollback()  # Rollback in case of error
    
    finally:
        # Close the cursor and connection
        if cursor:
            cursor.close()
        if conn:
            conn.close()
        logging.info("Database connection closed.")


def check_existing_news(link):
    """
    Checks if the news with the given link already exists in the database.
    :param link: The news link to check.
    :return: True if the link exists, False otherwise.
    """
    conn = None
    cursor = None
    try:
        conn = connect_to_db()
        cursor = conn.cursor()
        check_query = "SELECT EXISTS(SELECT 1 FROM public.news_ WHERE link = %s)"
        cursor.execute(check_query, (link,))
        exists = cursor.fetchone()[0]
        logging.info(f"Checked existing news for link {link}, exists: {exists}")
        return exists
    except Exception as e:
        logging.error(f"Error checking existing news: {e}")
        return False
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

def check_for_empty_content(publisher_filter=None):
    """
    Fetches rows where the content field is empty, created within the last two hours,
    and applies an optional filter for specific companies or stocks.
    
    :param publisher_filter: Optional list of publisher names to filter by.
    :return: List of rows with empty content as dictionaries, sorted by newest added.
    """
    conn = None
    cursor = None
    try:
        conn = connect_to_db()
        cursor = conn.cursor(cursor_factory=RealDictCursor)  # Use RealDictCursor to return rows as dictionaries

        # Define the time range for the last two hours
        two_hours_ago = datetime.now() - timedelta(hours=2)
        
        # Base query for fetching rows with empty content from the last two hours
        query = """
        SELECT * FROM public.news_
        WHERE content = ''
        AND insert_date >= %s
        """
        
        # If a publisher filter is provided, add it to the query
        if publisher_filter:
            query += " AND publisher = %s"
        
        query += " ORDER BY published_date DESC"

        if publisher_filter:
            cursor.execute(query, (two_hours_ago, publisher_filter))
        else:
            cursor.execute(query, (two_hours_ago,))
        
        logging.info("Fetched rows with empty content.")
        return cursor.fetchall()  # Returns rows as a list of dictionaries

    except Exception as e:
        logging.error(f"Error fetching empty content rows: {e}")
        return []
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()



def update_news_content(row):
    """
    Updates the content of a news row in the database, including the update_date.
    :param row: The row data to update.
    """
    conn = None
    cursor = None
    try:
        conn = connect_to_db()
        cursor = conn.cursor()

        # Update the row with the new content and update_date
        row['update_date'] = datetime.now()  # Add current timestamp for update_date

        update_query = """
        UPDATE public.news_
        SET content = %(content)s, update_date = %(update_date)s
        WHERE link = %(link)s
        """
        cursor.execute(update_query, row)
        conn.commit()

        logging.info(f"Updated content for link {row['link']}.")
    except Exception as e:
        logging.error(f"Error updating news content: {e}")
        if conn:
            conn.rollback()
    
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()
