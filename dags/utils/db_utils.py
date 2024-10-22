import psycopg2
from psycopg2 import sql
from datetime import datetime, timedelta
import logging
from psycopg2.extras import RealDictCursor
import pytz

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
    Inserts news data into the PostgreSQL database, checking for duplicates based on the link.
    
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
    )
    ON CONFLICT (link) DO NOTHING;;
    """
    
    conn = None
    cursor = None
    try:
        # Connect to the PostgreSQL database
        conn = connect_to_db()
        cursor = conn.cursor()

        # Insert each row of data after checking for duplicate links
        for row in data:
            if check_existing_news(row['link']):
                logging.info(f"News with link {row['link']} already exists. Skipping insertion.")
                continue  # Skip this row if it already exists
            
            row['insert_date'] = datetime.now(pytz.utc) # Add current timestamp for insert_date
            row['update_date'] = datetime.now(pytz.utc) # Add current timestamp for update_date (same as insert date)
            
            cursor.execute(insert_query, row)
        
        # Commit the transaction
        conn.commit()
        logging.info(f"{len(data)} new records inserted successfully.")

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

def check_for_empty_fields(fields, publisher_filter=None, time_range_hours=24):
    """
    Fetches rows where specified fields are empty or meet a specific condition, 
    created within the last `time_range_hours`, and applies an optional filter for specific publishers.

    :param fields: A list of field names to check for being empty or null.
    :param publisher_filter: Optional publisher name to filter by.
    :param time_range_hours: Time range in hours to limit the search.
    :return: List of rows with empty or null fields as dictionaries, sorted by the newest added.
    """
    conn = None
    cursor = None
    try:
        conn = connect_to_db()
        cursor = conn.cursor(cursor_factory=RealDictCursor)  # Use RealDictCursor to return rows as dictionaries

        # Define the time range
        time_threshold = datetime.now(pytz.utc) - timedelta(hours=time_range_hours)
        
        # Build the query dynamically based on the provided fields
        conditions = " OR ".join([f"{field} = ''" for field in fields])
        
        # Update query to cast 'insert_date' to a timestamp for comparison
        query = f"""
        SELECT * FROM public.news_
        WHERE ({conditions})
        AND insert_date::timestamp >= %s
        """
        logging.info(f"time_threshold: {time_threshold}" )
        logging.info(f"running query with time_threshold: {query}" )
        # If a publisher filter is provided, add it to the query
        if publisher_filter:
            query += " AND publisher = %s"
        
        query += " ORDER BY published_date DESC"

        if publisher_filter:
            cursor.execute(query, (time_threshold, publisher_filter))
        else:
            cursor.execute(query, (time_threshold,))
        
        logging.info(f"Fetched rows with empty fields: {fields}")
        return cursor.fetchall()  # Returns rows as a list of dictionaries

    except Exception as e:
        logging.error(f"Error fetching rows with empty fields: {e}")
        return []
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()





def update_news_row(row, fields_to_update):
    """
    Updates specified fields of a news row in the database, including the update_date.

    :param row: The row data to update, must contain the 'link' key.
    :param fields_to_update: A dictionary of fields and their new values to update.
    """
    if 'link' not in row:
        logging.error("Link is missing from the row data, cannot perform update.")
        return

    conn = None
    cursor = None
    try:
        conn = connect_to_db()
        cursor = conn.cursor()

        # Prepare the SQL query dynamically based on fields to update
        set_clauses = []
        for field in fields_to_update:
            set_clauses.append(f"{field} = %s")
        
        # Add the update_date field to always update it
        set_clauses.append("update_date = %s")
        
        update_query = f"""
        UPDATE public.news_
        SET {', '.join(set_clauses)}
        WHERE link = %s
        """
        
        # Prepare values for the fields
        update_values = list(fields_to_update.values())
        update_values.append(datetime.now(pytz.utc))  # Add current timestamp for update_date
        update_values.append(row['link'])     # Add the link value to the query's WHERE clause

        cursor.execute(update_query, update_values)
        conn.commit()

        logging.info(f"Updated fields {', '.join(fields_to_update.keys())} for link {row['link']}.")
    except Exception as e:
        logging.error(f"Error updating news row: {e}")
        if conn:
            conn.rollback()
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

