import psycopg2
from psycopg2 import sql

# Function to insert RSS data into PostgreSQL
db_url = 'postgresql://finespresso_user:7RkY1Sj27bzcoChbP7BlFLzOa9w77CxZ@dpg-cro7gpi3esus73bv7uvg-a.frankfurt-postgres.render.com/finespresso'


def connect_to_db():
    conn = psycopg2.connect(db_url)
    cursor = conn.cursor()
    return cursor
    
def insert_news_data(data):
    """
    Inserts news data into the PostgreSQL database.
    
    :param data: A list of dictionaries containing the news data to be inserted.
    :param db_url: The connection string for the PostgreSQL database.
    """
    insert_query = """
    INSERT INTO public.news_ (
        title, link, company, published_date, published_date_gmt,
        publisher, industry, content, ticker, ai_summary, publisher_topic,
        status, timezone, publisher_summary
    ) VALUES (
        %(title)s, %(link)s, %(company)s, %(published_date)s, %(published_date_gmt)s,
        %(publisher)s, %(industry)s, %(content)s, %(ticker)s, %(ai_summary)s, %(publisher_topic)s,
        %(status)s, %(timezone)s, %(publisher_summary)s
    );
    """
    
    try:
        # Connect to the PostgreSQL database
        cursor = connect_to_db()

        # Insert each row of data
        for row in data:
            cursor.execute(insert_query, row)
        
        # Commit the transaction
        conn.commit()
        print(f"{len(data)} records inserted successfully.")

    except Exception as e:
        print(f"An error occurred while inserting data: {e}")
        conn.rollback()  # Rollback in case of error
    
    finally:
        # Close the cursor and connection
        if cursor:
            cursor.close()
        if conn:
            conn.close()
