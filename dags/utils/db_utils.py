import logging
from datetime import datetime, timedelta
import pytz
import json
from google.cloud import bigquery
import os

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    filename='news_data.log',
    filemode='a'  # 'a' for append mode
)

# Initialize BigQuery client
client = bigquery.Client()

# Read BigQuery dataset and table IDs from environment variables
dataset_id = os.environ.get('BIGQUERY_DATASET')
table_id = 'news'#os.environ.get('BIGQUERY_TABLE', 'news')
full_table_id = f"{client.project}.{dataset_id}.{table_id}"

# Load schema from JSON file
with open('schemas/news.json', 'r') as f:
    schema_json = json.load(f)

def json_to_bq_schema(json_schema):
    bq_schema = []
    for field in json_schema['fields']:
        bq_schema.append(
            bigquery.SchemaField(
                name=field['name'],
                field_type=field['type'].upper(),
                mode=field.get('mode', 'NULLABLE')
            )
        )
    return bq_schema

def create_table_if_not_exists():
    try:
        client.get_table(full_table_id)
        logging.info(f"Table {full_table_id} already exists.")
    except Exception:
        # Table does not exist
        bq_schema = json_to_bq_schema(schema_json)
        table = bigquery.Table(full_table_id, schema=bq_schema)
        client.create_table(table)
        logging.info(f"Created table {full_table_id}.")

def get_existing_links(links):
    create_table_if_not_exists()
    query = f"""
    SELECT link FROM `{full_table_id}`
    WHERE link IN UNNEST(@links)
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ArrayQueryParameter("links", "STRING", links)
        ]
    )
    query_job = client.query(query, job_config=job_config)
    results = query_job.result()
    return set(row.link for row in results)

def insert_news_data(data):
    create_table_if_not_exists()
    existing_links = get_existing_links([row['link'] for row in data])
    data_to_insert = []
    for row in data:
        if row['link'] in existing_links:
            logging.info(f"News with link {row['link']} already exists. Skipping insertion.")
            continue
        row['insert_date'] = datetime.now(pytz.utc).isoformat()
        row['update_date'] = datetime.now(pytz.utc).isoformat()
        data_to_insert.append(row)
    if data_to_insert:
        errors = client.insert_rows_json(full_table_id, data_to_insert)
        if not errors:
            logging.info(f"{len(data_to_insert)} new records inserted successfully.")
        else:
            logging.error(f"Errors occurred while inserting data: {errors}")
    else:
        logging.info("No new records to insert.")

def check_existing_news(link):
    create_table_if_not_exists()
    query = f"""
    SELECT COUNT(1) as cnt FROM `{full_table_id}`
    WHERE link = @link
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("link", "STRING", link)
        ]
    )
    query_job = client.query(query, job_config=job_config)
    result = query_job.result()
    exists = any(row.cnt > 0 for row in result)
    logging.info(f"Checked existing news for link {link}, exists: {exists}")
    return exists

def check_for_empty_fields(fields, publisher_filter=None, time_range_hours=24):
    create_table_if_not_exists()
    time_threshold = (datetime.now(pytz.utc) - timedelta(hours=time_range_hours)).isoformat()
    conditions = " OR ".join([f"({field} IS NULL OR {field} = '')" for field in fields])
    query = f"""
    SELECT * FROM `{full_table_id}`
    WHERE ({conditions})
    AND insert_date >= @time_threshold
    """
    query_parameters = [
        bigquery.ScalarQueryParameter("time_threshold", "TIMESTAMP", time_threshold)
    ]
    if publisher_filter:
        query += " AND publisher = @publisher"
        query_parameters.append(bigquery.ScalarQueryParameter("publisher", "STRING", publisher_filter))
    query += " ORDER BY published_date DESC"
    job_config = bigquery.QueryJobConfig(query_parameters=query_parameters)
    query_job = client.query(query, job_config=job_config)
    results = query_job.result()
    rows = [dict(row) for row in results]
    logging.info(f"Fetched rows with empty fields: {fields}")
    return rows

def update_news_row(row, fields_to_update):
    if 'link' not in row:
        logging.error("Link is missing from the row data, cannot perform update.")
        return
    create_table_if_not_exists()
    fields_to_update['update_date'] = datetime.now(pytz.utc).isoformat()
    set_clauses = ", ".join([f"{field} = @{field}" for field in fields_to_update.keys()])
    query = f"""
    UPDATE `{full_table_id}`
    SET {set_clauses}
    WHERE link = @link
    """
    query_parameters = [
        bigquery.ScalarQueryParameter("link", "STRING", row['link'])
    ]
    for field, value in fields_to_update.items():
        if isinstance(value, int):
            param_type = "INT64"
        elif isinstance(value, float):
            param_type = "FLOAT64"
        elif isinstance(value, bool):
            param_type = "BOOL"
        elif isinstance(value, datetime):
            param_type = "TIMESTAMP"
            value = value.isoformat()
        else:
            param_type = "STRING"
        query_parameters.append(bigquery.ScalarQueryParameter(field, param_type, value))
    job_config = bigquery.QueryJobConfig(query_parameters=query_parameters)
    client.query(query, job_config=job_config).result()
    logging.info(f"Updated fields {', '.join(fields_to_update.keys())} for link {row['link']}.")
