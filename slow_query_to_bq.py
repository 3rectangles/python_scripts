#!/usr/bin/python
import csv
import ijson
import json
import datetime
import os
import time
import subprocess
from google.cloud import bigquery
# Initialize BigQuery client
client = bigquery.Client()
# Generate the Google Cloud Storage URL for the slow query logs
def generate_gcs_url():
    # Get the current time minus 1 hour
    now = datetime.datetime.now() - datetime.timedelta(hours=1)
    # Format the date and hour strings
    date_str = now.strftime('%Y/%m/%d')
    hour_str = now.strftime('%H:00:00_%H:59:59')
    # Return the formatted GCS URL
    return f'gs://smartcoin-stackdriver-logs/cloudsql.googleapis.com/mysql-slow.log/{date_str}/{hour_str}_S0.json'
# Process the slow query logs downloaded from GCS
def process_slow_query_logs():
    try:
        # Generate the GCS URL for the slow query logs
        gs_slow_qry_json_url = generate_gcs_url()
        print("Generated GCS URL: ", gs_slow_qry_json_url)
        # Download the JSON file from the GCS URL
        subprocess.call(['gsutil', 'cp', gs_slow_qry_json_url, '/tmp/slow_query.json'])
        print("Downloaded JSON file from: ", gs_slow_qry_json_url)
        # Read the downloaded JSON file and write the execution times and timestamps to separate CSV files
        with open('/tmp/slow_query.json', 'r') as f, \
             open('/home/play/slow_query_trend_new/exec_time.csv', 'w') as time_file, \
             open('/home/play/slow_query_trend_new/exec_timestamp.csv', 'w') as timestamp_file:
            # Initialize CSV writers for both files
            time_writer = csv.writer(time_file)
            timestamp_writer = csv.writer(timestamp_file)
            # Process each line in the JSON file
            for line in f:
                # Load the JSON object from the line
                item = json.loads(line)
                # Check if the 'textPayload' key is present in the item
                if 'textPayload' in item:
                    # Split the 'textPayload' value into lines
                    text_payload_lines = item['textPayload'].splitlines()
                    # Find the line containing the 'Query_time' value
                    query_time_line = [line for line in text_payload_lines if 'Query_time' in line]
                    # If a line with 'Query_time' is found
                    if query_time_line:
                        # Extract the query time value and convert it to a float
                        query_time_str = query_time_line[0].split("Query_time:")[1].split()[0].strip()
                        query_time = float(query_time_str)
                        # Extract the timestamp from the item
                        timestamp = item['timestamp']
                        # Write the query time and timestamp to the respective CSV files
                        time_writer.writerow([query_time])
                        timestamp_writer.writerow([timestamp])
    except Exception as e:
        print(f"An error occurred while processing slow query logs: {e}")
def main():
    # Process the slow query logs
    process_slow_query_logs()
    # Get the current time
    current_time = datetime.datetime.now()
    print("Current time: ", current_time)
    # Initialize variables for storing execution time, count, and timestamps
    exec_time = 0
    count = 0
    init_timestamp = 0
    final_timestamp = 0
    try:
        # Read the execution time CSV file and calculate the total execution time and number of slow queries
        with open('/home/play/slow_query_trend_new/exec_time.csv', 'r') as file:
            reader = csv.reader(file)
            for row in reader:
                count += 1
                exec_time += float(row[0])
        # Read the timestamp CSV file and get the initial and final timestamps
        with open('/home/play/slow_query_trend_new/exec_timestamp.csv', 'r') as file1:
            reader = csv.reader(file1)
            init_timestamp = next(reader)[0]
            final_timestamp = init_timestamp
            for row in reader:
                final_timestamp = row[0]
    except Exception as e:
        print(f"An error occurred while reading CSV files: {e}")
    # Print the calculated values
    print("Total number of slow queries: ", count)
    print("Execution_time: ", exec_time)
    print("Initial timestamp: ", init_timestamp)
    print("Final timestamp: ", final_timestamp)
    try:
        table_id = "intense-nexus-126408.dev_ops.slow_query_hourly_details"
        table_ref = client.dataset("dev_ops", project="intense-nexus-126408").table("slow_query_hourly_details")
        table = client.get_table(table_ref)
        row = (current_time.isoformat(), count, exec_time, init_timestamp, final_timestamp)
        errors = client.insert_rows(table, [row])
        if errors == []:
            print("Row inserted successfully")
        else:
            print(f"Encountered errors while inserting row: {errors}")
    except Exception as e:
        print(f"An error occurred while inserting row into BigQuery table: {e}")
if __name__ == '__main__':
    main()
