import os
import time
import requests
import uuid
import base64
from datetime import datetime
from google.cloud import storage
# Replace these with your Redash info
redash_url = "https://data2.smartcoin.co.in/"
api_key = "HLVu90qIqVGS5OyOGlo6daBCpVZNVWngJzbA0Imu"
query_ids = ["19765", "19766"]  # list of query IDs
# The headers for the API request
headers = {
    "Authorization": f"Key {api_key}",
    "Content-Type": "application/json"
}
# The data of the API request
data = {
    "parameters": {
        # Add your parameters here
    },
    "max_age": 0
}
# Function to upload to Google Cloud Storage
def upload_to_gcs(file_name, bucket_name):
    client = storage.Client()
    bucket = client.get_bucket(bucket_name)
    blob = bucket.blob(file_name)
    blob.upload_from_filename(file_name)
    return f'gs://{bucket_name}/{file_name}'
# Function to obtain the access token
def get_access_token():
    clientId = "055CEC0B5190822AB8E6FE03934B0DBD"
    clientSecret = "EA915784041A4A6FDC426A3CB8C6382C"
    oauthTokenUrl = "http://auth.smartcoin.co.in/oauth/token"
    credentials = f"{clientId}:{clientSecret}"
    print("credentials", credentials)
    base64_credentials = base64.b64encode(credentials.encode()).decode()
    print("Base64:", base64_credentials)
    headers = {
        'Content-Type': 'application/json',
        'Authorization': f'Basic {base64_credentials}'
    }
    data = {
        'scope': 'readonly'
    }
    try:
        response = requests.post(oauthTokenUrl, headers=headers, json=data)
        response.raise_for_status()  # Raise an exception if the request fails with an HTTP error status code
        print("Response:", response)
        response_data = response.json()
        print("Response_data:", response_data)
        access_token = response_data.get('accessToken')  # Fix the key name to 'accessToken'
        print("Access Token:", access_token)
        return access_token
    except requests.exceptions.RequestException as e:
        print("Error occurred during request:", e)
        return None
# Function to send the email
def send_email(access_token):
    CURR_TIME = datetime.today().strftime('%Y-%m-%d')
    json_data = {
        "messageId": str(uuid.uuid1()),
        "receivers": ["Pramod Mane <pramodmane@google.com>", "Anurag Sabharwal <anuragsl@google.com>", "Shimpy Khillan <shimpyk@google.com>"],
        "subject": "SmartCoin <> Spot: Weekly disbursal and funnel reports {}".format(CURR_TIME),
        "sender": "help <help@smartcoin.co.in>",
        "body": "Hi Team,\n \nAttached with this mail are disbursal and funnel reports for the period of last 60 days.\n\nPlease do not respond to this mail in case of any queries. Kindly reach out to SmartCoin POC to get your queries resolved.\n\nThanks & Regards \nSmartCoin Admin",
        "emailType": "kyc",
        "metadata": {"test": "01"},
        "send": 1,
        "emailAttachments": [
            'gs://sc-backup-admin/query_data-19766-{}.csv'.format(CURR_TIME),
            'gs://sc-backup-admin/query_data-19765-{}.csv'.format(CURR_TIME)
        ],
        "bcc": ["sagar.t@smartcoin.co.in", "swaujas.chatterjee@smartcoin.co.in", "yaswanth.kumar@smartcoin.co.in", "shristi.basak@smartcoin.co.in", "mayank.singh@smartcoin.co.in", "shashwat.gupta@smartcoin.co.in"]
    }
    headers = {
        'Content-Type': 'application/json',
        'Authorization': f'Bearer {access_token}',
    }
    response = requests.post('https://notification.smartcoin.co.in/email/sendNow', headers=headers, json=json_data)
    print("Email_Response:", response.text)
    return response
def main():
    for query_id in query_ids:
        # The URL of the query API
        query_url = f"{redash_url}/api/queries/{query_id}/results"
        # Send the API request
        response = requests.post(query_url, headers=headers, json=data)
        response.raise_for_status()
        # The API request returns a job that we can poll for the result
        job = response.json()["job"]
        while job["status"] not in (3, 4):
            response = requests.get(f"{redash_url}/api/jobs/{job['id']}", headers=headers)
            response.raise_for_status()
            job = response.json()["job"]
            time.sleep(1)
        # Once the job has finished, we can get the result
        result_url = f"{redash_url}/api/queries/{query_id}/results/{job['query_result_id']}.csv"
        response = requests.get(result_url, headers=headers)
        response.raise_for_status()
        # Save the result to a CSV file
        curr_time = datetime.today().strftime('%Y-%m-%d')
        file_name = f"query_data-{query_id}-{curr_time}.csv"
        with open(file_name, "w") as f:
            f.write(response.text)
        # Upload the CSV file to Google Cloud Storage
        bucket_name = "sc-backup-admin"
        gcs_url = upload_to_gcs(file_name, bucket_name)
        print(f"File uploaded to: {gcs_url}")
    # Now that the CSV files are uploaded, send the email
    access_token = get_access_token()
    if not access_token:
        print("Failed to obtain access token.")
        exit(1)
    response = send_email(access_token)
    print(response.content)
if __name__ == "__main__":
    main()
