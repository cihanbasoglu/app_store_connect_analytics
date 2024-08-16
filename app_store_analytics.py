import jwt
import gzip
import os
import requests
import pandas as pd
import json
import time
from datetime import datetime
from io import BytesIO, StringIO

def convert_to_snake_case(text):
    import re
    # Replace spaces and punctuation marks with underscores
    text = re.sub(r'[\s\W]+', '_', text)
    return text.lower()

def generate_token(api_key, api_issuer_id, private_key_path):
    with open(private_key_path, 'r') as f:
        private_key = f.read()
    headers = {
        'alg': 'ES256',
        'kid': api_key,
        'typ': 'JWT',
    }
    payload = {
        'iss': api_issuer_id,
        'iat': int(time.time()),
        'exp': int(time.time()) + 1200,  # 20 minutes expiration
        'aud': 'appstoreconnect-v1'
    }
    token = jwt.encode(
        payload,
        private_key,
        algorithm='ES256',
        headers=headers
    )
    return token


# request function to request reports. once you request your report, it takes 1-2 days to be ready before you can download it.
# select ONE_TIME_SNAPSHOT as accessType for historical data.
def get_analytics_report_requests(key_id, issuer_id, private_key_path, app_id):
    token = generate_token(key_id, issuer_id, private_key_path)
    url = f"https://api.appstoreconnect.apple.com/v1/analyticsReportRequests"
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }
    payload = {
        "data": {
            "type": "analyticsReportRequests",
            "attributes": {
                "accessType": "ONGOING"
        },
            "relationships": {
                "app": {
                    "data": {
                        "type": "apps",
                        "id": f"{app_id}"
                    }
                }
            }
        }
    }
    response = requests.post(url, headers=headers, data=json.dumps(payload))
    
    if response.status_code == 201 or response.status_code == 409:
        return response.json()
    else:
        raise Exception(f"Error: {response.status_code} - {response.text}")

api_key = "YOUR_API_KEY"
api_issuer_id = "YOUR_ISSUER_ID"
vendor_id = "YOUR_VENDOR_ID"
private_key_path = "PATH_TO_PRIVATE_KEY"
ds = datetime.today().strftime('%Y-%m-%d')

# get app id list
def get_apps_list(api_key, api_issuer_id, private_key_path):
    url = f"https://api.appstoreconnect.apple.com/v1/apps"
    token = generate_token(api_key, api_issuer_id, private_key_path)
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }
    params= {
    "fields[apps]": "bundleId"
    }

    response = requests.get(url, headers=headers, params=params)

    if response.status_code == 200:
        return response.json()
    else:
        raise Exception(f"Error: {response.status_code} - {response.text}")

# create an app id list
apps1 = get_apps_list(api_key, api_issuer_id, private_key_path)
app_list = [app['id'] for app in apps1['data']]
print(app_list)

# request ongoing reports for every game published under the account.
report_requests = []
for app_id in app_list:
    try:
        report_request = get_analytics_report_requests(api_key, api_issuer_id, private_key_path, app_id)
        report_requests.append(report_request)
    except Exception as e:
        print(f"Failed to create report request for app {app_id}: {e}")

for report in report_requests:
    print(report)

# request with app_id to get report id
def read_reports_app(api_key, api_issuer_id, private_key_path, app_id):
    url = f"https://api.appstoreconnect.apple.com/v1/apps/{app_id}/analyticsReportRequests"
    token = generate_token(api_key, api_issuer_id, private_key_path)
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }
    params= {
    "fields[analyticsReportRequests]": "reports",
    "fields[analyticsReports]": "instances",
    "filter[accessType]": "ONGOING"
    }

    response = requests.get(url, headers=headers, params=params)

    if response.status_code == 200:
        return response.json()
    else:
        raise Exception(f"Error: {response.status_code} - {response.text}")
    
# get reports
def read_reports(api_key, api_issuer_id, private_key_path, report_id, metric):
    url = f"https://api.appstoreconnect.apple.com/v1/analyticsReportRequests/{report_id}/reports"
    token = generate_token(api_key, api_issuer_id, private_key_path)
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }
    params= {
    "fields[analyticsReports]": "instances",
    "filter[category]": f"{metric}"
    }

    response = requests.get(url, headers=headers, params=params)

    if response.status_code == 200:
        return response.json()
    else:
        raise Exception(f"Error: {response.status_code} - {response.text}")
    
# get instance id
def get_instance(api_key, api_issuer_id, private_key_path, report_id):
    url = f"https://api.appstoreconnect.apple.com/v1/analyticsReports/{report_id}/instances"
    token = generate_token(api_key, api_issuer_id, private_key_path)
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }
    params= {
    "fields[analyticsReportInstances]": "segments",
    "filter[granularity]": "DAILY",
    "filter[processingDate]": f"{ds}"
    }

    response = requests.get(url, headers=headers, params=params)

    if response.status_code == 200:
        return response.json()
    else:
        raise Exception(f"Error: {response.status_code} - {response.text}")
    
# download instance
def download_segment(api_key, api_issuer_id, private_key_path, report_id):
    url = f"https://api.appstoreconnect.apple.com/v1/analyticsReportInstances/{report_id}/segments"
    token = generate_token(api_key, api_issuer_id, private_key_path)
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }
    params= {
        "fields[analyticsReportSegments]": "url"
    }

    response = requests.get(url, headers=headers, params=params)

    if response.status_code == 200:
        return response.json()
    else:
        raise Exception(f"Error: {response.status_code} - {response.text}")
    
# download the report via the download url from the previous step
def extract_url_and_get_response(url, save_path):
    # Extract the URL from the JSON response
    response = requests.get(url)
    
    # Check for a successful response
    if response.status_code == 200:
        # Decompress the gzip content
        content_gzipped = BytesIO(response.content)
        content_decompressed = gzip.GzipFile(fileobj=content_gzipped).read().decode('utf-8')
        
        # Load the decompressed content into a DataFrame
        df = pd.read_csv(StringIO(content_decompressed), sep=',')
        
        # Save the DataFrame to the specified path
        df.to_csv(save_path, index=False)
        return save_path
    else:
        print(f"Request failed with status code: {response.status_code}")
        return None

# call all the functions to get the csv. change your save_path as desired.
def get_reports(api_key, api_issuer_id, private_key_path, app_id, metric):
    ds = datetime.today().strftime('%Y-%m-%d')
    data2 = read_reports_app(api_key, api_issuer_id, private_key_path, app_id)
    report_id_1 = data2['data'][0]['id']
    data3 = read_reports(api_key, api_issuer_id, private_key_path, report_id_1, metric)
    report_id_2 = []
    for item in data3["data"]:
        if item["attributes"]["name"] in ["App Downloads Standard", "App Store Discovery and Engagement Standard", "App Store Installation and Deletion Standard"]:
            report_id_2.append(item["id"])
    data4 = []
    for id in report_id_2:
        data4.append(get_instance(api_key, api_issuer_id, private_key_path, id))
    report_id_3 = [item['id'] for entry in data4 for item in entry['data'] if 'id' in item]
    data5 = []
    for id in report_id_3:
        data5.append(download_segment(api_key, api_issuer_id, private_key_path, id))
    urls = []
    for report in data5:
        if 'data' in report:
            for item in report['data']:
                if 'attributes' in item and 'url' in item['attributes']:
                    urls.append(item['attributes']['url'])
    save_paths = []
    loop_count = 0
    for url in urls:
        i = loop_count
        save_path = f"{metric}_{ds}_{app_id}_{i}.csv"
        save_paths.append(extract_url_and_get_response(url, save_path))
        loop_count += 1
    return save_paths

# get app store analytics file for each metric
# Possible Values: APP_USAGE, APP_STORE_ENGAGEMENT, COMMERCE, FRAMEWORK_USAGE, PERFORMANCE
metric_list = ["APP_STORE_ENGAGEMENT", "APP_USAGE", "COMMERCE"]
errors = []

for metric in metric_list:
    for app_id in app_list:
        attempt_count = 0
        max_attempts = 5  # Set a max attempt count to avoid infinite loops
        while attempt_count < max_attempts:
            try:
                print(f"Downloading {app_id}")
                save_paths = get_reports(api_key, api_issuer_id, private_key_path, app_id, metric)
                # check if the file exists before attempting to read it
                for save_path in save_paths:
                    if os.path.exists(save_path):
                        with open(save_path, 'r') as file:
                            df = pd.read_csv(file, sep='\t')
                        if not df.empty:
                            rev_col = [convert_to_snake_case(col) for col in df.columns]
                            df.columns = rev_col
                            df.insert(1, "platform", "IOS")
                            df['app_id'] = 'id' + str(app_id)
                            df = df.astype(str)
                            UPLOAD_TO_YOUR_DATABASE(df, project, dataset, table, credentials, 'append')
                            del df
                            os.remove(save_path)
                            time.sleep(5)
                        else:
                            print(f"Empty file found for {app_id}. Skipping...")
                    else:
                        print(f"No file found for {app_id}")
                break  # Success, exit the loop
            except Exception as e:
                attempt_count += 1
                time.sleep(5)
                print(f'Attempt {attempt_count}: {e}. Retrying...')
                if attempt_count == max_attempts:
                    errors.append((app_id, str(e)))
        else:
            print(f"Failed to process {app_id} after {max_attempts} attempts.")

if errors:
    error_messages = [f"{app_id}: {error}" for app_id, error in errors]
    raise Exception("Errors occurred while downloading files:\n" + "\n".join(error_messages))
