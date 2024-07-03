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
def extract_url_and_get_response(data, save_path):
    # extract the url from the json response
    url = data['data'][0]['attributes']['url']
    response = requests.get(url)

    if response.status_code == 200:
        # decompress the gzip content
        content_gzipped = BytesIO(response.content)
        content_decompressed = gzip.GzipFile(fileobj=content_gzipped).read().decode('utf-8')
        df = pd.read_csv(StringIO(content_decompressed), sep=',')
        df.to_csv(save_path, index=False)
        return save_path
    else:
        print(f"Request failed with status code: {response.status_code}")
        return None

# call all the functions to get the csv. change your save_path as desired.
def get_reports(api_key, api_issuer_id, private_key_path, app_id, metric):
    data1 = read_reports_app(api_key, api_issuer_id, private_key_path, app_id)
    report_id_1 = data1['data'][0]['id']
    data2 = read_reports(api_key, api_issuer_id, private_key_path, report_id_1, metric)
    report_id_2 = data2['data'][0]['id']
    data3 = get_instance(api_key, api_issuer_id, private_key_path, report_id_2)
    report_id_3 = data3['data'][0]['id']
    data4 = download_segment(api_key, api_issuer_id, private_key_path, report_id_3)
    save_path = f"{metric}_{ds}_{app_id}.csv"
    save_path = extract_url_and_get_response(data4, save_path)
    return save_path

# get app store analytics file for each metric. remember, it takes 1-2 days for your report to be ready before you can download it.
# possible values as a metric are APP_USAGE, APP_STORE_ENGAGEMENT, COMMERCE, FRAMEWORK_USAGE, PERFORMANCE
metric_list = ["APP_STORE_ENGAGEMENT", "APP_USAGE", "COMMERCE", "FRAMEWORK_USAGE", "PERFORMANCE"]
for metric in metric_list:
    for app_id in app_list:
        try:
            print(f"Downloading {app_id}")
            save_path = get_reports(api_key, api_issuer_id, private_key_path, app_id, metric)
            # check if the file exists before attempting to read it
            if os.path.exists(save_path):
                df = pd.read_csv(save_path, sep='\t')
                if df is not None:
                    rev_col = []
                    for col in df.columns:
                        rev_col.append(convert_to_snake_case(col))
                else:
                    break
                df.columns = rev_col
                df['app_id'] = app_id
                df['app_id'] = 'id' + df['app_id'].astype(str)
                time.sleep(5)
            else:
                print(f"No file found for {app_id}")
        
        except Exception as e:
            print(f"Error processing {app_id}: {str(e)}")
            continue