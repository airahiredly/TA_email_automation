import requests
import csv
import snowflake.connector
from datetime import datetime
import pandas
import os

# === CONFIGURATION FROM ENV ===
API_KEY = os.getenv("GOOGLE_API_KEY")
SHEET_ID = os.getenv("GOOGLE_SHEET_ID")
SHEET_NAME = os.getenv("GOOGLE_SHEET_NAME", "Jobs")
POST_ENDPOINT = os.getenv("POST_ENDPOINT")

SNOWFLAKE_USER = os.getenv("SNOWFLAKE_USER")
SNOWFLAKE_PASSWORD = os.getenv("SNOWFLAKE_PASSWORD")
SNOWFLAKE_ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT")
SNOWFLAKE_WAREHOUSE = "COMPUTE_WH"
SNOWFLAKE_DATABASE = "INTERMEDIATE"
SNOWFLAKE_SCHEMA = "N8N"
WEBHOOK_URL = os.getenv("WEBHOOK_URL")

# === FETCH SHEET DATA ===
sheet_url = f"https://sheets.googleapis.com/v4/spreadsheets/{SHEET_ID}/values/{SHEET_NAME}?key={API_KEY}"
response = requests.get(sheet_url)
response.raise_for_status()
sheet_data = response.json()

# Extract header + rows
values = sheet_data.get("values", [])
headers = values[0]
rows = values[1:]

# Find the index of "global_id" column
try:
    global_id_index = headers.index("global_id")
except ValueError:
    raise Exception("❌ 'global_id' column not found in sheet header.")

# Connect to Snowflake
conn = snowflake.connector.connect(
    user=SNOWFLAKE_USER,
    password=SNOWFLAKE_PASSWORD,
    account=SNOWFLAKE_ACCOUNT,
    warehouse=SNOWFLAKE_WAREHOUSE,
    database=SNOWFLAKE_DATABASE,
    schema=SNOWFLAKE_SCHEMA
)
cursor = conn.cursor()

recommend_at = datetime.now().strftime('%Y-%m-%d')

executed_candidate = cursor.execute("""SELECT JOB_ID from intermediate.n8n.internal_job_candidate_recs""")
executed_candidate_list = pandas.DataFrame(executed_candidate)
newlist = executed_candidate_list[0]
string_list = newlist.tolist()
quoted_list = [f'"{item}"' for item in string_list]

# Process each job
for job_global_id in [row[global_id_index] for row in rows if len(row) > global_id_index]:
    try:
        api_response = requests.post(POST_ENDPOINT, json={
            "global_id": job_global_id,
            "exclude_global_ids": quoted_list, 
            "limit": 5,
            "similar": False,
            "version": "default",
            "minimum_topk": 20
        })
        api_response.raise_for_status()
        result = api_response.json()

        recommended_users = result.get("recommended_users", [])
        candidate_ids = [user.get("global_id") for user in recommended_users]
        print(candidate_ids)

        for i in candidate_ids:
            myobj = {'candidate_ids': i, 'job_global_id': job_global_id}
            x = requests.post(WEBHOOK_URL, json=myobj)
            print(x.text)

        for candidate_id in candidate_ids:
            cursor.execute("""
                INSERT INTO intermediate.n8n.internal_job_candidate_recs (job_id, recommend_at, candidate_id)
                SELECT %s, %s, parse_json(%s)
            """, (job_global_id, recommend_at, f'"{candidate_id}"'))

    except Exception as e:
        print(f"❌ Failed for job_id: {job_global_id} — {e}")

cursor.close()
conn.close()
