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
    user="AIRA",
    password="UKs6f9TAUfDR@f3",
    account="A4216615408961-LK73781",
    warehouse="COMPUTE_WH",
    database="INTERMEDIATE",
    schema="N8N"
)
cursor = conn.cursor()

# Get current date (or datetime if needed)
recommend_at = datetime.now().strftime('%Y-%m-%d')

# Process each job
for job_global_id in [row[global_id_index] for row in rows if len(row) > global_id_index]:
    executed_candidate = cursor.execute("""with jobs as (
select
title
, id as job_id
, global_id as job_global_id
from base.postgresql_hiredly_my.jobs 
where company_id = '44ea6c51-c84d-423f-8649-96cb592ea995'
and is_active = true
and lower(title) not like '%career fair%'
)

, applied_candidates as (
select
j.job_global_id
, array_agg(u.global_id) as user_global_id
from base.postgresql_hiredly_my.job_applications ja
inner join jobs j on j.job_id = ja.job_id
inner join base.postgresql_hiredly_my.users u on u.id = ja.user_id
group by j.job_global_id
)

, recommended_candidates as (
select 
job_id as job_global_id
, array_agg(candidate_id) as user_global_id 
from intermediate.n8n.internal_job_candidate_recs
group by job_global_id
)

select
    array_distinct(
    array_cat(
      coalesce(ac.user_global_id, array_construct()),
      coalesce(rc.user_global_id, array_construct())
    )
  ) as user_global_id
from recommended_candidates rc
left join applied_candidates ac on ac.job_global_id = rc.job_global_id
where rc.job_global_id = '661e6499c33b0f5ac80caff0'
                                        
                                        
                                        """)
    executed_candidate_list = pandas.DataFrame(executed_candidate)
    newlist = executed_candidate_list[0]
    string_list = newlist.tolist()
    # Print as a quoted list (e.g. ["id1", "id2", ...])
    quoted_list = [f'"{item}"' for item in string_list]
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
