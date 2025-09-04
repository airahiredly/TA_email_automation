import os
import requests
import snowflake.connector
from datetime import datetime
import pandas
import time
import json

# === CONFIGURATION ===
API_KEY = os.getenv("GOOGLE_API_KEY")
SHEET_ID = os.getenv("GOOGLE_SHEET_ID_HX")
SHEET_NAME = "Jobs"
POST_ENDPOINT = os.getenv("POST_ENDPOINT")
WEBHOOK_URL = os.getenv("WEBHOOK_URL_HX")

SNOWFLAKE_USER = os.getenv("SNOWFLAKE_USER")
SNOWFLAKE_PASSWORD = os.getenv("SNOWFLAKE_PASSWORD")
SNOWFLAKE_ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT")

# === Load job list from Google Sheet ===
sheet_url = f"https://sheets.googleapis.com/v4/spreadsheets/{SHEET_ID}/values/{SHEET_NAME}?key={API_KEY}"
response = requests.get(sheet_url)
response.raise_for_status()
sheet_data = response.json()

values = sheet_data.get("values", [])
headers = values[0]
rows = values[1:]

try:
    global_id_index = headers.index("global_id")
    sent_by_index = headers.index("sent_by")
    agent_name_index = headers.index("agent_name")
    recruiter_email_index = headers.index("recruiter_email")
    status_index = headers.index("status")
except ValueError as e:
    raise Exception(f"❌ Missing column in sheet header: {e}")

job_lookup = {}
for row in rows:
    if len(row) > max(global_id_index, sent_by_index,agent_name_index,status_index,recruiter_email_index):
        if row[status_index].lower() == "active":
            job_lookup[row[global_id_index]] = {
                "sent_by": row[sent_by_index],
                "agent_name": row[agent_name_index],
                "recruiter_email": row[recruiter_email_index]
            }

# === Connect to Snowflake ===
conn = snowflake.connector.connect(
    user=SNOWFLAKE_USER,
    password=SNOWFLAKE_PASSWORD,
    account=SNOWFLAKE_ACCOUNT,
    warehouse="COMPUTE_WH",
    database="INTERMEDIATE",
    schema="N8N"
)
cursor = conn.cursor()

recommend_at = datetime.now().strftime('%Y-%m-%d')

# === Process each job ===
for job_global_id in job_lookup.keys():
    try:
        cursor.execute(f"""
            with jobs as (
                select title, id as job_id, global_id as job_global_id
                from base.postgresql_hiredly_my.jobs 
                where is_active = true
            ),
            applied_candidates as (
                select j.job_global_id, array_agg(u.global_id) as user_global_id
                from base.postgresql_hiredly_my.job_applications ja
                inner join jobs j on j.job_id = ja.job_id
                inner join base.postgresql_hiredly_my.users u on u.id = ja.user_id
                group by j.job_global_id
            ),
            recommended_candidates as (
                select job_id as job_global_id, array_agg(candidate_id) as user_global_id 
                from intermediate.n8n.internal_job_candidate_recs
                group by job_global_id
            ),
            hiredly_employees as (
                select array_agg(user_global_id) as user_global_id
                from intermediate.n8n.personal_email_list
            )
            select array_distinct(
                array_cat(
                    array_cat(
                        coalesce(ac.user_global_id, array_construct()),
                        coalesce(rc.user_global_id, array_construct())
                    ),
                    coalesce(he.user_global_id, array_construct())
                )
            ) as user_global_id
            from jobs j
            left join recommended_candidates rc on rc.job_global_id = j.job_global_id
            left join applied_candidates ac on ac.job_global_id = j.job_global_id
            cross join hiredly_employees he
            where j.job_global_id = '{job_global_id}'
        """)

        new_rows = cursor.fetchall()
        final_list = []

        if new_rows:
            columns = [col[0] for col in cursor.description]
            executed_candidate_list = pandas.DataFrame(new_rows, columns=columns)

            if not executed_candidate_list.empty:
                raw_array = executed_candidate_list.at[0, "USER_GLOBAL_ID"]
                if raw_array:
                    raw_string = str(raw_array)
                    cleaned_string = raw_string.strip('"[]').replace('\n', '').replace(' ', '')
                    items = cleaned_string.split(',')
                    final_list = [item.strip(' "\'[]') for item in items if item]

            # === POST request to Ashley API ===
            api_payload = {
                "global_id": job_global_id,
                "exclude_global_ids": final_list,
                "limit": 100,
                "similar": False,
                "version": "v1.2",
                "minimum_topk": 100,
                "nationality": []
            }

            api_response = requests.post(POST_ENDPOINT, json=api_payload)
            api_response.raise_for_status()
            result = api_response.json()

            recommended_users = result.get("recommended_users", [])
            candidate_ids = [user.get("global_id") for user in recommended_users if user.get("global_id")]

            print(f"Job {job_global_id} → Recommended {len(candidate_ids)} users")

            # === Send webhook with extra fields ===
            sent_by = job_lookup[job_global_id]["sent_by"]
            name = job_lookup[job_global_id]["agent_name"]
            recruiter_email = job_lookup[job_global_id]["recruiter_email"]

            for candidate in candidate_ids:
                myobj = {
                    "candidate_ids": candidate,
                    "job_global_id": job_global_id,
                    "sent_by": sent_by,
                    "agent_name": name,
                    "recruiter_email": recruiter_email
                }
                x = requests.post(WEBHOOK_URL, json=myobj)
                print(f"Webhook response: {x.text}")

                time.sleep(5)

                cursor.execute("""
                    INSERT INTO intermediate.n8n.internal_job_candidate_recs (job_id, recommend_at, candidate_id)
                    SELECT %s, %s, parse_json(%s)
                """, (job_global_id, recommend_at, f'"{candidate}"'))

                print("Candidate added to Snowflake")
    except Exception as e:
        print(f"❌ Failed for job_id: {job_global_id} — {e}")

cursor.close()
conn.close()
