import os
import requests
import csv
import snowflake.connector
from datetime import datetime
import pandas
import time

# === CONFIGURATION ===
API_KEY = os.getenv("GOOGLE_API_KEY", "AIzaSyADynlfr6qm28II06W6tp08rBOgfuSGyhs")
SHEET_ID = os.getenv("GOOGLE_SHEET_ID", "1qioNekBHJyyb9gSr39Fm67pPIhTFeZamay-Mp9F2f-4")
SHEET_NAME = "Jobs"
POST_ENDPOINT = os.getenv("POST_ENDPOINT", "https://my-ashley-api.hiredly.com/recommender/recommended_users")
WEBHOOK_URL = os.getenv("WEBHOOK_URL", "https://n8n-app-p68zu.ondigitalocean.app/webhook-test/6f77db62-5349-4076-9577-be546c054dc0")

# Snowflake credentials from environment variables
SNOWFLAKE_USER = os.getenv("SNOWFLAKE_USER", "AIRA")
SNOWFLAKE_PASSWORD = os.getenv("SNOWFLAKE_PASSWORD", "UKs6f9TAUfDR@f3")
SNOWFLAKE_ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT", "A4216615408961-LK73781")

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
    warehouse="COMPUTE_WH",
    database="INTERMEDIATE",
    schema="N8N"
)
cursor = conn.cursor()

# Get current date
recommend_at = datetime.now().strftime('%Y-%m-%d')

# Process each job
for job_global_id in [row[global_id_index] for row in rows if len(row) > global_id_index]:
    executed_candidate = cursor.execute(f"""with jobs as (
        select
        -- get job_global_id of internal jobs
        title
        , id as job_id
        , global_id as job_global_id
        from base.postgresql_hiredly_my.jobs 
        where company_id = '44ea6c51-c84d-423f-8649-96cb592ea995'
        and is_active = true
        and lower(title) not like '%career fair%'
        )

        , applied_candidates as (
        -- get candidates who applied to jobs in jobs cte
        select
        j.job_global_id
        , array_agg(u.global_id) as user_global_id
        from base.postgresql_hiredly_my.job_applications ja
        inner join jobs j on j.job_id = ja.job_id
        inner join base.postgresql_hiredly_my.users u on u.id = ja.user_id
        group by j.job_global_id
        )

        , recommended_candidates as (
        -- get candidates who were recommended previously for jobs in jobs cte
        select 
        job_id as job_global_id
        , array_agg(candidate_id) as user_global_id 
        from intermediate.n8n.internal_job_candidate_recs
        group by job_global_id
        )

        , hiredly_employees as (
        -- get user_global_id of hiredly employees
        select 
        array_agg(user_global_id) as user_global_id
        from intermediate.n8n.personal_email_list
        )

        select
        array_distinct(
            array_cat(
            array_cat(
                coalesce(ac.user_global_id, array_construct())
            , coalesce(rc.user_global_id, array_construct())
            )
            , coalesce(he.user_global_id, array_construct())
            )
        ) as user_global_id
        from recommended_candidates rc
        left join applied_candidates ac on ac.job_global_id = rc.job_global_id
        cross join hiredly_employees he
        where rc.job_global_id = '{job_global_id}'""")                               

    executed_candidate_list = pandas.DataFrame(executed_candidate)
    newlist = executed_candidate_list[0]
    string_list = newlist.tolist()
    quoted_list = [f'"{item}"' for item in string_list]

    # Step 1–4: Clean and split list
    raw_string = quoted_list[0]
    cleaned_string = raw_string.strip('"[]').replace('\n', '').replace(' ', '')
    items = cleaned_string.split(',')
    final_list = [item.strip(' "\'[]') for item in items]

    try:
        api_response = requests.post(POST_ENDPOINT, json={
            "global_id": job_global_id,
            "exclude_global_ids": final_list, 
            "limit": 1,
            "similar": False,
            "version": "v1.2",
            "minimum_topk": 20,
            "nationality": ["Malaysian"]
        })
        api_response.raise_for_status()
        result = api_response.json()

        recommended_users = result.get("recommended_users", [])
        candidate_ids = [user.get("global_id") for user in recommended_users]
        print(candidate_ids)

        for candidate in candidate_ids:
            myobj = {'candidate_ids': candidate, 'job_global_id': job_global_id}
            x = requests.post(WEBHOOK_URL, json=myobj)
            print(x.text)

            # Add delay
            time.sleep(3)

            cursor.execute("""
                INSERT INTO intermediate.n8n.internal_job_candidate_recs (job_id, recommend_at, candidate_id)
                SELECT %s, %s, parse_json(%s)
            """, (job_global_id, recommend_at, f'"{candidate}"'))

            cursor.execute("""
                DELETE FROM intermediate.n8n.internal_job_candidate_recs WHERE recommend_at < DATEADD(DAY, -180, CURRENT_DATE);
            """)

            print("Candidate Added")
    except Exception as e:
        print(f"❌ Failed for job_id: {job_global_id} — {e}")

# Close Snowflake connection
cursor.close()
conn.close()
