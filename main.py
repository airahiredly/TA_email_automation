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
WEBHOOK_URL = os.getenv("WEBHOOK_URL", "https://n8n-app-p68zu.ondigitalocean.app/webhook/6f77db62-5349-4076-9577-be546c054dc0")

# Snowflake credentials from environment variables
SNOWFLAKE_USER = os.getenv("SNOWFLAKE_USER", "AIRA")
SNOWFLAKE_PASSWORD = os.getenv("SNOWFLAKE_PASSWORD", "UKs6f9TAUfDR@f3")
SNOWFLAKE_ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT", "A4216615408961-LK73781")

# Load job list from Google Sheet
sheet_url = f"https://sheets.googleapis.com/v4/spreadsheets/{SHEET_ID}/values/{SHEET_NAME}?key={API_KEY}"
response = requests.get(sheet_url)
response.raise_for_status()
sheet_data = response.json()

values = sheet_data.get("values", [])
headers = values[0]
rows = values[1:]

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

recommend_at = datetime.now().strftime('%Y-%m-%d')

# Process each job
for job_global_id in [row[global_id_index] for row in rows if len(row) > global_id_index]:
    try:
        cursor.execute(f"""
            WITH jobs AS (
                SELECT title, id AS job_id, global_id AS job_global_id
                FROM base.postgresql_hiredly_my.jobs 
                WHERE company_id = '44ea6c51-c84d-423f-8649-96cb592ea995'
                  AND is_active = TRUE
                  AND LOWER(title) NOT LIKE '%career fair%'
            ),
            applied_candidates AS (
                SELECT j.job_global_id, ARRAY_AGG(u.global_id) AS user_global_id
                FROM base.postgresql_hiredly_my.job_applications ja
                INNER JOIN jobs j ON j.job_id = ja.job_id
                INNER JOIN base.postgresql_hiredly_my.users u ON u.id = ja.user_id
                GROUP BY j.job_global_id
            ),
            recommended_candidates AS (
                SELECT job_id AS job_global_id, ARRAY_AGG(candidate_id) AS user_global_id 
                FROM intermediate.n8n.internal_job_candidate_recs
                GROUP BY job_global_id
            ),
            hiredly_employees AS (
                SELECT ARRAY_AGG(user_global_id) AS user_global_id
                FROM intermediate.n8n.personal_email_list
            )
            SELECT ARRAY_DISTINCT(
                ARRAY_CAT(
                    ARRAY_CAT(
                        COALESCE(ac.user_global_id, ARRAY_CONSTRUCT()),
                        COALESCE(rc.user_global_id, ARRAY_CONSTRUCT())
                    ),
                    COALESCE(he.user_global_id, ARRAY_CONSTRUCT())
                )
            ) AS user_global_id
            FROM recommended_candidates rc
            LEFT JOIN applied_candidates ac ON ac.job_global_id = rc.job_global_id
            CROSS JOIN hiredly_employees he
            WHERE rc.job_global_id = '{job_global_id}'
        """)

        # Fetch and convert to DataFrame
        new_rows = cursor.fetchall()
        columns = [col[0] for col in cursor.description]
        executed_candidate_list = pandas.DataFrame(new_rows, columns=columns)

        # Skip if empty
        if executed_candidate_list.empty:
            print(f"⚠️ No candidates to exclude for job_id: {job_global_id}")
            continue

        # Extract and clean list
        raw_array = executed_candidate_list.at[0, "USER_GLOBAL_ID"]  # column name might be lowercase depending on Snowflake
        if not raw_array:
            final_list = []
        else:
            raw_string = str(raw_array)
            cleaned_string = raw_string.strip('"[]').replace('\n', '').replace(' ', '')
            items = cleaned_string.split(',')
            final_list = [item.strip(' "\'[]') for item in items if item]

        # Post request to recommend users
        api_response = requests.post(POST_ENDPOINT, json={
            "global_id": job_global_id,
            "exclude_global_ids": final_list,
            "limit": 30,
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

            time.sleep(3)  # Delay to avoid overload

            cursor.execute("""
                INSERT INTO intermediate.n8n.internal_job_candidate_recs (job_id, recommend_at, candidate_id)
                SELECT %s, %s, parse_json(%s)
            """, (job_global_id, recommend_at, f'"{candidate}"'))

            cursor.execute("""
                DELETE FROM intermediate.n8n.internal_job_candidate_recs 
                WHERE recommend_at < DATEADD(DAY, -180, CURRENT_DATE);
            """)

            print("Candidate Added")

    except Exception as e:
        print(f"❌ Failed for job_id: {job_global_id} — {e}")

cursor.close()
conn.close()

