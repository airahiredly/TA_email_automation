import requests
import os
import time

# === CONFIGURATION ===
API_KEY = os.getenv("GSHEETS_API_KEY")
SHEET1_ID = "129BnqQCSd8cQqxCDLsc6benKxKDkcHqmndWK3Tb0vhM"
SHEET2_ID = "1K8xrRJgJSnd8-6jiRWNXs4FmI50Pf-oB_nEGybL7olU"
SHEET1_RANGE = "Candidate!A:C"
SHEET2_RANGE = "Candidate!A:C"
DATA_WEBHOOK_URL = "https://n8n-app-p68zu.ondigitalocean.app/webhook/6f77db62-5349-4076-9577-be546c054dc0"
FINAL_TRIGGER_WEBHOOK = "https://n8n-app-p68zu.ondigitalocean.app/webhook/3111bd21-a846-4bc2-ac45-c2e76e1d0a2a"  # <--- Triggered after all rows are processed

def get_sheet_data(sheet_id, range_):
    url = f"https://sheets.googleapis.com/v4/spreadsheets/{sheet_id}/values/{range_}?key={API_KEY}"
    response = requests.get(url)
    if response.status_code == 200:
        return response.json().get("values", [])
    else:
        print(f"Error fetching sheet {sheet_id}: {response.status_code}, {response.text}")
        return []

def row_key_two_columns(row):
    try:
        return (row[0].strip().lower(), row[1].strip().lower())
    except IndexError:
        return ("", "")

def send_to_webhook(row):
    try:
        response = requests.post(DATA_WEBHOOK_URL, json={"row": row})
        response.raise_for_status()
        print(f"✅ Sent row to webhook: {row}")
    except Exception as e:
        print(f"❌ Failed to send row: {e}")

def trigger_final_webhook():
    try:
        response = requests.post(FINAL_TRIGGER_WEBHOOK, json={"status": "completed"})
        response.raise_for_status()
        print("✅ Final webhook triggered successfully.")
    except Exception as e:
        print(f"❌ Failed to trigger final webhook: {e}")

def main():
    print("Fetching Sheet 1...")
    sheet1_data = get_sheet_data(SHEET1_ID, SHEET1_RANGE)

    print("Fetching Sheet 2...")
    sheet2_data = get_sheet_data(SHEET2_ID, SHEET2_RANGE)

    data1 = sheet1_data[1:] if sheet1_data else []  # Skip header
    data2 = sheet2_data[1:] if sheet2_data else []

    # Build set of (colA, colB) from Sheet2
    existing_keys = set(row_key_two_columns(row) for row in data2)

    for row in data1:
        key = row_key_two_columns(row)
        if key != ("", "") and key not in existing_keys:
            send_to_webhook(row)
            time.sleep(5)
        else:
            print(f"⏩ Skipping row (already exists or invalid): {row}")

    trigger_final_webhook()

if __name__ == "__main__":
    main()
