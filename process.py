# File: process.py
import os
import requests
from typing import List, Dict, Any

def run_candidate_processing(candidate_data: List[Dict[str, Any]], job_details: Dict[str, Any], api_key: str):
    # --- 1. CONFIGURATION ---
    SPREADSHEET_ID = "129BnqQCSd8cQqxCDLsc6benKxKDkcHqmndWK3Tb0vhM"
    SEARCH_RANGE = "Candidate!A:B"
    WEBHOOK_URL = "https://n8n-app-p68zu.ondigitalocean.app/webhook-test/6f77db62-5349-4076-9577-be546c054dc0"

    # --- Helper Functions (Nested) ---
    def email_exists_in_google_sheet(email: str) -> bool:
        if not email: return False
        url = f"https://sheets.googleapis.com/v4/spreadsheets/{SPREADSHEET_ID}/values/{SEARCH_RANGE}?key={api_key}"
        try:
            response = requests.get(url, timeout=15)
            response.raise_for_status()
            data = response.json()
            sheet_values = data.get('values', [])
            for row in sheet_values:
                if len(row) > 1 and row[1].strip().lower() == email.strip().lower():
                    return True
            return False
        except Exception as e:
            print(f"❌ Error during Google Sheet check: {e}")
            return True

    def send_to_webhook(payload: Dict[str, Any]):
        try:
            headers = {"Content-Type": "application/json"}
            response = requests.post(WEBHOOK_URL, json=payload, headers=headers, timeout=15)
            response.raise_for_status()
            print(f"✅ Successfully sent data for {payload.get('EMAIL')} to webhook.")
        except requests.exceptions.RequestException as e:
            print(f"❌ Error sending data to webhook for {payload.get('EMAIL')}: {e}")

    # --- Main Logic ---
    if not candidate_data:
        print("No candidates to process.")
        return

    processed_emails = set()
    print(f"🚀 Starting to process {len(candidate_data)} candidate record(s)...")
    
    for candidate in candidate_data:
        email = candidate.get("EMAIL")
        if not email:
            print(f"⚠️ Skipping a record because it has no EMAIL: {candidate.get('NAME')}")
            continue
        
        normalized_email = email.strip().lower()
        print(f"\nProcessing candidate: {candidate.get('NAME')} ({email})")

        if normalized_email in processed_emails:
            print(f"-> ⏭️  Skipping: Email {email} was already processed in this run.")
            continue

        if email_exists_in_google_sheet(email):
            print(f"-> ⏭️  Skipping: Email {email} already exists in Google Sheet.")
        else:
            print(f"-> ✨ New Candidate: Preparing to send data for {email} to webhook.")
            webhook_payload = candidate.copy()
            webhook_payload["jobDetails"] = job_details
            send_to_webhook(webhook_payload)
        
        processed_emails.add(normalized_email)
            
    print("\nProcessing complete.")