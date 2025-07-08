# File: run_workflow.py
import os
import json
from process import run_candidate_processing # Import the main function

if __name__ == "__main__":
    # Get the JSON strings from environment variables set by the workflow
    candidate_json_str = os.environ.get("CANDIDATE_INPUT")
    job_details_json_str = os.environ.get("JOB_DETAILS_INPUT")
    google_api_key = os.environ.get("API_KEY_SECRET")

    # Check if the inputs are provided
    if not all([candidate_json_str, job_details_json_str, google_api_key]):
        print("❌ Error: Missing one or more required inputs (candidate data, job details, or API key).")
        exit(1)

    try:
        # Parse the JSON strings into Python objects
        candidates = json.loads(candidate_json_str)
        job_details = json.loads(job_details_json_str)
    except json.JSONDecodeError as e:
        print(f"❌ Error decoding JSON input: {e}")
        exit(1)
    
    # Call the main processing function with the parsed data
    run_candidate_processing(candidates, job_details, google_api_key)