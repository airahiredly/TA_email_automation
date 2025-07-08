# File: run_workflow.py

import os
import json
import sys
from process import run_candidate_processing # Import your main function

def main():
    """
    Reads a single JSON payload from an environment variable,
    parses it, and runs the candidate processing script.
    """
    # 1. Get the combined JSON payload and API key from environment variables
    payload_str = os.environ.get("PAYLOAD_INPUT")
    api_key = os.environ.get("API_KEY_SECRET")

    # 2. Validate that the inputs exist
    if not payload_str:
        print("❌ Error: PAYLOAD_INPUT environment variable not set.")
        sys.exit(1) # Exit with a failure code
        
    if not api_key:
        print("❌ Error: GOOGLE_API_KEY secret not set in GitHub repository.")
        sys.exit(1)

    # 3. Parse the JSON string and extract the data
    try:
        data = json.loads(payload_str)
        candidate_data = data.get("candidate_json")
        job_details = data.get("job_details_json")

        if candidate_data is None or job_details is None:
            print("❌ Error: JSON payload must contain 'candidate_json' and 'job_details_json' keys.")
            sys.exit(1)

    except json.JSONDecodeError:
        print("❌ Error: Invalid JSON format in PAYLOAD_INPUT.")
        sys.exit(1)

    # 4. Run the main processing function from process.py
    print("✅ Inputs parsed successfully. Starting the main process...")
    run_candidate_processing(candidate_data, job_details, api_key)


if __name__ == "__main__":
    main()