"""
Simulate the gateway_verify_submission flow for a lead from leads.json.

Usage:
    python data/simulator_verify_submission.py <lead_id>

Example:
    python data/simulator_verify_submission.py 42
"""

import argparse
import hashlib
import json
import os
import sys
import uuid
from datetime import datetime, timezone

DATA_DIR = os.path.dirname(os.path.abspath(__file__))
GATEWAY_URL = os.getenv("GATEWAY_URL", "http://52.91.135.79:8000")
BUILD_ID = os.getenv("BUILD_ID", "miner-client")


def load_lead_by_id(lead_id: int) -> dict | None:
    """Find a lead by its 'id' field across all leads*.json files."""
    import glob

    for fp in sorted(glob.glob(os.path.join(DATA_DIR, "leads*.json"))):
        with open(fp, "r", encoding="utf-8") as f:
            leads = json.load(f)
        for lead in leads:
            if lead.get("id") == lead_id:
                return lead
    return None


def simulate_presign_event(hotkey: str, lead_data: dict) -> dict:
    """Build the SUBMISSION_REQUEST event (mirrors gateway_get_presigned_url)."""
    lead_id = "07391bac-5a05-447e-b08e-972e67fe1d2f"
    lead_blob = json.dumps(lead_data, sort_keys=True, default=str)
    lead_blob_hash = hashlib.sha256(lead_blob.encode()).hexdigest()
    email = lead_data.get("email", "").strip().lower()
    email_hash = hashlib.sha256(email.encode()).hexdigest()

    payload = {
        "lead_id": lead_id,
        "lead_blob_hash": lead_blob_hash,
        "email_hash": email_hash,
    }
    payload_json = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    payload_hash = hashlib.sha256(payload_json.encode()).hexdigest()

    nonce = str(uuid.uuid4())
    ts = datetime.now(timezone.utc).isoformat()
    message = f"SUBMISSION_REQUEST:{hotkey}:{nonce}:{ts}:{payload_hash}:{BUILD_ID}"

    return {
        "event_type": "SUBMISSION_REQUEST",
        "actor_hotkey": hotkey,
        "nonce": nonce,
        "ts": ts,
        "payload_hash": payload_hash,
        "build_id": BUILD_ID,
        "signature": hashlib.sha256(message.encode()).hexdigest(),
        "payload": payload,
        "_meta": {
            "lead_blob_hash": lead_blob_hash,
            "email_hash": email_hash,
            "message": message,
        },
    }


def simulate_verify_event(hotkey: str, lead_id: str) -> dict:
    """Build the SUBMIT_LEAD event (mirrors gateway_verify_submission)."""
    payload = {"lead_id": lead_id}
    payload_json = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    payload_hash = hashlib.sha256(payload_json.encode()).hexdigest()

    nonce = str(uuid.uuid4())
    ts = datetime.now(timezone.utc).isoformat()
    message = f"SUBMIT_LEAD:{hotkey}:{nonce}:{ts}:{payload_hash}:{BUILD_ID}"

    return {
        "event_type": "SUBMIT_LEAD",
        "actor_hotkey": hotkey,
        "nonce": nonce,
        "ts": ts,
        "payload_hash": payload_hash,
        "build_id": BUILD_ID,
        "signature": hashlib.sha256(message.encode()).hexdigest(),
        "payload": payload,
        "_meta": {
            "message": message,
        },
    }


def simulate_expected_response(lead_id: str) -> dict:
    """Build what the gateway would return on success."""
    return {
        "lead_id": lead_id,
        "status": "verified",
        "storage_backends": ["s3", "minio"],
        "submission_timestamp": datetime.now(timezone.utc).isoformat(),
        "merkle_proof": hashlib.sha256(lead_id.encode()).hexdigest(),
    }


def main():
    parser = argparse.ArgumentParser(
        description="Simulate gateway_verify_submission for a lead from leads.json"
    )
    parser.add_argument("lead_id", type=int, help="The 'id' of the lead in leads.json")
    parser.add_argument(
        "--hotkey",
        default="5GTorxG9Qobt4kLBchHkB3VWQNgSuLDjkQCbrBkbgiDAZ4UC",
        help="Simulated hotkey SS58 address (default: placeholder)",
    )
    args = parser.parse_args()

    lead = load_lead_by_id(args.lead_id)

    print(f"{'=' * 70}")
    print(f"Lead #{lead['id']}: {lead.get('business', 'Unknown')}")
    print(f"  Contact : {lead.get('full_name', '')} ({lead.get('email', '')})")
    print(f"  Industry: {lead.get('industry', '')}")
    print(f"{'=' * 70}")

    # Step 1 - presign event
    presign_event = simulate_presign_event(args.hotkey, lead)
    generated_lead_id = presign_event["payload"]["lead_id"]

    print("\n--- Step 1: SUBMISSION_REQUEST event (presign) ---")
    print(json.dumps(presign_event, indent=2))

    # Step 2 - verify event
    verify_event = simulate_verify_event(args.hotkey, generated_lead_id)

    print("\n--- Step 2: SUBMIT_LEAD event (verify) ---")
    print(json.dumps(verify_event, indent=2))

    # Step 3 - expected response
    expected_response = simulate_expected_response(generated_lead_id)

    print("\n--- Step 3: Expected gateway response ---")
    print(json.dumps(expected_response, indent=2))

    # Save all to a file for inspection
    output = {
        "lead": lead,
        "presign_event": presign_event,
        "verify_event": verify_event,
        "expected_response": expected_response,
    }
    out_path = os.path.join(DATA_DIR, "simulated_submission.json")
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(output, f, indent=2, ensure_ascii=False)
    print(f"\nAll data saved to {out_path}")


if __name__ == "__main__":
    main()
