#!/usr/bin/env python3
"""
List all EBS snapshots (owned by the caller) in a specific region and print
their name and creation time.

Defaults to region "sa-east-1".

Output: one line per snapshot, tab-separated: "<name>\t<created-utc-ISO8601>"

Examples:
  python3 list_ebs_snapshots.py --profile my-sso --region sa-east-1
  python3 list_ebs_snapshots.py --profile my-sso --region sa-east-1 --output snapshots.txt
"""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
from typing import List

import boto3
from botocore.config import Config
from botocore.exceptions import BotoCoreError, ClientError


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="List EBS snapshots and their creation time.")
    parser.add_argument("--profile", help="AWS profile name (SSO or static). Optional.")
    parser.add_argument(
        "--region",
        default="sa-east-1",
        help="AWS region to query (default: sa-east-1).",
    )
    parser.add_argument(
        "--output",
        help="Optional path to write results. If omitted, only prints to stdout.",
    )
    return parser.parse_args()


def build_session(profile_name: str | None) -> boto3.session.Session:
    if profile_name:
        return boto3.session.Session(profile_name=profile_name)
    return boto3.session.Session()


def main() -> None:
    args = parse_args()

    try:
        session = build_session(args.profile)
    except Exception as exc:  # noqa: BLE001
        raise SystemExit(f"Failed to create AWS session: {exc}")

    client = session.client(
        "ec2",
        region_name=args.region,
        config=Config(retries={"max_attempts": 10, "mode": "standard"}),
    )

    rows: List[tuple[str, str]] = []
    try:
        paginator = client.get_paginator("describe_snapshots")
        for page in paginator.paginate(OwnerIds=["self"]):  # type: ignore[call-arg]
            for snap in page.get("Snapshots", []):
                start_time: datetime | None = snap.get("StartTime")
                if not isinstance(start_time, datetime):
                    continue
                tags = snap.get("Tags", []) or []
                name_tag = next((t.get("Value") for t in tags if t.get("Key") == "Name"), None)
                description = snap.get("Description")
                snapshot_id = snap.get("SnapshotId")
                display_name = name_tag or description or snapshot_id or "<unnamed>"
                created_utc = start_time.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
                rows.append((str(display_name), created_utc))
    except (BotoCoreError, ClientError) as exc:
        raise SystemExit(f"Failed to list EBS snapshots in {args.region}: {exc}")

    # Sort oldest first
    rows.sort(key=lambda r: (r[1], r[0]))

    for name, created in rows:
        print(f"{name}\t{created}")

    if args.output:
        try:
            with open(args.output, "w", encoding="utf-8") as f:
                for name, created in rows:
                    f.write(f"{name}\t{created}\n")
            print(f"Wrote {len(rows)} records to {args.output}")
        except OSError as exc:
            raise SystemExit(f"Failed to write output file {args.output}: {exc}")


if __name__ == "__main__":
    main()


