#!/usr/bin/env python3
"""
Extract AWS snapshots older than N days and write their names and creation times to a text file.

Supports:
- EBS snapshots (EC2)
- RDS DB snapshots (instance) and RDS DB cluster snapshots (Aurora)

Authentication:
- Works with AWS SSO profiles configured via awscli.

Output format:
- Each line: "<name>\t<created>" (tab-separated), no header.

Usage examples:
  python3 extract_snapshots.py --profile my-sso --all-regions --service ebs --days 30 --output snapshots.txt
  python3 extract_snapshots.py --profile my-sso --region us-east-1 --service both --days 45 --output out.txt
"""

from __future__ import annotations

import argparse
from datetime import datetime, timedelta, timezone
from typing import Dict, Iterable, List, Optional

import boto3
from botocore.config import Config
from botocore.exceptions import BotoCoreError, ClientError


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Extract snapshot names and creation times older than N days.")
    parser.add_argument(
        "--profile",
        help="AWS profile name (SSO or static). If omitted, uses default resolution (env vars or default profile).",
    )
    parser.add_argument(
        "--region",
        help="AWS region to query. If omitted and --all-regions is not set, uses the profile's default region.",
    )
    parser.add_argument(
        "--all-regions",
        action="store_true",
        help="Query all opted-in regions (for both EC2 and RDS).",
    )
    parser.add_argument(
        "--service",
        choices=["ebs", "rds", "both"],
        default="ebs",
        help="Which service snapshots to include.",
    )
    parser.add_argument(
        "--days",
        type=int,
        default=30,
        help="Age threshold in days. Snapshots created at or before now - days are included.",
    )
    parser.add_argument(
        "--output",
        default="snapshots.txt",
        help="Output text file path.",
    )
    return parser.parse_args()


def build_session(profile_name: Optional[str]) -> boto3.session.Session:
    if profile_name:
        return boto3.session.Session(profile_name=profile_name)
    return boto3.session.Session()


def list_opted_in_regions(session: boto3.session.Session) -> List[str]:
    client = session.client("ec2", config=Config(retries={"max_attempts": 10, "mode": "standard"}))
    regions = []
    try:
        resp = client.describe_regions(AllRegions=False)
        for r in resp.get("Regions", []):
            name = r.get("RegionName")
            status = r.get("OptInStatus")
            if name and status in (None, "opt-in-not-required", "opted-in"):
                regions.append(name)
    except (BotoCoreError, ClientError) as exc:
        raise RuntimeError(f"Failed to list regions: {exc}") from exc
    return sorted(set(regions))


def get_ebs_snapshots(
    session: boto3.session.Session,
    region: str,
    cutoff_dt: datetime,
) -> Iterable[Dict[str, str]]:
    client = session.client("ec2", region_name=region, config=Config(retries={"max_attempts": 10, "mode": "standard"}))

    paginator = client.get_paginator("describe_snapshots")
    # Limit to snapshots owned by the caller.
    page_iterator = paginator.paginate(OwnerIds=["self"])  # type: ignore[call-arg]

    for page in page_iterator:
        for snap in page.get("Snapshots", []):
            start_time: datetime = snap.get("StartTime")
            if not isinstance(start_time, datetime):
                continue
            # Include snapshots created at or before the cutoff
            if start_time <= cutoff_dt:
                tags = snap.get("Tags", []) or []
                name_tag = next((t.get("Value") for t in tags if t.get("Key") == "Name"), None)
                description = snap.get("Description")
                snapshot_id = snap.get("SnapshotId")
                # Determine a human-friendly name
                display_name = name_tag or description or snapshot_id or "<unnamed>"
                yield {
                    "name": str(display_name),
                    "created": start_time.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
                }


def get_rds_instance_snapshots(
    session: boto3.session.Session,
    region: str,
    cutoff_dt: datetime,
) -> Iterable[Dict[str, str]]:
    client = session.client("rds", region_name=region, config=Config(retries={"max_attempts": 10, "mode": "standard"}))
    paginator = client.get_paginator("describe_db_snapshots")
    page_iterator = paginator.paginate()

    for page in page_iterator:
        for snap in page.get("DBSnapshots", []):
            created: datetime = snap.get("SnapshotCreateTime")
            if not isinstance(created, datetime):
                continue
            if created <= cutoff_dt:
                name = snap.get("DBSnapshotIdentifier") or "<unnamed>"
                yield {
                    "name": str(name),
                    "created": created.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
                }


def get_rds_cluster_snapshots(
    session: boto3.session.Session,
    region: str,
    cutoff_dt: datetime,
) -> Iterable[Dict[str, str]]:
    client = session.client("rds", region_name=region, config=Config(retries={"max_attempts": 10, "mode": "standard"}))
    paginator = client.get_paginator("describe_db_cluster_snapshots")
    page_iterator = paginator.paginate()

    for page in page_iterator:
        for snap in page.get("DBClusterSnapshots", []):
            created: datetime = snap.get("SnapshotCreateTime")
            if not isinstance(created, datetime):
                continue
            if created <= cutoff_dt:
                name = snap.get("DBClusterSnapshotIdentifier") or "<unnamed>"
                yield {
                    "name": str(name),
                    "created": created.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
                }


def main() -> None:
    args = parse_args()

    try:
        session = build_session(args.profile)
    except Exception as exc:  # noqa: BLE001
        raise SystemExit(f"Failed to create AWS session: {exc}")

    now_utc = datetime.now(timezone.utc)
    cutoff_dt = now_utc - timedelta(days=int(args.days))

    if args.all_regions:
        try:
            regions = list_opted_in_regions(session)
        except Exception as exc:  # noqa: BLE001
            raise SystemExit(str(exc))
    else:
        # Use provided region or session's default region
        region = args.region or session.region_name
        if not region:
            raise SystemExit("No region specified and no default region configured in the profile. Use --region or --all-regions.")
        regions = [region]

    rows: List[Dict[str, str]] = []

    include_ebs = args.service in ("ebs", "both")
    include_rds = args.service in ("rds", "both")

    for region in regions:
        if include_ebs:
            try:
                rows.extend(get_ebs_snapshots(session, region, cutoff_dt))
            except (BotoCoreError, ClientError) as exc:
                print(f"Warning: Failed to list EBS snapshots in {region}: {exc}")

        if include_rds:
            try:
                rows.extend(get_rds_instance_snapshots(session, region, cutoff_dt))
            except (BotoCoreError, ClientError) as exc:
                print(f"Warning: Failed to list RDS DB snapshots in {region}: {exc}")
            try:
                rows.extend(get_rds_cluster_snapshots(session, region, cutoff_dt))
            except (BotoCoreError, ClientError) as exc:
                print(f"Warning: Failed to list RDS cluster snapshots in {region}: {exc}")

    # Sort by created time ascending then name
    rows.sort(key=lambda r: (r["created"], r["name"]))

    try:
        with open(args.output, "w", encoding="utf-8") as f:
            for r in rows:
                f.write(f"{r['name']}\t{r['created']}\n")
    except OSError as exc:
        raise SystemExit(f"Failed to write output file {args.output}: {exc}")

    print(f"Wrote {len(rows)} records to {args.output}")


if __name__ == "__main__":
    main()


