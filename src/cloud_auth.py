"""
Resolve AWS credentials into process env so Spark (JVM) can use them.

When running locally with an AWS profile (e.g. SSO), the profile is not visible
to Spark's Hadoop S3A client. Running `aws configure export-credentials` and
injecting the result into os.environ makes both boto3 and Spark use the same
credentials. No-op when AWS_ACCESS_KEY_ID is already set (e.g. in CI).
"""

import os
import subprocess


def ensure_aws_credentials_from_profile(default_profile: str = "default") -> None:
    """
    If credentials are not in the environment, run `aws configure export-credentials`
    for the current (or default) profile and set AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY,
    and AWS_SESSION_TOKEN in os.environ. Spark and boto3 will then pick them up.

    Skips when AWS_ACCESS_KEY_ID is already set (e.g. CI injects from secrets).
    Set the AWS_PROFILE env var to override the profile used.
    """
    if os.environ.get("AWS_ACCESS_KEY_ID"):
        return
    profile = os.environ.get("AWS_PROFILE", default_profile)
    try:
        out = subprocess.run(
            ["aws", "configure", "export-credentials", "--profile", profile, "--format", "env"],
            capture_output=True,
            text=True,
            timeout=15,
        )
        if out.returncode != 0:
            _export_profile_via_boto3(profile)
            return
        for line in out.stdout.strip().splitlines():
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            if "=" in line:
                key, _, value = line.partition("=")
                key = key.strip()
                value = value.strip().strip('"').strip("'")
                if key in ("AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_SESSION_TOKEN"):
                    os.environ[key] = value
    except (FileNotFoundError, subprocess.TimeoutExpired, Exception):
        _export_profile_via_boto3(profile)


def _export_profile_via_boto3(profile: str) -> None:
    """Fallback: resolve profile via boto3 and set env vars (e.g. when AWS CLI not installed)."""
    if os.environ.get("AWS_ACCESS_KEY_ID"):
        return
    try:
        import boto3
        session = boto3.Session(profile_name=profile)
        creds = session.get_credentials()
        if not creds:
            return
        frozen = creds.get_frozen_credentials()
        os.environ["AWS_ACCESS_KEY_ID"] = frozen.access_key
        os.environ["AWS_SECRET_ACCESS_KEY"] = frozen.secret_key
        if frozen.token:
            os.environ["AWS_SESSION_TOKEN"] = frozen.token
    except Exception:
        pass
