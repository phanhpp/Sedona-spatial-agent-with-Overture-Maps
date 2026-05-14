"""
Pytest configuration. Load .env and resolve AWS credentials so tests can use S3 when needed.
"""
from pathlib import Path

import pytest


def _load_env():
    try:
        from dotenv import load_dotenv
        root = Path(__file__).resolve().parent.parent
        load_dotenv(root / ".env")
    except ImportError:
        pass


# Load .env (S3_ENRICHED_BASE etc.) then ensure AWS creds are in env when using a profile
# (no-op in CI where AWS_ACCESS_KEY_ID is already set by the workflow)
_load_env()
try:
    from src.cloud_auth import ensure_aws_credentials_from_profile
    ensure_aws_credentials_from_profile()
except Exception:
    pass
