#!/usr/bin/env python3
"""
Create a new merchant API key.

Usage:
    docker-compose exec api python scripts/create_api_key.py \
        --merchant-id merchant_001 \
        --name "Production Key"

The script prints the raw key ONCE — store it securely.
Only the SHA-256 hash is stored in the database.
"""
import asyncio
import hashlib
import secrets
import argparse
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))


async def create_key(merchant_id: str, name: str) -> None:
    from app.database import get_db_context
    from app.models.transaction import APIKey

    raw_key = secrets.token_urlsafe(32)
    key_hash = hashlib.sha256(raw_key.encode()).hexdigest()

    async with get_db_context() as db:
        key = APIKey(
            merchant_id=merchant_id,
            key_hash=key_hash,
            name=name,
            is_active=True,
        )
        db.add(key)

    print(f"\n✓ API key created for merchant: {merchant_id}")
    print(f"  Name:      {name}")
    print(f"  Key hash:  {key_hash[:16]}... (stored in DB)")
    print(f"\n  RAW KEY (copy now — shown only once):")
    print(f"  {raw_key}\n")
    print(f"  Use as: X-API-Key: {raw_key}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Create merchant API key")
    parser.add_argument("--merchant-id", required=True)
    parser.add_argument("--name", default="API Key")
    args = parser.parse_args()
    asyncio.run(create_key(args.merchant_id, args.name))
