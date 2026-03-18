#!/usr/bin/env python3
"""
Standalone script to fetch and display user details from Mindbody.

Usage:
    python scripts/get_mindbody_users.py
    python scripts/get_mindbody_users.py --search "John"
    python scripts/get_mindbody_users.py --client-id 12345
    python scripts/get_mindbody_users.py --all --output users.json
"""

from __future__ import annotations

import argparse
import asyncio
import json
import sys
from pathlib import Path

# Allow running from project root without installing the package
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import httpx

from app.config import Settings


async def get_token(client: httpx.AsyncClient, settings: Settings) -> str:
    base = settings.mindbody_api_base_url.rstrip("/")
    resp = await client.post(
        f"{base}/usertoken/issue",
        headers={"Api-Key": settings.mindbody_api_key, "SiteId": settings.mindbody_site_id},
        json={"Username": settings.mindbody_username, "Password": settings.mindbody_password},
    )
    resp.raise_for_status()
    return resp.json()["AccessToken"]


def make_headers(settings: Settings, token: str) -> dict[str, str]:
    return {
        "Api-Key": settings.mindbody_api_key,
        "SiteId": settings.mindbody_site_id,
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }


async def fetch_clients(
    client: httpx.AsyncClient,
    settings: Settings,
    token: str,
    *,
    search_text: str = "",
    limit: int = 200,
    offset: int = 0,
) -> list[dict]:
    base = settings.mindbody_api_base_url.rstrip("/")
    params: dict = {"request.limit": limit, "request.offset": offset}
    if search_text:
        params["request.searchText"] = search_text
    resp = await client.get(
        f"{base}/client/clients",
        headers=make_headers(settings, token),
        params=params,
    )
    resp.raise_for_status()
    return resp.json().get("Clients", [])


async def fetch_all_clients(
    client: httpx.AsyncClient,
    settings: Settings,
    token: str,
) -> list[dict]:
    all_clients: list[dict] = []
    offset = 0
    page_size = 200
    while True:
        page = await fetch_clients(client, settings, token, limit=page_size, offset=offset)
        if not page:
            break
        all_clients.extend(page)
        print(f"  Fetched {len(all_clients)} clients so far...", end="\r")
        if len(page) < page_size:
            break
        offset += page_size
    print()
    return all_clients


async def fetch_memberships(
    client: httpx.AsyncClient,
    settings: Settings,
    token: str,
    client_id: str,
) -> list[dict]:
    base = settings.mindbody_api_base_url.rstrip("/")
    resp = await client.get(
        f"{base}/client/activeclientmemberships",
        headers=make_headers(settings, token),
        params={"request.clientId": client_id},
    )
    resp.raise_for_status()
    return resp.json().get("ClientMemberships", [])


def format_client(c: dict) -> str:
    lines = [
        f"ID          : {c.get('Id', 'N/A')}",
        f"Unique ID   : {c.get('UniqueId', 'N/A')}",
        f"Name        : {c.get('FirstName', '')} {c.get('LastName', '')}".strip(),
        f"Email       : {c.get('Email', 'N/A')}",
        f"Phone       : {c.get('MobilePhone') or c.get('HomePhone') or c.get('WorkPhone') or 'N/A'}",
        f"Status      : {c.get('Status', 'N/A')}",
        f"Active      : {c.get('Active', 'N/A')}",
        f"Birth date  : {c.get('BirthDate', 'N/A')}",
        f"Gender      : {c.get('Gender', 'N/A')}",
        f"Created     : {c.get('CreationDate', 'N/A')}",
        f"Last visit  : {c.get('LastFormulaNotes', c.get('LastModifiedDateTime', 'N/A'))}",
    ]
    return "\n".join(lines)


async def main(args: argparse.Namespace) -> None:
    settings = Settings()

    if not settings.mindbody_api_key:
        print("ERROR: MINDBODY_API_KEY is not set. Check your .env file.", file=sys.stderr)
        sys.exit(1)

    async with httpx.AsyncClient(timeout=30) as http:
        print("Authenticating with Mindbody...")
        try:
            token = await get_token(http, settings)
        except httpx.HTTPStatusError as e:
            print(
                f"ERROR: Authentication failed: {e.response.status_code} {e.response.text}",
                file=sys.stderr,
            )
            sys.exit(1)

        print("Authentication successful.\n")

        # --- Fetch by specific client ID ---
        if args.client_id:
            clients = await fetch_clients(
                http, settings, token, search_text=args.client_id, limit=1
            )
            if not clients:
                print(f"No client found with ID: {args.client_id}")
                sys.exit(0)
            c = clients[0]
            print("=" * 50)
            print(format_client(c))
            if args.memberships:
                memberships = await fetch_memberships(http, settings, token, c.get("Id", ""))
                print(f"Memberships ({len(memberships)}):")
                for m in memberships:
                    print(
                        f"  - {m.get('Name', 'N/A')} | expires: {m.get('ExpirationDate', 'ongoing')}"
                    )
            print("=" * 50)
            if args.output:
                Path(args.output).write_text(json.dumps(c, indent=2))
                print(f"\nSaved to {args.output}")
            return

        # --- Search by text ---
        if args.search:
            print(f"Searching for: {args.search!r}")
            clients = await fetch_clients(http, settings, token, search_text=args.search)
        elif args.all:
            print("Fetching all clients (this may take a while)...")
            clients = await fetch_all_clients(http, settings, token)
        else:
            print(
                "Fetching first page of clients (use --all for all, --search TEXT to filter)...\n"
            )
            clients = await fetch_clients(http, settings, token, limit=args.limit)

        print(f"Found {len(clients)} client(s).\n")

        if args.output:
            Path(args.output).write_text(json.dumps(clients, indent=2))
            print(f"Saved to {args.output}")
        else:
            for i, c in enumerate(clients, 1):
                print(f"[{i}] " + "-" * 46)
                print(format_client(c))
                print()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Fetch user details from Mindbody")
    group = parser.add_mutually_exclusive_group()
    group.add_argument("--search", metavar="TEXT", help="Search clients by name/email/phone")
    group.add_argument(
        "--client-id", metavar="ID", help="Fetch a specific client by their Mindbody ID"
    )
    group.add_argument("--all", action="store_true", help="Fetch all clients (auto-paginates)")
    parser.add_argument(
        "--limit", type=int, default=20, help="Number of clients to fetch (default: 20)"
    )
    parser.add_argument(
        "--memberships", action="store_true", help="Also fetch active memberships for each client"
    )
    parser.add_argument("--output", metavar="FILE", help="Save results as JSON to this file")
    return parser.parse_args()


if __name__ == "__main__":
    asyncio.run(main(parse_args()))
