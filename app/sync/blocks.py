from __future__ import annotations

from prefect.blocks.core import Block
from pydantic import SecretStr


class MindBodyCredentials(Block):
    """
    Prefect Block for MindBody API credentials.

    Stored encrypted at rest in the Prefect server database.
    Create instance named "production" via Prefect UI → Blocks → New.

    Usage in tasks:
        creds = await MindBodyCredentials.load("production")
        client = MindBodyClient(
            api_key=creds.api_key.get_secret_value(),
            site_id=creds.site_id,
            username=creds.username,
            password=creds.password.get_secret_value(),
        )
    """

    _block_type_name = "MindBody Credentials"
    _block_type_slug = "mindbody-credentials"
    _logo_url = "https://www.mindbodyonline.com/favicon.ico"  # type: ignore[assignment]

    api_key: SecretStr
    site_id: str
    username: str
    password: SecretStr
    base_url: str = "https://api.mindbodyonline.com/public/v6"


if __name__ == "__main__":
    # Run: python -m app.sync.blocks
    # Registers block type with connected Prefect server
    MindBodyCredentials.register_type_and_schema()
    print("MindBodyCredentials block type registered.")
