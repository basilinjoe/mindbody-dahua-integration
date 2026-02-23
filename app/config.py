from __future__ import annotations

from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    # -- MindBody API --
    mindbody_api_key: str = ""
    mindbody_site_id: str = ""
    mindbody_api_base_url: str = "https://api.mindbodyonline.com/public/v6"
    mindbody_username: str = ""
    mindbody_password: str = ""
    mindbody_webhook_signature_key: str = ""

    # -- Dahua (default device, additional devices managed via admin UI) --
    dahua_default_host: str = ""
    dahua_default_port: int = 80
    dahua_default_username: str = "admin"
    dahua_default_password: str = ""
    dahua_default_door_ids: str = "0"

    # -- Application --
    database_url: str = "sqlite:///./data/sync.db"
    sync_interval_minutes: int = 30
    device_health_interval_minutes: int = 5
    log_level: str = "INFO"
    photo_max_size_kb: int = 190

    # -- Admin UI --
    admin_username: str = "admin"
    admin_password: str = "changeme"
    secret_key: str = "change-this-to-a-random-secret-key"
    session_expire_hours: int = 8

    model_config = {"env_file": ".env", "env_file_encoding": "utf-8"}
