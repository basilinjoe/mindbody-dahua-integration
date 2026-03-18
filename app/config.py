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

    # -- Dahua multi-device seeding (JSON array, optional) --
    # Example: [{"name":"Main Gate","host":"192.168.1.100","password":"pass"},...]
    dahua_devices: str = ""

    # -- Application --
    database_url: str = "postgresql://postgres:postgres@localhost/sync"
    sync_interval_minutes: int = 30
    device_health_interval_minutes: int = 5
    log_level: str = "INFO"

    # -- Admin UI --
    admin_username: str = "admin"
    admin_password: str
    secret_key: str
    session_expire_hours: int = 8
    secure_cookies: bool = True

    model_config = {"env_file": ".env", "env_file_encoding": "utf-8"}
