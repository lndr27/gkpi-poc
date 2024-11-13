from functools import lru_cache

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=[".env", ".env.local"],
        extra="ignore",
        env_file_encoding="utf-8",
    )

    cognite_base_uri: str
    cognite_app_id: str
    cognite_project: str

    auth_token_uri: str
    auth_client_id: str
    auth_secret: str
    auth_token_override: str | None = None
    auth_scopes_str: str = Field(alias="auth_scopes")

    @property
    def auth_scopes(self):
        return self.auth_scopes_str.split(",")


@lru_cache
def get_settings():
    return Settings()  # type: ignore
