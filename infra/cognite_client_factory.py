from functools import lru_cache

from cognite.client import ClientConfig, CogniteClient
from cognite.client.credentials import CredentialProvider, OAuthClientCredentials, Token

from .settings import Settings, get_settings


class CogniteClientFactory:
    @staticmethod
    def _create_credentials(settings: Settings) -> CredentialProvider:
        if settings.auth_token_override:
            return Token(settings.auth_token_override)

        return OAuthClientCredentials(
            token_url=settings.auth_token_uri,
            client_id=settings.auth_client_id,
            client_secret=settings.auth_secret,
            scopes=settings.auth_scopes,
        )

    @staticmethod
    def _create_client_config(settings: Settings) -> ClientConfig:
        return ClientConfig(
            client_name=settings.cognite_app_id,
            project=settings.cognite_project,
            credentials=CogniteClientFactory._create_credentials(settings),
            base_url=settings.cognite_base_uri,
        )

    @staticmethod
    def create(
        settings: Settings,
    ) -> CogniteClient:
        return CogniteClient(
            config=CogniteClientFactory._create_client_config(settings)
        )


@lru_cache
def create_cognite_client():
    return CogniteClientFactory.create(get_settings())
