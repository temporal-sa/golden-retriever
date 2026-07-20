"""Worker adapter factories for Google Drive plus Lakebase."""

from __future__ import annotations

from .client import GoogleDriveApiClient
from .config import GoogleDriveConfig
from .provider import GoogleDriveProviderGateway
from .staging import GoogleDriveStagingStore


async def create_staging_store() -> GoogleDriveStagingStore:
    config = GoogleDriveConfig.from_env()
    store = GoogleDriveStagingStore(config.staging_directory)
    await store.prepare()
    return store


async def create_provider_gateway() -> GoogleDriveProviderGateway:
    config = GoogleDriveConfig.from_env()
    staging = GoogleDriveStagingStore(config.staging_directory)
    await staging.prepare()
    return GoogleDriveProviderGateway(
        config,
        GoogleDriveApiClient.from_config(config),
        staging,
    )


async def create_adapter_bundle():
    """Create one production worker bundle with Lakebase and Google Drive."""

    from retrieval.lakebase.config import LakebaseConfig
    from retrieval.lakebase.connection import LakebaseConnectionProvider
    from retrieval.lakebase.repository import LakebaseRetrievalRepository
    from retrieval.temporal.worker import AdapterBundle

    config = GoogleDriveConfig.from_env()
    lakebase_config = LakebaseConfig.from_env(default_pool_max_size=20)
    connection_provider = LakebaseConnectionProvider(lakebase_config)
    drive_api = None
    try:
        await connection_provider.open()
        await connection_provider.wait()
        staging = GoogleDriveStagingStore(config.staging_directory)
        await staging.prepare()
        drive_api = GoogleDriveApiClient.from_config(config)
        gateway = GoogleDriveProviderGateway(config, drive_api, staging)
        return AdapterBundle(
            repository=LakebaseRetrievalRepository(
                connection_provider,
                transaction_retry_limit=lakebase_config.transaction_retry_limit,
                owns_provider=True,
            ),
            staging_store=staging,
            provider_gateway=gateway,
        )
    except BaseException:
        if drive_api is not None:
            await drive_api.aclose()
        await connection_provider.aclose()
        raise


__all__ = [
    "create_adapter_bundle",
    "create_provider_gateway",
    "create_staging_store",
]
