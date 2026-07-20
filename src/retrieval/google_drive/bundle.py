"""Worker adapter factories for Google Drive plus Lakebase."""

from __future__ import annotations

from .client import GoogleDriveApiClient
from .config import GoogleDriveConfig
from .provider import GoogleDriveProviderGateway
from .staging import GoogleDriveStagingStore


async def create_staging_store() -> GoogleDriveStagingStore:
    config = GoogleDriveConfig.from_env()
    if config.staging_directory is None:
        raise ValueError("standalone filesystem staging requires GOOGLE_DRIVE_STAGING_DIRECTORY")
    store = GoogleDriveStagingStore(config.staging_directory)
    await store.prepare()
    return store


async def create_provider_gateway() -> GoogleDriveProviderGateway:
    config = GoogleDriveConfig.from_env()
    if config.staging_directory is None:
        raise ValueError("standalone filesystem staging requires GOOGLE_DRIVE_STAGING_DIRECTORY")
    staging = GoogleDriveStagingStore(config.staging_directory)
    await staging.prepare()
    return GoogleDriveProviderGateway(
        config,
        GoogleDriveApiClient.from_config(config),
        staging,
    )


async def create_adapter_bundle():
    """Create one production worker bundle with Lakebase and Google Drive."""

    from retrieval.demo.config import DemoConfig
    from retrieval.demo.events import DemoIngestionEventSink
    from retrieval.demo.google_drive_provider import DemoGoogleDriveProvider
    from retrieval.demo.ingestion_gate import DemoBeforeDocumentCommitHook
    from retrieval.demo.store import PostgresDemoStateStore
    from retrieval.embeddings import create_embedding_provider
    from retrieval.lakebase.config import LakebaseConfig
    from retrieval.lakebase.connection import LakebaseConnectionProvider
    from retrieval.lakebase.indexing import LakebaseHybridIndexRefresher
    from retrieval.lakebase.repository import LakebaseRetrievalRepository
    from retrieval.temporal.worker import AdapterBundle

    config = GoogleDriveConfig.from_env()
    if config.root_folder_id is None:
        raise ValueError("GOOGLE_DRIVE_ROOT_FOLDER_ID is required for the deployed demo")
    if config.held_file_id is None:
        raise ValueError("GOOGLE_DRIVE_HELD_FILE_ID is required for the deployed demo")
    demo_config = DemoConfig.from_env()
    demo_config.require_enabled()
    lakebase_config = LakebaseConfig.from_env(default_pool_max_size=20)
    connection_provider = LakebaseConnectionProvider(lakebase_config)
    drive_api = None
    try:
        await connection_provider.open()
        await connection_provider.wait()
        from .lakebase import GoogleDriveLakebaseStore

        staging = GoogleDriveLakebaseStore(
            connection_provider,
            root_folder_id=config.root_folder_id,
        )
        drive_api = GoogleDriveApiClient.from_config(config)
        gateway = GoogleDriveProviderGateway(
            config,
            drive_api,
            staging,
            state_store=staging,
        )
        demo_state = PostgresDemoStateStore(connection_provider)
        embedding_provider = create_embedding_provider()
        return AdapterBundle(
            repository=LakebaseRetrievalRepository(
                connection_provider,
                transaction_retry_limit=lakebase_config.transaction_retry_limit,
                owns_provider=True,
            ),
            staging_store=staging,
            provider_gateway=DemoGoogleDriveProvider(
                gateway,
                demo_state,
                embedding_provider=embedding_provider,
            ),
            before_document_commit=DemoBeforeDocumentCommitHook(
                demo_state,
                config=demo_config,
            ),
            ingestion_event_sink=DemoIngestionEventSink(demo_state),
            embedding_provider=embedding_provider,
            search_index_refresher=LakebaseHybridIndexRefresher(connection_provider),
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
