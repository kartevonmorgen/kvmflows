from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from uvicorn import Server, Config
from loguru import logger

from src.kvmflows.config.config import config
from src.kvmflows.apis.router.router import router
from src.kvmflows.database.db import initialize_database, db
from src.kvmflows.database.subscription import SubscriptionModel


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    logger.info("Starting up application...")
    try:
        # Initialize database and ensure connection
        await initialize_database([SubscriptionModel])
        logger.info("Database initialized successfully")

        # Test database connection
        if db.is_connection_usable():
            logger.info("Database connection is active and usable")
        else:
            logger.warning(
                "Database connection is not usable, attempting to connect..."
            )
            if db.is_closed():
                db.connect()
            logger.info("Database connection established")
    except Exception as e:
        logger.error(f"Failed to initialize database: {e}")
        raise

    yield

    # Shutdown
    logger.info("Shutting down application...")
    try:
        if not db.is_closed():
            db.close()
            logger.info("Database connection closed")
    except Exception as e:
        logger.warning(f"Error closing database connection: {e}")


app = FastAPI(
    title=config.app.title,
    openapi_url=config.app.openapi_url,
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=config.app.cors.allowed_origins,
    allow_credentials=config.app.cors.allow_credentials,
    allow_methods=config.app.cors.allowed_methods,
    allow_headers=config.app.cors.allowed_headers,
)

app.include_router(router)


async def main():
    server = Server(
        config=Config(
            app=app,
            host=config.app.host,
            port=config.app.port,
        )
    )
    await server.serve()


if __name__ == "__main__":
    import asyncio

    asyncio.run(main())
