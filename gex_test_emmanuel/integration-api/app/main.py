from fastapi import FastAPI

from app.core.config import settings
from app.routers import health_router, webhook_router


def create_app() -> FastAPI:
    app = FastAPI(
        title=settings.app_name,
        version=settings.app_version,
        debug=settings.debug,
    )

    app.include_router(health_router, tags=["health"])
    app.include_router(webhook_router, tags=["webhooks"])

    return app


app = create_app()


def main():
    import uvicorn

    uvicorn.run("app.main:app", host="0.0.0.0", port=8080, reload=settings.debug)


if __name__ == "__main__":
    main()
