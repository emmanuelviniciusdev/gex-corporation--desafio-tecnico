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


# Initialize RabbitMQ publisher instance on startup (lazy connect)
@app.on_event("startup")
async def startup_event():
    try:
        from app.utils.rabbitmq import RabbitPublisher
        import aio_pika

        # create publisher instance only; connection is established lazily on first publish
        app.state.rabbit = RabbitPublisher(settings.rabbitmq_url)

        # Ensure RabbitMQ queues exist early so publishes from the API aren't dropped
        try:
            connection = await aio_pika.connect_robust(settings.rabbitmq_url)
            async with connection:
                channel = await connection.channel()
                for dlq in (
                    "lead.received",
                    "dist.sms",
                    "lead.dead.decrypt_failed",
                    "lead.dead.schema_invalid",
                    "lead.dead.consumer_failed",
                    "channels.dead.consumer_failed",
                ):
                    await channel.declare_queue(dlq, durable=True)
        except Exception:
            import logging

            logging.getLogger(__name__).exception("Failed to declare DLQ queues during API startup")
    except Exception:
        import logging

        logging.getLogger(__name__).exception("Failed to initialize RabbitMQ publisher instance")


@app.on_event("shutdown")
async def shutdown_event():
    rabbit = getattr(app.state, "rabbit", None)
    if rabbit:
        await rabbit.close()


def main():
    import uvicorn

    uvicorn.run("app.main:app", host="0.0.0.0", port=8080, reload=settings.debug)


if __name__ == "__main__":
    main()
