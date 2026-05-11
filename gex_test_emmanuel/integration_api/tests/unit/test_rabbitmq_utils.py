import asyncio
from unittest.mock import AsyncMock

from app.main import app
from app.utils.rabbitmq import publish_message_from_app


def test_publish_no_publisher_does_not_raise():
    if hasattr(app.state, "rabbit"):
        delattr(app.state, "rabbit")
    # Should not raise
    asyncio.run(publish_message_from_app(app, "test.key", {"a": 1}))


def test_publish_calls_publisher_publish():
    mock_pub = type("MP", (), {})()
    mock_pub.publish = AsyncMock()
    app.state.rabbit = mock_pub
    try:
        asyncio.run(publish_message_from_app(app, "test.key", {"a": 1}))
        mock_pub.publish.assert_awaited_once()
        # publish was called with keyword args in our helper
        called_kwargs = mock_pub.publish.call_args[1]
        assert called_kwargs["routing_key"] == "test.key"
        assert called_kwargs["message"] == {"a": 1}
    finally:
        if hasattr(app.state, "rabbit"):
            delattr(app.state, "rabbit")
