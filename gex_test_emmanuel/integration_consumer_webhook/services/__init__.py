"""Service package for queue handlers.

Each module should expose a make_handler(pool, publish_channel) factory and (optionally) QUEUE_NAME.
"""

__all__ = ["lead_received"]
