"""
Simple uvicorn runner for the FastAPI application.

Usage:
    python run.py

Starts the server with live reload enabled for development.
Configuration is pulled from app.config.settings.
"""

import uvicorn

from app.config import settings


def main() -> None:
    """Run the uvicorn ASGI server."""
    uvicorn.run(
        "app.main:app",
        host=settings.host,
        port=settings.port,
        reload=settings.debug,
        log_level="info",
    )


if __name__ == "__main__":
    main()
