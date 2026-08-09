from collections.abc import Callable, Generator

from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker

from app.core.config import get_settings

settings = get_settings()
connect_args = (
    {"check_same_thread": False}
    if settings.database_url.startswith("sqlite")
    else {}
)

engine = create_engine(
    settings.database_url,
    connect_args=connect_args,
    pool_pre_ping=True,
)
SessionLocal = sessionmaker(bind=engine, autoflush=False, expire_on_commit=False)


def get_db() -> Generator[Session, None, None]:
    with SessionLocal() as session:
        yield session


def get_session_factory() -> Callable[[], Session]:
    """FastAPI dependency exposing the raw session factory (not a single
    request-scoped Session) for services that manage their own transaction
    boundaries across multiple short-lived sessions, e.g.
    app.services.recovery.recover_stale_tasks. Overridable in tests the same
    way as get_db."""

    return SessionLocal
