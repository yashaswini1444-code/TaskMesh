from fastapi import FastAPI

from app.core.config import get_settings

settings = get_settings()

app = FastAPI(
    title=settings.app_name,
    version=settings.app_version,
    debug=settings.debug,
)


@app.get("/")
def read_root() -> dict[str, str]:
    return {"name": settings.app_name, "message": "Asynchronous job processing system"}


@app.get("/health")
def read_health() -> dict[str, str]:
    return {"status": "healthy"}
