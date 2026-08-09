from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles

from app.api.dashboard import WEB_DIR, router as dashboard_router
from app.api.monitoring import router as monitoring_router
from app.api.recovery import router as recovery_router
from app.api.tasks import router as tasks_router
from app.core.config import get_settings

settings = get_settings()

app = FastAPI(
    title=settings.app_name,
    version=settings.app_version,
    debug=settings.debug,
)
app.include_router(tasks_router)
app.include_router(monitoring_router)
app.include_router(recovery_router)
app.include_router(dashboard_router)
app.mount("/static", StaticFiles(directory=WEB_DIR), name="static")


@app.get("/")
def read_root() -> dict[str, str]:
    return {"name": settings.app_name, "message": "Asynchronous job processing system"}


@app.get("/health")
def read_health() -> dict[str, str]:
    return {"status": "healthy"}
