"""Health + readiness probes for terraformer."""

from __future__ import annotations

import shutil

from fastapi import APIRouter, status
from fastapi.responses import JSONResponse

from services.terraformer.src.settings import get_settings

router = APIRouter(tags=["health"])


@router.get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok"}


@router.get("/ready")
async def ready() -> JSONResponse:
    settings = get_settings()
    tf = shutil.which(settings.terraform_binary) or (
        settings.terraform_binary if shutil.os.path.exists(settings.terraform_binary) else None
    )
    modules = settings.terraform_modules_root.exists()
    workdir_writable = False
    try:
        settings.terraform_workdir_root.mkdir(parents=True, exist_ok=True)
        workdir_writable = True
    except OSError:
        workdir_writable = False
    ok = bool(tf) and modules and workdir_writable
    payload = {
        "ready": ok,
        "terraform_binary": tf,
        "terraform_modules_root_exists": modules,
        "terraform_workdir_writable": workdir_writable,
    }
    return JSONResponse(
        payload,
        status_code=status.HTTP_200_OK if ok else status.HTTP_503_SERVICE_UNAVAILABLE,
    )
