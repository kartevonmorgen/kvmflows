from fastapi import APIRouter

from src.kvmflows.apis.router.v1.router import router as v1_router


router = APIRouter()

routers = [v1_router]
for r in routers:
    router.include_router(r)
