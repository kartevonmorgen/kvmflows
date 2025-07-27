from fastapi import APIRouter
from src.kvmflows.apis.router.v1.subscription.router import router as subscription_router


router = APIRouter(prefix="/v1")

routers = [subscription_router]
for r in routers:
    router.include_router(r)
