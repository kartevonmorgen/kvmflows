from fastapi import APIRouter, HTTPException, status, Depends, Response
from pydantic import BaseModel, EmailStr
from uuid import UUID
from loguru import logger

from src.kvmflows.models.subscription_interval import SubscriptionInterval
from src.kvmflows.models.subscription_types import EntrySubscriptionType
from src.kvmflows.models.supported_languages import SupportedLanguages
from src.kvmflows.database.subscription import SubscriptionModel
from src.kvmflows.database.dependencies import get_async_db_connection


router = APIRouter()


class CreateSubscriptionRequest(BaseModel):
    title: str
    email: EmailStr
    lat_min: float
    lon_min: float
    lat_max: float
    lon_max: float
    interval: SubscriptionInterval
    subscription_type: EntrySubscriptionType
    language: SupportedLanguages


class SubscriptionResponse(BaseModel):
    id: UUID
    title: str
    email: EmailStr
    lat_min: float
    lon_min: float
    lat_max: float
    lon_max: float
    interval: SubscriptionInterval
    subscription_type: EntrySubscriptionType
    language: SupportedLanguages
    is_active: bool


@router.post(
    "/subscriptions",
    responses={
        status.HTTP_200_OK: {"description": "Subscription created successfully"},
        status.HTTP_409_CONFLICT: {
            "description": "Similar subscription already exists",
            "content": {
                "application/json": {
                    "example": {
                        "detail": {
                            "message": "Similar subscription already exists",
                            "subscription": {
                                "id": "3fa85f64-5717-4562-b3fc-2c963f66afa6"
                            },
                        }
                    }
                }
            },
        },
        status.HTTP_500_INTERNAL_SERVER_ERROR: {
            "description": "Failed to create subscription"
        },
    },
)
async def create_subscription(
    subscription: CreateSubscriptionRequest,
    db=Depends(get_async_db_connection),
) -> SubscriptionResponse:
    logger.info(f"Creating subscription for email: {subscription.email}")

    # Check for existing subscription using async method
    existing_subscription = await SubscriptionModel.aio_get_or_none(
        SubscriptionModel.email == subscription.email,
        SubscriptionModel.interval == subscription.interval,
        SubscriptionModel.lat_min == subscription.lat_min,
        SubscriptionModel.lon_min == subscription.lon_min,
        SubscriptionModel.lat_max == subscription.lat_max,
        SubscriptionModel.lon_max == subscription.lon_max,
        SubscriptionModel.subscription_type == subscription.subscription_type.value,
        SubscriptionModel.language == subscription.language.value,
    )

    if existing_subscription:
        logger.warning(
            f"Similar subscription already exists for email: {subscription.email}"
        )
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail={"message": "Similar subscription already exists"},
        )

    # Create new subscription using async method
    try:
        subscription_instance = await SubscriptionModel.aio_create(
            title=subscription.title,
            email=subscription.email,
            lat_min=subscription.lat_min,
            lon_min=subscription.lon_min,
            lat_max=subscription.lat_max,
            lon_max=subscription.lon_max,
            interval=subscription.interval.value,
            subscription_type=subscription.subscription_type.value,
            language=subscription.language.value,
            is_active=False,
        )
        logger.debug(f"Subscription created with ID: {subscription_instance.id}")

    except Exception as e:
        logger.error(f"Error creating subscription: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to create subscription",
        )

    response = subscription_instance.to_subscription_response()

    return response


@router.get("/subscriptions/{subscription_id}/unsubscribe")
async def unsubscribe(
    subscription_id: str, db=Depends(get_async_db_connection)
) -> Response:
    existing_subscription = await SubscriptionModel.aio_get_or_none(
        SubscriptionModel.id == subscription_id
    )
    if not existing_subscription:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Subscription not found"
        )

    # Update the is_active field using the helper method
    existing_subscription.set_active(False)
    await existing_subscription.aio_save()

    return Response(
        content="""
            <html>
                <head><title>Unsubscribed</title></head>
                <body>
                    <h2>You are unsubscribed successfully!</h2>
                </body>
            </html>
        """,
        media_type="text/html",
    )


@router.get("/subscriptions/{subscription_id}/activate")
async def activate_subscription(
    subscription_id: str, db=Depends(get_async_db_connection)
):
    """Activate a subscription by setting is_active=True and return an HTML confirmation."""
    subscription = await SubscriptionModel.aio_get_or_none(
        SubscriptionModel.id == subscription_id
    )
    if not subscription:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Subscription not found"
        )
    if subscription.get_is_active():
        return Response(
            content="""
                <html>
                    <head><title>Subscription Activated</title></head>
                    <body>
                        <h2>Your subscription is already active.</h2>
                    </body>
                </html>
            """,
            media_type="text/html",
        )
    subscription.set_active(True)
    await subscription.aio_save()
    return Response(
        content="""
            <html>
                <head><title>Subscription Activated</title></head>
                <body>
                    <h2>Your subscription is activated successfully!</h2>
                </body>
            </html>
        """,
        media_type="text/html",
    )
