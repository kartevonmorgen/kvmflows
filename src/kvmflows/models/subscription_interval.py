from enum import StrEnum
from datetime import timedelta, datetime, timezone
from dateutil.relativedelta import relativedelta
from pydantic import BaseModel
from rich import print


class IntervalDatetimes(BaseModel):
    start_datetime: datetime
    end_datetime: datetime


class SubscriptionInterval(StrEnum):
    HOURLY = "hourly"
    DAILY = "daily"
    WEEKLY = "weekly"
    MONTHLY = "monthly"
    YEARLY = "yearly"

    @property
    def passed_interval_datestime(self) -> IntervalDatetimes:
        now = datetime.now(tz=timezone.utc)
        today_datetime = now.replace(hour=0, minute=0, second=0, microsecond=0)

        match self:
            case SubscriptionInterval.HOURLY:
                end_datetime = now.replace(minute=0, second=0, microsecond=0)
                start_datetime = end_datetime - timedelta(hours=1)
                return IntervalDatetimes(
                    start_datetime=start_datetime,
                    end_datetime=end_datetime,
                )
            case SubscriptionInterval.DAILY:
                return IntervalDatetimes(
                    start_datetime=today_datetime - timedelta(days=1),
                    end_datetime=today_datetime,
                )
            case SubscriptionInterval.WEEKLY:
                end_datetime = today_datetime - timedelta(days=today_datetime.weekday())
                start_datetime = end_datetime - timedelta(days=7)
                
                return IntervalDatetimes(
                    start_datetime=start_datetime,
                    end_datetime=end_datetime,
                )
            case SubscriptionInterval.MONTHLY:
                end_datetime = today_datetime.replace(day=1)
                start_datetime = end_datetime - relativedelta(months=1)
                
                return IntervalDatetimes(
                    start_datetime=start_datetime,
                    end_datetime=end_datetime,
                )
            case SubscriptionInterval.YEARLY:
                end_datetime = today_datetime.replace(month=1, day=1)
                start_datetime = end_datetime - relativedelta(years=1)
                
                return IntervalDatetimes(
                    start_datetime=start_datetime,
                    end_datetime=end_datetime,
                )
            case _:
                raise ValueError(f"Invalid interval: {self}")


def test_subscription_interval():
    print(datetime.now(tz=timezone.utc))
    interval = SubscriptionInterval.DAILY
    print(interval.passed_interval_datestime)


if __name__ == "__main__":
    test_subscription_interval()
