import hydra

from datetime import datetime
from dotenv import load_dotenv
from omegaconf import OmegaConf
from pydantic import BaseModel, Field
from typing import List, Optional, Tuple, Union
from rich import print


load_dotenv()

class SourceOfdb(BaseModel):
    url: str


class Sources(BaseModel):
    ofdb: SourceOfdb


class Area(BaseModel):
    name: str
    lats: Tuple[float, float]
    lngs: Tuple[float, float]
    lat_n_chunks: int
    lng_n_chunks: int


class AppCorsConfig(BaseModel):
    allowed_origins: List[str]
    allowed_methods: List[str]
    allowed_headers: List[str]
    allow_credentials: bool


class AppConfig(BaseModel):
    title: str
    host: str
    port: int
    cors: AppCorsConfig
    openapi_url: str


class EmailMetadataConfig(BaseModel):
    sender: str
    subject: str


class EmailTemplatesConfig(BaseModel):
    activation_email: str
    subscription_email: str


class EmailConfig(BaseModel):
    domain: str
    api_key: str
    url: str
    unsubscribe_url: str
    activation_url: str
    rate_limit: int
    max_retries: int
    retry_delay: int
    concurrency: int
    test_email_recipient: Optional[str] = None
    templates: EmailTemplatesConfig
    area_subscription_creates: EmailMetadataConfig
    subscription_activation: EmailMetadataConfig


class OfdbConfig(BaseModel):
    url: str
    limit: int = 2000
    max_retries: int = 10
    retry_delay: int = 5
    concurrency: int = 10


class DBConfig(BaseModel):
    name: str
    user: str
    password: str
    host: str
    port: int


class CronTriggerModel(BaseModel):
    year: Optional[Union[str, int]] = Field(None, description="Year to run the job")
    month: Optional[Union[str, int]] = Field(None, description="Month to run the job")
    day: Optional[Union[str, int]] = Field(
        None, description="Day of the month to run the job"
    )
    week: Optional[Union[str, int]] = Field(
        None, description="Week of the year to run the job"
    )
    day_of_week: Optional[Union[str, int]] = Field(
        None, description="Day of the week to run the job"
    )
    hour: Optional[Union[str, int]] = Field(None, description="Hour to run the job")
    minute: Optional[Union[str, int]] = Field(None, description="Minute to run the job")
    second: Optional[Union[str, int]] = Field(None, description="Second to run the job")
    start_date: Optional[str] = Field(
        None, description="Earliest possible date/time to run the job"
    )
    end_date: Optional[str] = Field(
        None, description="Latest possible date/time to run the job"
    )
    timezone: Optional[str] = Field(
        None, description="Timezone to use for the date/time calculations"
    )


class CronConfig(BaseModel):
    enabled: bool = True
    trigger: CronTriggerModel


class CronsConfig(BaseModel):
    sync_entries: CronConfig
    send_subscription_emails_daily: CronConfig
    send_subscription_emails_weekly: CronConfig
    send_subscription_emails_monthly: CronConfig


class Config(BaseModel):
    start_datetime: datetime
    app: AppConfig
    email: EmailConfig
    ofdb: OfdbConfig
    db: DBConfig
    crons: CronsConfig
    areas: List[Area]


hydra.initialize(version_base=None, config_path="../../..")
cfg = hydra.compose("config")
resolved_cfg = OmegaConf.to_container(cfg, resolve=True)
config = Config.model_validate(resolved_cfg)


if __name__ == "__main__":
    print(config)
