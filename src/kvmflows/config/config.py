import hydra

from datetime import datetime
from omegaconf import OmegaConf
from pydantic import BaseModel
from typing import List, Optional, Tuple
from rich import print


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
    unsubscribe_url: Optional[str] = None
    start_to_close_timeout_seconds: int


class EmailConfig(BaseModel):
    domain: str
    api_key: str
    url: str
    rate_limit: int
    max_retries: int
    retry_delay: int
    concurrency: int
    test_email_recipient: Optional[str] = None
    area_subscription_creates: EmailMetadataConfig


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


class Config(BaseModel):
    start_datetime: datetime
    app: AppConfig
    email: EmailConfig
    ofdb: OfdbConfig
    db: DBConfig
    areas: List[Area]


hydra.initialize(version_base=None, config_path="../../..")
cfg = hydra.compose("config")
resolved_cfg = OmegaConf.to_container(cfg, resolve=True)
config = Config.model_validate(resolved_cfg)

if __name__ == "__main__":
    print(config)
