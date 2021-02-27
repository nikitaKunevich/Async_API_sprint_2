from pydantic import BaseSettings, Field


class Settings(BaseSettings):
    es_url: str = Field('http://127.0.0.1:9200', env='ES_URL')
    api_host: str = Field('http://127.0.0.1:8888', env='API_HOST')
    redis_host: str = Field('localhost', env='REDIS_HOST')
    redis_port: str = Field('6379', env='REDIS_PORT')
    log_level: str = Field('INFO', env='LOG_LEVEL')
