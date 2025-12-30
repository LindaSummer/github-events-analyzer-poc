from pydantic import field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="gh_",
        json_file=None  # Disable JSON parsing for env vars
    )
    
    git_secret: str
    kafka_bootstrap_servers: str
    kafka_realtime_topic: str = "github_events_realtime"
    kafka_archive_topic: str = "github_events_archive"
    gh_interval: int = 10  # seconds
    metric_port: int = 8000
    
    # @field_validator('kafka_bootstrap_servers', mode='before')
    # @classmethod
    # def parse_kafka_servers(cls, v):
    #     if isinstance(v, str):
    #         return [server.strip() for server in v.split(',')]
    #     return v
    
    @property
    def kafka_servers_list(self) -> list[str]:
        """Get kafka servers as list"""
        if isinstance(self.kafka_bootstrap_servers, list):
            return self.kafka_bootstrap_servers
        return [s.strip() for s in self.kafka_bootstrap_servers.split(',')]

        
settings = Settings() # type: ignore