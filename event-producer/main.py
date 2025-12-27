#!/usr/bin/env python3
"""
GitHub Events to Kafka Producer
Consumes public GitHub events and pushes them to Kafka
"""
import json
import logging
import signal
import time
from datetime import datetime
from typing import Optional

import requests
from kafka import KafkaProducer
from kafka.errors import KafkaError
from prometheus_client import Counter, Gauge, Histogram, start_http_server

from config import settings

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Prometheus Metrics
EVENTS_FETCHED = Counter('github_events_fetched_total', 'Total GitHub events fetched')
EVENTS_SENT = Counter('github_events_sent_total', 'Total events sent to Kafka', ['topic'])
EVENTS_FAILED = Counter('github_events_failed_total', 'Total events failed to send')
GITHUB_API_REQUESTS = Counter('github_api_requests_total', 'Total GitHub API requests', ['status_code'])
GITHUB_RATE_LIMIT = Gauge('github_api_rate_limit_remaining', 'GitHub API rate limit remaining')
KAFKA_SEND_LATENCY = Histogram('kafka_send_duration_seconds', 'Kafka send latency in seconds')
PRODUCER_UP = Gauge('github_producer_up', 'Whether the producer is running')
LAST_SUCCESSFUL_FETCH = Gauge('github_last_successful_fetch_timestamp', 'Timestamp of last successful GitHub fetch')


class GithubEventProducer:
    def __init__(self, kafka_bootstrap_servers: list[str], kafka_topics: list[str], github_token: Optional[str] = None):
        self.kafka_topics = kafka_topics
        self.github_token = github_token
        self.producer = KafkaProducer(
            bootstrap_servers=kafka_bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            retries=5
        )
        PRODUCER_UP.set(1)
        self.github_api_url = "https://api.github.com/events"
        self.cursor = None
        self.running = True
        logger.info("Kafka producer initialized successfully")
        
    def _github_headers(self) -> dict[str, str]:
        headers = {'Content-Type': 'application/json'}
        if self.github_token:
            headers['Authorization'] = f'Bearer {self.github_token}'
        return headers
    
    def fetch_github_events_by_graphql_with_page(self) -> list[dict]:
        query = """
        query($cursor: String) {
          search(query: "is:public updated:>2025-12-20", type: ISSUE, first: 100, after: $cursor) {
            pageInfo {
              endCursor
              hasNextPage
            }
            edges {
              node {
                ... on Issue {
                  id
                  title
                  createdAt
                  updatedAt
                  author {
                    login
                  }
                  repository {
                    nameWithOwner
                  }
                }
              }
            }
          }
        }
        """
        
        variables = {"cursor": self.cursor} if self.cursor else {}
        response = requests.post(
            "https://api.github.com/graphql",
            headers=self._github_headers(),
            json={"query": query, "variables": variables}
        )
        GITHUB_API_REQUESTS.labels(status_code=response.status_code).inc()
        
        if response.status_code != 200:
            logger.error(f"GitHub GraphQL API request failed with status code {response.status_code}: {response.text}")
            return []
        
        data = response.json()
        
        if 'errors' in data:
            logger.error(f"GraphQL errors: {data['errors']}")
            return []
        
        edges = data.get('data', {}).get('search', {}).get('edges', [])
        events = [edge.get('node', {}) for edge in edges]
        page_info = data.get('data', {}).get('search', {}).get('pageInfo', {})
        
        self.cursor = page_info.get('endCursor') if page_info.get('hasNextPage') else None
        rate_limit_remaining = int(response.headers.get('X-RateLimit-Remaining', 0))
        GITHUB_RATE_LIMIT.set(rate_limit_remaining)
        
        if events:
            EVENTS_FETCHED.inc(len(events))
            LAST_SUCCESSFUL_FETCH.set_to_current_time()
        
        return events
    
    def send_event_to_kafka(self, event: dict):
        for topic in self.kafka_topics:
            start_time = time.time()
            future = self.producer.send(topic, value=event)
            try:
                record_metadata = future.get(timeout=10)
                latency = time.time() - start_time
                KAFKA_SEND_LATENCY.observe(latency)
                EVENTS_SENT.labels(topic=topic).inc()
                logger.info(f"Event sent to topic {record_metadata.topic} partition {record_metadata.partition} offset {record_metadata.offset}")
            except KafkaError as e:
                EVENTS_FAILED.inc()
                logger.error(f"Failed to send event to Kafka: {e}")
                
    def close(self):
        """Gracefully close producer"""
        logger.info("Closing Kafka producer...")
        self.producer.flush(timeout=30)
        self.producer.close()
        PRODUCER_UP.set(0)
        logger.info("Kafka producer closed successfully")
    
    def stop(self):
        """Signal to stop polling"""
        self.running = False
        
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_value, traceback):
        self.close()

def signal_handler(signum, frame):
    """Handle SIGTERM and SIGINT signals"""
    logger.info(f"Received signal {signum}, initiating graceful shutdown...")
    global producer_instance
    if producer_instance:
        if producer_instance.running:
            producer_instance.stop()
        else:
            logger.info("Producer already stopping...")
            exit(0)


def main():
    global producer_instance
    logger.info(f"Starting GitHub Event Producer, metrics on port {settings.metric_port}")
    # start_http_server(settings.metric_port)
    
    # Register signal handlers for graceful shutdown
    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)
    
    kafka_bootstrap_servers = settings.kafka_bootstrap_servers
    kafka_topics = [settings.kafka_realtime_topic]
    github_token = settings.git_secret
    
    producer_instance = GithubEventProducer(kafka_bootstrap_servers, kafka_topics, github_token)
    
    try:
        while producer_instance.running:
            try:
                events = producer_instance.fetch_github_events_by_graphql_with_page()
                if events:
                    EVENTS_FETCHED.inc(len(events))
                    LAST_SUCCESSFUL_FETCH.set_to_current_time()
                    for event in events:
                        if not producer_instance.running:
                            logger.info("Shutdown signal received, stopping event processing")
                            break
                        producer_instance.send_event_to_kafka(event)
                else:
                    logger.info("No new events fetched.")
                time.sleep(settings.gh_interval)
            except Exception as e:
                logger.exception(f"Error during event fetching: {e}")
                if producer_instance.running:
                    time.sleep(settings.gh_interval)
    except KeyboardInterrupt:
        logger.info("Keyboard interrupt received")
    finally:
        logger.info("Shutting down producer...")
        producer_instance.close()
        logger.info("Shutdown complete")
    


if __name__ == "__main__":
    main()
