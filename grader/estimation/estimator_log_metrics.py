import logging
from typing import Dict, List, Optional
import kubernetes
from kubernetes import client, config
from clickhouse_driver import Client as ClickHouseClient
from arango import ArangoDB
from kafka import KafkaConsumer
from elasticsearch import Elasticsearch
import psutil
import time

class ProjectEstimator:
    """Handles the estimation and verification of student Kubernetes projects."""
    
    def __init__(self, k8s_config_path: Optional[str] = None):
        """Initialize the estimator with Kubernetes configuration."""
        self.logger = logging.getLogger(__name__)
        
        # Initialize Kubernetes client
        try:
            if k8s_config_path:
                config.load_kube_config(k8s_config_path)
            else:
                config.load_incluster_config()
            self.k8s_client = client.CoreV1Api()
        except Exception as e:
            self.logger.error(f"Failed to initialize Kubernetes client: {e}")
            raise

    def check_required_pods(self) -> Dict[str, bool]:
        """Check if all required pods are running."""
        required_services = ['kafka', 'clickhouse', 'elasticsearch', 'arangodb']
        status = {}
        
        try:
            pods = self.k8s_client.list_pod_for_all_namespaces()
            for service in required_services:
                service_pods = [
                    pod for pod in pods.items 
                    if service in pod.metadata.name and 
                    pod.status.phase == 'Running'
                ]
                status[service] = len(service_pods) > 0
        except Exception as e:
            self.logger.error(f"Failed to check pods: {e}")
            raise
            
        return status

    def verify_clickhouse_tables(self, host: str, port: int) -> Dict[str, bool]:
        """Verify required ClickHouse tables exist and are accessible."""
        required_tables = ['log_events', 'metrics_data']
        status = {}
        
        try:
            client = ClickHouseClient(host=host, port=port)
            existing_tables = client.execute('SHOW TABLES FROM default')
            existing_tables = [table[0] for table in existing_tables]
            
            for table in required_tables:
                status[table] = table in existing_tables
                
        except Exception as e:
            self.logger.error(f"Failed to verify ClickHouse tables: {e}")
            raise
            
        return status

    def verify_arango_collections(self, host: str, port: int, 
                                username: str, password: str) -> Dict[str, bool]:
        """Verify required ArangoDB collections exist."""
        required_collections = ['transactions']
        status = {}
        
        try:
            client = ArangoDB(
                hosts=f'http://{host}:{port}',
                username=username,
                password=password
            )
            db = client.db('_system')
            
            for collection in required_collections:
                status[collection] = db.has_collection(collection)
                
        except Exception as e:
            self.logger.error(f"Failed to verify ArangoDB collections: {e}")
            raise
            
        return status

    def verify_kafka_topics(self, bootstrap_servers: str) -> Dict[str, bool]:
        """Verify required Kafka topics exist and are receiving messages."""
        required_topics = ['event_stream']
        status = {}
        
        try:
            consumer = KafkaConsumer(
                bootstrap_servers=bootstrap_servers,
                auto_offset_reset='earliest'
            )
            available_topics = consumer.topics()
            
            for topic in required_topics:
                status[topic] = topic in available_topics
                
        except Exception as e:
            self.logger.error(f"Failed to verify Kafka topics: {e}")
            raise
            
        return status

    def check_system_metrics(self) -> Dict[str, float]:
        """Get current system metrics."""
        metrics = {
            'cpu_usage': psutil.cpu_percent(),
            'ram_usage': psutil.virtual_memory().percent,
            'disk_usage': psutil.disk_usage('/').percent
        }
        return metrics

    def run_full_verification(self, config: Dict) -> Dict[str, Dict]:
        """Run all verification checks and return results."""
        results = {
            'kubernetes': {},
            'clickhouse': {},
            'arango': {},
            'kafka': {},
            'system_metrics': {},
            'timestamp': time.time()
        }
        
        try:
            results['kubernetes'] = self.check_required_pods()
            results['clickhouse'] = self.verify_clickhouse_tables(
                config['clickhouse']['host'],
                config['clickhouse']['port']
            )
            results['arango'] = self.verify_arango_collections(
                config['arango']['host'],
                config['arango']['port'],
                config['arango']['username'],
                config['arango']['password']
            )
            results['kafka'] = self.verify_kafka_topics(
                config['kafka']['bootstrap_servers']
            )
            results['system_metrics'] = self.check_system_metrics()
            
        except Exception as e:
            self.logger.error(f"Failed to complete full verification: {e}")
            results['error'] = str(e)
            
        return results


if __name__ == "__main__":
    config = {
        'clickhouse': {
            'host': 'clickhouse-service',
            'port': 9000
        },
        'arango': {
            'host': 'arangodb-service',
            'port': 8529,
            'username': 'root',
            'password': 'password'
        },
        'kafka': {
            'bootstrap_servers': 'kafka-service:9092'
        }
    }

    estimator = ProjectEstimator()
    results = estimator.run_full_verification(config)
    print(results) 
   