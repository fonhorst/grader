from typing import Dict, List, Optional
import logging
from kubernetes import client, config
from elasticsearch import Elasticsearch
from arango import ArangoClient
from clickhouse_driver import Client as ClickHouseClient
from kafka import KafkaAdminClient, KafkaConsumer
from .estimator import BaseEstimator
from .estimator_log_metrics import LogMetricsEstimator

class SocialNetworkEstimator(BaseEstimator):
    """Estimator for checking social network components and functionality."""
    
    def __init__(self):
        super().__init__()
        self.logger = logging.getLogger(__name__)
        self.required_objects = {
            'collections': ['profiles'],
            'services': ['streams'],
            'databases': ['clickhouse'],
            'dashboards': ['monitoring'],
            'data_marts': ['dm_lifecycle']
        }
        
        # Initialize clients
        try:
            config.load_kube_config()
            self.k8s_client = client.CoreV1Api()
            self.clickhouse_client = ClickHouseClient('localhost')
            self.arango_client = ArangoClient(hosts='http://localhost:8529')
            self.es_client = Elasticsearch(['http://localhost:9200'])
            self.kafka_admin = KafkaAdminClient(bootstrap_servers='localhost:9092')
        except Exception as e:
            self.logger.error(f"Failed to initialize clients: {str(e)}")
            raise

    def check_required_objects(self) -> Dict[str, bool]:
        """Check if all required objects exist in the system."""
        results = {}
        
        try:
            # Check Kubernetes services
            services = self.k8s_client.list_service_for_all_namespaces()
            service_names = [svc.metadata.name for svc in services.items]
            results['streams_service'] = 'streams' in service_names
            
            # Check ClickHouse tables
            tables = self.clickhouse_client.execute('SHOW TABLES')
            results['clickhouse_tables'] = all(
                table in [t[0] for t in tables] 
                for table in self.required_objects['collections']
            )
            
            # Check ArangoDB collections
            db = self.arango_client.db('_system')
            collections = db.collections()
            results['arango_collections'] = all(
                coll in [c['name'] for c in collections] 
                for coll in self.required_objects['collections']
            )
            
            # Check Elasticsearch indices
            indices = self.es_client.indices.get_alias().keys()
            results['elasticsearch_indices'] = 'profiles' in indices
            
        except Exception as e:
            self.logger.error(f"Error checking required objects: {str(e)}")
            return {'error': str(e)}
            
        return results

    def check_data_ingestion(self) -> Dict[str, bool]:
        """Verify data ingestion functionality."""
        results = {}
        
        try:
            # Check Kafka topics
            topics = self.kafka_admin.list_topics()
            results['kafka_topics_exist'] = 'profiles' in topics
            
            # Check for duplicates in ClickHouse
            duplicates = self.clickhouse_client.execute('''
                SELECT COUNT(*) as cnt 
                FROM profiles 
                GROUP BY user_id 
                HAVING cnt > 1
            ''')
            results['no_duplicates'] = len(duplicates) == 0
            
            # Check recent updates
            recent_updates = self.clickhouse_client.execute('''
                SELECT COUNT(*) 
                FROM profiles 
                WHERE last_updated > NOW() - INTERVAL 1 DAY
            ''')
            results['recent_updates'] = recent_updates[0][0] > 0
            
        except Exception as e:
            self.logger.error(f"Error checking data ingestion: {str(e)}")
            return {'error': str(e)}
            
        return results

    def check_fault_tolerance(self) -> Dict[str, bool]:
        """Test system fault tolerance."""
        results = {}
        
        try:
            # Test Kafka consumer group health
            consumer = KafkaConsumer(
                'profiles',
                bootstrap_servers='localhost:9092',
                group_id='test_group'
            )
            results['kafka_consumer_healthy'] = consumer.bootstrap_connected()
            
            # Check system resource limits
            pods = self.k8s_client.list_pod_for_all_namespaces()
            results['resource_limits_set'] = all(
                pod.spec.containers[0].resources.limits is not None 
                for pod in pods.items
            )
            
        except Exception as e:
            self.logger.error(f"Error checking fault tolerance: {str(e)}")
            return {'error': str(e)}
            
        return results

    def check_performance(self) -> Dict[str, float]:
        """Test system performance under load."""
        results = {}
        
        try:
            # Test ClickHouse query performance
            start_time = time.time()
            self.clickhouse_client.execute('SELECT city, COUNT(*) FROM profiles GROUP BY city')
            query_time = time.time() - start_time
            results['query_performance'] = query_time
            
            # Get pod metrics
            metrics = self.k8s_client.list_pod_metrics_for_all_namespaces()
            results['pod_cpu_usage'] = sum(
                container.usage.cpu for pod in metrics.items 
                for container in pod.containers
            )
            results['pod_memory_usage'] = sum(
                container.usage.memory for pod in metrics.items 
                for container in pod.containers
            )
            
        except Exception as e:
            self.logger.error(f"Error checking performance: {str(e)}")
            return {'error': str(e)}
            
        return results

    def estimate(self) -> Dict[str, any]:
        """Run all checks and return comprehensive results."""
        return {
            'required_objects': self.check_required_objects(),
            'data_ingestion': self.check_data_ingestion(),
            'fault_tolerance': self.check_fault_tolerance(),
            'performance': self.check_performance()
        }
