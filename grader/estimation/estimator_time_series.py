from typing import Dict, List, Optional
import logging
import time
from datetime import datetime, timedelta
from kubernetes import client, config
from clickhouse_driver import Client as ClickHouseClient
from kafka import KafkaAdminClient, KafkaConsumer, KafkaProducer
from .base_estimator import BaseEstimator

class TimeSeriesEstimator(BaseEstimator):
    """Estimator for checking time series data processing capabilities."""
    
    def __init__(self):
        super().__init__()
        self.logger = logging.getLogger(__name__)
        self.required_objects = {
            'tables': ['sensor_data_raw', 'sensor_lens'],
            'jobs': ['data_ingestor', 'workload_generator'],
            'pods': ['request_handler'],
            'kafka_topics': ['kafka_topic_sensors']
        }
        
        # Initialize clients
        try:
            config.load_kube_config()
            self.k8s_client = client.CoreV1Api()
            self.k8s_batch_client = client.BatchV1Api()
            self.clickhouse_client = ClickHouseClient('localhost')
            self.kafka_admin = KafkaAdminClient(bootstrap_servers='localhost:9092')
        except Exception as e:
            self.logger.error(f"Failed to initialize clients: {str(e)}")
            raise

    def _check_required_objects(self) -> Dict[str, bool]:
        """Check if all required objects exist in the system."""
        results = {}
        
        try:
            # Check ClickHouse tables
            tables = self.clickhouse_client.execute('SHOW TABLES')
            table_names = [t[0] for t in tables]
            results['clickhouse_tables'] = all(
                table in table_names 
                for table in self.required_objects['tables']
            )
            
            # Check Kubernetes jobs
            jobs = self.k8s_batch_client.list_job_for_all_namespaces()
            job_names = [job.metadata.name for job in jobs.items]
            results['kubernetes_jobs'] = all(
                job in job_names 
                for job in self.required_objects['jobs']
            )
            
            # Check Kubernetes pods
            pods = self.k8s_client.list_pod_for_all_namespaces()
            pod_names = [pod.metadata.name for pod in pods.items]
            results['kubernetes_pods'] = all(
                pod in pod_names 
                for pod in self.required_objects['pods']
            )
            
            # Check Kafka topics
            topics = self.kafka_admin.list_topics()
            results['kafka_topics'] = all(
                topic in topics 
                for topic in self.required_objects['kafka_topics']
            )
            
        except Exception as e:
            self.logger.error(f"Error checking required objects: {str(e)}")
            return {'error': str(e)}
            
        return results

    def _check_data_ingestion(self) -> Dict[str, bool]:
        """Verify data ingestion functionality."""
        results = {}
        
        try:
            # Check data presence in raw table
            raw_count = self.clickhouse_client.execute(
                'SELECT COUNT(*) FROM sensor_data_raw'
            )[0][0]
            results['raw_data_present'] = raw_count > 0
            
            # Check data presence in lens table
            lens_count = self.clickhouse_client.execute(
                'SELECT COUNT(*) FROM sensor_lens'
            )[0][0]
            results['lens_data_present'] = lens_count > 0
            
            # Check data freshness
            latest_data = self.clickhouse_client.execute('''
                SELECT MAX(timestamp) 
                FROM sensor_data_raw
            ''')[0][0]
            results['data_freshness'] = (
                datetime.now() - latest_data
            ) < timedelta(minutes=5)
            
        except Exception as e:
            self.logger.error(f"Error checking data ingestion: {str(e)}")
            return {'error': str(e)}
            
        return results

    def _check_data_retention(self) -> Dict[str, bool]:
        """Check if data retention policies are being followed."""
        results = {}
        
        try:
            # Check if data older than one month exists
            oldest_data = self.clickhouse_client.execute('''
                SELECT MIN(timestamp) 
                FROM sensor_data_raw
            ''')[0][0]
            
            results['month_retention'] = (
                datetime.now() - oldest_data
            ) >= timedelta(days=30)
            
            # Check replication status
            replicas = self.clickhouse_client.execute('''
                SELECT * 
                FROM system.replicas 
                WHERE table = 'sensor_data_raw'
            ''')
            results['replication_active'] = len(replicas) > 0
            
        except Exception as e:
            self.logger.error(f"Error checking data retention: {str(e)}")
            return {'error': str(e)}
            
        return results

    def _run_workload_test(self) -> Dict[str, float]:
        """Run performance tests under specified workload."""
        results = {}
        
        try:
            # Test query performance
            start_time = time.time()
            self.clickhouse_client.execute('''
                SELECT * 
                FROM sensor_lens 
                WHERE timestamp 
                BETWEEN NOW() - INTERVAL 1 DAY AND NOW()
                LIMIT 1000
            ''')
            query_time = time.time() - start_time
            results['query_latency'] = query_time
            
            # Check ingestion rate
            initial_count = self.clickhouse_client.execute(
                'SELECT COUNT(*) FROM sensor_data_raw'
            )[0][0]
            time.sleep(10)  # Wait 10 seconds
            final_count = self.clickhouse_client.execute(
                'SELECT COUNT(*) FROM sensor_data_raw'
            )[0][0]
            results['ingestion_rate'] = (final_count - initial_count) / 10
            
            # Get system metrics
            metrics = self.k8s_client.list_pod_metrics_for_all_namespaces()
            results['system_cpu_usage'] = sum(
                container.usage.cpu for pod in metrics.items 
                for container in pod.containers
            )
            results['system_memory_usage'] = sum(
                container.usage.memory for pod in metrics.items 
                for container in pod.containers
            )
            
        except Exception as e:
            self.logger.error(f"Error running workload test: {str(e)}")
            return {'error': str(e)}
            
        return results

    def estimate(self) -> Dict[str, any]:
        """Run all checks and return comprehensive results."""
        return {
            'required_objects': self._check_required_objects(),
            'data_ingestion': self._check_data_ingestion(),
            'data_retention': self._check_data_retention(),
            'workload_test': self._run_workload_test()
        }
