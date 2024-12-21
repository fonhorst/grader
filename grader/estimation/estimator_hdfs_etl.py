import subprocess
import json
import time
from typing import Dict, List, Tuple
import requests
from kubernetes import client, config

class HDFSETLEstimator:
    def __init__(self, 
                 k8s_context: str,
                 elasticsearch_url: str,
                 grafana_url: str):
        """Initialize the estimator with necessary connection details."""
        self.elasticsearch_url = elasticsearch_url
        self.grafana_url = grafana_url
        
        # Initialize kubernetes client
        config.load_kube_config(context=k8s_context)
        self.k8s_client = client.CoreV1Api()

    def check_prerequisites(self) -> Tuple[bool, List[str]]:
        """Check all preliminary requirements."""
        issues = []
        
        # Check Kubernetes access
        try:
            self.k8s_client.list_pod_for_all_namespaces()
        except Exception as e:
            issues.append(f"Cannot access Kubernetes cluster: {str(e)}")

        # Check HDFS tools
        if subprocess.call(['which', 'hdfs']) != 0:
            issues.append("HDFS tools not installed")

        # Check monitoring tools
        try:
            requests.get(f"{self.grafana_url}/api/health")
        except Exception as e:
            issues.append(f"Cannot access Grafana: {str(e)}")

        return len(issues) == 0, issues

    def verify_kubernetes_objects(self) -> Tuple[bool, List[str]]:
        """Verify required Kubernetes objects exist and are running."""
        issues = []
        
        # Check required pods
        required_pods = ['etl_pipeline', 'file_watcher']
        pods = self.k8s_client.list_pod_for_all_namespaces()
        
        found_pods = [pod.metadata.name for pod in pods.items]
        for required_pod in required_pods:
            if not any(required_pod in pod for pod in found_pods):
                issues.append(f"Required pod {required_pod} not found")
            else:
                # Check pod status
                pod_status = next(pod.status.phase for pod in pods.items 
                                if required_pod in pod.metadata.name)
                if pod_status != 'Running':
                    issues.append(f"Pod {required_pod} is not running (status: {pod_status})")

        return len(issues) == 0, issues

    def verify_hdfs_structure(self) -> Tuple[bool, List[str]]:
        """Verify HDFS directories and permissions."""
        issues = []
        
        required_dirs = ['/input_data', '/processed_data']
        for directory in required_dirs:
            result = subprocess.run(['hdfs', 'dfs', '-ls', directory], 
                                  capture_output=True, text=True)
            if result.returncode != 0:
                issues.append(f"Required HDFS directory {directory} not found")

        return len(issues) == 0, issues

    def verify_etl_functionality(self, test_file: str) -> Tuple[bool, List[str]]:
        """Verify ETL pipeline functionality."""
        issues = []
        
        # Upload test file
        result = subprocess.run(['hdfs', 'dfs', '-put', test_file, '/input_data/'],
                              capture_output=True, text=True)
        if result.returncode != 0:
            issues.append("Failed to upload test file to HDFS")
            return False, issues

        # Wait for processing
        time.sleep(30)  # Allow time for processing

        # Check processed output
        result = subprocess.run(['hdfs', 'dfs', '-cat', 
                               f'/processed_data/processed_{test_file}'],
                              capture_output=True, text=True)
        if result.returncode != 0:
            issues.append("Failed to find processed output file")
        else:
            # Verify data transformations
            processed_data = result.stdout
            if "'status': 'failed'" in processed_data:
                issues.append("Found failed transactions in processed data")
            if not all(currency == 'USD' for currency in processed_data):
                issues.append("Not all amounts converted to USD")

        return len(issues) == 0, issues

    def verify_fault_tolerance(self) -> Tuple[bool, List[str]]:
        """Verify system fault tolerance."""
        issues = []
        
        # Get a running ETL pod
        pods = self.k8s_client.list_namespaced_pod(namespace='default')
        etl_pod = next(pod for pod in pods.items if 'etl_pipeline' in pod.metadata.name)
        
        # Delete the pod
        self.k8s_client.delete_namespaced_pod(
            name=etl_pod.metadata.name,
            namespace='default'
        )
        
        # Wait for recovery
        time.sleep(60)
        
        # Verify system is still processing
        pods = self.k8s_client.list_namespaced_pod(namespace='default')
        new_etl_pod = next((pod for pod in pods.items if 'etl_pipeline' in pod.metadata.name), None)
        
        if not new_etl_pod or new_etl_pod.status.phase != 'Running':
            issues.append("System did not recover after pod deletion")

        return len(issues) == 0, issues

    def perform_load_test(self, num_files: int = 100) -> Tuple[bool, Dict]:
        """Perform load testing and collect metrics."""
        results = {
            'files_processed': 0,
            'avg_processing_time': 0,
            'success_rate': 0
        }
        
        start_time = time.time()
        
        # Generate and upload test files
        for i in range(num_files):
            subprocess.run(['hdfs', 'dfs', '-put', f'test_data_{i}.csv', '/input_data/'])
        
        # Monitor processing
        processed_files = 0
        timeout = time.time() + 600  # 10 minute timeout
        
        while time.time() < timeout and processed_files < num_files:
            result = subprocess.run(['hdfs', 'dfs', '-ls', '/processed_data'],
                                  capture_output=True, text=True)
            processed_files = len(result.stdout.splitlines()) - 1  # Subtract header line
            time.sleep(5)
        
        end_time = time.time()
        
        results['files_processed'] = processed_files
        results['avg_processing_time'] = (end_time - start_time) / processed_files
        results['success_rate'] = processed_files / num_files
        
        return results['success_rate'] >= 0.95, results

    def run_full_verification(self) -> Dict:
        """Run all verification checks and return results."""
        results = {
            'prerequisites': {},
            'kubernetes': {},
            'hdfs': {},
            'functionality': {},
            'fault_tolerance': {},
            'load_test': {},
            'overall_status': 'FAILED'
        }
        
        # Run all checks
        results['prerequisites']['status'], results['prerequisites']['issues'] = self.check_prerequisites()
        results['kubernetes']['status'], results['kubernetes']['issues'] = self.verify_kubernetes_objects()
        results['hdfs']['status'], results['hdfs']['issues'] = self.verify_hdfs_structure()
        results['functionality']['status'], results['functionality']['issues'] = self.verify_etl_functionality('test_data.csv')
        results['fault_tolerance']['status'], results['fault_tolerance']['issues'] = self.verify_fault_tolerance()
        results['load_test']['status'], results['load_test']['metrics'] = self.perform_load_test()
        
        # Calculate overall status
        all_passed = all(results[key]['status'] for key in results if key != 'overall_status')
        results['overall_status'] = 'PASSED' if all_passed else 'FAILED'
        
        return results
