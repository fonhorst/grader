import logging
from typing import Dict, List, Optional
import subprocess
import json
import re
from .base_estimator import BaseEstimator

class SparkDataComputeParallelismEstimator(BaseEstimator):
    def __init__(self):
        super().__init__()
        self.logger = logging.getLogger(__name__)
        
    def _check_kubernetes_infrastructure(self) -> Dict[str, bool]:
        """Verify Kubernetes cluster and Spark configuration"""
        results = {
            "pods_running": False,
            "spark_cluster_available": False,
            "dynamic_allocation_enabled": False
        }
        
        try:
            # Check pods
            pod_output = subprocess.check_output(["kubectl", "get", "pods"]).decode()
            results["pods_running"] = "Running" in pod_output
            
            # Check Spark cluster
            spark_pods = subprocess.check_output(
                ["kubectl", "get", "pods", "-l", "app=spark"]
            ).decode()
            results["spark_cluster_available"] = "spark-master" in spark_pods
            
            # Check dynamic allocation
            spark_conf = subprocess.check_output([
                "kubectl", "exec", "-it", "spark-master-0", "--",
                "cat", "/opt/spark/conf/spark-defaults.conf"
            ]).decode()
            results["dynamic_allocation_enabled"] = "spark.dynamicAllocation.enabled=true" in spark_conf
            
        except subprocess.CalledProcessError as e:
            self.logger.error(f"Infrastructure check failed: {str(e)}")
            
        return results

    def _verify_spark_jobs(self) -> Dict[str, bool]:
        """Verify the three main Spark jobs"""
        results = {
            "data_preprocessing": False,
            "model_training": False,
            "hyperparam_search": False
        }
        
        job_classes = {
            "data_preprocessing": "com.example.DataPreprocessing",
            "model_training": "com.example.ModelTraining", 
            "hyperparam_search": "com.example.HyperparamSearch"
        }
        
        for job_name, class_name in job_classes.items():
            try:
                # Submit job and capture output
                cmd = [
                    "spark-submit",
                    "--master", "k8s://kubernetes-master",
                    "--class", class_name,
                    "local:///opt/spark/jars/app.jar"
                ]
                output = subprocess.check_output(cmd).decode()
                
                # Check for successful completion
                results[job_name] = "SUCCESS" in output
                
            except subprocess.CalledProcessError as e:
                self.logger.error(f"Job {job_name} failed: {str(e)}")
                
        return results

    def _check_monitoring(self) -> Dict[str, bool]:
        """Verify monitoring and metrics collection"""
        results = {
            "grafana_dashboard": False,
            "metrics_available": False,
            "logs_accessible": False
        }
        
        try:
            # Check Grafana dashboard
            dashboard_response = subprocess.check_output([
                "curl", "-s", "http://grafana:3000/api/dashboards/uid/monitoring_dashboard"
            ]).decode()
            results["grafana_dashboard"] = "monitoring_dashboard" in dashboard_response
            
            # Check metrics
            metrics_response = subprocess.check_output([
                "curl", "-s", "http://prometheus:9090/api/v1/query?query=spark_executor_count"
            ]).decode()
            results["metrics_available"] = len(json.loads(metrics_response)["data"]) > 0
            
            # Check logs
            logs = subprocess.check_output([
                "kubectl", "logs", "spark-executor-0"
            ]).decode()
            results["logs_accessible"] = len(logs) > 0
            
        except (subprocess.CalledProcessError, json.JSONDecodeError) as e:
            self.logger.error(f"Monitoring check failed: {str(e)}")
            
        return results

    def _run_performance_tests(self) -> Dict[str, float]:
        """Run performance and fault tolerance tests"""
        results = {
            "avg_job_duration": 0.0,
            "parallel_tasks": 0,
            "error_rate": 0.0
        }
        
        try:
            # Generate test load
            subprocess.run(["python", "load_generator.py"])
            
            # Measure metrics
            metrics_output = subprocess.check_output([
                "curl", "-s", 
                "http://prometheus:9090/api/v1/query?query=spark_job_duration_seconds"
            ]).decode()
            metrics_data = json.loads(metrics_output)
            
            if metrics_data.get("data"):
                results["avg_job_duration"] = float(metrics_data["data"]["result"][0]["value"][1])
                results["parallel_tasks"] = len(metrics_data["data"]["result"])
                
                # Calculate error rate
                error_metrics = subprocess.check_output([
                    "curl", "-s",
                    "http://prometheus:9090/api/v1/query?query=spark_job_failures_total"
                ]).decode()
                error_data = json.loads(error_metrics)
                total_errors = float(error_data["data"]["result"][0]["value"][1])
                total_jobs = float(metrics_data["data"]["result"][0]["value"][1])
                results["error_rate"] = total_errors / total_jobs if total_jobs > 0 else 1.0
                
        except (subprocess.CalledProcessError, json.JSONDecodeError) as e:
            self.logger.error(f"Performance test failed: {str(e)}")
            
        return results

    def _check_fault_tolerance(self) -> bool:
        """Test fault tolerance by killing an executor"""
        try:
            # Start a job
            job_process = subprocess.Popen([
                "spark-submit",
                "--master", "k8s://kubernetes-master",
                "--class", "com.example.ModelTraining",
                "local:///opt/spark/jars/app.jar"
            ])
            
            # Kill an executor
            subprocess.run(["kubectl", "delete", "pod", "spark-executor-1"])
            
            # Wait for job completion
            job_process.wait()
            
            # Check if job completed successfully
            return job_process.returncode == 0
            
        except subprocess.CalledProcessError as e:
            self.logger.error(f"Fault tolerance test failed: {str(e)}")
            return False

    def estimate(self) -> Dict[str, float]:
        """
        Run all checks and return final estimation.
        
        Returns:
            Dict[str, float]: Dictionary containing:
                - score: float value representing the total score
                - max_score: float value representing maximum possible score (100.0)
                - details: dictionary of component scores
        """
        scores = {
            "infrastructure": 0.0,
            "functionality": 0.0,
            "monitoring": 0.0,
            "performance": 0.0,
            "fault_tolerance": 0.0
        }
        
        # Check infrastructure (20%)
        infra_results = self._check_kubernetes_infrastructure()
        scores["infrastructure"] = sum(infra_results.values()) / len(infra_results) * 20
        
        # Check functionality (30%)
        func_results = self._verify_spark_jobs()
        scores["functionality"] = sum(func_results.values()) / len(func_results) * 30
        
        # Check monitoring (15%)
        monitoring_results = self._check_monitoring()
        scores["monitoring"] = sum(monitoring_results.values()) / len(monitoring_results) * 15
        
        # Check performance (20%)
        perf_results = self._run_performance_tests()
        # Normalize performance metrics
        normalized_duration = min(1.0, 60.0 / max(perf_results["avg_job_duration"], 1))
        normalized_parallel = min(1.0, perf_results["parallel_tasks"] / 10)
        normalized_errors = 1.0 - perf_results["error_rate"]
        scores["performance"] = ((normalized_duration + normalized_parallel + normalized_errors) / 3) * 20
        
        # Check fault tolerance (15%)
        fault_tolerance_result = self._check_fault_tolerance()
        scores["fault_tolerance"] = 15.0 if fault_tolerance_result else 0.0
        
        # Calculate total score
        total_score = sum(scores.values())
        
        return {
            "score": total_score,
            "max_score": 100.0,
            "details": scores
        }
