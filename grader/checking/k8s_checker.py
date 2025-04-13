import logging
from kubernetes import client, config
from kubernetes.client import ApiException

from grader.checking.base import CheckerReport, LabChecker

logger = logging.getLogger(__name__)


class KubernetesChecker(LabChecker):
    def __init__(self, namespace: str):
        """Initialize the Kubernetes checker
        
        Args:
            namespace: The namespace to check resources in
        """
        self.namespace = namespace
        try:
            config.load_kube_config()
            self.v1 = client.CoreV1Api()
            self.apps_v1 = client.AppsV1Api()
        except Exception as e:
            logger.error(f"Failed to initialize Kubernetes client: {str(e)}")
            raise e

    def check_configmap(self) -> CheckerReport:
        """Check if the ConfigMap is properly configured"""
        report = CheckerReport()
        try:
            cm = self.v1.read_namespaced_config_map(
                name="lab0-jupyter-cm",
                namespace=self.namespace
            )
            
            # Check if ConfigMap exists
            report.success("ConfigMap 'lab0-jupyter-cm' exists")
            
            # Check ConfigMap data
            if "jupyter_notebook_config.py" in cm.data:
                config_data = cm.data["jupyter_notebook_config.py"]
                if "c.NotebookApp.trust_xheaders = True" in config_data:
                    report.success("ConfigMap has correct trust_xheaders setting")
                else:
                    report.fail("ConfigMap trust_xheaders setting", 
                              "trust_xheaders should be set to True")
                
                if "c.NotebookApp.quit_button = False" in config_data:
                    report.success("ConfigMap has correct quit_button setting")
                else:
                    report.fail("ConfigMap quit_button setting",
                              "quit_button should be set to False")
            else:
                report.fail("ConfigMap data", "Missing jupyter_notebook_config.py data")
                
        except ApiException as e:
            if e.status == 404:
                report.fail("ConfigMap existence", "ConfigMap 'lab0-jupyter-cm' not found")
            else:
                report.fail("ConfigMap check", f"Error checking ConfigMap: {str(e)}")
                
        return report

    def check_deployment(self) -> CheckerReport:
        """Check if the Deployment is properly configured"""
        report = CheckerReport()
        try:
            deployment = self.apps_v1.read_namespaced_deployment(
                name="lab0-jupyter",
                namespace=self.namespace
            )
            
            # Check if Deployment exists
            report.success("Deployment 'lab0-jupyter' exists")
            
            # Check replicas
            if deployment.spec.replicas == 1:
                report.success("Deployment has correct number of replicas")
            else:
                report.fail("Deployment replicas", "Should have exactly 1 replica")
            
            # Check container configuration
            container = deployment.spec.template.spec.containers[0]
            
            # Check image
            if container.image == "node03.st:5000/pyspark-hdfs-jupyter:aal-372475":
                report.success("Deployment has correct container image")
            else:
                report.fail("Deployment image", "Incorrect container image")
            
            # Check ports
            if any(p.container_port == 8282 for p in container.ports):
                report.success("Deployment has correct container port")
            else:
                report.fail("Deployment port", "Container should expose port 8282")
            
            # Check resources
            if container.resources.requests and container.resources.limits:
                if (container.resources.requests["memory"] == "2Gi" and 
                    container.resources.requests["cpu"] == "1"):
                    report.success("Deployment has correct resource requests")
                else:
                    report.fail("Deployment resource requests", 
                              "Incorrect resource requests (should be 2Gi memory, 1 CPU)")
                
                if (container.resources.limits["memory"] == "2Gi" and 
                    container.resources.limits["cpu"] == "1"):
                    report.success("Deployment has correct resource limits")
                else:
                    report.fail("Deployment resource limits", 
                              "Incorrect resource limits (should be 2Gi memory, 1 CPU)")
            else:
                report.fail("Deployment resources", "Missing resource requests/limits")
            
            # Check volume mounts
            volume_mounts = container.volume_mounts
            required_mounts = {
                "/home/jovyan/shared-data": "shared-storage",
                "/home/jovyan/nfs-home": "nfs-home-storage",
                "/home/jovyan/.jupyter/jupyter_notebook_config.py": "jupyter-config"
            }
            
            for mount_path, volume_name in required_mounts.items():
                if any(vm.mount_path == mount_path and vm.name == volume_name 
                      for vm in volume_mounts):
                    report.success(f"Volume mount '{mount_path}' exists")
                else:
                    report.fail("Volume mount", f"Missing required volume mount: {mount_path}")
            
        except ApiException as e:
            if e.status == 404:
                report.fail("Deployment existence", "Deployment 'lab0-jupyter' not found")
            else:
                report.fail("Deployment check", f"Error checking Deployment: {str(e)}")
                
        return report

    def check_service(self) -> CheckerReport:
        """Check if the Service is properly configured"""
        report = CheckerReport()
        try:
            service = self.v1.read_namespaced_service(
                name="lab0-jupyter-service",
                namespace=self.namespace
            )
            
            # Check if Service exists
            report.success("Service 'lab0-jupyter-service' exists")
            
            # Check service type
            if service.spec.type == "NodePort":
                report.success("Service has correct type (NodePort)")
            else:
                report.fail("Service type", "Service should be of type NodePort")
            
            # Check ports
            if any(p.port == 80 and p.target_port == 8282 and p.protocol == "TCP" 
                  for p in service.spec.ports):
                report.success("Service has correct port configuration")
            else:
                report.fail("Service ports", 
                          "Service should have port 80 targeting container port 8282")
            
            # Check selector
            if service.spec.selector.get("jupyter") == "lab0":
                report.success("Service has correct selector")
            else:
                report.fail("Service selector", "Service should select pods with jupyter=lab0")
            
        except ApiException as e:
            if e.status == 404:
                report.fail("Service existence", "Service 'lab0-jupyter-service' not found")
            else:
                report.fail("Service check", f"Error checking Service: {str(e)}")
                
        return report

    def run_checks(self) -> CheckerReport:
        """Run all Kubernetes lab checks"""
        report = CheckerReport()
        
        # Run individual checks
        report.include(self.check_configmap(), "ConfigMap")
        report.include(self.check_deployment(), "Deployment")
        report.include(self.check_service(), "Service")
        
        return report 