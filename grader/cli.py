import logging
import sys
import click
import yaml
import requests
from pydantic import BaseModel
from typing import Optional, List, Dict
import os
import json
import uuid
from datetime import datetime


logger = logging.getLogger(__name__)


def get_api_url() -> str:
    """Get the API URL from environment variable or use default."""
    api_url = os.getenv('GRADER_API_URL', 'http://localhost:8080')
    logger.info(f"Using API URL: {api_url}")
    return api_url


def format_task_info(task_data: dict) -> str:
    """Format task information in a human-readable way with colors."""
    status = task_data.get('status', 'UNKNOWN')
    status_color = {
        'PENDING': 'yellow',
        'RUNNING': 'blue',
        'COMPLETED': 'green',
        'FAILED': 'red',
        'CANCELLED': 'red',
        'ERROR': 'red'
    }.get(status, 'white')

    formatted = [
        click.style(f"Task ID: {task_data.get('id')}", bold=True),
        click.style(f"Status: {status}", fg=status_color, bold=True),
        f"Name: {task_data.get('name', 'N/A')}",
        f"User ID: {task_data.get('user_id', 'N/A')}",
        f"Tag: {task_data.get('tag', 'N/A')}",
        f"Submit Time: {task_data.get('submit_time', 'N/A')}",
        f"End Time: {task_data.get('end_time', 'N/A')}"
    ]
    
    return "\n".join(formatted)


@click.group()
@click.option('--verbose', is_flag=True, help='Enable verbose logging')
def cli(verbose: bool):
    """Grader CLI tool for managing and running checks."""
    logging.basicConfig(
        level=logging.DEBUG if verbose else logging.INFO,
        format="%(asctime)s [%(levelname)8s] %(message)s (%(filename)s:%(lineno)s)", 
        datefmt="%Y-%m-%d %H:%M:%S"
    )

    if not verbose:
        logging.getLogger("requests").setLevel(logging.WARNING)
        logging.getLogger("urllib3").setLevel(logging.WARNING)
        logging.getLogger("httpx").setLevel(logging.WARNING)
    else:
        # In verbose mode, set all loggers to DEBUG
        logging.getLogger("requests").setLevel(logging.DEBUG)
        logging.getLogger("urllib3").setLevel(logging.DEBUG)
        logging.getLogger("httpx").setLevel(logging.DEBUG)

    logger.info("Starting Grader CLI")
    ctx = click.get_current_context()
    if ctx.parent is None:
        logger.info(f"CLI invoked with args: {sys.argv}")


@cli.group()
def task():
    pass


@cli.group()
def checker():
    pass


@cli.group()
def api():
    pass


@cli.group()
def k8s():
    pass


@task.command()
@click.option('--check-type', '-t', required=True, type=str, help='Type of check to perform (e.g. "clickhouse")')
@click.option('--user-id', '-u', required=True, type=str, help='User ID for the task')
@click.option('--name', '-n', type=str, help='Optional name for the task')
@click.option('--tag', type=str, help='Optional tag for grouping tasks')
@click.option_group(
    'args_source',
    mutually_exclusive=True,
    help='Source of checker arguments (either direct JSON or file)',
    cls=click.MutuallyExclusiveOptionGroup,
    options=[
        click.Option(['--args'], type=str, help='JSON string with arguments for the checker'),
        click.Option(['--args-file'], type=click.Path(exists=True, dir_okay=False), help='Path to JSON file containing arguments for the checker')
    ]
)
def submit(check_type: str, user_id: str, name: str, tag: str, args: str, args_file: str):
    """Submit a new checking task."""
    logger.info(f"Submitting new task for user {user_id} with check type {check_type}")
    try:
        checker_args = {}
        if args:
            logger.debug("Parsing args from command line JSON string")
            checker_args = json.loads(args)
        elif args_file:
            logger.debug(f"Loading args from file: {args_file}")
            with open(args_file) as f:
                checker_args = json.load(f)
        
        data = {
            "check_type": check_type,
            "user_id": user_id,
            "name": name,
            "tag": tag,
            "args": checker_args
        }
        logger.info(f"Submitting task with data: {data}")
        
        api_url = get_api_url()
        response = requests.post(f"{api_url}/tasks/", json=data)
        response.raise_for_status()
        result = response.json()
        logger.info(f"Task submitted successfully with ID: {result.get('id')}")
        click.echo(json.dumps(result, indent=2))
    except Exception as e:
        logger.error(f"Error submitting task: {str(e)}", exc_info=True)
        click.echo(f"Failed to submit task: {str(e)}", err=True)
        sys.exit(1)


@task.command()
@click.option('--user-id', '-u', type=str, help='Filter tasks by user ID')
@click.option('--tag', '-t', type=str, help='Filter tasks by tag')
@click.option('--status', '-s', type=str, help='Filter tasks by status')
def list(user_id: str, tag: str, status: str):
    """List tasks with optional filtering."""
    logger.info("Listing tasks with filters")
    try:
        params = {}
        if user_id is not None:
            params['user_id'] = user_id
        if tag is not None:
            params['tag'] = tag
        if status is not None:
            params['status'] = status
        
        logger.info(f"Using filter params: {params}")
        api_url = get_api_url()
        response = requests.get(f"{api_url}/tasks/", params=params)
        response.raise_for_status()
        result = response.json()
        logger.info(f"Found {len(result.get('tasks', []))} tasks")
        click.echo(json.dumps(result, indent=2))
    except Exception as e:
        logger.error(f"Error listing tasks: {str(e)}", exc_info=True)
        click.echo(f"Failed to list tasks: {str(e)}", err=True)
        sys.exit(1)


@task.command()
@click.option('--task-id', '-i', required=True, type=str, help='ID of the task to retrieve')
@click.option('--json-file', type=click.Path(dir_okay=False), help='Save task info to this JSON file')
@click.option('--report-file', type=click.Path(dir_okay=False), help='Save task report to this Markdown file if available')
def get(task_id: str, json_file: str, report_file: str):
    """Get information about a specific task."""
    logger.info(f"Getting task info for ID: {task_id}")
    try:
        api_url = get_api_url()
        response = requests.get(f"{api_url}/tasks/{task_id}")
        response.raise_for_status()
        task_data = response.json()
        
        logger.debug(f"Retrieved task data: {task_data}")
        
        # Print formatted task info
        click.echo(format_task_info(task_data))
        
        # Save JSON if requested
        if json_file:
            logger.debug(f"Saving task info to JSON file: {json_file}")
            with open(json_file, 'w') as f:
                json.dump(task_data, f, indent=2)
            click.echo(f"\nTask info saved to {json_file}")
        
        # Save report if requested and available
        if report_file and task_data.get('report'):
            logger.debug(f"Saving report to file: {report_file}")
            with open(report_file, 'w') as f:
                f.write(task_data['report'])
            click.echo(f"Report saved to {report_file}")
        elif report_file:
            click.echo("\nNo report available for this task", err=True)
            
    except Exception as e:
        logger.error(f"Error getting task: {str(e)}", exc_info=True)
        click.echo(f"Failed to get task: {str(e)}", err=True)
        sys.exit(1)


@task.command()
@click.option('--task-id', '-i', required=True, type=str, help='ID of the task to cancel')
@click.option('--json-file', type=click.Path(dir_okay=False), help='Save response to this JSON file')
def cancel(task_id: str, json_file: str):
    """Cancel a running task."""
    logger.info(f"Canceling task with ID: {task_id}")
    try:
        api_url = get_api_url()
        response = requests.post(f"{api_url}/tasks/{task_id}/cancel")
        response.raise_for_status()
        task_data = response.json()
        
        logger.debug(f"Task cancel response: {task_data}")
        
        # Print formatted task info
        click.echo(format_task_info(task_data))
        
        # Save JSON if requested
        if json_file:
            logger.debug(f"Saving response to JSON file: {json_file}")
            with open(json_file, 'w') as f:
                json.dump(task_data, f, indent=2)
            click.echo(f"\nResponse saved to {json_file}")
            
    except Exception as e:
        logger.error(f"Error canceling task: {str(e)}", exc_info=True)
        click.echo(f"Failed to cancel the task: {str(e)}", err=True)
        sys.exit(1)


@task.command()
@click.option('--task-id', '-i', required=True, type=str, help='ID of the task to delete')
@click.option('--json-file', type=click.Path(dir_okay=False), help='Save response to this JSON file')
def delete(task_id: str, json_file: str):
    """Delete a task."""
    logger.info(f"Deleting task with ID: {task_id}")
    try:
        api_url = get_api_url()
        response = requests.delete(f"{api_url}/tasks/{task_id}")
        response.raise_for_status()
        task_data = response.json()
        
        logger.debug(f"Task deletion response: {task_data}")
        
        # Print formatted task info
        click.echo(format_task_info(task_data))
        
        # Save JSON if requested
        if json_file:
            logger.debug(f"Saving response to JSON file: {json_file}")
            with open(json_file, 'w') as f:
                json.dump(task_data, f, indent=2)
            click.echo(f"\nResponse saved to {json_file}")
            
    except Exception as e:
        logger.error(f"Error deleting task: {str(e)}", exc_info=True)
        click.echo(f"Failed to delete the task: {str(e)}", err=True)
        sys.exit(1)


@task.command()
@click.option('--task-id', '-i', required=True, type=str, help='ID of the task to get report for')
@click.option('--output-file', '-o', type=click.Path(dir_okay=False), required=True, help='Save report to this file (Markdown format)')
def report(task_id: str, output_file: str):
    """Get the report from a finished task."""
    logger.info(f"Getting report for task ID: {task_id}")
    try:
        api_url = get_api_url()
        
        # First get task info to check status
        task_response = requests.get(f"{api_url}/tasks/{task_id}")
        task_response.raise_for_status()
        task_data = task_response.json()
        logger.debug(f"Retrieved task data: {task_data}")
        
        # Get report
        report_response = requests.get(f"{api_url}/tasks/{task_id}/report")
        report_response.raise_for_status()
        report_data = report_response.json()
        
        if not report_data.get('report'):
            click.echo(format_task_info(task_data))
            click.echo("\nNo report available for this task", err=True)
            return
        
        # Save report in markdown format
        logger.debug(f"Saving report to file: {output_file}")
        with open(output_file, 'w') as f:
            f.write(report_data['report'])
        click.echo(f"Report saved to {output_file}")
        
    except Exception as e:
        logger.error(f"Error getting report: {str(e)}", exc_info=True)
        click.echo(f"Failed to get the report: {str(e)}", err=True)
        sys.exit(1)


@checker.command()
@click.option('--host', '-h', default="localhost", show_default=True, help='ClickHouse host address')
@click.option('--user', '-u', default="admin", show_default=True, help='Admin username')
@click.option('--student', '-s', required=True, help='Student username to check')
@click.option('--cluster-name', '-c', default="main_cluster", show_default=True, help='ClickHouse cluster name')
@click.option('--output', '-o', type=click.Path(dir_okay=False), required=True, help='Path to save the report in Markdown format')
@click.option('--log-file', '-l', type=click.Path(dir_okay=False), help='Path to save logs')
def clickhouse(host: str, user: str, student: str, cluster_name: str, output: str, log_file: str):
    """Run ClickHouse checker directly.
    
    This command runs the ClickHouse checker without using the task queue service.
    It will prompt for the admin password securely during execution.
    """
    from grader.checking.checking import run_checking, CheckType
    import getpass
    
    try:
        # Get password securely
        password = getpass.getpass(f"Enter ClickHouse password for {user}: ")
        
        report = run_checking(
            check_type=CheckType.CLICKHOUSE,
            host=host,
            user=user,
            password=password,
            student_username=student,
            cluster_name=cluster_name
        )
        
        # Save report as Markdown
        markdown_report = report.to_markdown()
        with open(output, 'w') as f:
            f.write(markdown_report)
        click.echo(f"Report saved to {output}")
        
        if not report.has_success():
            click.echo("Checking failed!", err=True)
            exit(1)
        click.echo("Checking completed successfully!")
        
    except Exception as e:
        click.echo(f"Error running checker: {str(e)}", err=True)
        exit(1)


@checker.command()
@click.option('--checker', required=True, type=str, help='Fully qualified name of the checker class (e.g. grader.checking.ch_checker.ClickHouseChecker)')
@click.option('--arguments', required=True, type=click.Path(exists=True, dir_okay=False), help='Path to JSON file with checker arguments')
@click.option('--output', required=True, type=click.Path(dir_okay=False), help='Output path for the report in Markdown format')
def run(checker: str, arguments: str, output: str):
    """Run an arbitrary checker directly.
    
    This command allows running any checker by specifying its fully qualified class name
    and providing arguments through a JSON file.
    """
    try:
        # Import the checker class dynamically
        module_path, class_name = checker.rsplit('.', 1)
        import importlib
        module = importlib.import_module(module_path)
        checker_class = getattr(module, class_name)
        
        # Load arguments
        with open(arguments) as f:
            checker_args = json.load(f)
        
        # Initialize and run checker
        checker_instance = checker_class(**checker_args)
        report = checker_instance.run_checks()
        
        # Save report as Markdown
        markdown_report = report.to_markdown()
        with open(output, 'w') as f:
            f.write(markdown_report)
        click.echo(f"Report saved to {output}")
        
        if not report.has_success():
            click.echo("Checking failed!", err=True)
            exit(1)
        click.echo("Checking completed successfully!")
        
    except Exception as e:
        click.echo(f"Error running checker: {str(e)}", err=True)
        sys.exit(1)


@api.command()
@click.option('--host', '-h', default="0.0.0.0", show_default=True, help='Host address to bind to')
@click.option('--port', '-p', default=8080, show_default=True, type=int, help='Port to listen on')
@click.option('--reload', '-r', is_flag=True, help='Enable auto-reload on code changes')
def start(host: str, port: int, reload: bool):
    """Start the REST API server."""
    logger.info(f"Starting API server on {host}:{port}")
    import uvicorn
    from grader.app import app
    
    logger.info("API server configured with Swagger UI at /docs")
    try:
        uvicorn.run(
            app,
            host=host,
            port=port,
            reload=reload,
            log_level="debug" if logger.getEffectiveLevel() <= logging.DEBUG else "info"
        )
    except Exception as e:
        logger.error(f"Error starting API server: {str(e)}", exc_info=True)
        click.echo(f"Failed to start API server: {str(e)}", err=True)
        sys.exit(1)


@k8s.command()
def info():
    """Show instructions for installing components on Kubernetes."""
    instructions = """
Kubernetes Installation Instructions
=================================

Prerequisites:
- Kubernetes cluster with Helm installed
- Storage class 'ess-dn2' available in the cluster
- Access to the required container registries

Installation Steps:

1. Install HDFS Chart
-------------------
cd k8s
helm install hdfs ./hdfs-chart -f hdfs-values.yaml

This will deploy:
- HDFS NameNode with 30Gi storage
- HDFS DataNode with 100Gi storage
- Services for NameNode (NodePort) and DataNode
- Default replication factor: 1

2. Install ClickHouse Chart
------------------------
cd k8s
helm install clickhouse ./ch-chart -f ch-values.yaml

This will deploy:
- ClickHouse cluster with 3 replicas
- Using storage class 'ess-dn2'

3. Install Workspace
-----------------
cd k8s
helm install workspace ./Workspace

Monitor the Installation:
-----------------------
kubectl get pods    # Check pod status
kubectl get pvc    # Check persistent volume claims
kubectl get svc    # Check services

Notes:
- Make sure all pods are in Running state before proceeding
- Check logs if any pod fails to start: kubectl logs <pod-name>
- For troubleshooting: kubectl describe pod <pod-name>
"""
    click.echo(instructions)


@k8s.command()
@click.option('--output', '-o', type=click.Path(dir_okay=False), required=True, help='Path to save the installation script')
def install_script(output: str):
    """Generate a bash script for installing all components on Kubernetes."""
    script_content = """#!/bin/bash
set -e

echo "Starting Grader components installation..."

# Function to check if a command exists
check_command() {
    if ! command -v $1 &> /dev/null; then
        echo "Error: $1 is required but not installed."
        exit 1
    fi
}

# Check prerequisites
echo "Checking prerequisites..."
check_command kubectl
check_command helm

# Check if we can connect to the cluster
kubectl cluster-info || {
    echo "Error: Cannot connect to Kubernetes cluster"
    exit 1
}

# Check if storage class exists
kubectl get storageclass ess-dn2 || {
    echo "Error: Storage class 'ess-dn2' not found"
    exit 1
}

# Function to wait for pods to be ready
wait_for_pods() {
    namespace=$1
    echo "Waiting for pods in namespace $namespace to be ready..."
    kubectl wait --for=condition=ready pod --all -n $namespace --timeout=300s
}

# Create namespace if it doesn't exist
kubectl create namespace grader 2>/dev/null || true

echo "Installing HDFS..."
cd k8s
helm install hdfs ./hdfs-chart -f hdfs-values.yaml -n grader || {
    echo "Error installing HDFS chart"
    exit 1
}

echo "Installing ClickHouse..."
helm install clickhouse ./ch-chart -f ch-values.yaml -n grader || {
    echo "Error installing ClickHouse chart"
    exit 1
}

echo "Installing Workspace..."
helm install workspace ./Workspace -n grader || {
    echo "Error installing Workspace chart"
    exit 1
}

echo "Waiting for all pods to be ready..."
wait_for_pods grader

echo "Installation complete! Checking component status..."
kubectl get pods -n grader
kubectl get pvc -n grader
kubectl get svc -n grader

echo "
Installation successful! Here are some useful commands:

Check pod status:    kubectl get pods -n grader
Check services:      kubectl get svc -n grader
Check PVCs:          kubectl get pvc -n grader
View pod logs:       kubectl logs -n grader <pod-name>
Pod details:         kubectl describe pod -n grader <pod-name>
"
"""
    
    try:
        with open(output, 'w') as f:
            f.write(script_content)
        os.chmod(output, 0o755)  # Make the script executable
        click.echo(f"Installation script saved to {output}")
        click.echo("You can now run the script to install all components.")
    except Exception as e:
        click.echo(f"Error creating installation script: {str(e)}", err=True)


if __name__ == "__main__":
    cli()

