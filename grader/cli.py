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


# # Config class using pydantic for validation
# class Config(BaseModel):
#     api_base_url: str
#     user_id: str
#     project: str


# # Load configuration from a `.grader` file
# def load_config():
#     if os.path.exists('.grader'):
#         with open('.grader') as f:
#             return Config(**yaml.safe_load(f))
#     else:
#         raise FileNotFoundError("Configuration file '.grader' not found.")


# # Context object to store config
# class Context:
#     def __init__(self):
#         self.config = load_config()


# pass_context = click.make_pass_decorator(Context, ensure=True)

@click.option('--verbose', is_flag=True, help='Enable verbose logging')
def cli(verbose: bool):
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)8s] %(message)s (%(filename)s:%(lineno)s)", 
        datefmt="%Y-%m-%d %H:%M:%S"
    )

    if not verbose:
        # Set higher log levels for HTTP client libraries to suppress request logs
        # todo: a subject of being switched to INFO if user asks for more verbose logs with '--verbose' flag
        logging.getLogger("requests").setLevel(logging.WARNING)
        logging.getLogger("urllib3").setLevel(logging.WARNING)
        logging.getLogger("httpx").setLevel(logging.WARNING)

    click.echo("Grader CLI")
    ctx = click.get_current_context()
    if ctx.parent is None:  # Only log at the top level
        logger.info(f"CLI invoked with args: {sys.argv}")
    pass


# # Main group command 'tasks' with alias 'task'
# @cli.group(invoke_without_command=True, cls=click.Group, )
# @pass_context
# def task(ctx):
#     """Task management utility."""
#     click.echo("Task management utility loaded.")


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
    """Submit a new checking task.
    
    The checker arguments can be provided either directly via --args as a JSON string,
    or through a JSON file specified with --args-file. These options are mutually exclusive.
    """
    try:
        checker_args = {}
        if args:
            checker_args = json.loads(args)
        elif args_file:
            with open(args_file) as f:
                checker_args = json.load(f)
        
        data = {
            "check_type": check_type,
            "user_id": user_id,
            "name": name,
            "tag": tag,
            "args": checker_args
        }
        response = requests.post("http://localhost:8000/tasks/", json=data)
        response.raise_for_status()
        click.echo(json.dumps(response.json(), indent=2))
    except Exception as e:
        click.echo(f"Error submitting task: {str(e)}", err=True)


@task.command()
@click.option('--user-id', '-u', type=str, help='Filter tasks by user ID')
@click.option('--tag', '-t', type=str, help='Filter tasks by tag')
@click.option('--status', '-s', type=str, help='Filter tasks by status')
def list(user_id: str, tag: str, status: str):
    """List tasks with optional filtering.
    
    All filter parameters are optional. If none are provided, all tasks will be listed.
    """
    try:
        params = {}
        if user_id is not None:
            params['user_id'] = user_id
        if tag is not None:
            params['tag'] = tag
        if status is not None:
            params['status'] = status
            
        response = requests.get("http://localhost:8000/tasks/", params=params)
        response.raise_for_status()
        click.echo(json.dumps(response.json(), indent=2))
    except Exception as e:
        click.echo(f"Error listing tasks: {str(e)}", err=True)

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

@task.command()
@click.option('--task-id', '-i', required=True, type=str, help='ID of the task to retrieve')
@click.option('--json-file', type=click.Path(dir_okay=False), help='Save task info to this JSON file')
@click.option('--report-file', type=click.Path(dir_okay=False), help='Save task report to this Markdown file if available')
def get(task_id: str, json_file: str, report_file: str):
    """Get information about a specific task.
    
    Displays task information in a human-readable format with color highlighting.
    Optionally saves the raw data to a JSON file and/or the report to a Markdown file.
    """
    try:
        response = requests.get(f"http://localhost:8000/tasks/{task_id}")
        response.raise_for_status()
        task_data = response.json()
        
        # Print formatted task info
        click.echo(format_task_info(task_data))
        
        # Save JSON if requested
        if json_file:
            with open(json_file, 'w') as f:
                json.dump(task_data, f, indent=2)
            click.echo(f"\nTask info saved to {json_file}")
        
        # Save report if requested and available
        if report_file and task_data.get('report'):
            with open(report_file, 'w') as f:
                f.write(task_data['report'])
            click.echo(f"Report saved to {report_file}")
        elif report_file:
            click.echo("\nNo report available for this task", err=True)
            
    except Exception as e:
        click.echo(f"Error getting task: {str(e)}", err=True)

@task.command()
@click.option('--task-id', '-i', required=True, type=str, help='ID of the task to cancel')
@click.option('--json-file', type=click.Path(dir_okay=False), help='Save response to this JSON file')
def cancel(task_id: str, json_file: str):
    """Cancel a running task."""
    try:
        response = requests.post(f"http://localhost:8000/tasks/{task_id}/cancel")
        response.raise_for_status()
        task_data = response.json()
        
        # Print formatted task info
        click.echo(format_task_info(task_data))
        
        # Save JSON if requested
        if json_file:
            with open(json_file, 'w') as f:
                json.dump(task_data, f, indent=2)
            click.echo(f"\nResponse saved to {json_file}")
            
    except Exception as e:
        click.echo(f"Error canceling task: {str(e)}", err=True)

@task.command()
@click.option('--task-id', '-i', required=True, type=str, help='ID of the task to delete')
@click.option('--json-file', type=click.Path(dir_okay=False), help='Save response to this JSON file')
def delete(task_id: str, json_file: str):
    """Delete a task."""
    try:
        response = requests.delete(f"http://localhost:8000/tasks/{task_id}")
        response.raise_for_status()
        task_data = response.json()
        
        # Print formatted task info
        click.echo(format_task_info(task_data))
        
        # Save JSON if requested
        if json_file:
            with open(json_file, 'w') as f:
                json.dump(task_data, f, indent=2)
            click.echo(f"\nResponse saved to {json_file}")
            
    except Exception as e:
        click.echo(f"Error deleting task: {str(e)}", err=True)


# TODO: save the report to a markdown file instead of JSON
# TODO: if report is not available, print an error message and specify the task status
@task.command()
@click.option('--task-id', '-i', required=True, type=str, help='ID of the task to get report for')
@click.option('--output-file', '-o', type=click.Path(dir_okay=False), required=True, help='Save report to this file (Markdown format)')
def report(task_id: str, output_file: str):
    """Get the report from a finished task.
    
    Saves the report to the specified file in Markdown format.
    If the report is not available, displays the task status and an error message.
    """
    try:
        # First get task info to check status
        task_response = requests.get(f"http://localhost:8000/tasks/{task_id}")
        task_response.raise_for_status()
        task_data = task_response.json()
        
        # Get report
        report_response = requests.get(f"http://localhost:8000/tasks/{task_id}/report")
        report_response.raise_for_status()
        report_data = report_response.json()
        
        if not report_data.get('report'):
            click.echo(format_task_info(task_data))
            click.echo("\nNo report available for this task", err=True)
            return
        
        # Save report in markdown format
        with open(output_file, 'w') as f:
            f.write(report_data['report'])
        click.echo(f"Report saved to {output_file}")
        
    except Exception as e:
        click.echo(f"Error getting report: {str(e)}", err=True)


# TODO: add another universal command for running an arbitrary checker directly from the CLI
# Here is the example of how it should work:
# grader checker run --checker=<fully qualified name of the checker class> --arguments=<path to a json file with checker arguments> --output="Output path for the report in .md format"

# TODO: remove --output-json and --output-markdown options from the command. Add --output option instead. We only allow Markdown output for now.
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
        exit(1)


# TODO: add swagger UI endpoint to the API server
@api.command()
@click.option('--host', '-h', default="0.0.0.0", show_default=True, help='Host address to bind to')
@click.option('--port', '-p', default=8080, show_default=True, type=int, help='Port to listen on')
@click.option('--reload', '-r', is_flag=True, help='Enable auto-reload on code changes')
def start(host: str, port: int, reload: bool):
    """Start the REST API server.
    
    Starts a Uvicorn server for the REST API. The server can be configured to
    auto-reload on code changes using the --reload flag.
    """
    import uvicorn
    from grader.app import app
    
    try:
        uvicorn.run(
            app,
            host=host,
            port=port,
            reload=reload
        )
    except Exception as e:
        click.echo(f"Error starting API server: {str(e)}", err=True)
        exit(1)


# TODO: add a command 'install-script' that will generate a bash script for installing all the components on Kubernetes
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
