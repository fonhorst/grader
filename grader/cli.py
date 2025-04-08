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
        # TODO: DO NOT use locals() here, build a proper params dict instead with explicit keys
        params = {k: v for k, v in locals().items() if v is not None}
        response = requests.get("http://localhost:8000/tasks/", params=params)
        response.raise_for_status()
        click.echo(json.dumps(response.json(), indent=2))
    except Exception as e:
        click.echo(f"Error listing tasks: {str(e)}", err=True)

# TODO: format beautifully in a human-readable format the info about the task and print it on screen 
# (use different colors to highlight the most important fields like id, status, name. BAD statuses should be RED)
# TODO: add an option to save the info to a json file, but make printing the default behavior
# TODO: add an option to save report to a markdown file if it is available. DO NOT print report on the screen in any sutuations.
@task.command()
@click.option('--task-id', '-i', required=True, type=str, help='ID of the task to retrieve')
def get(task_id: str):
    """Get information about a specific task."""
    try:
        response = requests.get(f"http://localhost:8000/tasks/{task_id}")
        response.raise_for_status()
        click.echo(json.dumps(response.json(), indent=2))
    except Exception as e:
        click.echo(f"Error getting task: {str(e)}", err=True)


# TODO:work with the status the same way as described in the TODO for the get command
@task.command()
@click.option('--task-id', '-i', required=True, type=str, help='ID of the task to cancel')
def cancel(task_id: str):
    """Cancel a running task."""
    try:
        response = requests.post(f"http://localhost:8000/tasks/{task_id}/cancel")
        response.raise_for_status()
        click.echo(json.dumps(response.json(), indent=2))
    except Exception as e:
        click.echo(f"Error canceling task: {str(e)}", err=True)


# TODO:work with the status the same way as described in the TODO for the get command
@task.command()
@click.option('--task-id', '-i', required=True, type=str, help='ID of the task to delete')
def delete(task_id: str):
    """Delete a task."""
    try:
        response = requests.delete(f"http://localhost:8000/tasks/{task_id}")
        response.raise_for_status()
        click.echo(json.dumps(response.json(), indent=2))
    except Exception as e:
        click.echo(f"Error deleting task: {str(e)}", err=True)


# TODO: save the report to a markdown file instead of JSON
# TODO: if report is not available, print an error message and specify the task status
@task.command()
@click.option('--task-id', '-i', required=True, type=str, help='ID of the task to get report for')
@click.option('--output-file', '-o', type=click.Path(dir_okay=False), help='Save report to this file (JSON format)')
def report(task_id: str, output_file: str):
    """Get the report from a finished task.
    
    If --output-file is specified, saves the report to the file in JSON format.
    Otherwise, prints the report to stdout.
    """
    try:
        response = requests.get(f"http://localhost:8000/tasks/{task_id}/report")
        response.raise_for_status()
        report_data = response.json()
        
        if output_file:
            with open(output_file, 'w') as f:
                json.dump(report_data, f, indent=2)
            click.echo(f"Report saved to {output_file}")
        else:
            click.echo(json.dumps(report_data, indent=2))
    except Exception as e:
        click.echo(f"Error getting report: {str(e)}", err=True)


# TODO: add another universal command for running an arbitrary checker directly from the CLI
# Here is the example of how it should work:
# grader checker run --checker=<fully qualified name of the checker class> --arguments=<path to a json file with checker arguments> --output="Output path for the report in .md format"

# TODO: remove --output-json and --output-markdown options from the command. Add --output option instead. We only allow Markdown output for now.
@click.option('--host', '-h', default="localhost", show_default=True, help='ClickHouse host address')
@click.option('--user', '-u', default="admin", show_default=True, help='Admin username')
@click.option('--student', '-s', required=True, help='Student username to check')
@click.option('--cluster-name', '-c', default="main_cluster", show_default=True, help='ClickHouse cluster name')
@click.option('--output-json', '-j', default="checker_report.json", show_default=True, help='Path to save the JSON report')
@click.option('--output-markdown', '-m', default="checker_report.md", show_default=True, help='Path to save the Markdown report')
@click.option('--log-file', '-l', type=click.Path(dir_okay=False), help='Path to save logs')
def clickhouse(host: str, user: str, student: str, cluster_name: str, output_json: str, output_markdown: str, log_file: str):
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
        
        # Save report as JSON
        with open(output_json, 'w') as f:
            f.write(report.json(indent=2))
        click.echo(f"Saved JSON report to {output_json}")
        
        # Save report as Markdown
        markdown_report = report.to_markdown()
        with open(output_markdown, 'w') as f:
            f.write(markdown_report)
        click.echo(f"Saved Markdown report to {output_markdown}")
        
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

if __name__ == "__main__":
    cli()
