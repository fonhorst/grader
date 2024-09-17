import click
import yaml
import requests
from pydantic import BaseModel
from typing import Optional, List, Dict
import os
import json


# Config class using pydantic for validation
class Config(BaseModel):
    api_base_url: str
    user_id: str
    project: str


# Load configuration from a `.grader` file
def load_config():
    if os.path.exists('.grader'):
        with open('.grader') as f:
            return Config(**yaml.safe_load(f))
    else:
        raise FileNotFoundError("Configuration file '.grader' not found.")


# Context object to store config
class Context:
    def __init__(self):
        self.config = load_config()


pass_context = click.make_pass_decorator(Context, ensure=True)


@click.group()
def cli():
    pass


# Main group command 'tasks' with alias 'task'
@cli.group(invoke_without_command=True, cls=click.Group, )
@pass_context
def task(ctx):
    """Task management utility."""
    click.echo("Task management utility loaded.")


# List tasks
@task.command()
@click.option('--all', 'list_all', is_flag=True, help="List all tasks.")
@click.option('--running', is_flag=True, help="List running tasks.")
@click.option('--failed', is_flag=True, help="List failed tasks.")
@click.option('--project', type=str, help="List tasks for a specific project.")
@click.option('--student', type=str, help="List tasks for a specific student.")
@pass_context
def list(ctx, list_all, running, failed, project, student):
    """List existing tasks."""
    config = ctx.config
    url = f"{config.api_base_url}/tasks"
    params = {}

    if not (list_all or running or failed or project or student):
        params['requester'] = config.user_id
        params['status'] = 'running'

    if list_all:
        params['requester'] = config.user_id
    if running:
        params['status'] = 'running'
    if failed:
        params['status'] = 'failed'
    if project:
        params['project'] = project
    if student:
        params['student'] = student

    response = requests.get(url, params=params)
    if response.status_code == 200:
        tasks = response.json()
        click.echo(f"{'UID':<36} {'Status':<10} {'Submit Time'}")
        for task in tasks:
            click.echo(f"{task['uid']:<36} {task['status']:<10} {task['submit_time']}")
    else:
        click.echo(f"Error: {response.status_code}")


# Create a task
@task.command()
@click.option('--name', required=True, help="Name of the task.")
@click.option('--param', '-p', multiple=True, help="Task parameters in the form <key>=<value>.")
@click.option('-f', '--file', 'file_path', type=click.Path(), help="YAML file with task details.")
@pass_context
def submit(ctx, name, param, file_path):
    """Create a new task."""
    config = ctx.config

    if file_path:
        with open(file_path, 'r') as file:
            task_data = yaml.safe_load(file)
    else:
        task_data = {}
        task_data['name'] = name
        task_data['parameters'] = dict([p.split('=') for p in param])

    task_data['requester'] = config.user_id
    task_data['project'] = config.project

    url = f"{config.api_base_url}/task/start"
    response = requests.post(url, json=task_data)

    if response.status_code == 200:
        click.echo(f"Task {name} submitted successfully!")
    else:
        click.echo(f"Error: {response.status_code}")


# Cancel a task
@task.command()
@click.argument('task_ids', nargs=-1)
@pass_context
def cancel(ctx, task_ids):
    """Cancel one or more tasks by their IDs."""
    config = ctx.config
    for task_id in task_ids:
        url = f"{config.api_base_url}/task/{task_id}/cancel"
        response = requests.get(url)
        if response.status_code == 200:
            click.echo(f"Task {task_id} cancelled.")
        else:
            click.echo(f"Error cancelling task {task_id}: {response.status_code}")


# Get task information
@task.command()
@click.argument('task_id')
@click.option('--output', type=click.Choice(['console', 'yaml']), default='console', help="Output format.")
@pass_context
def get(ctx, task_id, output):
    """Get information about a task."""
    config = ctx.config
    url = f"{config.api_base_url}/task/{task_id}"
    response = requests.get(url)
    if response.status_code == 200:
        task_info = response.json()
        if output == 'yaml':
            with open(f"{task_id}.yaml", 'w') as f:
                yaml.dump(task_info, f)
            click.echo(f"Task info saved to {task_id}.yaml")
        else:
            click.echo(json.dumps(task_info, indent=2))
    else:
        click.echo(f"Error: {response.status_code}")


# Get task logs
@task.command()
@click.argument('task_id')
@pass_context
def logs(ctx, task_id):
    """Get logs of a task."""
    config = ctx.config
    url = f"{config.api_base_url}/task/{task_id}/log"
    response = requests.get(url)
    if response.status_code == 200:
        logs = response.json().get('log', '')
        click.echo(logs)
    else:
        click.echo(f"Error: {response.status_code}")


# Get task results
@task.command()
@click.argument('task_id')
@click.option('--output-file', type=click.Path(), help="Path to store the task result.")
@pass_context
def results(ctx, task_id, output_file):
    """Retrieve task results and save to file."""
    config = ctx.config
    url = f"{config.api_base_url}/task/{task_id}/result"
    response = requests.get(url)
    if response.status_code == 200:
        result = response.json().get('results', {})
        if output_file:
            with open(output_file, 'w') as f:
                json.dump(result, f)
            click.echo(f"Results saved to {output_file}")
        else:
            click.echo(json.dumps(result, indent=2))
    else:
        click.echo(f"Error: {response.status_code}")


if __name__ == "__main__":
    cli()
