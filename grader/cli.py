import click
import yaml
import requests
from pydantic import BaseModel
from typing import Optional, List, Dict
import os
import json


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


@click.group()
def cli():
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
def k8s():
    pass

# TODO: implement commands for task group:
# - submit
# - list
# - get
# - cancel
# - delete
# - report
# All these commands should go to the REST API and present the response 
# in a human-readable format or save the answer to files if appropriate

# TODO: implement commands for checker group:
# This group should be used to check the lab without a service and a queue, by direct running of the checker instead
# implemnt for the available checkers in grader/checking


# TODO: implement commands for k8s group:
# - info
# This command provides an instruction for how to install all the components using Helm on k8s
# The sequence of installation steps should include setps for installing of ch-chart, hdfs-chart and Workspace from ./k8s folder
# Also, it should use values from ./k8s/ch-values.yaml and ./k8s/hdfs-values.yaml files when appropriate


if __name__ == "__main__":
    cli()
