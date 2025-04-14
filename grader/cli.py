import logging
import sys
import click
import asyncio
from typing import Optional
import os
import json
import uuid

import grader
from grader.checking.base import CheckerReport
from grader.client.grader import GraderAPIClient, GraderApiException, GraderApiTimeoutException
from grader.schemes import TaskSubmitRequest
from grader.db.init_db import init_database

logger = logging.getLogger(__name__)


def get_api_url() -> str:
    """Get the API URL from environment variable or use default."""
    api_url = os.getenv('GRADER_API_URL', 'http://localhost:8080')
    logger.info(f"Using API URL: {api_url}")
    return api_url


def get_log_level() -> str:
    return "debug" if logger.getEffectiveLevel() <= logging.DEBUG else "info"


def get_log_level_value() -> int:
    return logging.DEBUG if logger.getEffectiveLevel() <= logging.DEBUG else logging.INFO


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
def test():
    pass


@cli.group()
def ui():
    pass


@cli.group()
def task():
    pass


@cli.group()
def checker():
    pass


@cli.group()
def serve():
    pass


@cli.group()
def k8s():
    pass


@test.command()
def fill_db_with_mock_data():
    # TODO: implement the following logic
    # 1. check if the database is empty
    # 2. if it is, create tables (using appopriate models)
    # 3. fill it with mock data. 
    # Table representeted with Task class should contain 15 records with different statuses and different users
    # At least 5 records should be in the finished state with Non empty reports
    # At least 10 records should have tags
    asyncio.run(init_database())

    pass


@ui.command()
def run():
    import nest_asyncio
    from streamlit.web import cli

    cli.main_run([os.path.join(grader.__path__[0], "ui", "main.py")])


@task.command()
@click.option('--check-type', '-t', required=True, type=str, help='Type of check to perform (e.g. "clickhouse")')
@click.option('--user-id', '-u', required=True, type=str, help='User ID for the task')
@click.option('--name', '-n', type=str, help='Optional name for the task')
@click.option('--tag', type=str, help='Optional tag for grouping tasks')
@click.option('--args', type=str, help='JSON string with arguments for the checker. Mutually exclusive with --args-file')
@click.option('--args-file', type=click.Path(exists=True, dir_okay=False), help='Path to JSON file containing arguments for the checker. Mutually exclusive with --args')
@click.option('--wait', '-w', type=int, help='Wait for task completion (timeout in seconds, 0 for indefinite wait)')
@click.option('--poll-interval', '-p', type=float, help='Poll interval for task completion in seconds. Default is 1.0 second.', default=1.0)
@click.option('--report-file', '-r', type=click.Path(dir_okay=False), help='Save task report to this Markdown file if task completes successfully. Only used when --wait is specified.')
def submit(check_type: str, user_id: str, name: str, tag: str, args: str, args_file: str, wait: Optional[int], poll_interval: float, report_file: str):
    """Submit a new checking task and optionally wait for completion."""
    logger.info(f"Running task for user {user_id} with check type {check_type}")
    
    # Check mutual exclusivity of args and args_file
    if args and args_file:
        error_msg = "Error: --args and --args-file are mutually exclusive. Please provide only one of them."
        click.echo(error_msg, err=True)
        sys.exit(1)
    
    # Check that report-file is only used with wait
    if report_file and not wait:
        error_msg = "Error: --report-file can only be used when --wait is specified."
        click.echo(error_msg, err=True)
        sys.exit(1)
    
    async def run_task():
        try:
            checker_args = {}
            if args:
                logger.debug("Parsing args from command line JSON string")
                checker_args = json.loads(args)
            elif args_file:
                logger.debug(f"Loading args from file: {args_file}")
                with open(args_file) as f:
                    checker_args = json.load(f)
            
            request = TaskSubmitRequest(
                check_type=check_type,
                student_id=user_id,
                name=name,
                tag=tag,
                args=checker_args
            )
            
            async with GraderAPIClient(base_url=get_api_url()) as client:
                response = await client.submit_task(request, wait_timeout=wait, poll_interval=poll_interval)
                logger.info(f"Task {'completed' if wait else 'submitted'} with ID: {response.id}")
                click.echo(format_task_info(response.model_dump()))
                
                # Save report if requested and available
                if report_file and wait and response.report:
                    logger.debug(f"Saving report to file: {report_file}")
                    report = CheckerReport.model_validate_json(response.report)
                    with open(report_file, 'w') as f:
                        f.write(report.to_markdown())
                    click.echo(f"Report saved to {report_file}")
                elif report_file and wait:
                    click.echo("\nNo report available for this task", err=True)
                
        except json.JSONDecodeError as e:
            error_msg = f"Error: Invalid JSON format - {str(e)}"
            logger.error(error_msg)
            click.echo(error_msg, err=True)
            sys.exit(1)
        except GraderApiTimeoutException as e:
            logger.error(f"Timeout waiting for task completion: {e.message}", exc_info=True)
            click.echo(f"Task did not complete within timeout: {e.detail or e.message}", err=True)
            sys.exit(1)
        except GraderApiException as e:
            logger.error(f"API Error: {e.message}", exc_info=True)
            click.echo(f"Failed to run task: {e.detail or e.message}", err=True)
            sys.exit(1)
        except Exception as e:
            logger.error(f"Error running task: {str(e)}", exc_info=True)
            click.echo(f"Failed to run task: {str(e)}", err=True)
            sys.exit(1)
    
    asyncio.run(run_task())


@task.command()
@click.option('--user-id', '-u', type=str, help='Filter tasks by user ID')
@click.option('--tag', '-t', type=str, help='Filter tasks by tag')
@click.option('--status', '-s', type=str, help='Filter tasks by status')
def list(user_id: str, tag: str, status: str):
    """List tasks with optional filtering."""
    logger.info("Listing tasks with filters")
    
    async def list_tasks():
        try:
            async with GraderAPIClient(base_url=get_api_url()) as client:
                response = await client.list_tasks(user_id=user_id, tag=tag, status=status)
                logger.info(f"Found {len(response.tasks)} tasks")
                for task in response.tasks:
                    click.echo(format_task_info(task.model_dump()))
                    click.echo("---")
        except GraderApiException as e:
            logger.error(f"API Error: {e.message}", exc_info=True)
            click.echo(f"Failed to list tasks: {e.detail or e.message}", err=True)
            sys.exit(1)
        except Exception as e:
            logger.error(f"Error listing tasks: {str(e)}", exc_info=True)
            click.echo(f"Failed to list tasks: {str(e)}", err=True)
            sys.exit(1)
    
    asyncio.run(list_tasks())


@task.command()
@click.option('--task-id', '-i', required=True, type=str, help='ID of the task to retrieve')
@click.option('--json-file', type=click.Path(dir_okay=False), help='Save task info to this JSON file')
@click.option('--report-file', type=click.Path(dir_okay=False), help='Save task report to this Markdown file if available')
def get(task_id: str, json_file: str, report_file: str):
    """Get information about a specific task."""
    logger.info(f"Getting task info for ID: {task_id}")
    
    async def get_task():
        try:
            async with GraderAPIClient(base_url=get_api_url()) as client:
                task_data = await client.get_task(uuid.UUID(task_id))
                
                logger.debug(f"Retrieved task data: {task_data}")
                click.echo(format_task_info(task_data.model_dump()))
                
                # Save JSON if requested
                if json_file:
                    logger.debug(f"Saving task info to JSON file: {json_file}")
                    with open(json_file, 'w') as f:
                        json.dump(task_data.model_dump(), f, indent=2)
                    click.echo(f"\nTask info saved to {json_file}")
                
                # Save report if requested and available
                if report_file and task_data.report:
                    logger.debug(f"Saving report to file: {report_file}")
                    report = CheckerReport.model_validate_json(task_data.report)
                    with open(report_file, 'w') as f:
                        f.write(report.to_markdown())
                    click.echo(f"Report saved to {report_file}")
                elif report_file:
                    click.echo("\nNo report available for this task", err=True)
                    
        except GraderApiException as e:
            logger.error(f"API Error: {e.message}", exc_info=True)
            click.echo(f"Failed to get task: {e.detail or e.message}", err=True)
            sys.exit(1)
        except Exception as e:
            logger.error(f"Error getting task: {str(e)}", exc_info=True)
            click.echo(f"Failed to get task: {str(e)}", err=True)
            sys.exit(1)
    
    asyncio.run(get_task())


@task.command()
@click.option('--task-id', '-i', required=True, type=str, help='ID of the task to cancel')
@click.option('--json-file', type=click.Path(dir_okay=False), help='Save response to this JSON file')
def cancel(task_id: str, json_file: str):
    """Cancel a running task."""
    logger.info(f"Canceling task with ID: {task_id}")
    
    async def cancel_task():
        try:
            async with GraderAPIClient(base_url=get_api_url()) as client:
                task_data = await client.cancel_task(uuid.UUID(task_id))
                
                logger.debug(f"Task cancel response: {task_data}")
                click.echo(format_task_info(task_data.model_dump()))
                
                # Save JSON if requested
                if json_file:
                    logger.debug(f"Saving response to JSON file: {json_file}")
                    with open(json_file, 'w') as f:
                        json.dump(task_data.model_dump(), f, indent=2)
                    click.echo(f"\nResponse saved to {json_file}")
                    
        except GraderApiException as e:
            logger.error(f"API Error: {e.message}", exc_info=True)
            click.echo(f"Failed to cancel task: {e.detail or e.message}", err=True)
            sys.exit(1)
        except Exception as e:
            logger.error(f"Error canceling task: {str(e)}", exc_info=True)
            click.echo(f"Failed to cancel the task: {str(e)}", err=True)
            sys.exit(1)
    
    asyncio.run(cancel_task())


@task.command()
@click.option('--task-id', '-i', required=True, type=str, help='ID of the task to delete')
@click.option('--json-file', type=click.Path(dir_okay=False), help='Save response to this JSON file')
def delete(task_id: str, json_file: str):
    """Delete a task."""
    logger.info(f"Deleting task with ID: {task_id}")
    
    async def delete_task():
        try:
            async with GraderAPIClient(base_url=get_api_url()) as client:
                await client.delete_task(uuid.UUID(task_id))
                click.echo("Task deleted successfully")
                    
        except GraderApiException as e:
            logger.error(f"API Error: {e.message}", exc_info=True)
            click.echo(f"Failed to delete task: {e.detail or e.message}", err=True)
            sys.exit(1)
        except Exception as e:
            logger.error(f"Error deleting task: {str(e)}", exc_info=True)
            click.echo(f"Failed to delete the task: {str(e)}", err=True)
            sys.exit(1)
    
    asyncio.run(delete_task())


@task.command()
@click.option('--task-id', '-i', required=True, type=str, help='ID of the task to get report for')
@click.option('--output-file', '-o', type=click.Path(dir_okay=False), required=True, help='Save report to this file (Markdown format)')
def report(task_id: str, output_file: str):
    """Get the report from a finished task."""
    logger.info(f"Getting report for task ID: {task_id}")
    
    async def get_report():
        try:
            async with GraderAPIClient(base_url=get_api_url()) as client:
                report_data = await client.get_task_report(uuid.UUID(task_id))
                
                if not report_data.report:
                    click.echo("\nNo report available for this task", err=True)
                    return
                
                report = CheckerReport.model_validate_json(report_data.report)
                
                # Save report in markdown format
                logger.debug(f"Saving report to file: {output_file}")
                with open(output_file, 'w') as f:
                    f.write(report.to_markdown())
                click.echo(f"Report saved to {output_file}")
                
        except GraderApiException as e:
            logger.error(f"API Error: {e.message}", exc_info=True)
            click.echo(f"Failed to get report: {e.detail or e.message}", err=True)
            sys.exit(1)
        except Exception as e:
            logger.error(f"Error getting report: {str(e)}", exc_info=True)
            click.echo(f"Failed to get the report: {str(e)}", err=True)
            sys.exit(1)
    
    asyncio.run(get_report())


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
@click.option('--input', '-i', type=click.Path(exists=True, dir_okay=True), required=True, help='Path to input dataset')
@click.option('--gold', '-g', type=click.Path(exists=True, dir_okay=True), required=True, help='Path to gold standard data')
@click.option('--output', '-o', type=click.Path(dir_okay=True), required=True, help='Directory to save results')
@click.option('--script', '-s', required=True, help='Path to student\'s script (local or HDFS URL)')
@click.option('--timeout', '-t', type=int, default=30, show_default=True, help='Maximum time in seconds to wait for script')
@click.option('--report', '-r', type=click.Path(dir_okay=False), required=True, help='Path to save the report in Markdown format')
@click.option('--log-file', '-l', type=click.Path(dir_okay=False), help='Path to save logs')
@click.option('--hdfs-host', help='HDFS host for downloading scripts (required if script is in HDFS)')
@click.option('--hdfs-port', type=int, help='HDFS port for downloading scripts (required if script is in HDFS)')
def spark(input: str, gold: str, output: str, script: str, timeout: int, report: str, log_file: str, hdfs_host: str, hdfs_port: int):
    """Run Spark checker directly.
    
    This command runs the Spark checker without using the task queue service.
    It will execute the student's script and compare its output with gold standard data.
    """
    from grader.checking.checking import run_checking, CheckType
    
    try:
        # Run checker
        report = run_checking(
            check_type=CheckType.SPARK,
            script_path=script,
            input_data_path=input,
            gold_data_path=gold,
            output_dir=output,
            timeout=timeout,
            hdfs_host=hdfs_host,
            hdfs_port=hdfs_port
        )
        
        # Save report as Markdown
        markdown_report = report.to_markdown()
        with open(report, 'w') as f:
            f.write(markdown_report)
        click.echo(f"Report saved to {report}")
        
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


@checker.command()
@click.option('--namespace', '-n', required=True, help='Kubernetes namespace to check resources in')
@click.option('--report', '-r', type=click.Path(dir_okay=False), required=True, help='Path to save the report in Markdown format')
@click.option('--log-file', '-l', type=click.Path(dir_okay=False), help='Path to save logs')
def kube(namespace: str, report: str, log_file: str):
    """Run Kubernetes checker directly.
    
    This command runs the Kubernetes checker without using the task queue service.
    It will check the configuration of Kubernetes resources in the specified namespace.
    """
    from grader.checking.k8s_checker import KubernetesChecker
    
    try:
        # Run checker
        checker = KubernetesChecker(namespace=namespace)
        report_result = checker.run_checks()
        
        # Save report as Markdown
        markdown_report = report_result.to_markdown()
        with open(report, 'w') as f:
            f.write(markdown_report)
        click.echo(f"Report saved to {report}")
        
        if not report_result.has_success():
            click.echo("Checking failed!", err=True)
            exit(1)
        click.echo("Checking completed successfully!")
        
    except Exception as e:
        click.echo(f"Error running checker: {str(e)}", err=True)
        exit(1)


@checker.command()
@click.option('--hdfs-url', required=True, help='HDFS WebHDFS URL')
@click.option('--base-dir', required=True, help='Base directory path in HDFS where temporary directories will be created')
@click.option('--test-data', type=click.Path(exists=True), required=True, help='Path to JSON file containing test data')
@click.option('--golden-data', type=click.Path(exists=True), required=True, help='Path to JSON file containing golden data')
@click.option('--report', '-r', type=click.Path(dir_okay=False), required=True, help='Path to save the report in Markdown format')
@click.option('--log-file', '-l', type=click.Path(dir_okay=False), help='Path to save logs')
@click.option('--process-start-delay', type=int, default=2, help='Delay in seconds after starting the ETL process')
@click.option('--file-write-interval', type=int, default=5, help='Interval in seconds between writing test files')
@click.option('--final-wait-time', type=int, default=5, help='Time to wait after writing the last file')
@click.option('--etl-duration', type=int, default=30, help='Duration in minutes for the ETL process to run')
@click.option('--etl-check-interval', type=int, default=5, help='Interval in seconds for the ETL process to check for new files')
def hdfs(hdfs_url: str, base_dir: str, test_data: str, golden_data: str, report: str, log_file: str,
         process_start_delay: int, file_write_interval: int, final_wait_time: int,
         etl_duration: int, etl_check_interval: int):
    """Run HDFS checker directly.
    
    This command runs the HDFS checker without using the task queue service.
    It will check the ETL pipeline functionality by:
    1. Starting the ETL pipeline
    2. Writing test data to HDFS
    3. Verifying the output against golden data
    """
    from grader.checking.hdfs_checker import HDFSChecker
    import json
    import pandas as pd
    
    try:
        # Load test and golden data
        with open(test_data, 'r') as f:
            test_data_list = json.load(f)
        
        with open(golden_data, 'r') as f:
            golden_data_dict = {
                date: pd.DataFrame(data)
                for date, data in json.load(f).items()
            }
        
        # Run checker
        checker = HDFSChecker(
            hdfs_url=hdfs_url,
            base_dir=base_dir,
            test_data=test_data_list,
            golden_data=golden_data_dict,
            process_start_delay=process_start_delay,
            file_write_interval=file_write_interval,
            final_wait_time=final_wait_time,
            etl_duration=etl_duration,
            etl_check_interval=etl_check_interval
        )
        report_result = checker.run_checks()
        
        # Save report as Markdown
        markdown_report = report_result.to_markdown()
        with open(report, 'w') as f:
            f.write(markdown_report)
        click.echo(f"Report saved to {report}")
        
        if not report_result.has_success():
            click.echo("Checking failed!", err=True)
            exit(1)
        click.echo("Checking completed successfully!")
        
    except Exception as e:
        click.echo(f"Error running checker: {str(e)}", err=True)
        exit(1)


@serve.command()
@click.option('--host', '-h', default="0.0.0.0", show_default=True, help='Host address to bind to')
@click.option('--port', '-p', default=8080, show_default=True, type=int, help='Port to listen on')
@click.option('--reload', '-r', is_flag=True, help='Enable auto-reload on code changes')
def start_api(host: str, port: int, reload: bool):
    """Start the REST API server."""
    logger.info(f"Starting API server on {host}:{port}")
    
    import uvicorn
    from fastapi import FastAPI
    from grader.api.tasks_api import router as tasks_router
    from grader.api.students_api import router as students_router
    from grader.api.courses_api import router as courses_router
    
    app = FastAPI()
    app.include_router(tasks_router)
    app.include_router(students_router)
    app.include_router(courses_router)
    
    logger.info("API server configured with Swagger UI at /docs")
    try:
        uvicorn.run(
            app,
            host=host,
            port=port,
            reload=reload,
            log_level=get_log_level()
        )
    except Exception as e:
        logger.error(f"Error starting API server: {str(e)}", exc_info=True)
        click.echo(f"Failed to start API server: {str(e)}", err=True)
        sys.exit(1)


@serve.command()
@click.option('--create-tables', '-c', is_flag=True, default=False, help='Create database tables before starting')
def start_faststream(create_tables: bool):
    """Start the FastStream worker for processing tasks."""
    logger.info("Starting FastStream worker")  
    
    # Import and start FastStream app
    try:
        from faststream.cli.main import _run_imported_app
        from grader.faststream_tasks.tasks import app
        from grader.db.tasks import create_tables
        import asyncio

        if create_tables:
            logger.info("Creating database tables...")
            asyncio.run(create_tables())
            logger.info("Database tables created successfully")
        
        logger.info("Starting FastStream app")
        # TODO: we don't currently support multiple workers in CLI
        # Consider using FastStream CLI instead
        _run_imported_app(
            app,
            extra_options=dict(),
            log_level=get_log_level_value()
        )
    except Exception as e:
        logger.error(f"Error starting FastStream app: {str(e)}", exc_info=True)
        click.echo(f"Failed to start FastStream app: {str(e)}", err=True)
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

