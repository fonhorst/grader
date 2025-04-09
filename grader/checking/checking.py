import argparse
from enum import Enum
import logging
import sys
from grader.checking.base import CheckerReport
from grader.checking.ch_checker import ClickHouseChecker


logger = logging.getLogger(__name__)


class CheckType(str, Enum):
    CLICKHOUSE = "clickhouse"


def run_checking(check_type: CheckType, **kwargs) -> CheckerReport:
    match check_type:
        case CheckType.CLICKHOUSE:
            checker = ClickHouseChecker(**kwargs)
        
        case _:
            raise ValueError(f"Unsupported check type: {check_type}")

    return checker.run_checks()


def main():
    parser = argparse.ArgumentParser(description='Check ClickHouse lab implementation')
    parser.add_argument('--host', default='localhost', help='ClickHouse host address')
    parser.add_argument('--user', default='admin', help='Admin username')
    parser.add_argument('--student', required=True, help='Student username')
    parser.add_argument('--cluster-name', default='main_cluster', help='ClickHouse cluster name')
    parser.add_argument('--output-json', default='checker_report.json', help='Path to save the checker report as JSON')
    parser.add_argument('--output-markdown', default='checker_report.md', help='Path to save the checker report as Markdown')
    parser.add_argument('--log-file', help='Path to save logs')
    
    args = parser.parse_args()
    
    # Configure file logging if requested
    # if args.log_file:
    #     file_handler = logging.FileHandler(args.log_file)
    #     file_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
    #     logger.addHandler(file_handler)
    
    # Get password securely
    password = input(f"Enter ClickHouse password for {args.user}: ")
    
    report = run_checking(
        check_type=CheckType.CLICKHOUSE, 
        host=args.host,
        user=args.user,
        password=password,
        student_username=args.student,
        cluster_name=args.cluster_name
    )
    
    # Save report as JSON if requested
    if args.output_json:
        with open(args.output_json, 'w') as f:
            f.write(report.json(indent=2))
        logger.info(f"Saved JSON report to {args.output_json}")
    
    # Save report as Markdown if requested
    if args.output_markdown:
        markdown_report = report.to_markdown()
        with open(args.output_markdown, 'w') as f:
            f.write(markdown_report)
        logger.info(f"Saved Markdown report to {args.output_markdown}")
    
    sys.exit(0 if report.has_success() else 1)

if __name__ == "__main__":
    main() 

