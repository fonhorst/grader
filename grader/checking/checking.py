from enum import Enum
import logging
from grader.checking.base import CheckerReport


logger = logging.getLogger(__name__)


class CheckType(str, Enum):
    CLICKHOUSE = "clickhouse"
    SPARK = "spark"
    K8S = "k8s"
    HDFS = "hdfs"


def run_checking(check_type: CheckType, **kwargs) -> CheckerReport:
    try:
        match check_type:
            case CheckType.CLICKHOUSE:
                from grader.checking.ch_checker import ClickHouseChecker
                checker = ClickHouseChecker(**kwargs)

            case CheckType.SPARK:
                from grader.checking.spark_checker import SparkChecker
                checker = SparkChecker(**kwargs)

            case CheckType.K8S:
                from grader.checking.k8s_checker import KubernetesChecker
                checker = KubernetesChecker(**kwargs)

            case CheckType.HDFS:
                from grader.checking.hdfs_checker import HDFSChecker
                checker = HDFSChecker(**kwargs)
            
            case _:
                raise ValueError(f"Unsupported check type: {check_type}")

        return checker.run_checks()
        
    except Exception as e:
        # Log the full stacktrace
        logger.error(f"Checker failed with error: {str(e)}", exc_info=True)
        
        # Create a report with a single failed check
        report = CheckerReport(checks=[])
        report.fail(
            description="Check execution",
            reason="The checker has failed. Plese, try later or contact the administrator to solve the problem.",
            required=True
        )
        return report

