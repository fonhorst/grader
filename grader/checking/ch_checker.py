#!/usr/bin/env python3
import logging
from clickhouse_driver import Client
from typing import List, Optional, Tuple

from grader.checking.base import CheckableQuery, CheckerReport, LabChecker

logger = logging.getLogger(__name__)


def execute_query(client: Client, query: str) -> Optional[List[Tuple]]:
    """Execute a query and return the result
    
    Args:
        client: Clickhouse client
        query: SQL query to execute
        
    Returns:
        Query result or None if execution failed
    """
    try:
        return client.execute(query)
    except Exception as e:
        logger.error(f"Query execution failed: {query}\nError: {str(e)}")
        raise e


def get_table_if_exists(client: Client, db_name: str, table_name: str) -> Optional[str]:
    """Get the table if it exists
    
    Args:
        client: Clickhouse client
        db_name: Database name
        table_name: Table name
    
    Returns:
        Create query of the table if it exists, None otherwise
    """
    query = f"""
    SELECT create_table_query 
    FROM system.tables 
    WHERE database = '{db_name}' AND name = '{table_name}'
    """
    
    result = execute_query(client, query)
    
    if not result:
        return None
        
    return result[0][0]


def check_table_exists(client: Client, db_name: str, table_name: str, expected_engine: Optional[str] = None) -> Tuple[CheckerReport, Optional[str]]:
    """Check if a table exists and has the expected engine
    
    Args:
        client: Clickhouse client
        db_name: Database name
        table_name: Table name
        expected_engine: Expected engine type (optional)
        
    Returns:
        Tuple of (success, report, create_query)
    """
    checker_report = CheckerReport()
    
    if not db_name:
        error_msg = "Database name not provided. Cannot check tables."
        logger.error(error_msg)
        checker_report.fail(
            description=f"Check if table {table_name} exists",
            reason=error_msg,
            required=True
        )
        return checker_report, None
        
    query = f"""
    SELECT engine, create_table_query 
    FROM system.tables 
    WHERE database = '{db_name}' AND name = '{table_name}'
    """
    
    result = execute_query(client, query)
    
    if not result:
        error_msg = f"Table {db_name}.{table_name} does not exist"
        logger.error(error_msg)
        checker_report.fail(
            description=f"Check if table {table_name} exists",
            reason=error_msg,
            required=expected_engine is not None
        )
        return checker_report, None
        
    engine, create_query = result[0]
    
    if expected_engine and not engine.startswith(expected_engine):
        error_msg = f"Table {db_name}.{table_name} has engine {engine}, expected {expected_engine}"
        logger.error(error_msg)
        checker_report.fail(
            description=f"Check if table {table_name} has engine {expected_engine}",
            reason=error_msg,
            required=True
        )
        return checker_report, create_query
        
    success_msg = f"Table {db_name}.{table_name} exists with engine {engine}"
    logger.info(success_msg)
    checker_report.success(
        description=success_msg,
        required=expected_engine is not None
    )
    return checker_report, create_query


def check_table_schema(client: Client, db_name: str, table_name: str, expected_columns: List[str]) -> CheckerReport:
    """Check if a table has the expected columns
    
    Args:
        client: Clickhouse client
        db_name: Database name
        table_name: Table name
        expected_columns: List of column names that should exist
        
    Returns:
        Tuple of (success, report)
    """
    checker_report = CheckerReport()
    
    if not db_name:
        error_msg = "Database name not provided. Cannot check table schema."
        logger.error(error_msg)
        checker_report.fail(
            description=f"Check schema of table {table_name}",
            reason=error_msg,
            required=True
        )
        return checker_report
        
    query = f"""
    SELECT name, type
    FROM system.columns
    WHERE database = '{db_name}' AND table = '{table_name}'
    """
    
    columns = execute_query(client, query)
    
    if not columns:
        error_msg = f"Could not retrieve columns for {db_name}.{table_name}"
        logger.error(error_msg)
        checker_report.fail(
            description=f"Check schema of table {table_name}",
            reason=error_msg,
            required=True
        )
        return checker_report
        
    column_dict = {name: type_ for name, type_ in columns}
    
    missing_columns = [col for col in expected_columns if col not in column_dict]
    
    if missing_columns:
        error_msg = f"Table {db_name}.{table_name} is missing columns: {missing_columns}"
        logger.error(error_msg)
        checker_report.fail(
            description=f"Check required columns in table {table_name}",
            reason=error_msg,
            required=True
        )
        return checker_report
        
    success_msg = f"Table {db_name}.{table_name} has all required columns"
    logger.info(success_msg)
    checker_report.success(
        description=success_msg,
        required=True
    )
    return checker_report


def check_distributed_table(client: Client, db_name: str, table_name: str, expected_base_table: str, cluster_name: str) -> CheckerReport:
    """Check distributed table configuration
    
    Args:
        client: Clickhouse client
        db_name: Database name
        table_name: Table name
        expected_base_table: Name of the expected base table
        cluster_name: Expected cluster name
        
    Returns:
        CheckerReport containing check results
    """
    checker_report = CheckerReport()
    
    create_query = get_table_if_exists(client, db_name, table_name)
    
    if not create_query:
        error_msg = f"Table {db_name}.{table_name} does not exist"
        logger.error(error_msg)
        checker_report.fail(
            description=f"Check if table {table_name} exists",
            reason=error_msg,
            required=True
        )
        return checker_report
    
    # Check if it's a distributed table
    query = f"""
    SELECT engine FROM system.tables 
    WHERE database = '{db_name}' AND name = '{table_name}'
    """
    result = execute_query(client, query)
    if not result or not result[0][0].startswith("Distributed"):
        error_msg = f"Table {db_name}.{table_name} is not a Distributed table"
        logger.error(error_msg)
        checker_report.fail(
            description=f"Check if table {table_name} has engine Distributed",
            reason=error_msg,
            required=True
        )
        return checker_report
        
    # Check cluster name
    if cluster_name not in create_query:
        error_msg = f"Distributed table {db_name}.{table_name} doesn't use cluster {cluster_name}"
        logger.error(error_msg)
        checker_report.fail(
            description=f"Check if table {table_name} uses correct cluster",
            reason=error_msg,
            required=True
        )
        return checker_report
        
    # Check base table
    if expected_base_table not in create_query:
        error_msg = f"Distributed table {db_name}.{table_name} doesn't use {expected_base_table} as base table"
        logger.error(error_msg)
        checker_report.fail(
            description=f"Check if table {table_name} uses correct base table",
            reason=error_msg,
            required=True
        )
        return checker_report
        
    # Check if sharding key is specified
    if "xxHash64" not in create_query and "rand()" not in create_query.lower():
        error_msg = f"Distributed table {db_name}.{table_name} doesn't have a proper sharding expression"
        logger.error(error_msg)
        checker_report.fail(
            description=f"Check if table {table_name} has a sharding expression",
            reason=error_msg,
            required=True
        )
        return checker_report
        
    success_msg = f"Distributed table {db_name}.{table_name} is configured correctly"
    logger.info(success_msg)
    checker_report.success(
        description=success_msg,
        required=True
    )
    return checker_report


# TODO: check if the logic of checking is correct
def check_materialized_view(client: Client, db_name: str, mv_name: str, expected_to_table: Optional[str] = None) -> CheckerReport:
    """Check materialized view configuration
    
    Args:
        client: Clickhouse client
        db_name: Database name
        mv_name: Materialized view name
        expected_to_table: Expected target table (optional)
        
    Returns:
        Tuple of (success, report)
    """
    checker_report = CheckerReport()
    required = expected_to_table is not None
    
    query = f"""
    SELECT engine, create_table_query
    FROM system.tables
    WHERE database = '{db_name}' AND name = '{mv_name}'
    """
    
    result = execute_query(client, query)
    
    if not result:
        if required:
            error_msg = f"Materialized view {db_name}.{mv_name} does not exist"
            logger.error(error_msg)
            checker_report.fail(
                description=f"Check if materialized view {mv_name} exists",
                reason=error_msg,
                required=required
            )
        return checker_report
        
    _, create_query = result[0]
    
    if not create_query.lower().startswith("create materialized view"):
        error_msg = f"{db_name}.{mv_name} is not a materialized view"
        logger.error(error_msg)
        checker_report.fail(
            description=f"Check if {mv_name} is a materialized view",
            reason=error_msg,
            required=required
        )
        return checker_report
        
    if expected_to_table and f"from {db_name}.{expected_to_table}" not in create_query.lower():
        error_msg = f"Materialized view {db_name}.{mv_name} doesn't take data from {expected_to_table}"
        logger.error(error_msg)
        checker_report.fail(
            description=f"Check if materialized view {mv_name} takes data from {expected_to_table}",
            reason=error_msg,
            required=required
        )
        return checker_report
        
    success_msg = f"Materialized view {db_name}.{mv_name} is configured correctly"
    logger.info(success_msg)
    checker_report.success(
        description=success_msg,
        required=required
    )
    return checker_report


def check_view(client: Client, db_name: str, view_name: str) -> CheckerReport:
    """Check view configuration
    
    Args:
        client: Clickhouse client
        db_name: Database name
        view_name: View name
        
    Returns:
        Tuple of (success, report)
    """
    checker_report = CheckerReport()
    
    query = f"""
    SELECT engine, create_table_query
    FROM system.tables
    WHERE database = '{db_name}' AND name = '{view_name}'
    """
    
    result = execute_query(client, query)
    
    if not result:
        error_msg = f"View {db_name}.{view_name} does not exist"
        logger.error(error_msg)
        checker_report.fail(
            description=f"Check if view {view_name} exists",
            reason=error_msg,
            required=False
        )
        return checker_report
        
    engine, _ = result[0]
    
    if not engine == "View":
        error_msg = f"{db_name}.{view_name} is not a view"
        logger.error(error_msg)
        checker_report.fail(
            description=f"Check if {view_name} is a view",
            reason=error_msg,
            required=False
        )
        return checker_report
        
    success_msg = f"View {db_name}.{view_name} is configured correctly"
    logger.info(success_msg)
    checker_report.success(
        description=success_msg,
        required=False
    )
    return checker_report


def check_data_distribution(client: Client, db_name: str, required_tables: List[str], cluster_name: str) -> CheckerReport:
    """Check that data is properly distributed across all nodes in the cluster
    
    Args:
        client: Clickhouse client
        db_name: Database name
        required_tables: List of distributed tables to check
        cluster_name: Cluster name
        
    Returns:
        Tuple of (success, report)
    """
    checker_report = CheckerReport()
    logger.info("=== Checking data distribution across cluster ===")
    
    if not required_tables:
        error_msg = "No required distributed tables found to check data distribution"
        logger.error(error_msg)
        checker_report.fail(
            description="Check if required distributed tables exist",
            reason=error_msg,
            required=True
        )
        return checker_report
        
    # Get information about cluster shards
    shard_query = f"""
    SELECT shard_num
    FROM system.clusters
    WHERE cluster = '{cluster_name}'
    ORDER BY shard_num
    """
    
    shards = execute_query(client, shard_query)
    if not shards:
        error_msg = f"Could not retrieve shard information for cluster {cluster_name}"
        logger.error(error_msg)
        checker_report.fail(
            description="Check cluster configuration",
            reason=error_msg,
            required=True
        )
        return checker_report
        
    shard_count = len(shards)
    success_msg = f"Found {shard_count} shards in cluster {cluster_name}"
    logger.info(success_msg)
    checker_report.success(
        description=success_msg,
        required=True
    )
    
    # Check distribution for each required table
    for table_name in required_tables:
        logger.info(f"Checking distribution for table {table_name}...")
        
        # Query to check distribution using shardNum() function
        distribution_query = f"""
        SELECT shardNum() as shard, count() as row_count
        FROM {db_name}.{table_name}
        GROUP BY shard
        ORDER BY shard
        """
        
        try:
            # Execute the distribution query
            result = execute_query(client, distribution_query)
            
            if not result:
                error_msg = f"Could not check distribution for table {table_name}"
                logger.error(error_msg)
                checker_report.fail(
                    description=f"Check data distribution for table {table_name}",
                    reason=error_msg,
                    required=True
                )
                continue
                
            # Process the results
            shard_rows = []
            for shard, count in result:
                if shard > 0:  # Only include actual shards (shardNum > 0)
                    shard_rows.append((shard, count))
            
            # Calculate total rows
            total_rows = sum(count for _, count in shard_rows)
            
            # Check if data exists on all shards
            if len(shard_rows) < shard_count:
                error_msg = f"Data not found on all shards. Found on {len(shard_rows)}/{shard_count} shards."
                logger.error(error_msg)
                checker_report.fail(
                    description=f"Check if data exists on all shards for table {table_name}",
                    reason=error_msg,
                    required=True
                )
                continue
            
            # Skip skew check for small tables
            if total_rows < 100:
                warning_msg = f"Table {table_name} has too few rows ({total_rows}) to check for skew"
                logger.warning(warning_msg)
                checker_report.success(
                    description=f"Check data skew for table {table_name}",
                    required=False
                )
                continue
            
            # Calculate min and max rows to check for skew
            min_rows = min(count for _, count in shard_rows)
            max_rows = max(count for _, count in shard_rows)
            
            # Print distribution information
            for shard, count in shard_rows:
                percent = (count / total_rows) * 100
                logger.info(f"  Shard {shard}: {count} rows ({percent:.2f}%)")
            
            # Calculate and check skew percentage
            if min_rows > 0:
                skew_percentage = ((max_rows - min_rows) / min_rows) * 100
                
                if skew_percentage > 20:
                    error_msg = f"Significant data skew: {skew_percentage:.2f}% (min: {min_rows}, max: {max_rows})"
                    logger.error(error_msg)
                    checker_report.fail(
                        description=f"Check data skew for table {table_name}",
                        reason=error_msg,
                        required=True
                    )
                else:
                    success_msg = f"Table {table_name} has acceptable data distribution with {skew_percentage:.2f}% skew"
                    logger.info(success_msg)
                    checker_report.success(
                        description=f"Check data skew for table {table_name}",
                        required=True
                    )
            else:
                error_msg = f"Table has empty shards (min: {min_rows}, max: {max_rows})"
                logger.error(error_msg)
                checker_report.fail(
                    description=f"Check data distribution for table {table_name}",
                    reason=error_msg,
                    required=True
                )
            
        except Exception as e:
            error_msg = f"Error checking distribution: {str(e)}"
            logger.error(error_msg)
            checker_report.fail(
                description=f"Check data distribution for table {table_name}",
                reason=error_msg,
                required=True
            )
            
    return checker_report


class ClickHouseChecker(LabChecker):
    def __init__(self, host='localhost', user='admin', password=None, student_username=None, cluster_name='main_cluster'):
        self.client = Client(host=host, user=user, password=password)
        self.cluster_name = cluster_name
        self.student_username = student_username
        self.student_db = f"{student_username}_db" if student_username else None
        self.checker_report = CheckerReport(checks=[])
        
    def execute_validation_queries(self, validation_queries: List[CheckableQuery]) -> bool:
        """Execute validation queries to check data correctness
        
        Args:
            validation_queries: List of queries to validate
            
        Returns:
            bool: True if all required queries passed
        """
        logger.info("=== Executing validation queries ===")
        
        # Execute and validate each query
        for query in validation_queries:
            try:
                result = execute_query(self.client, query.query)
                if query.validate(result):
                    success_msg = f"Successfully executed query: {query.query}"
                    logger.info(success_msg)
                    self.checker_report.success(
                        description=query.description
                    )
                else:
                    error_msg = f"Query returned no results: {query.query}"
                    logger.error(error_msg)
                    self.checker_report.fail(
                        description=query.description,
                        reason=error_msg
                    )
            except Exception as e:
                error_msg = f"Error executing query: {query.query}\nError: {str(e)}"
                logger.error(error_msg)
                self.checker_report.fail(
                    description=query.description,
                    reason=error_msg,
                    required=True
                )
        
        return True

    # TODO: make it a little bit more structured and clearly splitted on explicit steps 
    def run_checks(self):
        """Run all checks for the ClickHouse lab implementation"""
        # Create a fresh report
        self.checker_report = CheckerReport(checks=[])
        
        logger.info(f"Starting checks for student: {self.student_username}")
        logger.info(f"Database: {self.student_db}")
        logger.info(f"Cluster: {self.cluster_name}")
        
        logger.info("=== Checking base tables ===")
        # Check base tables
        table_report, create_query = check_table_exists(
            self.client, 
            self.student_db, 
            "transactions", 
            "MergeTree"
        )
        self.checker_report.include(table_report)
        
        if create_query:
            schema_report = check_table_schema(
                self.client,
                self.student_db,
                "transactions",
                ["user_id_out", "user_id_in", "important", "amount", "datetime"]
            )
            self.checker_report.include(schema_report)
            
        # Check distributed tables
        logger.info("=== Checking distributed tables ===")
        dist_report = check_distributed_table(
            self.client,
            self.student_db,
            "transactions_distributed",
            "transactions",
            self.cluster_name
        )
        self.checker_report.include(dist_report)

        distributed_tables_to_check = [
            "transactions_distributed"
        ]
        
        # Check for MVs (at least 2 should exist)
        logger.info("=== Checking materialized views ===")
        
        mv_count = 0
        
        # Check transactions_aggregated if it exists (helper table for MVs)
        aggregated_report, aggregated_create_query = check_table_exists(
            self.client,
            self.student_db,
            "transactions_aggregated",
            "AggregatingMergeTree"
        )
        self.checker_report.include(aggregated_report)
        
        if aggregated_create_query:
            logger.info("Found transactions_aggregated helper table")
            aggregated_dist_report = check_distributed_table(
                self.client,
                self.student_db,
                "transactions_aggregated_distributed",
                "transactions_aggregated",
                self.cluster_name
            )
            self.checker_report.include(aggregated_dist_report)
            distributed_tables_to_check.append("transactions_aggregated_distributed")
            
            # Check helper MVs if they exist
            income_mv_report = check_materialized_view(
                self.client,
                self.student_db,
                "income_aggregated",
                "transactions"
            )
            self.checker_report.include(income_mv_report)
            
            outcome_mv_report = check_materialized_view(
                self.client,
                self.student_db,
                "outcome_aggregated",
                "transactions"
            )
            self.checker_report.include(outcome_mv_report)
        
        # MV option 1: Average amounts
        avg_create_query = get_table_if_exists(self.client, self.student_db, "avg_amount")
        
        if avg_create_query:
            mv_count += 1
            logger.info("Found MV option 1: Average amounts")
            # Check if this is an actual MV
            avg_mv_report = check_materialized_view(
                self.client,
                self.student_db,
                "avg_amount"
            )
            self.checker_report.include(avg_mv_report)
            distributed_tables_to_check.append("avg_amount_distributed")
            
        # MV option 2: Important transactions
        imp_create_query = get_table_if_exists(self.client, self.student_db, "important_transactions")
        
        if imp_create_query:
            mv_count += 1
            logger.info("Found MV option 2: Important transactions")
            # Check if this is an actual MV
            imp_mv_report = check_materialized_view(
                self.client,
                self.student_db,
                "important_transactions"
            )
            self.checker_report.include(imp_mv_report)
            distributed_tables_to_check.append("important_transactions_distributed")
            
        # MV option 3: Sum by months
        sum_create_query = get_table_if_exists(self.client, self.student_db, "sum_tot_month")
        
        if sum_create_query:
            mv_count += 1
            logger.info("Found MV option 3: Sum by months")
            # Check if there's a MV writing to this table
            sum_mv_report = check_materialized_view(
                self.client,
                self.student_db,
                "sum_tot_month", 
                "transactions_aggregated"
            )
            self.checker_report.include(sum_mv_report)
            distributed_tables_to_check.append("sum_tot_month_distributed")
            
        # MV option 4: Users saldos
        saldo_create_query = get_table_if_exists(self.client, self.student_db, "users_saldos")
        
        if saldo_create_query:
            mv_count += 1
            logger.info("Found MV option 4: Users saldos")
            # Check if there's a MV writing to this table
            saldo_mv_report = check_materialized_view(
                self.client,
                self.student_db,
                "users_saldos", 
                "transactions_aggregated"
            )
            self.checker_report.include(saldo_mv_report)
            distributed_tables_to_check.append("users_saldos_distributed")
            
        if mv_count < 2:
            error_msg = f"Found only {mv_count} materialized views. At least 2 are required."
            logger.error(error_msg)
            self.checker_report.fail(
                description="Check if at least 2 materialized views are implemented",
                reason=error_msg,
                required=True
            )
        
        # Check queryability of all tables
        tables = [
            "transactions",
            "transactions_distributed",
            "avg_amount",
            "important_transactions",
            "sum_tot_month", 
            "users_saldos"
        ]
        
        validation_queries = []
        for table in tables:
            create_query = get_table_if_exists(self.client, self.student_db, table)
            if create_query:
                validation_queries.append(
                    CheckableQuery(
                        # TODO: at least verifye the count is not 0
                        query=f"SELECT count() as count FROM {self.student_db}.{table} LIMIT 5",
                        description=f"Check if {table} is queryable"
                    )
                )
            
        # Validate data by querying
        self.execute_validation_queries(validation_queries)

        # Check data distribution
        data_distribution_to_check = []
        for dtable in distributed_tables_to_check:
            table_report, create_query = check_table_exists(
                self.client,
                self.student_db,
                dtable,
            )
            self.checker_report.include(table_report)

            if create_query:
                data_distribution_to_check.append(dtable)
    
        distribution_report = check_data_distribution(
            self.client,
            self.student_db,
            data_distribution_to_check,
            self.cluster_name
        )
        self.checker_report.include(distribution_report)
        
        # Summary
        logger.info("=== Summary ===")
        
        errors = [check for check in self.checker_report.checks if not check.passed and check.required]
        
        if self.checker_report.has_success():
            logger.info("All checks passed!")
        else:
            logger.error(f"{len(errors)} checks failed!")
            
            logger.error("Errors:")
            for i, check in enumerate(errors, 1):
                logger.error(f"{i}. {check.check_description}: {check.reason}")
                
        return self.checker_report

