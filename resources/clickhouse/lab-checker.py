#!/usr/bin/env python3
import logging
import sys
import argparse
from clickhouse_driver import Client
import pandas as pd
import re
from typing import List, Optional
from pydantic import BaseModel


# Configure logger
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)


class CheckReport(BaseModel):
    """Represents a result of an individual check"""
    required: bool
    passed: bool
    check_description: str
    reason: Optional[str] = None

class CheckerReport(BaseModel):
    """Structured log of checking"""
    checks: List[CheckReport]

    def has_success(self) -> bool:
        """Check if all checks are successful"""
        return all(check.passed for check in self.checks if check.required)
    
    def has_warnings(self) -> bool:
        """Check if any checks are failed"""
        return any(not check.passed for check in self.checks if not check.required)

    def success(self, description: str, required: bool = True):
        """Add a success check report to the checker report"""
        self.checks.append(CheckReport(required=required, passed=True, check_description=description))

    def fail(self, description: str, reason: str, required: bool = True):
        """Add an error check report to the checker report"""
        self.checks.append(CheckReport(required=required, passed=False, check_description=description, reason=reason))

    def add(self, other: 'CheckReport'):
        """Add a check report to the checker report"""
        self.checks.append(other)

    def include(self, other: 'CheckerReport'):
        """Include another checker report into this one"""
        self.checks.extend(other.checks)

class ClickHouseChecker:
    def __init__(self, host='localhost', user='admin', password=None, student_username=None, cluster_name='main_cluster'):
        self.client = Client(host=host, user=user, password=password)
        self.cluster_name = cluster_name
        self.student_username = student_username
        self.student_db = f"{student_username}_db" if student_username else None
        self.checker_report = CheckerReport(checks=[])
        
    def execute_query(self, query):
        """Execute a query and return the result"""
        try:
            return self.client.execute(query)
        except Exception as e:
            logger.error(f"Query execution failed: {query}\nError: {str(e)}")
            return None
            
    def check_table_exists(self, table_name, expected_engine=None):
        """Check if a table exists and has the expected engine"""
        if not self.student_db:
            error_msg = "Student username not provided. Cannot check tables."
            logger.error(error_msg)
            self.checker_report.fail(
                description=f"Check if table {table_name} exists",
                reason=error_msg,
                required=True
            )
            return False
            
        query = f"""
        SELECT engine, create_table_query 
        FROM system.tables 
        WHERE database = '{self.student_db}' AND name = '{table_name}'
        """
        
        result = self.execute_query(query)
        
        if not result:
            error_msg = f"Table {self.student_db}.{table_name} does not exist"
            logger.error(error_msg)
            self.checker_report.fail(
                description=f"Check if table {table_name} exists",
                reason=error_msg,
                required=expected_engine is not None
            )
            return False
            
        engine, create_query = result[0]
        
        if expected_engine and not engine.startswith(expected_engine):
            error_msg = f"Table {self.student_db}.{table_name} has engine {engine}, expected {expected_engine}"
            logger.error(error_msg)
            self.checker_report.fail(
                description=f"Check if table {table_name} has engine {expected_engine}",
                reason=error_msg,
                required=True
            )
            return False
            
        success_msg = f"Table {self.student_db}.{table_name} exists with engine {engine}"
        logger.info(success_msg)
        self.checker_report.success(
            description=success_msg,
            required=expected_engine is not None
        )
        return create_query
        
    def check_table_schema(self, table_name, expected_columns):
        """Check if a table has the expected columns"""
        if not self.student_db:
            error_msg = "Student username not provided. Cannot check table schema."
            logger.error(error_msg)
            self.checker_report.fail(
                description=f"Check schema of table {table_name}",
                reason=error_msg,
                required=True
            )
            return False
            
        query = f"""
        SELECT name, type
        FROM system.columns
        WHERE database = '{self.student_db}' AND table = '{table_name}'
        """
        
        columns = self.execute_query(query)
        
        if not columns:
            error_msg = f"Could not retrieve columns for {self.student_db}.{table_name}"
            logger.error(error_msg)
            self.checker_report.fail(
                description=f"Check schema of table {table_name}",
                reason=error_msg,
                required=True
            )
            return False
            
        column_dict = {name: type_ for name, type_ in columns}
        
        missing_columns = [col for col in expected_columns if col not in column_dict]
        
        if missing_columns:
            error_msg = f"Table {self.student_db}.{table_name} is missing columns: {missing_columns}"
            logger.error(error_msg)
            self.checker_report.fail(
                description=f"Check required columns in table {table_name}",
                reason=error_msg,
                required=True
            )
            return False
            
        success_msg = f"Table {self.student_db}.{table_name} has all required columns"
        logger.info(success_msg)
        self.checker_report.success(
            description=success_msg,
            required=True
        )
        return True
        
    def check_distributed_table(self, table_name, expected_base_table):
        """Check distributed table configuration"""
        create_query = self.check_table_exists(table_name, "Distributed")
        
        if not create_query:
            return False
            
        # Check cluster name
        if self.cluster_name not in create_query:
            error_msg = f"Distributed table {self.student_db}.{table_name} doesn't use cluster {self.cluster_name}"
            logger.error(error_msg)
            self.checker_report.fail(
                description=f"Check if table {table_name} uses correct cluster",
                reason=error_msg,
                required=True
            )
            return False
            
        # Check base table
        if expected_base_table not in create_query:
            error_msg = f"Distributed table {self.student_db}.{table_name} doesn't use {expected_base_table} as base table"
            logger.error(error_msg)
            self.checker_report.fail(
                description=f"Check if table {table_name} uses correct base table",
                reason=error_msg,
                required=True
            )
            return False
            
        # Check if sharding key is specified
        if "xxHash64" not in create_query and "rand()" not in create_query.lower():
            error_msg = f"Distributed table {self.student_db}.{table_name} doesn't have a proper sharding expression"
            logger.error(error_msg)
            self.checker_report.fail(
                description=f"Check if table {table_name} has a sharding expression",
                reason=error_msg,
                required=True
            )
            return False
            
        success_msg = f"Distributed table {self.student_db}.{table_name} is configured correctly"
        logger.info(success_msg)
        self.checker_report.success(
            description=success_msg,
            required=True
        )
        return True
        
    def check_materialized_view(self, mv_name, expected_to_table=None):
        """Check materialized view configuration"""
        query = f"""
        SELECT engine, create_table_query
        FROM system.tables
        WHERE database = '{self.student_db}' AND name = '{mv_name}'
        """
        
        result = self.execute_query(query)
        
        required = expected_to_table is not None
        
        if not result:
            if required:
                error_msg = f"Materialized view {self.student_db}.{mv_name} does not exist"
                logger.error(error_msg)
                self.checker_report.fail(
                    description=f"Check if materialized view {mv_name} exists",
                    reason=error_msg,
                    required=required
                )
            return False
            
        engine, create_query = result[0]
        
        if not engine.startswith("Materialized"):
            error_msg = f"{self.student_db}.{mv_name} is not a materialized view"
            logger.error(error_msg)
            self.checker_report.fail(
                description=f"Check if {mv_name} is a materialized view",
                reason=error_msg,
                required=required
            )
            return False
            
        if expected_to_table and f"TO {self.student_db}.{expected_to_table}" not in create_query:
            error_msg = f"Materialized view {self.student_db}.{mv_name} doesn't write to {expected_to_table}"
            logger.error(error_msg)
            self.checker_report.fail(
                description=f"Check if materialized view {mv_name} writes to {expected_to_table}",
                reason=error_msg,
                required=required
            )
            return False
            
        success_msg = f"Materialized view {self.student_db}.{mv_name} is configured correctly"
        logger.info(success_msg)
        self.checker_report.success(
            description=success_msg,
            required=required
        )
        return True
        
    def check_view(self, view_name):
        """Check view configuration"""
        query = f"""
        SELECT engine, create_table_query
        FROM system.tables
        WHERE database = '{self.student_db}' AND name = '{view_name}'
        """
        
        result = self.execute_query(query)
        
        if not result:
            error_msg = f"View {self.student_db}.{view_name} does not exist"
            logger.error(error_msg)
            self.checker_report.fail(
                description=f"Check if view {view_name} exists",
                reason=error_msg,
                required=False
            )
            return False
            
        engine, create_query = result[0]
        
        if not engine == "View":
            error_msg = f"{self.student_db}.{view_name} is not a view"
            logger.error(error_msg)
            self.checker_report.fail(
                description=f"Check if {view_name} is a view",
                reason=error_msg,
                required=False
            )
            return False
            
        success_msg = f"View {self.student_db}.{view_name} is configured correctly"
        logger.info(success_msg)
        self.checker_report.success(
            description=success_msg,
            required=False
        )
        return True
        
    def execute_validation_queries(self):
        """Execute validation queries to check data correctness"""
        logger.info("=== Executing validation queries ===")
        
        # Check if base transactions table has data
        count_query = f"SELECT count() FROM {self.student_db}.transactions"
        count_result = self.execute_query(count_query)
        
        if not count_result or count_result[0][0] == 0:
            error_msg = f"No data found in {self.student_db}.transactions"
            logger.error(error_msg)
            self.checker_report.fail(
                description="Check if transactions table has data",
                reason=error_msg,
                required=True
            )
            return False
        else:
            success_msg = f"Found {count_result[0][0]} records in {self.student_db}.transactions"
            logger.info(success_msg)
            self.checker_report.success(
                description=success_msg,
                required=True
            )
            
        # Check if distributed table works
        dist_query = f"SELECT count() FROM {self.student_db}.transactions_distributed"
        dist_result = self.execute_query(dist_query)
        
        if not dist_result:
            error_msg = f"Could not query {self.student_db}.transactions_distributed"
            logger.error(error_msg)
            self.checker_report.fail(
                description="Check if distributed table is queryable",
                reason=error_msg,
                required=True
            )
            return False
        else:
            success_msg = "Successfully queried distributed table"
            logger.info(success_msg)
            self.checker_report.success(
                description=success_msg,
                required=True
            )
            
        # Check MV results if they exist
        
        # Check avg amount (MV option 1)
        if self.check_table_exists("avg_amount", None):
            avg_query = f"SELECT * FROM {self.student_db}.avg_amount WHERE user_id = (SELECT user_id_out FROM {self.student_db}.transactions LIMIT 1) LIMIT 5"
            avg_result = self.execute_query(avg_query)
            if avg_result:
                success_msg = "Successfully queried avg_amount materialized view"
                logger.info(success_msg)
                self.checker_report.success(
                    description=success_msg,
                    required=False
                )
            else:
                error_msg = "Could not query avg_amount view"
                logger.error(error_msg)
                self.checker_report.fail(
                    description="Check if avg_amount materialized view is queryable",
                    reason=error_msg,
                    required=False
                )
            
        # Check important transactions (MV option 2)
        if self.check_table_exists("important_transactions", None):
            important_query = f"SELECT * FROM {self.student_db}.important_transactions WHERE user_id = (SELECT user_id_out FROM {self.student_db}.transactions LIMIT 1) LIMIT 5"
            important_result = self.execute_query(important_query)
            if important_result:
                success_msg = "Successfully queried important_transactions materialized view"
                logger.info(success_msg)
                self.checker_report.success(
                    description=success_msg,
                    required=False
                )
            else:
                error_msg = "Could not query important_transactions view"
                logger.error(error_msg)
                self.checker_report.fail(
                    description="Check if important_transactions materialized view is queryable",
                    reason=error_msg,
                    required=False
                )
                
        # Check transaction sums (MV option 3)
        if self.check_table_exists("sum_tot_month", None):
            sum_query = f"SELECT * FROM {self.student_db}.sum_tot_month WHERE user_id = (SELECT user_id_out FROM {self.student_db}.transactions LIMIT 1) LIMIT 5"
            sum_result = self.execute_query(sum_query)
            if sum_result:
                success_msg = "Successfully queried sum_tot_month materialized view"
                logger.info(success_msg)
                self.checker_report.success(
                    description=success_msg,
                    required=False
                )
            else:
                error_msg = "Could not query sum_tot_month view"
                logger.error(error_msg)
                self.checker_report.fail(
                    description="Check if sum_tot_month materialized view is queryable",
                    reason=error_msg,
                    required=False
                )
                
        # Check user saldos (MV option 4)
        if self.check_table_exists("users_saldos", None):
            saldo_query = f"SELECT * FROM {self.student_db}.users_saldos WHERE user_id = (SELECT user_id_out FROM {self.student_db}.transactions LIMIT 1) LIMIT 5"
            saldo_result = self.execute_query(saldo_query)
            if saldo_result:
                success_msg = "Successfully queried users_saldos materialized view"
                logger.info(success_msg)
                self.checker_report.success(
                    description=success_msg,
                    required=False
                )
            else:
                error_msg = "Could not query users_saldos view"
                logger.error(error_msg)
                self.checker_report.fail(
                    description="Check if users_saldos materialized view is queryable",
                    reason=error_msg,
                    required=False
                )
                
        return True
    
    def check_data_distribution(self):
        """Check that data is properly distributed across all nodes in the cluster"""
        logger.info("=== Checking data distribution across cluster ===")
        
        # Define the required distributed tables based on lab task
        required_tables = [
            "transactions_distributed",  # Base data table
        ]
        
        # Add MV-related distributed tables if they exist
        if self.check_table_exists("transactions_aggregated_distributed", "Distributed"):
            required_tables.append("transactions_aggregated_distributed")
        
        # Check if any of the potential materialized view distributed tables exist
        potential_mv_tables = [
            "avg_amount_distributed",
            "important_transactions_distributed",
            "sum_tot_month_distributed", 
            "users_saldos_distributed"
        ]
        
        for table in potential_mv_tables:
            if self.check_table_exists(table, "Distributed"):
                required_tables.append(table)
        
        if not required_tables:
            error_msg = "No required distributed tables found to check data distribution"
            logger.error(error_msg)
            self.checker_report.fail(
                description="Check if required distributed tables exist",
                reason=error_msg,
                required=True
            )
            return False
            
        # Get information about cluster shards
        shard_query = f"""
        SELECT shard_num
        FROM system.clusters
        WHERE cluster = '{self.cluster_name}'
        ORDER BY shard_num
        """
        
        shards = self.execute_query(shard_query)
        if not shards:
            error_msg = f"Could not retrieve shard information for cluster {self.cluster_name}"
            logger.error(error_msg)
            self.checker_report.fail(
                description="Check cluster configuration",
                reason=error_msg,
                required=True
            )
            return False
            
        shard_count = len(shards)
        success_msg = f"Found {shard_count} shards in cluster {self.cluster_name}"
        logger.info(success_msg)
        self.checker_report.success(
            description=success_msg,
            required=True
        )
        
        # Check distribution for each required table
        for table_name in required_tables:
            logger.info(f"Checking distribution for table {table_name}...")
            
            # Query to check distribution using shardNum() function
            distribution_query = f"""
            SELECT shardNum() as shard, count() as row_count
            FROM {self.student_db}.{table_name}
            GROUP BY shard
            ORDER BY shard
            """
            
            try:
                # Execute the distribution query
                result = self.execute_query(distribution_query)
                
                if not result:
                    error_msg = f"Could not check distribution for table {table_name}"
                    logger.error(error_msg)
                    self.checker_report.fail(
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
                    self.checker_report.fail(
                        description=f"Check if data exists on all shards for table {table_name}",
                        reason=error_msg,
                        required=True
                    )
                    continue
                
                # Skip skew check for small tables
                if total_rows < 100:
                    warning_msg = f"Table {table_name} has too few rows ({total_rows}) to check for skew"
                    logger.warning(warning_msg)
                    self.checker_report.success(
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
                        self.checker_report.fail(
                            description=f"Check data skew for table {table_name}",
                            reason=error_msg,
                            required=True
                        )
                    else:
                        success_msg = f"Table {table_name} has acceptable data distribution with {skew_percentage:.2f}% skew"
                        logger.info(success_msg)
                        self.checker_report.success(
                            description=f"Check data skew for table {table_name}",
                            required=True
                        )
                else:
                    error_msg = f"Table has empty shards (min: {min_rows}, max: {max_rows})"
                    logger.error(error_msg)
                    self.checker_report.fail(
                        description=f"Check data distribution for table {table_name}",
                        reason=error_msg,
                        required=True
                    )
                
            except Exception as e:
                error_msg = f"Error checking distribution: {str(e)}"
                logger.error(error_msg)
                self.checker_report.fail(
                    description=f"Check data distribution for table {table_name}",
                    reason=error_msg,
                    required=True
                )
                
        return True

    def run_checks(self):
        """Run all checks for the ClickHouse lab implementation"""
        # Create a fresh report
        self.checker_report = CheckerReport(checks=[])
        
        logger.info(f"Starting checks for student: {self.student_username}")
        logger.info(f"Database: {self.student_db}")
        logger.info(f"Cluster: {self.cluster_name}")
        
        logger.info("=== Checking base tables ===")
        # Check base tables
        base_tables_exist = self.check_table_exists("transactions", "MergeTree")
        if base_tables_exist:
            self.check_table_schema("transactions", ["user_id_out", "user_id_in", "important", "amount", "datetime"])
            
        # Check distributed tables
        logger.info("=== Checking distributed tables ===")
        self.check_distributed_table("transactions_distributed", "transactions")
        
        # Check for MVs (at least 2 should exist)
        logger.info("=== Checking materialized views ===")
        
        mv_count = 0
        
        # Check transactions_aggregated if it exists (helper table for MVs)
        if self.check_table_exists("transactions_aggregated", "AggregatingMergeTree"):
            logger.info("Found transactions_aggregated helper table")
            self.check_distributed_table("transactions_aggregated_distributed", "transactions_aggregated")
            
            # Check helper MVs if they exist
            self.check_materialized_view("income_aggregated", "transactions_aggregated_distributed")
            self.check_materialized_view("outcome_aggregated", "transactions_aggregated_distributed")
        
        # MV option 1: Average amounts
        if self.check_table_exists("avg_amount", None):
            mv_count += 1
            logger.info("Found MV option 1: Average amounts")
            # Check if this is an actual MV
            self.check_materialized_view("avg_amount")
            
        # MV option 2: Important transactions
        if self.check_table_exists("important_transactions", None):
            mv_count += 1
            logger.info("Found MV option 2: Important transactions")
            # Check if this is an actual MV
            self.check_materialized_view("important_transactions")
            
        # MV option 3: Sum by months
        if self.check_table_exists("sum_tot_month", "SummingMergeTree"):
            mv_count += 1
            logger.info("Found MV option 3: Sum by months")
            # Check if there's a MV writing to this table
            self.check_materialized_view("sum_tot_month_mv", "sum_tot_month")
            
        # MV option 4: Users saldos
        if self.check_table_exists("users_saldos", "SummingMergeTree"):
            mv_count += 1
            logger.info("Found MV option 4: Users saldos")
            # Check if there's a MV writing to this table
            self.check_materialized_view("users_saldos_mv", "users_saldos")
            
        # Check for additional MVs with different naming conventions
        query = f"""
        SELECT name FROM system.tables 
        WHERE database = '{self.student_db}' AND engine LIKE 'Materialized%'
        """
        
        additional_mvs = self.execute_query(query)
        if additional_mvs:
            for mv_name in additional_mvs:
                mv_name = mv_name[0]
                # Skip MVs we've already checked
                if mv_name not in ["avg_amount", "important_transactions", "sum_tot_month_mv", "users_saldos_mv", "income_aggregated", "outcome_aggregated"]:
                    logger.info(f"Found additional materialized view: {mv_name}")
                    self.check_materialized_view(mv_name)
                    mv_count += 1
            
        if mv_count < 2:
            error_msg = f"Found only {mv_count} materialized views. At least 2 are required."
            logger.error(error_msg)
            self.checker_report.fail(
                description="Check if at least 2 materialized views are implemented",
                reason=error_msg,
                required=True
            )
            
        # Validate data by querying
        self.execute_validation_queries()

        # Check data distribution across cluster nodes and check for data skew
        self.check_data_distribution()
        
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

def main():
    parser = argparse.ArgumentParser(description='Check ClickHouse lab implementation')
    parser.add_argument('--host', default='localhost', help='ClickHouse host address')
    parser.add_argument('--user', default='admin', help='Admin username')
    parser.add_argument('--student', required=True, help='Student username')
    parser.add_argument('--cluster-name', default='main_cluster', help='ClickHouse cluster name')
    parser.add_argument('--output-json', help='Path to save the checker report as JSON')
    parser.add_argument('--log-file', help='Path to save logs')
    
    args = parser.parse_args()
    
    # Configure file logging if requested
    if args.log_file:
        file_handler = logging.FileHandler(args.log_file)
        file_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
        logger.addHandler(file_handler)
    
    # Get password securely
    password = input(f"Enter ClickHouse password for {args.user}: ")
    
    checker = ClickHouseChecker(
        host=args.host,
        user=args.user,
        password=password,
        student_username=args.student,
        cluster_name=args.cluster_name
    )
    
    report = checker.run_checks()
    
    # Save report as JSON if requested
    if args.output_json:
        with open(args.output_json, 'w') as f:
            f.write(report.json(indent=2))
        logger.info(f"Saved report to {args.output_json}")
    
    sys.exit(0 if report.has_success() else 1)

if __name__ == "__main__":
    main() 