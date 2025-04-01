#!/usr/bin/env python3
import sys
import argparse
from clickhouse_driver import Client
import pandas as pd
import re

class ClickHouseChecker:
    def __init__(self, host='localhost', user='admin', password=None, student_username=None, cluster_name='main_cluster'):
        self.client = Client(host=host, user=user, password=password)
        self.cluster_name = cluster_name
        self.student_username = student_username
        self.student_db = f"{student_username}_db" if student_username else None
        self.all_checks_passed = True
        self.errors = []
        
    def log_error(self, message):
        """Log an error message and mark the checks as failed"""
        self.errors.append(message)
        self.all_checks_passed = False
        print(f"❌ ERROR: {message}")
        
    def log_success(self, message):
        """Log a success message"""
        print(f"✅ SUCCESS: {message}")
        
    def execute_query(self, query):
        """Execute a query and return the result"""
        try:
            return self.client.execute(query)
        except Exception as e:
            self.log_error(f"Query execution failed: {query}\nError: {str(e)}")
            return None
            
    def check_table_exists(self, table_name, expected_engine=None):
        """Check if a table exists and has the expected engine"""
        if not self.student_db:
            self.log_error("Student username not provided. Cannot check tables.")
            return False
            
        query = f"""
        SELECT engine, create_table_query 
        FROM system.tables 
        WHERE database = '{self.student_db}' AND name = '{table_name}'
        """
        
        result = self.execute_query(query)
        
        if not result:
            self.log_error(f"Table {self.student_db}.{table_name} does not exist")
            return False
            
        engine, create_query = result[0]
        
        if expected_engine and not engine.startswith(expected_engine):
            self.log_error(f"Table {self.student_db}.{table_name} has engine {engine}, expected {expected_engine}")
            return False
            
        self.log_success(f"Table {self.student_db}.{table_name} exists with engine {engine}")
        return create_query
        
    def check_table_schema(self, table_name, expected_columns):
        """Check if a table has the expected columns"""
        if not self.student_db:
            self.log_error("Student username not provided. Cannot check table schema.")
            return False
            
        query = f"""
        SELECT name, type
        FROM system.columns
        WHERE database = '{self.student_db}' AND table = '{table_name}'
        """
        
        columns = self.execute_query(query)
        
        if not columns:
            self.log_error(f"Could not retrieve columns for {self.student_db}.{table_name}")
            return False
            
        column_dict = {name: type_ for name, type_ in columns}
        
        missing_columns = [col for col in expected_columns if col not in column_dict]
        
        if missing_columns:
            self.log_error(f"Table {self.student_db}.{table_name} is missing columns: {missing_columns}")
            return False
            
        self.log_success(f"Table {self.student_db}.{table_name} has all required columns")
        return True
        
    def check_distributed_table(self, table_name, expected_base_table):
        """Check distributed table configuration"""
        create_query = self.check_table_exists(table_name, "Distributed")
        
        if not create_query:
            return False
            
        # Check cluster name
        if self.cluster_name not in create_query:
            self.log_error(f"Distributed table {self.student_db}.{table_name} doesn't use cluster {self.cluster_name}")
            return False
            
        # Check base table
        if expected_base_table not in create_query:
            self.log_error(f"Distributed table {self.student_db}.{table_name} doesn't use {expected_base_table} as base table")
            return False
            
        # Check if sharding key is specified
        if "xxHash64" not in create_query and "rand()" not in create_query.lower():
            self.log_error(f"Distributed table {self.student_db}.{table_name} doesn't have a proper sharding expression")
            return False
            
        self.log_success(f"Distributed table {self.student_db}.{table_name} is configured correctly")
        return True
        
    def check_materialized_view(self, mv_name, expected_to_table=None):
        """Check materialized view configuration"""
        query = f"""
        SELECT engine, create_table_query
        FROM system.tables
        WHERE database = '{self.student_db}' AND name = '{mv_name}'
        """
        
        result = self.execute_query(query)
        
        if not result:
            self.log_error(f"Materialized view {self.student_db}.{mv_name} does not exist")
            return False
            
        engine, create_query = result[0]
        
        if not engine.startswith("Materialized"):
            self.log_error(f"{self.student_db}.{mv_name} is not a materialized view")
            return False
            
        if expected_to_table and f"TO {self.student_db}.{expected_to_table}" not in create_query:
            self.log_error(f"Materialized view {self.student_db}.{mv_name} doesn't write to {expected_to_table}")
            return False
            
        self.log_success(f"Materialized view {self.student_db}.{mv_name} is configured correctly")
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
            self.log_error(f"View {self.student_db}.{view_name} does not exist")
            return False
            
        engine, create_query = result[0]
        
        if not engine == "View":
            self.log_error(f"{self.student_db}.{view_name} is not a view")
            return False
            
        self.log_success(f"View {self.student_db}.{view_name} is configured correctly")
        return True
        
    def execute_validation_queries(self):
        """Execute validation queries to check data correctness"""
        print("\n=== Executing validation queries ===")
        
        # Check if base transactions table has data
        count_query = f"SELECT count() FROM {self.student_db}.transactions"
        count_result = self.execute_query(count_query)
        
        if not count_result or count_result[0][0] == 0:
            self.log_error(f"No data found in {self.student_db}.transactions")
            return False
        else:
            self.log_success(f"Found {count_result[0][0]} records in {self.student_db}.transactions")
            
        # Check if distributed table works
        dist_query = f"SELECT count() FROM {self.student_db}.transactions_distributed"
        dist_result = self.execute_query(dist_query)
        
        if not dist_result:
            self.log_error(f"Could not query {self.student_db}.transactions_distributed")
            return False
        else:
            self.log_success(f"Successfully queried distributed table")
            
        # Check MV results if they exist
        
        # Check avg amount (MV option 1)
        if self.check_table_exists("avg_amount", None):
            avg_query = f"SELECT * FROM {self.student_db}.avg_amount WHERE user_id = (SELECT user_id_out FROM {self.student_db}.transactions LIMIT 1) LIMIT 5"
            avg_result = self.execute_query(avg_query)
            if avg_result:
                self.log_success(f"Successfully queried avg_amount materialized view")
            
        # Check important transactions (MV option 2)
        if self.check_table_exists("important_transactions", None):
            important_query = f"SELECT * FROM {self.student_db}.important_transactions WHERE user_id = (SELECT user_id_out FROM {self.student_db}.transactions LIMIT 1) LIMIT 5"
            important_result = self.execute_query(important_query)
            if important_result:
                self.log_success(f"Successfully queried important_transactions materialized view")
                
        # Check transaction sums (MV option 3)
        if self.check_table_exists("sum_tot_month", None):
            sum_query = f"SELECT * FROM {self.student_db}.sum_tot_month WHERE user_id = (SELECT user_id_out FROM {self.student_db}.transactions LIMIT 1) LIMIT 5"
            sum_result = self.execute_query(sum_query)
            if sum_result:
                self.log_success(f"Successfully queried sum_tot_month materialized view")
                
        # Check user saldos (MV option 4)
        if self.check_table_exists("users_saldos", None):
            saldo_query = f"SELECT * FROM {self.student_db}.users_saldos WHERE user_id = (SELECT user_id_out FROM {self.student_db}.transactions LIMIT 1) LIMIT 5"
            saldo_result = self.execute_query(saldo_query)
            if saldo_result:
                self.log_success(f"Successfully queried users_saldos materialized view")
                
        return True
                
    def run_checks(self):
        """Run all checks for the ClickHouse lab implementation"""
        print(f"Starting checks for student: {self.student_username}")
        print(f"Database: {self.student_db}")
        print(f"Cluster: {self.cluster_name}")
        
        print("\n=== Checking base tables ===")
        # Check base tables
        base_tables_exist = self.check_table_exists("transactions", "MergeTree")
        if base_tables_exist:
            self.check_table_schema("transactions", ["user_id_out", "user_id_in", "important", "amount", "datetime"])
            
        # Check distributed tables
        print("\n=== Checking distributed tables ===")
        self.check_distributed_table("transactions_distributed", "transactions")
        
        # Check for MVs (at least 2 should exist)
        print("\n=== Checking materialized views ===")
        
        mv_count = 0
        
        # Check transactions_aggregated if it exists (helper table for MVs)
        if self.check_table_exists("transactions_aggregated", "AggregatingMergeTree"):
            self.log_success("Found transactions_aggregated helper table")
            self.check_distributed_table("transactions_aggregated_distributed", "transactions_aggregated")
            
            # Check helper MVs if they exist
            self.check_materialized_view("income_aggregated", "transactions_aggregated_distributed")
            self.check_materialized_view("outcome_aggregated", "transactions_aggregated_distributed")
        
        # MV option 1: Average amounts
        if self.check_table_exists("avg_amount", None):
            mv_count += 1
            self.log_success("Found MV option 1: Average amounts")
            # Check if this is an actual MV
            self.check_materialized_view("avg_amount")
            
        # MV option 2: Important transactions
        if self.check_table_exists("important_transactions", None):
            mv_count += 1
            self.log_success("Found MV option 2: Important transactions")
            # Check if this is an actual MV
            self.check_materialized_view("important_transactions")
            
        # MV option 3: Sum by months
        if self.check_table_exists("sum_tot_month", "SummingMergeTree"):
            mv_count += 1
            self.log_success("Found MV option 3: Sum by months")
            # Check if there's a MV writing to this table
            self.check_materialized_view("sum_tot_month_mv", "sum_tot_month")
            
        # MV option 4: Users saldos
        if self.check_table_exists("users_saldos", "SummingMergeTree"):
            mv_count += 1
            self.log_success("Found MV option 4: Users saldos")
            # Check if there's a MV writing to this table
            self.check_materialized_view("users_saldos_mv", "users_saldos")
            
        if mv_count < 2:
            self.log_error(f"Found only {mv_count} materialized views. At least 2 are required.")
            
        # Validate data by querying
        self.execute_validation_queries()
        
        # Print summary
        print("\n=== Summary ===")
        if self.all_checks_passed:
            print("✅ All checks passed!")
        else:
            print(f"❌ {len(self.errors)} checks failed!")
            print("\nErrors:")
            for i, error in enumerate(self.errors, 1):
                print(f"{i}. {error}")
                
        return self.all_checks_passed

def main():
    parser = argparse.ArgumentParser(description='Check ClickHouse lab implementation')
    parser.add_argument('--host', default='localhost', help='ClickHouse host address')
    parser.add_argument('--user', default='admin', help='Admin username')
    parser.add_argument('--student', required=True, help='Student username')
    parser.add_argument('--cluster-name', default='main_cluster', help='ClickHouse cluster name')
    
    args = parser.parse_args()
    
    # Get password securely
    password = input(f"Enter ClickHouse password for {args.user}: ")
    
    checker = ClickHouseChecker(
        host=args.host,
        user=args.user,
        password=password,
        student_username=args.student,
        cluster_name=args.cluster_name
    )
    
    success = checker.run_checks()
    sys.exit(0 if success else 1)

if __name__ == "__main__":
    main() 