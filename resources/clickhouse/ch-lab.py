from clickhouse_driver import Client
import pandas as pd
from pathlib import Path

class ClickHouseSetup:
    def __init__(self, host='localhost', user='tutor', password=None):
        self.client = Client(host=host, user=user, password=password)
        self.cluster_name = 'kube_clickhouse_cluster'
        self.database = f"{user}_db"

    def execute_query(self, query):
        return self.client.execute(query)

    def create_base_tables(self):
        # Create source table
        self.execute_query(f'''
        CREATE TABLE IF NOT EXISTS {self.database}.transactions ON CLUSTER {self.cluster_name}
        (
            `user_id_out` Int64,
            `user_id_in` Int64,
            `important` Int8,
            `amount` Float64,
            `datetime` DateTime
        )
        ENGINE = MergeTree
        PARTITION BY toYYYYMM(datetime)
        ORDER BY user_id_out
        ''')

        # Create distributed table
        self.execute_query(f'''
        CREATE TABLE IF NOT EXISTS {self.database}.transactions_distributed ON CLUSTER {self.cluster_name} 
        AS {self.database}.transactions
        ENGINE = Distributed({self.cluster_name}, {self.database}, transactions, xxHash64(user_id_out))
        ''')

    def create_aggregation_tables(self):
        # Create aggregation table
        self.execute_query(f'''
        CREATE TABLE IF NOT EXISTS {self.database}.transactions_aggregated ON CLUSTER {self.cluster_name}
        (
            user_id Int64,
            income_amount AggregateFunction(sum, Float64),
            outcome_amount AggregateFunction(sum, Float64),
            month DateTime
        )
        ENGINE = AggregatingMergeTree
        ORDER BY (user_id, month)
        ''')

        # Create distributed aggregation table
        self.execute_query(f'''
        CREATE TABLE IF NOT EXISTS {self.database}.transactions_aggregated_distributed ON CLUSTER {self.cluster_name} 
        AS {self.database}.transactions_aggregated
        ENGINE = Distributed({self.cluster_name}, {self.database}, transactions_aggregated, xxHash64(user_id))
        ''')

    def create_helper_mvs(self):
        # Create outgoing transactions MV
        self.execute_query(f'''
        CREATE MATERIALIZED VIEW IF NOT EXISTS {self.database}.outcome_aggregated ON CLUSTER {self.cluster_name} 
        TO {self.database}.transactions_aggregated_distributed
        AS
        SELECT
            user_id_out AS user_id,
            sumState(amount) AS outcome_amount,
            toDate(toStartOfMonth(datetime)) AS month
        FROM {self.database}.transactions
        GROUP BY user_id, month
        ''')

        # Create incoming transactions MV
        self.execute_query(f'''
        CREATE MATERIALIZED VIEW IF NOT EXISTS {self.database}.income_aggregated ON CLUSTER {self.cluster_name} 
        TO {self.database}.transactions_aggregated_distributed
        AS
        SELECT
            user_id_in AS user_id,
            sumState(amount) AS income_amount,
            toDate(toStartOfMonth(datetime)) AS month
        FROM {self.database}.transactions
        GROUP BY user_id, month
        ''')

    def create_final_mvs(self):
        # Create monthly sums MV
        self.execute_query(f'''
        CREATE MATERIALIZED VIEW IF NOT EXISTS {self.database}.sum_tot_month ON CLUSTER {self.cluster_name}
        ENGINE = SummingMergeTree
        ORDER BY (user_id, month)
        AS
        SELECT
            user_id AS user_id,
            sumMerge(income_amount) AS income_total,
            sumMerge(outcome_amount) AS outcome_total,
            formatDateTime(month, '%m.%Y') AS month
        FROM {self.database}.transactions_aggregated
        GROUP BY user_id, month
        ''')

        # Create user saldos MV
        self.execute_query(f'''
        CREATE MATERIALIZED VIEW IF NOT EXISTS {self.database}.users_saldos ON CLUSTER {self.cluster_name}
        ENGINE = SummingMergeTree
        ORDER BY user_id
        AS
        SELECT
            user_id AS user_id,
            sumMerge(income_amount) - sumMerge(outcome_amount) AS saldo
        FROM {self.database}.transactions_aggregated
        GROUP BY user_id
        ''')

    def load_data(self, parquet_path):
        # Read parquet file
        df = pd.read_parquet(parquet_path)
        
        # Insert data into distributed table
        self.client.execute(
            f'INSERT INTO {self.database}.transactions_distributed VALUES',
            df.to_dict('records')
        )

def main():
    # Initialize ClickHouse setup
    password = input("Enter ClickHouse password: ")
    ch_setup = ClickHouseSetup(password=password)

    # Create all necessary tables and views
    print("Creating base tables...")
    ch_setup.create_base_tables()
    
    print("Creating aggregation tables...")
    ch_setup.create_aggregation_tables()
    
    print("Creating helper materialized views...")
    ch_setup.create_helper_mvs()
    
    print("Creating final materialized views...")
    ch_setup.create_final_mvs()

    # Load data
    parquet_path = './resources/clickhouse/data/transactions_12M.parquet'
    if Path(parquet_path).exists():
        print("Loading data from parquet file...")
        ch_setup.load_data(parquet_path)
        print("Data loaded successfully!")
    else:
        print(f"Error: Parquet file not found at {parquet_path}")

if __name__ == "__main__":
    main() 