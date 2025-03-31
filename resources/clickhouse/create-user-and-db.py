from clickhouse_driver import Client
import argparse
import getpass
import sys

def create_user_and_db(host, admin_user, admin_password, new_username, new_password, cluster_name='main_cluster'):
    try:
        # Connect as admin
        client = Client(
            host=host,
            user=admin_user,
            password=admin_password
        )

        # Create database
        db_name = f"{new_username}-db"
        
        # Create database on cluster
        client.execute(f"CREATE DATABASE IF NOT EXISTS `{db_name}` ON CLUSTER `{cluster_name}`")
        
        # Create user on cluster
        client.execute(f"""
            CREATE USER IF NOT EXISTS `{new_username}` ON CLUSTER `{cluster_name}`
            IDENTIFIED WITH sha256_password BY '{new_password}'
        """)
        
        # Grant privileges
        client.execute(f"""
            GRANT ALL ON `{db_name}`.* TO `{new_username}` ON CLUSTER `{cluster_name}`
        """)
        
        print(f"Successfully created user '{new_username}' and database '{db_name}'")
        print(f"Granted all privileges on '{db_name}' to '{new_username}'")
        
        # Test connection with new user
        test_client = Client(
            host=host,
            user=new_username,
            password=new_password,
            database=db_name
        )
        
        # Try to execute a simple query
        test_client.execute("SELECT 1")
        print(f"Successfully verified connection for user '{new_username}'")
        
    except Exception as e:
        print(f"Error: {str(e)}", file=sys.stderr)
        sys.exit(1)

def main():
    parser = argparse.ArgumentParser(description='Create a ClickHouse user and database')
    parser.add_argument('--host', required=True, help='ClickHouse host address')
    parser.add_argument('--admin-user', required=True, help='Admin username')
    parser.add_argument('--new-username', required=True, help='New username to create')
    parser.add_argument('--cluster-name', default='main_cluster', help='ClickHouse cluster name')
    
    args = parser.parse_args()
    
    # Get passwords securely
    admin_password = getpass.getpass('Enter admin password: ')
    new_user_password = getpass.getpass('Enter password for new user: ')
    confirm_password = getpass.getpass('Confirm password for new user: ')
    
    if new_user_password != confirm_password:
        print("Error: Passwords do not match", file=sys.stderr)
        sys.exit(1)
    
    create_user_and_db(
        host=args.host,
        admin_user=args.admin_user,
        admin_password=admin_password,
        new_username=args.new_username,
        new_password=new_user_password,
        cluster_name=args.cluster_name
    )

if __name__ == "__main__":
    main() 