"""
Database initialization module.
Ensures tables are created when the application starts.
"""

from grader.db.tasks import create_tasks_table

# Initialize the database tables
create_tasks_table()
