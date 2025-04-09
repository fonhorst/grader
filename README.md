# Grader

A distributed grading system with support for various services and task processing.

## Local Installation with Poetry

### Prerequisites
- Python 3.10 or higher
- Poetry package manager

### Installation Steps

1. Install Poetry if you haven't already:
```bash
curl -sSL https://install.python-poetry.org | python3 -
```

2. Clone the repository and install dependencies:
```bash
git clone <repository-url>
cd grader
poetry install
```

3. Activate the virtual environment:
```bash
poetry shell
```

## Building and Running the Grader Service with Docker

### Prerequisites
- Docker
- Docker Compose

### Building the Image

```bash
./bin/build-tool build-app-image
```

The Dockerfile uses multi-stage builds to optimize the image size and build process:
- Base stage: Sets up Python and essential tools
- Exporter stage: Generates requirements.txt from poetry
- Builder stage: Builds the Python wheel
- Final stage: Creates the production image

## Deploying Local Compose

### Minimal Setup (for development)

```bash
cd docker
docker compose up -d postgres rabbitmq
```

This starts the essential services:
- PostgreSQL (available at localhost:5432)
- RabbitMQ (available at localhost:5672, management UI at localhost:15672)

### Full Setup

```bash
cd docker
docker compose --profile=grader up -d
```

This starts all services including:
- PostgreSQL
- RabbitMQ
- pgAdmin (available at localhost:5050)
- Grader API service (available at localhost:8080)
- Grader FastStream worker for task processing

### Environment Variables
The services are pre-configured with default development credentials:
- PostgreSQL: user=postgres, password=postgres, db=grader
- RabbitMQ: user=admin, password=admin
- pgAdmin: email=admin@admin.com, password=admin

Additionally, the Docker Compose sets the following environment variables for the grader services:
- GRADER_DB_CONN=postgresql+asyncpg://postgres:postgres@postgres:5432/grader
- GRADER_FASTSTREAM_BROKER=amqp://admin:admin@rabbitmq:5672/
- GRADER_FASTSTREAM_BROKER_QUEUE=grader-queue
- GRADER_FASTSTREAM_MAX_CONCURRENCY=1

## Running Tests

### Start Required Services for Testing

```bash
docker compose up -d postgres rabbitmq
```

### Run Tests

```bash
poetry run pytest -s ./tests
```

## Deploying on Kubernetes

### Prerequisites
- kubectl configured with your cluster
- Helm 3.x

### Installation Steps

1. Add required Helm repositories:
```bash
helm repo update
```

2. Install the grader chart:
```bash
cd k8s
helm upgrade --install --create-namespace --namespace=grader grader ./grader-chart -f grader-values.yaml
```

3. Install dependencies (if needed):
```bash
# Install HDFS
helm upgrade --install --create-namespace --namespace=grader-hdfs hdfs ./hdfs-chart -f hdfs-values.yaml

# Install ClickHouse
helm upgrade --install --create-namespace --namespace=grader-clickhouse clickhouse ./ch-chart -f ch-values.yaml
```

### Verify Installation

```bash
kubectl get pods -l app=grader
```

### Creating student workspace

To create a workspace for a student, you'll need to use the Workspace Helm chart. This will set up a dedicated namespace with appropriate resources and permissions for the student.

1. Create values file for the student (e.g., `student-values.yaml`):
```yaml
user: "student-username"  # Replace with actual username
student_dir_path: "/mnt/ess_storage/DN_1/students/student-username"  # Replace with actual path
ns_cpu_limit: "8"
ns_mem_limit: "64Gi"
jupyter_cpu_limits: "4"
jupyter_mem_limits: "32Gi"
shared_data_path: "/mnt/ess_storage/DN_1/students/shared-data"
```

2. Install the workspace for the student:
```bash
cd k8s
helm upgrade --install --create-namespace workspace-student-username ./Workspace -f student-values.yaml
```

This will create:
- A dedicated namespace for the student
- Resource quotas and limits
- PersistentVolumes and PersistentVolumeClaims for student data
- A Jupyter notebook deployment with Spark support
- Required RBAC permissions
- Access to shared data volume (read-only)

The student will have access to:
- Jupyter notebook environment with Spark support
- Personal storage space
- Shared data directory (read-only)
- Limited compute resources as specified in the values file

### Access Services

The services will be available at:
- Grader API: http://your-cluster-ip:8080
- RabbitMQ Management: http://your-cluster-ip:15672
- pgAdmin: http://your-cluster-ip:5050

## Using the CLI

The grader provides a comprehensive command-line interface for managing tasks, running checks, and controlling the grader service. Here are the main command groups and their functionality:

### Global Options

```bash
graderctl --verbose  # Enable verbose logging for all commands
```

### Task Management

1. Submit a new task:
```bash
graderctl task submit \
  --check-type clickhouse \  # Type of check to perform
  --user-id student123 \     # Student ID
  --name "Lab 1 Check" \     # Optional task name
  --tag "lab1" \            # Optional tag for grouping
  --args '{"key": "value"}' \ # Checker arguments as JSON
  --wait 60                  # Wait for completion (timeout in seconds)
```

You can also provide arguments from a file:
```bash
graderctl task submit -t clickhouse -u student123 --args-file args.json
```

2. List tasks with filtering:
```bash
graderctl task list \
  --user-id student123 \  # Filter by user
  --tag lab1 \           # Filter by tag
  --status COMPLETED     # Filter by status
```

3. Get task details:
```bash
graderctl task get \
  --task-id <task-id> \
  --json-file task.json \     # Save task info to JSON
  --report-file report.md     # Save report to Markdown
```

4. Cancel a running task:
```bash
graderctl task cancel --task-id <task-id>
```

5. Delete a task:
```bash
graderctl task delete --task-id <task-id>
```

6. Get task report:
```bash
graderctl task report \
  --task-id <task-id> \
  --output-file report.md
```

### Checker Commands

1. Run ClickHouse checker directly:
```bash
graderctl checker clickhouse \
  --host localhost \         # ClickHouse host
  --user admin \            # Admin username
  --student student123 \    # Student to check
  --cluster-name main_cluster \     # Cluster name
  --output report.md        # Output report path
```

2. Run a custom checker:
```bash
graderctl checker run \
  --checker grader.checking.ch_checker.ClickHouseChecker \
  --arguments args.json \
  --output report.md
```

### Serve Commands

1. Start the API server:
```bash
graderctl serve start-api \
  --host 0.0.0.0 \    # Host to bind to
  --port 8080 \       # Port to listen on
  --reload            # Enable auto-reload
```

2. Start the FastStream worker for task processing:
```bash
graderctl serve start-faststream \
  --create-tables     # Create database tables before starting
```

### Kubernetes Operations

1. Show installation instructions:
```bash
graderctl k8s info
```

2. Generate installation script:
```bash
graderctl k8s install-script --output install.sh
```

### Environment Variables

- `GRADER_API_URL`: API endpoint URL (default: http://localhost:8080)

## Contributing

Please refer to our contributing guidelines for information on how to propose changes and contribute to the project.

## License

[Add your license information here]
