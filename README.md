# grader

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
- Grader service (available at localhost:8080)

### Environment Variables
The services are pre-configured with default development credentials:
- PostgreSQL: user=postgres, password=postgres, db=grader
- RabbitMQ: user=admin, password=admin
- pgAdmin: email=admin@admin.com, password=admin

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

## Contributing

Please refer to our contributing guidelines for information on how to propose changes and contribute to the project.

## License

[Add your license information here]
