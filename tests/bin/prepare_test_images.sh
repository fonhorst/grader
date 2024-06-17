#!/bin/sh

set -e

GEOWSM_REGISTRY_ADDR="10.32.0.243:5000"
#GEOWSM_REGISTRY_URL="10.32.0.243:5000/"
GEOWSM_REGISTRY_URL=""
TAG="test-geowsm"

echo "====== Checking poetry lock ======"
poetry lock --check
echo "====== Exporting requirements ======"
poetry export --without-hashes > requirements.txt
echo "====== Building wheel ======"
poetry build

echo "====== Dividing requirements into base and rn ======"
python ./ci_stages/divide_requirements.py

echo "====== Building base images ======"
tag="${GEOWSM_REGISTRY_ADDR}/geowsm-fcache-base:latest"
docker build --target "base" --cache-from "$tag" -t "$tag" -f "docker/fcache.dockerfile" .

tagrn="${GEOWSM_REGISTRY_ADDR}/geowsm-fcache-rnseism-deps:latest"
docker build --target "rnseism-deps" --cache-from "$tagrn" --cache-from "$tag" -t "$tagrn" -f "docker/fcache.dockerfile" .

# shellcheck disable=SC2039
echo "============== Building images =================="

tag="${GEOWSM_REGISTRY_URL}geowsm-app:$TAG"
docker build -t "$tag" -f "docker/app.dockerfile" --build-arg="GEOWSM_REGISTRY_ADDR=${GEOWSM_REGISTRY_ADDR}" .

tag="${GEOWSM_REGISTRY_URL}iworker:$TAG"
docker build -t "$tag" -f "tests/docker/worker.dockerfile" --build-arg="GEOWSM_REGISTRY_ADDR=${GEOWSM_REGISTRY_ADDR}" .

echo "============== Building batch task image ================"

docker build -t "${GEOWSM_REGISTRY_URL}blauncher_batch_task:$TAG" \
  -f "tests/docker/blauncher_batch_task.dockerfile" --build-arg="GEOWSM_REGISTRY_ADDR=${GEOWSM_REGISTRY_ADDR}" .

docker build -t "${GEOWSM_REGISTRY_URL}pyspark-executor:$TAG" \
  -f "tests/docker/pyspark_executor.dockerfile" --build-arg="GEOWSM_REGISTRY_ADDR=${GEOWSM_REGISTRY_ADDR}" .
