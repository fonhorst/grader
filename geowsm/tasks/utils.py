import logging
import os
from typing import Optional, List

from docker import DockerClient
from docker.models.containers import Container
from kubernetes.client import CoreV1Api, V1Pod

from geowsm.env import ENV_VAR_DOCKER_BACKEND_PULL_IMAGE_BEFORE_START

logger = logging.getLogger(__name__)


def get_docker_container_logs(client: DockerClient, labels: List[str], tail: Optional[int]) -> Optional[str]:
    logger.info("Looking for a container with labels %s" % labels)
    containers: List[Container] = client.containers.list(all=True, filters={'label': labels})

    if len(containers) > 1:
        container = containers[0]
        logger.warning(
            "Found more than one container with labels %s. Using container with id %s" % (labels, container.id)
        )
    elif len(containers) == 0:
        logger.warning("Found no containers with labels %s" % labels)
        container = None
    else:
        container = containers[0]

    if container is None:
        return None

    logger.info("Getting logs for container with labels %s" % labels)

    logs = container.logs(tail=tail)
    logs = logs.decode('utf-8') if logs else None

    logger.info("Logs obtained for container with labels %s. Logs legth %s" % (labels, len(logs) if logs else -1))

    return logs


def get_kubernetes_container_logs(client: CoreV1Api, namespace: str, labels: List[str], tail: Optional[int]):
    logger.info("Looking for a container with labels %s" % labels)

    pods: List[V1Pod] = client.list_namespaced_pod(
        namespace=namespace,
        label_selector=','.join(labels)
    ).items

    if len(pods) > 1:
        pod = pods[0]
        logger.warning(
            "Found more than one container with labels %s. Using container with id %s" % (labels, pod.metadata.name)
        )
    elif len(pods) == 0:
        logger.warning("Found no containers with labels %s" % labels)
        pod = None
    else:
        pod = pods[0]

    if pod.status.phase not in ['Running', 'Succeeded', 'Failed']:
        logger.info("Pod of container with labels %s is not running or "
                    "succeeded or failed state (state: %s)" % (labels, pod.status.phase))
        return None

    if pod is None:
        return None

    logger.info("Getting logs for container with labels %s" % labels)

    logs = client.read_namespaced_pod_log(pod.metadata.name, namespace=namespace, tail_lines=tail)

    logger.info("Logs obtained for container with labels %s" % labels)

    return logs


def try_pull_image(client: DockerClient, image: str):
    pull_flag = os.environ.get(ENV_VAR_DOCKER_BACKEND_PULL_IMAGE_BEFORE_START, 'yes')
    pull_flag = pull_flag.lower() in ['yes', '1']
    parts = image.split('/')

    if pull_flag and len(parts) == 1:
        logger.info("Skipping pulling image %s because it looks like local image "
                    "and doesn't have repository name" % image)
    elif pull_flag:
        logger.info("Updating image %s on the node by pulling." % image)
        client.images.pull(image)


