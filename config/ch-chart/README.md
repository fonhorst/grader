# Clickhouse Helm Chart

This Helm chart deploys Clickhouse with persistent storage on Kubernetes, including a Zookeeper instance for coordination.

## Prerequisites

- Kubernetes 1.19+
- Helm 3.2.0+
- PV provisioner support in the underlying infrastructure

## Installing the Chart

```bash
helm install clickhouse ./config/ch-chart
```

## Configuration

The following table lists the configurable parameters of the Clickhouse chart and their default values.

| Parameter | Description | Default |
|-----------|-------------|---------|
| `namespace.create` | Create a new namespace | `true` |
| `namespace.name` | Name of the namespace | `clickhouse` |
| `storage.className` | Storage class name | `local-storage` |
| `storage.persistentVolumes` | List of persistent volumes | See values.yaml |
| `storage.persistentVolumeClaims` | List of persistent volume claims | See values.yaml |
| `labels.service` | Service label | `general-clickhouse` |

### Clickhouse Configuration

| Parameter | Description | Default |
|-----------|-------------|---------|
| `clickhouse.enabled` | Enable Clickhouse deployment | `true` |
| `clickhouse.replicas` | Number of Clickhouse replicas | `1` |
| `clickhouse.image.repository` | Clickhouse image repository | `clickhouse/clickhouse-server` |
| `clickhouse.image.tag` | Clickhouse image tag | `22.3.13` |
| `clickhouse.image.pullPolicy` | Image pull policy | `IfNotPresent` |
| `clickhouse.resources` | CPU/Memory resource requests/limits | See values.yaml |
| `clickhouse.service.type` | Kubernetes service type | `ClusterIP` |
| `clickhouse.service.httpPort` | HTTP port | `8123` |
| `clickhouse.service.tcpPort` | Native TCP port | `9000` |
| `clickhouse.persistence.enabled` | Enable persistence | `true` |
| `clickhouse.persistence.claimName` | PVC name to use | `pvc-clickhouse-8` |
| `clickhouse.persistence.mountPath` | Path to mount volume | `/var/lib/clickhouse` |
| `clickhouse.config.settings` | Additional Clickhouse settings | See values.yaml |

### Zookeeper Configuration

| Parameter | Description | Default |
|-----------|-------------|---------|
| `zookeeper.enabled` | Enable Zookeeper deployment | `true` |
| `zookeeper.replicas` | Number of Zookeeper replicas | `1` |
| `zookeeper.image.repository` | Zookeeper image repository | `zookeeper` |
| `zookeeper.image.tag` | Zookeeper image tag | `3.8.0` |
| `zookeeper.image.pullPolicy` | Image pull policy | `IfNotPresent` |
| `zookeeper.resources` | CPU/Memory resource requests/limits | See values.yaml |
| `zookeeper.service.type` | Kubernetes service type | `ClusterIP` |
| `zookeeper.service.clientPort` | Client port | `2181` |
| `zookeeper.service.serverPort` | Server port | `2888` |
| `zookeeper.service.leaderElectionPort` | Leader election port | `3888` |
| `zookeeper.persistence.enabled` | Enable persistence | `true` |
| `zookeeper.persistence.claimName` | PVC name to use | `pvc-zookeeper` |
| `zookeeper.persistence.mountPath` | Path to mount volume | `/data` |

## Adding Multiple Replicas

To add multiple Clickhouse or Zookeeper replicas, modify the `replicas` parameter and ensure you have the corresponding PVs and PVCs defined in the `storage` section.

## Storage Configuration

You can define multiple persistent volumes and claims by adding entries to the arrays in values.yaml.

## Adding Clickhouse Server Configuration

To add Clickhouse server deployment, you will need to extend this chart with additional templates. 