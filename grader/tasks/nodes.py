from typing import Optional

from pydantic import BaseModel, Extra

from grader.tasks.base import NodeType


class Node(BaseModel):
    # e.g. hostname
    uid: str
    name: str
    ip: str
    node_type: NodeType
    priority_project_id: Optional[str]
    scratch_size_bytes: int
    hdd_size_bytes: int

    class Config:
        extra = Extra.forbid


class NetworkStorage(BaseModel):
    uid: str
    size_bytes: int
    network_storage_base_host_path: str


class Volume(BaseModel):
    uid: Optional[str]
    # storage name this volume belongs to
    storage_uid: str
    project_uid: str
    size_bytes: int


class NetworkStorageVolume(BaseModel):
    volume_uid: Optional[str]
    # storage name this volume belongs to
    storage_uid: str
    project_uid: str
    volume_size_bytes: int
    storage_size_bytes: int
    allocated_size_bytes: int
    network_storage_base_host_path: str

class KubernetesManagerException(Exception):
    pass


class KubernetesException(KubernetesManagerException):
    pass


class NodeManagementException(KubernetesManagerException):
    pass


class NoSuchVolumeException(KubernetesManagerException):
    pass


class VolumeAlreadyExistsException(KubernetesManagerException):
    pass


class VolumeSizeException(KubernetesManagerException):
    pass


class UnknownNetworkStorageException(KubernetesManagerException):
    pass
