import os
import tempfile
from urllib.parse import urlparse
from hdfs import InsecureClient
import logging

logger = logging.getLogger(__name__)

def is_hdfs_path(path: str) -> bool:
    """Check if path is an HDFS URL
    
    Args:
        path: Path to check
        
    Returns:
        bool: True if path is an HDFS URL, False otherwise
    """
    try:
        result = urlparse(path)
        return result.scheme in ('hdfs', 'webhdfs')
    except:
        return False

def download_from_hdfs(hdfs_path: str, local_path: str, hdfs_host: str, hdfs_port: int) -> None:
    """Download file from HDFS to local path
    
    Args:
        hdfs_path: HDFS path to download from
        local_path: Local path to save to
        hdfs_host: HDFS host
        hdfs_port: HDFS port
        
    Raises:
        ValueError: If HDFS host or port are not provided
    """
    if not hdfs_host or not hdfs_port:
        raise ValueError("HDFS host and port must be provided for HDFS downloads")

    # Create HDFS client
    hdfs_client = InsecureClient(f'http://{hdfs_host}:{hdfs_port}')
    
    # Download file
    hdfs_client.download(hdfs_path, local_path, overwrite=True)
    logger.info(f"Downloaded {hdfs_path} to {local_path}")

def get_local_script_path(script_path: str, hdfs_host: str = None, hdfs_port: int = None) -> str:
    """Get local path to script, downloading from HDFS if necessary
    
    Args:
        script_path: Path to script (local or HDFS)
        hdfs_host: HDFS host (required if script is on HDFS)
        hdfs_port: HDFS port (required if script is on HDFS)
        
    Returns:
        str: Local path to script
        
    Raises:
        ValueError: If script is on HDFS but host/port not provided
        Exception: If download fails
    """
    if not is_hdfs_path(script_path):
        return script_path

    # Create temporary file for downloaded script
    temp_dir = tempfile.mkdtemp()
    local_path = os.path.join(temp_dir, os.path.basename(script_path))
    
    try:
        download_from_hdfs(script_path, local_path, hdfs_host, hdfs_port)
        return local_path
    except Exception as e:
        logger.error(f"Failed to download script from HDFS: {str(e)}")
        raise
