from abc import ABC, abstractmethod
from typing import Dict, Any

class BaseEstimator(ABC):
    """Abstract base class for all estimators."""
    
    @abstractmethod
    def check_required_objects(self) -> Dict[str, bool]:
        """Check if all required objects exist in the system.
        
        Returns:
            Dict[str, bool]: Status of required objects
        """
        pass

    @abstractmethod
    def check_data_ingestion(self) -> Dict[str, bool]:
        """Verify data ingestion functionality.
        
        Returns:
            Dict[str, bool]: Status of data ingestion checks
        """
        pass

    @abstractmethod
    def check_fault_tolerance(self) -> Dict[str, bool]:
        """Test system fault tolerance.
        
        Returns:
            Dict[str, bool]: Results of fault tolerance tests
        """
        pass

    @abstractmethod
    def check_performance(self) -> Dict[str, float]:
        """Test system performance under load.
        
        Returns:
            Dict[str, float]: Performance metrics
        """
        pass

    @abstractmethod
    def estimate(self) -> Dict[str, Any]:
        """Run all checks and return comprehensive results.
        
        Returns:
            Dict[str, Any]: Complete estimation results
        """
        return {
            'required_objects': self.check_required_objects(),
            'data_ingestion': self.check_data_ingestion(),
            'fault_tolerance': self.check_fault_tolerance(),
            'performance': self.check_performance()
        } 