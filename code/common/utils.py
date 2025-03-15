"""
Utility functions for the distributed platform.
"""
import json
import time
import logging
from typing import Any, Dict, List, Optional, Union

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

def get_logger(name: str) -> logging.Logger:
    """Get a logger with the specified name."""
    return logging.getLogger(name)

def serialize_to_json(obj: Any) -> str:
    """Serialize an object to JSON string."""
    if hasattr(obj, '__dict__'):
        return json.dumps(obj.__dict__)
    return json.dumps(obj)

def deserialize_from_json(json_str: str, cls: Any = None) -> Any:
    """Deserialize a JSON string to an object."""
    data = json.loads(json_str)
    if cls:
        return cls(**data)
    return data

class Timer:
    """Simple timer for measuring execution time."""
    
    def __init__(self, name: str = None):
        self.name = name
        self.start_time = None
        self.end_time = None
    
    def __enter__(self):
        self.start_time = time.time()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.end_time = time.time()
        if self.name:
            logging.info(f"{self.name} took {self.elapsed_ms():.2f} ms")
    
    def elapsed(self) -> float:
        """Return elapsed time in seconds."""
        if self.start_time is None:
            return 0
        end_time = self.end_time if self.end_time is not None else time.time()
        return end_time - self.start_time
    
    def elapsed_ms(self) -> float:
        """Return elapsed time in milliseconds."""
        return self.elapsed() * 1000

class RateLimiter:
    """Simple token bucket rate limiter."""
    
    def __init__(self, rate: float, capacity: float):
        """
        Initialize rate limiter.
        
        Args:
            rate: Tokens per second
            capacity: Maximum number of tokens
        """
        self.rate = rate
        self.capacity = capacity
        self.tokens = capacity
        self.last_refill = time.time()
    
    def _refill(self):
        """Refill tokens based on elapsed time."""
        now = time.time()
        elapsed = now - self.last_refill
        self.tokens = min(self.capacity, self.tokens + elapsed * self.rate)
        self.last_refill = now
    
    def acquire(self, tokens: float = 1.0) -> bool:
        """
        Try to acquire tokens.
        
        Args:
            tokens: Number of tokens to acquire
            
        Returns:
            True if tokens were acquired, False otherwise
        """
        self._refill()
        if self.tokens >= tokens:
            self.tokens -= tokens
            return True
        return False
    
    def wait_for_tokens(self, tokens: float = 1.0) -> float:
        """
        Wait for tokens to become available.
        
        Args:
            tokens: Number of tokens to acquire
            
        Returns:
            Time waited in seconds
        """
        self._refill()
        if self.tokens >= tokens:
            self.tokens -= tokens
            return 0.0
        
        # Calculate wait time
        wait_time = (tokens - self.tokens) / self.rate
        time.sleep(wait_time)
        self.tokens = 0
        self.last_refill = time.time()
        return wait_time

class CircuitBreaker:
    """Circuit breaker pattern implementation."""
    
    CLOSED = 'closed'  # Normal operation
    OPEN = 'open'      # Failing, don't allow operations
    HALF_OPEN = 'half-open'  # Testing if service is back
    
    def __init__(self, failure_threshold: int = 5, 
                 recovery_timeout: float = 30.0,
                 half_open_max_calls: int = 3):
        """
        Initialize circuit breaker.
        
        Args:
            failure_threshold: Number of failures before opening circuit
            recovery_timeout: Time in seconds before attempting recovery
            half_open_max_calls: Maximum calls in half-open state
        """
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.half_open_max_calls = half_open_max_calls
        
        self.state = self.CLOSED
        self.failure_count = 0
        self.last_failure_time = 0
        self.half_open_calls = 0
    
    def allow_request(self) -> bool:
        """
        Check if request should be allowed.
        
        Returns:
            True if request is allowed, False otherwise
        """
        if self.state == self.CLOSED:
            return True
        
        if self.state == self.OPEN:
            # Check if recovery timeout has elapsed
            if time.time() - self.last_failure_time > self.recovery_timeout:
                self.state = self.HALF_OPEN
                self.half_open_calls = 0
                return True
            return False
        
        # Half-open state
        if self.half_open_calls < self.half_open_max_calls:
            self.half_open_calls += 1
            return True
        return False
    
    def record_success(self):
        """Record a successful operation."""
        if self.state == self.HALF_OPEN:
            self.reset()
        elif self.state == self.CLOSED:
            self.failure_count = 0
    
    def record_failure(self):
        """Record a failed operation."""
        self.last_failure_time = time.time()
        
        if self.state == self.HALF_OPEN:
            self.state = self.OPEN
            return
        
        if self.state == self.CLOSED:
            self.failure_count += 1
            if self.failure_count >= self.failure_threshold:
                self.state = self.OPEN
    
    def reset(self):
        """Reset circuit breaker to closed state."""
        self.state = self.CLOSED
        self.failure_count = 0
        self.half_open_calls = 0
