"""
Messaging system implementation for the distributed platform.
"""
import asyncio
import json
import time
from typing import Any, Callable, Dict, List, Optional, Union
import uuid

from ..common.models import LocationData, Task, TaskResult
from ..common.utils import get_logger, Timer, RateLimiter, CircuitBreaker

logger = get_logger(__name__)

class Message:
    """Base message class for the messaging system."""
    
    def __init__(self, topic: str, key: str, value: Any, 
                 headers: Optional[Dict[str, str]] = None):
        """
        Initialize a message.
        
        Args:
            topic: Message topic
            key: Message key for partitioning
            value: Message payload
            headers: Optional message headers
        """
        self.id = str(uuid.uuid4())
        self.topic = topic
        self.key = key
        self.value = value
        self.headers = headers or {}
        self.timestamp = time.time()
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert message to dictionary."""
        return {
            "id": self.id,
            "topic": self.topic,
            "key": self.key,
            "value": self.value,
            "headers": self.headers,
            "timestamp": self.timestamp
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'Message':
        """Create message from dictionary."""
        msg = cls(
            topic=data["topic"],
            key=data["key"],
            value=data["value"],
            headers=data["headers"]
        )
        msg.id = data["id"]
        msg.timestamp = data["timestamp"]
        return msg
    
    def serialize(self) -> str:
        """Serialize message to JSON string."""
        return json.dumps(self.to_dict())
    
    @classmethod
    def deserialize(cls, data: str) -> 'Message':
        """Deserialize message from JSON string."""
        return cls.from_dict(json.loads(data))


class MessageBroker:
    """In-memory message broker implementation."""
    
    def __init__(self, max_queue_size: int = 10000):
        """
        Initialize message broker.
        
        Args:
            max_queue_size: Maximum number of messages per topic
        """
        self.topics: Dict[str, asyncio.Queue] = {}
        self.subscribers: Dict[str, List[Callable]] = {}
        self.max_queue_size = max_queue_size
        self.running = False
        self.processing_task = None
    
    async def start(self):
        """Start the message broker."""
        if self.running:
            return
        
        self.running = True
        self.processing_task = asyncio.create_task(self._process_messages())
        logger.info("Message broker started")
    
    async def stop(self):
        """Stop the message broker."""
        if not self.running:
            return
        
        self.running = False
        if self.processing_task:
            self.processing_task.cancel()
            try:
                await self.processing_task
            except asyncio.CancelledError:
                pass
        logger.info("Message broker stopped")
    
    async def publish(self, message: Message) -> bool:
        """
        Publish a message to a topic.
        
        Args:
            message: Message to publish
            
        Returns:
            True if message was published, False otherwise
        """
        if not self.running:
            logger.warning("Cannot publish message: broker not running")
            return False
        
        if message.topic not in self.topics:
            self.topics[message.topic] = asyncio.Queue(maxsize=self.max_queue_size)
        
        try:
            self.topics[message.topic].put_nowait(message)
            logger.debug(f"Published message to topic {message.topic}: {message.id}")
            return True
        except asyncio.QueueFull:
            logger.warning(f"Queue full for topic {message.topic}")
            return False
    
    def subscribe(self, topic: str, callback: Callable[[Message], None]):
        """
        Subscribe to a topic.
        
        Args:
            topic: Topic to subscribe to
            callback: Callback function to call when message is received
        """
        if topic not in self.subscribers:
            self.subscribers[topic] = []
        
        self.subscribers[topic].append(callback)
        logger.info(f"Subscribed to topic {topic}")
    
    def unsubscribe(self, topic: str, callback: Callable[[Message], None]):
        """
        Unsubscribe from a topic.
        
        Args:
            topic: Topic to unsubscribe from
            callback: Callback function to remove
        """
        if topic in self.subscribers:
            if callback in self.subscribers[topic]:
                self.subscribers[topic].remove(callback)
                logger.info(f"Unsubscribed from topic {topic}")
    
    async def _process_messages(self):
        """Process messages from all topics."""
        while self.running:
            # Process messages from all topics
            for topic, queue in list(self.topics.items()):
                if not queue.empty():
                    try:
                        message = queue.get_nowait()
                        queue.task_done()
                        
                        # Notify subscribers
                        if topic in self.subscribers:
                            for callback in self.subscribers[topic]:
                                try:
                                    callback(message)
                                except Exception as e:
                                    logger.error(f"Error in subscriber callback: {e}")
                    except asyncio.QueueEmpty:
                        pass
            
            # Sleep to avoid busy waiting
            await asyncio.sleep(0.001)


class MessageProducer:
    """Message producer for the messaging system."""
    
    def __init__(self, broker: MessageBroker, default_topic: Optional[str] = None):
        """
        Initialize message producer.
        
        Args:
            broker: Message broker to use
            default_topic: Default topic to publish to
        """
        self.broker = broker
        self.default_topic = default_topic
        self.rate_limiter = RateLimiter(rate=10000, capacity=10000)  # 10K messages/sec
    
    async def send(self, message: Union[Message, Any], 
                  topic: Optional[str] = None, 
                  key: Optional[str] = None) -> bool:
        """
        Send a message.
        
        Args:
            message: Message or payload to send
            topic: Topic to publish to (overrides default)
            key: Message key (required if message is not a Message instance)
            
        Returns:
            True if message was sent, False otherwise
        """
        # Apply rate limiting
        if not self.rate_limiter.acquire():
            logger.warning("Rate limit exceeded")
            return False
        
        # Create message if needed
        if not isinstance(message, Message):
            if topic is None:
                topic = self.default_topic
            if topic is None:
                raise ValueError("Topic must be specified")
            if key is None:
                raise ValueError("Key must be specified")
            
            message = Message(topic=topic, key=key, value=message)
        
        # Publish message
        return await self.broker.publish(message)


class MessageConsumer:
    """Message consumer for the messaging system."""
    
    def __init__(self, broker: MessageBroker, topics: List[str], 
                 group_id: str, auto_commit: bool = True):
        """
        Initialize message consumer.
        
        Args:
            broker: Message broker to use
            topics: List of topics to subscribe to
            group_id: Consumer group ID
            auto_commit: Whether to automatically commit offsets
        """
        self.broker = broker
        self.topics = topics
        self.group_id = group_id
        self.auto_commit = auto_commit
        self.callbacks: Dict[str, List[Callable]] = {}
        self.running = False
    
    async def start(self):
        """Start the consumer."""
        if self.running:
            return
        
        self.running = True
        
        # Subscribe to topics
        for topic in self.topics:
            self.broker.subscribe(topic, self._on_message)
        
        logger.info(f"Consumer {self.group_id} started")
    
    async def stop(self):
        """Stop the consumer."""
        if not self.running:
            return
        
        self.running = False
        
        # Unsubscribe from topics
        for topic in self.topics:
            self.broker.unsubscribe(topic, self._on_message)
        
        logger.info(f"Consumer {self.group_id} stopped")
    
    def on_message(self, topic: str, callback: Callable[[Message], None]):
        """
        Register callback for a topic.
        
        Args:
            topic: Topic to register callback for
            callback: Callback function to call when message is received
        """
        if topic not in self.callbacks:
            self.callbacks[topic] = []
        
        self.callbacks[topic].append(callback)
    
    def _on_message(self, message: Message):
        """
        Handle received message.
        
        Args:
            message: Received message
        """
        if not self.running:
            return
        
        topic = message.topic
        
        # Call topic-specific callbacks
        if topic in self.callbacks:
            for callback in self.callbacks[topic]:
                try:
                    callback(message)
                except Exception as e:
                    logger.error(f"Error in message callback: {e}")
        
        # Commit offset if auto-commit is enabled
        if self.auto_commit:
            # In a real implementation, this would commit the offset to a persistent store
            pass


class LocationDataProducer:
    """Producer for vehicle location data."""
    
    def __init__(self, broker: MessageBroker, topic: str = "vehicle-locations"):
        """
        Initialize location data producer.
        
        Args:
            broker: Message broker to use
            topic: Topic to publish to
        """
        self.producer = MessageProducer(broker, default_topic=topic)
    
    async def send_location(self, location_data: LocationData) -> bool:
        """
        Send vehicle location data.
        
        Args:
            location_data: Vehicle location data
            
        Returns:
            True if data was sent, False otherwise
        """
        # Use vehicle ID as key for partitioning
        return await self.producer.send(
            location_data.__dict__,
            key=location_data.vehicle_id
        )


class TaskProducer:
    """Producer for computation tasks."""
    
    def __init__(self, broker: MessageBroker, topic: str = "computation-tasks"):
        """
        Initialize task producer.
        
        Args:
            broker: Message broker to use
            topic: Topic to publish to
        """
        self.producer = MessageProducer(broker, default_topic=topic)
    
    async def send_task(self, task: Task) -> bool:
        """
        Send computation task.
        
        Args:
            task: Computation task
            
        Returns:
            True if task was sent, False otherwise
        """
        return await self.producer.send(
            task.__dict__,
            key=task.task_id
        )


class TaskResultProducer:
    """Producer for task results."""
    
    def __init__(self, broker: MessageBroker, topic: str = "task-results"):
        """
        Initialize task result producer.
        
        Args:
            broker: Message broker to use
            topic: Topic to publish to
        """
        self.producer = MessageProducer(broker, default_topic=topic)
    
    async def send_result(self, result: TaskResult) -> bool:
        """
        Send task result.
        
        Args:
            result: Task result
            
        Returns:
            True if result was sent, False otherwise
        """
        return await self.producer.send(
            result.__dict__,
            key=result.task_id
        )
