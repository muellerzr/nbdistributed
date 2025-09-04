"""
Communication layer for distributed process coordination in Jupyter notebooks.

This module implements the communication infrastructure between the coordinator
(Jupyter kernel) and worker processes. It uses ZMQ (ZeroMQ) for efficient
message passing with features including:
- Asynchronous message handling
- Request-response pattern
- Timeout handling
- Message queuing
- Worker targeting (all/specific ranks)
- Real-time streaming output support

The communication is built on ZMQ's ROUTER-DEALER pattern, which enables:
- Bidirectional communication
- Message routing by worker rank
- Non-blocking operations
- Reliable message delivery
"""

import os
import zmq
import pickle
import threading
import time
from typing import Any, Dict, List, Callable, Optional
from dataclasses import dataclass
import uuid

TIMEOUT = os.get('NBDISTRIBUTED_TIMEOUT', 300)
TIMEOUT = int(TIMEOUT)

@dataclass
class Message:
    """
    Message container for inter-process communication.
    
    This class represents a message in the distributed system. It contains:
    - Unique message identifier
    - Message type (e.g., execute, shutdown, stream_output)
    - Source/destination rank
    - Payload data
    - Timestamp for tracking
    
    Attributes:
        msg_id (str): Unique message identifier (UUID)
        msg_type (str): Type of message (e.g., "execute", "shutdown", "stream_output")
        rank (int): Source/destination rank (-1 for coordinator)
        data (Any): Message payload
        timestamp (float): Unix timestamp of message creation
        
    Example:
        >>> msg = Message(
        ...     msg_id=str(uuid.uuid4()),
        ...     msg_type="execute",
        ...     rank=0,
        ...     data="print('hello')",
        ...     timestamp=time.time()
        ... )
    """
    msg_id: str
    msg_type: str
    rank: int
    data: Any
    timestamp: float


class CommunicationManager:
    """
    Manager for coordinating communication between processes with streaming support.
    
    This class handles all communication between the Jupyter kernel (coordinator)
    and worker processes. It provides:
    - Message routing to specific workers
    - Asynchronous message handling
    - Response collection and timeout management
    - Real-time streaming output callbacks
    - Clean shutdown handling
    
    The manager uses ZMQ's ROUTER-DEALER pattern where:
    - The coordinator (ROUTER) can send messages to specific workers
    - Workers (DEALERS) are identified by their rank
    - Messages are handled asynchronously in a background thread
    - Streaming output is processed immediately via callbacks
    
    Attributes:
        num_processes (int): Total number of worker processes
        base_port (int): Base port for ZMQ communication
        context (zmq.Context): ZMQ context
        coordinator_socket (zmq.Socket): ROUTER socket for coordinator
        message_queue (dict): Queue of pending messages
        response_events (dict): Events for tracking responses
        running (bool): Control flag for message handler thread
        handler_thread (threading.Thread): Background message processing thread
        output_callback (Optional[Callable]): Callback for streaming output
    """

    def __init__(self, num_processes: int, base_port: int = 5555, output_callback: Optional[Callable] = None):
        """
        Initialize the communication manager.
        
        Args:
            num_processes (int): Number of worker processes to coordinate
            base_port (int): Base port for ZMQ communication
            output_callback (Optional[Callable]): Callback function for streaming output
                Should accept (rank: int, text: str, stream_type: str) parameters
            
        The initialization:
        1. Creates ZMQ context and sockets
        2. Sets up message queuing
        3. Configures streaming output callback
        4. Starts background message handler
        
        Note:
            The base_port must be available for binding the ROUTER socket.
            Workers will connect to this port automatically.
        """
        self.num_processes = num_processes
        self.base_port = base_port
        self.output_callback = output_callback
        self.context = zmq.Context()

        # Main process acts as coordinator
        self.coordinator_socket = self.context.socket(zmq.ROUTER)
        self.coordinator_socket.bind(f"tcp://*:{base_port}")

        self.worker_sockets = {}
        self.message_queue = {}
        self.response_events = {}

        # Start message handler thread
        self.running = True
        self.handler_thread = threading.Thread(target=self._message_handler)
        self.handler_thread.daemon = True
        self.handler_thread.start()

    def set_output_callback(self, callback: Callable[[int, str, str], None]):
        """
        Set the callback function for streaming output.
        
        Args:
            callback: Function that takes (rank, text, stream_type) and handles output display
        """
        self.output_callback = callback

    def _message_handler(self):
        """
        Background thread for processing incoming messages with streaming support.
        
        This method runs in a separate thread and:
        1. Polls for incoming messages
        2. Deserializes received messages
        3. Handles streaming output messages immediately via callback
        4. Queues response messages by ID
        5. Signals when all responses are received
        
        The handler uses a 100ms timeout when polling to allow for:
        - Regular checking of the running flag
        - Prevention of busy-waiting
        - Quick shutdown when requested
        
        Streaming output messages are processed immediately and not queued,
        allowing for real-time output display during code execution.
        
        Exceptions in message handling are caught and logged to prevent
        the thread from crashing.
        """
        while self.running:
            try:
                if self.coordinator_socket.poll(100):  # 100ms timeout
                    identity, message = self.coordinator_socket.recv_multipart()
                    msg = pickle.loads(message)

                    # Handle streaming output messages immediately
                    if msg.msg_type == "stream_output":
                        if self.output_callback:
                            try:
                                data = msg.data
                                text = data.get("text", "")
                                stream_type = data.get("stream", "stdout")
                                self.output_callback(msg.rank, text, stream_type)
                            except Exception as e:
                                print(f"Error in output callback: {e}")
                        continue  # Don't queue streaming messages
                    
                    # Handle regular response messages
                    if msg.msg_id not in self.message_queue:
                        self.message_queue[msg.msg_id] = {}

                    self.message_queue[msg.msg_id][msg.rank] = msg

                    # Signal if we have all responses
                    if (
                        msg.msg_id in self.response_events
                        and len(self.message_queue[msg.msg_id]) == self.num_processes
                    ):
                        self.response_events[msg.msg_id].set()

            except zmq.Again:
                continue
            except Exception as e:
                print(f"Error in message handler: {e}")

    def send_to_all(
        self, msg_type: str, data: Any, timeout: float = None
    ) -> Dict[int, Any]:
        """
        Send a message to all workers and collect responses.
        
        This method:
        1. Creates a unique message ID
        2. Sends the message to all workers
        3. Waits for responses from all workers
        4. Returns collected responses
        
        Args:
            msg_type (str): Type of message to send
            data (Any): Data payload for the message
            timeout (float): Maximum time to wait for responses
            
        Returns:
            Dict[int, Any]: Responses from workers, keyed by rank
            
        Raises:
            TimeoutError: If not all workers respond within timeout
            
        Example:
            >>> responses = manager.send_to_all("execute", "print(rank)")
            >>> print(responses)
            {0: {'output': '0\\n'}, 1: {'output': '1\\n'}}
        """
        if timeout is None:
            timeout = TIMEOUT
        msg_id = str(uuid.uuid4())
        message = Message(
            msg_id=msg_id,
            msg_type=msg_type,
            rank=-1,  # From coordinator
            data=data,
            timestamp=time.time(),
        )

        # Set up response collection
        self.response_events[msg_id] = threading.Event()

        # Send to all workers
        serialized = pickle.dumps(message)
        for rank in range(self.num_processes):
            worker_id = f"worker_{rank}".encode()
            self.coordinator_socket.send_multipart([worker_id, serialized])

        # Wait for all responses
        if self.response_events[msg_id].wait(timeout):
            responses = self.message_queue[msg_id]
            del self.message_queue[msg_id]
            del self.response_events[msg_id]
            return {rank: msg.data for rank, msg in responses.items()}
        else:
            raise TimeoutError(f"Timeout waiting for responses to {msg_id}")

    def send_to_rank(
        self, rank: int, msg_type: str, data: Any, timeout: float = None
    ) -> Any:
        """
        Send a message to a specific worker rank.
        
        This is a convenience wrapper around send_to_ranks() for
        single-rank communication.
        
        Args:
            rank (int): Worker rank to send to
            msg_type (str): Type of message to send
            data (Any): Data payload for the message
            timeout (float): Maximum time to wait for response
            
        Returns:
            Any: Response data from the worker
            
        Raises:
            TimeoutError: If worker doesn't respond within timeout
            
        Example:
            >>> response = manager.send_to_rank(0, "execute", "print('hello')")
            >>> print(response)
            {'output': 'hello\\n'}
        """
        if timeout is None:
            timeout = TIMEOUT
        responses = self.send_to_ranks([rank], msg_type, data, timeout)
        return responses[rank]

    def send_to_ranks(
        self, ranks: List[int], msg_type: str, data: Any, timeout: float = None
    ) -> Dict[int, Any]:
        """
        Send a message to specific worker ranks.
        
        This method:
        1. Creates a unique message ID
        2. Sends the message to specified ranks
        3. Waits for responses from those ranks
        4. Returns collected responses
        
        Args:
            ranks (List[int]): List of worker ranks to send to
            msg_type (str): Type of message to send
            data (Any): Data payload for the message
            timeout (float): Maximum time to wait for responses
            
        Returns:
            Dict[int, Any]: Responses from workers, keyed by rank
            
        Raises:
            TimeoutError: If any worker doesn't respond within timeout
            
        Example:
            >>> responses = manager.send_to_ranks([0,2], "execute", "print(rank)")
            >>> print(responses)
            {0: {'output': '0\\n'}, 2: {'output': '2\\n'}}
        """
        if timeout is None:
            timeout = TIMEOUT
        msg_id = str(uuid.uuid4())
        message = Message(
            msg_id=msg_id, msg_type=msg_type, rank=-1, data=data, timestamp=time.time()
        )

        self.response_events[msg_id] = threading.Event()

        serialized = pickle.dumps(message)
        for rank in ranks:
            worker_id = f"worker_{rank}".encode()
            self.coordinator_socket.send_multipart([worker_id, serialized])

        # Modified wait condition for subset of ranks
        start_time = time.time()
        while time.time() - start_time < timeout:
            if msg_id in self.message_queue and len(self.message_queue[msg_id]) == len(
                ranks
            ):
                responses = self.message_queue[msg_id]
                del self.message_queue[msg_id]
                del self.response_events[msg_id]
                return {rank: msg.data for rank, msg in responses.items()}
            time.sleep(0.01)

        raise TimeoutError(f"Timeout waiting for responses from ranks {ranks}")

    def shutdown(self):
        """
        Clean shutdown of the communication manager.
        
        This method:
        1. Stops the message handler thread
        2. Closes the ZMQ socket
        3. Terminates the ZMQ context
        
        This should be called when:
        - The notebook kernel is shutting down
        - The distributed system is being reset
        - Error recovery requires communication restart
        """
        self.running = False
        self.handler_thread.join()
        self.coordinator_socket.close()
        self.context.term()
