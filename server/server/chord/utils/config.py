M_BITS = 8
REPLICATION_K = 3

TIMEOUT = 2

TIMEOUT_LOAD = 4        # Load operations
TIMEOUT_SAVE = 5        # Save operations
TIMEOUT_EXISTS = 3      # Check existence
TIMEOUT_DELETE = 3      # Delete operations
TIMEOUT_FIND_SUCCESSOR = 3  # Find successor in chord ring
TIMEOUT_FIND_CLOSEST = 2    # Find closest preceding node
TIMEOUT_STABILIZE = 3   # Stabilization messages
TIMEOUT_REPLICATE = 4   # Replication messages

STABILIZE_INTERVAL = 2
REPLICATION_INTERVAL = 2

EVENT_TIME = "__event_time__"