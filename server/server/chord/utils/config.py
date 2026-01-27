M_BITS = 8
REPLICATION_K = 3

TIMEOUT = 6

TIMEOUT_LOAD = 12        # Load operations
TIMEOUT_SAVE = 15        # Save operations
TIMEOUT_EXISTS = 9      # Check existence
TIMEOUT_DELETE = 9      # Delete operations
TIMEOUT_FIND_SUCCESSOR = 9  # Find successor in chord ring
TIMEOUT_FIND_CLOSEST = 6    # Find closest preceding node
TIMEOUT_STABILIZE = 9   # Stabilization messages
TIMEOUT_REPLICATE = 12   # Replication messages

STABILIZE_INTERVAL = 3
REPLICATION_INTERVAL = 3
DISCOVERY_INTERVAL = 5
TIMER_INTERVAL = 5
ELECTOR_INTERVAL = 7

EVENT_TIME = "__timer_local_time__"