import time
import threading

class FailureDetector(threading.Thread):
    def __init__(self, node, interval):
        super().__init__(daemon=True)
        self.node = node
        self.interval = interval

    def run(self):
        while True:
            time.sleep(self.interval)
            try:
                self.node.ping_node(self.node.successor)
            except Exception:
                print("[FAILURE] Sucesor caído")
                self.node.successor = self.node.find_successor(self.node.id + 1)