import logging
import time
import threading

class CheckPredecessor(threading.Thread):
    def __init__(self, node, interval):
        super().__init__(daemon=True)
        self.node = node
        self.interval = interval
        self.logger = logging.getLogger('socialnet.server.chord.check_predecessor')

    def run(self):
        """Background thread to check predecessor periodically"""
        while True:
            try:
                time.sleep(self.interval)
                with self.node.lock:
                    pred = self.node.predecessor
                if not pred:
                    continue
                if not self.node.ping_node(pred):
                    with self.node.lock:
                        self.logger.info(f"Predecessor {pred.address} failed, clearing")
                        self.node.predecessor = None
            except Exception as e:
                self.logger.error(f"Check predecessor loop error: {e}")

            
                

