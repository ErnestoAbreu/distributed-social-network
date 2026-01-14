import logging
import time
import threading

from server.server.chord.utils.config import M_BITS

class FixFinger(threading.Thread):
    def __init__(self, node, interval):
        super().__init__(daemon=True)
        self.node = node
        self.interval = interval
        self.logger = logging.getLogger('socialnet.server.chord.fix_finger')

    def run(self):
        """Periodically refresh finger table entries"""
        while True:
            try:
                time.sleep(self.interval)
                with self.node.lock:
                    next_finger = self.node.next_finger
                    self.node.next_finger = (self.node.next_finger + 1) % M_BITS

                finger_id = (self.node.id + (2 ** next_finger)) % (2 ** M_BITS)

                try:
                    successor = self.node.find_successor(finger_id)
                    if successor and successor.address:
                        with self.node.lock:
                            self.node.finger[next_finger] = successor
                            if next_finger == 0:
                                self.node.finger[0] = successor
                except Exception as e:
                    self.logger.warning(f"fix_fingers[{next_finger}] failed: {e}")
            except Exception as e:
                self.logger.error(f"Fix fingers loop error: {e}")