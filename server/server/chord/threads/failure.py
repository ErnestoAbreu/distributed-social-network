import logging
import time
import threading

class FailureDetector(threading.Thread):
    def __init__(self, node, interval):
        super().__init__(daemon=True)
        self.node = node
        self.interval = interval
        self.logger = logging.getLogger('socialnet.server.chord.failure')

    def run(self):
        while True:
            time.sleep(self.interval)
            try:
                with self.node.lock:
                    succ = self.node.finger[0]

                if succ and not self.node.ping_node(succ):
                    raise Exception('successor unreachable')
            except Exception:
                with self.node.lock:
                    failed = self.node.finger[0].address if self.node.finger[0] else 'unknown'
                self.logger.warning(f'Successor {failed} failed. Finding new successor...')
                new_succ = self.node.find_successor(self.node.id + 1)
                with self.node.lock:
                    self.node.finger[0] = new_succ