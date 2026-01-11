class FingerTable:
    def __init__(self, node_id, m):
        self.node_id = node_id
        self.m = m
        self.table = [None] * m


    def start(self, i):
        return (self.node_id + 2 ** i) % (2 ** self.m)