import logging
import time
import threading

from typing import Dict, Tuple, Optional

from server.server.security import create_channel
from server.server.chord.protos.chord_pb2 import Empty, KeyValue, Key, ID, Partition
from server.server.chord.protos.chord_pb2_grpc import ChordServiceStub
from server.server.chord.utils.config import TIMEOUT, REPLICATION_K, REPLICATION_INTERVAL, M_BITS
from server.server.chord.utils.hashing import hash_key

from server.server.chord.storage import meta_ver_key, meta_del_key, is_meta_key, base_key_from_meta

class Replicator(threading.Thread):
    def __init__(self, node, interval = REPLICATION_INTERVAL):
        super().__init__(daemon=True)
        self.node = node
        self.interval = interval
        self.logger = logging.getLogger('socialnet.server.chord.replicator')

    # ---------------- Version/Deleted helpers ----------------

    def _local_version(self, key: str) -> int:
        return self.node.storage.get_version(key)

    def _local_deleted_version(self, key: str) -> int:
        return self.node.storage.get_deleted_version(key)

    def _remote_get_str(self, node, key: str) -> Tuple[Optional[str], bool]:
        """Get value from remote node. Returns (value, success) tuple.
        Returns (None, False) if node is unreachable, (value, True) if successful."""
        if not node or not node.address:
            return None, False
            
        channel = create_channel(node.address)
        try:
            stub = ChordServiceStub(channel)
            resp = stub.Get(Key(key=key), timeout=TIMEOUT)
            return resp.value if resp and resp.value is not None else "", True
        except Exception as e:
            self.logger.debug(f"Failed to get {key} from {node.address}: {e}")
            return None, False
        finally:
            channel.close()

    def _remote_get_int(self, node, key: str) -> Tuple[int, bool]:
        """Get int value from remote node. Returns (value, success) tuple.
        Returns (0, False) if node is unreachable, (value, True) if successful."""
        raw, success = self._remote_get_str(node, key)
        if not success:
            return 0, False
        try:
            return int(raw) if raw else 0, True
        except Exception:
            return 0, True

    def _replicate_put(self, node, key: str, value: str):
        channel = create_channel(node.address)
        try:
            stub = ChordServiceStub(channel)
            stub.Put(KeyValue(key=key, value=value), timeout=TIMEOUT)
        finally:
            channel.close()

    def _replicate_delete(self, node, key: str):
        channel = create_channel(node.address)
        try:
            stub = ChordServiceStub(channel)
            stub.Delete(Key(key=key), timeout=TIMEOUT)
        finally:
            channel.close()

    # ---------------- Partition RPC helpers ----------------

    def _remote_set_partition(self, node, values: Dict[str, str], versions: Dict[str, int], removed: Dict[str, int]) -> bool:
        channel = create_channel(node.address)
        try:
            stub = ChordServiceStub(channel)
            resp = stub.SetPartition(Partition(values=values, versions=versions, removed=removed), timeout=TIMEOUT)
            return bool(getattr(resp, 'ok', False))
        finally:
            channel.close()

    def _remote_resolve_data(self, node, values: Dict[str, str], versions: Dict[str, int], removed: Dict[str, int]) -> Tuple[bool, Dict[str, str], Dict[str, int], Dict[str, int]]:
        channel = create_channel(node.address)
        try:
            stub = ChordServiceStub(channel)
            resp = stub.ResolveData(Partition(values=values, versions=versions, removed=removed), timeout=TIMEOUT)

            if not resp or not getattr(resp, 'ok', False) or not getattr(resp, 'partition', None):
                return False, {}, {}, {}

            part = resp.partition
            return True, dict(part.values), dict(part.versions), dict(part.removed)
        finally:
            channel.close()

    def get_successor_list(self, count=REPLICATION_K):
        """Get a list of the next 'count' alive successor nodes for replication.

        This implementation always treats `alive_only` as True: it walks the
        successor chain (via RPC FindSuccessor) and collects up to `count`
        distinct, reachable successors (excluding self). It stops when enough
        successors are found or when the chain cannot be advanced further.
        """
        successors = []

        with self.node.lock:
            current = self.node.finger[0]

        if not current or current.address == self.node.address:
            return successors

        # Walk successors collecting only alive nodes until we have `count`
        visited = set()
        hops_remaining = max(count * 10, 50)
        node_candidate = current

        while len(successors) < count and node_candidate and hops_remaining > 0:
            hops_remaining -= 1

            # avoid infinite loops
            if not node_candidate.address or node_candidate.address in visited:
                break
            visited.add(node_candidate.address)

            # check alive and append if so
            if self.ping_node(node_candidate) and node_candidate.address != self.node.address:
                if not any(s.address == node_candidate.address for s in successors):
                    successors.append(node_candidate)

            # advance to next successor via RPC
            try:
                channel = create_channel(node_candidate.address)
                try:
                    stub = ChordServiceStub(channel)
                    next_node = stub.FindSuccessor(ID(id=(node_candidate.id + 1) % (2 ** self.node.m_bits)), timeout=TIMEOUT)
                finally:
                    channel.close()

                if not next_node or next_node.address == node_candidate.address:
                    break
                node_candidate = next_node
            except Exception as e:
                self.logger.debug(f"Failed to get successor from {getattr(node_candidate, 'address', 'unknown')}: {e}")
                break

        return successors
    
    def ping_node(self, node):
        """Check if a node is alive with a short timeout."""
        if not node or not node.address:
            return False
        try:
            channel = create_channel(node.address)
            try:
                stub = ChordServiceStub(channel)
                stub.Ping(Empty(), timeout=TIMEOUT)
                return True
            finally:
                channel.close()
        except Exception as e:
            self.logger.debug(f"Node {node.address} unreachable: {e}")
            return False

    def replicate_data(self):
        """Replica local data + tombstones to successor nodes (LWW by version)"""
        # Get exactly REPLICATION_K alive successors (excluding self)
        alive_successors = self.get_successor_list(REPLICATION_K, alive_only=True)

        if not alive_successors:
            self.logger.debug("No alive successors available for replication")
            return
        
        base_items = self.node.storage.base_items()
        deleted_items = self.node.storage.deleted_items()

        if not base_items and not deleted_items:
            self.logger.debug("No data to replicate")
            return

        self.logger.info(
            f"Replicating {len(base_items)} keys + {len(deleted_items)} tombstones to {len(alive_successors)} alive successors"
        )

        failed_replications = {}
        successful_replications = {}

        for successor in alive_successors:
            success_count = 0
            fail_count = 0
            
            for key in base_items.keys():
                try:
                    self._replicate_set_to_node(successor, key)
                    success_count += 1
                except Exception as e:
                    self.logger.debug(f"Failed to replicate set {key} to {successor.address}: {e}")
                    if successor.address not in failed_replications:
                        failed_replications[successor.address] = []
                    failed_replications[successor.address].append(('set', key))
                    fail_count += 1
                    break

            for key in deleted_items.keys():
                try:
                    self._replicate_remove_to_node(successor, key)
                    success_count += 1
                except Exception as e:
                    self.logger.debug(f"Failed to replicate delete {key} to {successor.address}: {e}")
                    if successor.address not in failed_replications:
                        failed_replications[successor.address] = []
                    failed_replications[successor.address].append(('remove', key))
                    fail_count += 1
                    break
            
            if fail_count == 0:
                self.logger.info(f"Successfully replicated {success_count} items to {successor.address}")
                successful_replications[successor.address] = success_count
            else:
                self.logger.warning(f"Replicated {success_count} items to {successor.address} with many failures")
        
        if failed_replications:
            self.logger.warning(f"Some replication failures found")
        else:
            self.logger.info(
                f"Successfully replicated all {len(base_items)} keys + {len(deleted_items)} tombstones to {len(alive_successors)} alive successors"
            )

    def _replicate_set_to_node(self, node, key: str):
        local_ver = self._local_version(key)
        local_del = self._local_deleted_version(key)

        if local_del >= local_ver and local_del > 0:
            return

        value = self.node.storage.get(key)
        if value is None:
            return

        remote_ver, remote_ver_ok = self._remote_get_int(node, meta_ver_key(key))
        remote_del, remote_del_ok = self._remote_get_int(node, meta_del_key(key))

        if not remote_ver_ok or not remote_del_ok:
            self.logger.debug(f"Cannot reliably get version info from {node.address}, will attempt replication anyway")

        if remote_ver_ok and local_ver <= remote_ver:
            return
        if remote_del_ok and remote_del >= local_ver:
            return

        self._replicate_put(node, key, value)
        self._replicate_put(node, meta_ver_key(key), str(local_ver))
        try:
            self._replicate_delete(node, meta_del_key(key))
        except Exception:
            pass

    def _replicate_remove_to_node(self, node, key: str):
        local_del = self._local_deleted_version(key)
        if local_del <= 0:
            return

        remote_del, remote_del_ok = self._remote_get_int(node, meta_del_key(key))
        remote_ver, remote_ver_ok = self._remote_get_int(node, meta_ver_key(key))

        if not remote_del_ok or not remote_ver_ok:
            self.logger.debug(f"Cannot reliably get version info from {node.address}, will attempt deletion anyway")

        if remote_del_ok and local_del <= remote_del:
            return
        if remote_ver_ok and local_del < remote_ver:
            return

        try:
            self._replicate_delete(node, key)
        except Exception:
            pass

        self._replicate_put(node, meta_del_key(key), str(local_del))
        try:
            self._replicate_delete(node, meta_ver_key(key))
        except Exception:
            pass

    def resolve_replicas(self):
        """Check and resolve replicas for consistency"""
        items = self.node.storage.base_items()
        keys_to_transfer = []
        for key in items.keys():
            # Hash the key to determine its proper owner (meta keys shouldn't be transferred)
            if is_meta_key(key):
                continue

            key_hash = hash_key(key, self.node.m_bits)

            # Find the successor responsible for this key
            try:
                responsible_node = self.node.find_successor(key_hash)

                # If we're not responsible and it's not a replica we should keep
                if responsible_node.address != self.node.address:
                    if not self._should_keep_replica(key_hash):
                        keys_to_transfer.append((key, responsible_node))

            except Exception as e:
                self.logger.error(f"Failed to resolve replica for key {key}: {e}")

        # Transfer keys that don't belong to us
        for key, target_node in keys_to_transfer:
            try:
                value = self.node.storage.get(key)
                if value is None:
                    continue
                    
                # Transfer value and metadata
                self._replicate_put(target_node, key, value)
                local_ver = self._local_version(key)
                if local_ver > 0:
                    self._replicate_put(target_node, meta_ver_key(key), str(local_ver))
                    
                # Also transfer tombstone if exists
                local_del = self._local_deleted_version(key)
                if local_del > 0:
                    self._replicate_put(target_node, meta_del_key(key), str(local_del))
                    
                # Only purge after successful transfer
                self.node.storage.purge(key)
                self.logger.info(f"Transferred key {key} to {target_node.address}")
            except Exception as e:
                self.logger.error(f"Failed to transfer key {key} to {target_node.address}: {e}")
                # Don't purge on failure - keep the key
    
    def _should_keep_replica(self, key_hash):
        """Determine if this node should keep the replica for the given key hash"""
        try:
            # Find the node responsible for this key
            responsible_node = self.node.find_successor(key_hash)

            # If we are the responsible node, we should keep it
            if responsible_node.address == self.node.address:
                return True
            
            # Check if we're within the first K successors of the responsible node
            # (i.e., we're one of the replica holders)
            try:
                channel = create_channel(responsible_node.address)
                try:
                    stub = ChordServiceStub(channel)
                    
                    # Get successors of the responsible node
                    current = responsible_node
                    for _ in range(REPLICATION_K - 1):
                        next_resp = stub.FindSuccessor(ID(id=(current.id + 1) % (2 ** self.node.m_bits)), timeout=TIMEOUT)
                        if next_resp and next_resp.address == self.node.address:
                            # We're one of the K replicas
                            return True
                        if not next_resp or next_resp.address == responsible_node.address:
                            break
                        current = next_resp
                finally:
                    channel.close()
            except Exception as e:
                self.logger.debug(f"Could not determine replica status for key hash {key_hash}: {e}")
            
            return False
        except Exception as e:
            self.logger.warning(f"Error checking if should keep replica for key hash {key_hash}: {e}")
            return False

    def fetch_replicas_from_successors(self):
        """Fetch replicas from K successors and merge (LWW)"""
        # Get alive successors for fetching replicas
        successors = self.get_successor_list(REPLICATION_K - 1, alive_only=True)

        if not successors:
            self.logger.debug("No valid alive successors to fetch replicas from")
            return
        
        self.logger.info(f"Fetching replicas from {len(successors)} alive successors")
        
        for successor in successors:
            try:
                channel = create_channel(successor.address)
                try:
                    stub = ChordServiceStub(channel)
                    response = stub.GetAllKeys(Empty(), timeout=TIMEOUT)
                finally:
                    channel.close()

                if response and response.items:
                    payload = {kv.key: kv.value for kv in response.items}

                    incoming_values: Dict[str, str] = {}
                    incoming_versions: Dict[str, int] = {}
                    incoming_removed: Dict[str, int] = {}

                    for k, v in payload.items():
                        if k.startswith("__meta_ver__"):
                            bk = base_key_from_meta(k)
                            try:
                                incoming_versions[bk] = int(v) if v else 0
                            except Exception:
                                incoming_versions[bk] = 0
                        elif k.startswith("__meta_del__"):
                            bk = base_key_from_meta(k)
                            try:
                                incoming_removed[bk] = int(v) if v else 0
                            except Exception:
                                incoming_removed[bk] = 0
                        else:
                            incoming_values[k] = v

                    # Apply partition locally (LWW)
                    ok = self.set_partition(incoming_values, incoming_versions, incoming_removed)
                    if ok:
                        self.logger.info(f"Successfully fetched+merged replicas from {successor.address}")

            except Exception as e:
                self.logger.error(f"Failed to fetch replicas from {successor.address}: {e}")

    # ---------------- Partition / resolve (server-side logic) ----------------

    def set_partition(self, values: Dict[str, str], versions: Dict[str, int], removed: Dict[str, int]) -> bool:
        """Merge incoming partition with union semantics and LWW on conflicts."""
        try:
            keys = set(values.keys()) | set(removed.keys()) | set(versions.keys())

            for key in keys:
                if is_meta_key(key):
                    continue

                inc_val = values.get(key)
                inc_ver = int(versions.get(key, 0))
                inc_del = int(removed.get(key, 0))

                local_val = self.node.storage.get(key)
                local_ver = self._local_version(key)
                local_del = self._local_deleted_version(key)

                winner = self._pick_winner(inc_val, inc_ver, inc_del, local_val, local_ver, local_del)
                if winner is None:
                    continue

                source, state, value, ver = winner

                # Only apply if the incoming side won
                if source != "inc":
                    continue

                if state == "del":
                    self.node.storage.delete(key, version=ver if ver > 0 else None)
                else:
                    self.node.storage.put(key, value, version=ver if ver > 0 else None)

            return True
        except Exception as e:
            self.logger.error(f"set_partition error: {e}")
            return False

    def resolve_data(self, values: Dict[str, str], versions: Dict[str, int], removed: Dict[str, int]) -> Tuple[Dict[str, str], Dict[str, int], Dict[str, int]]:
        """Resolve conflicts against our local storage and return what the caller should keep."""
        res_values: Dict[str, str] = {}
        res_versions: Dict[str, int] = {}
        res_removed: Dict[str, int] = {}

        # Consider union of incoming keys and our local keys/tombstones
        local_values = self.node.storage.base_items()
        local_removed = self.node.storage.deleted_items()

        keys = set(values.keys()) | set(versions.keys()) | set(removed.keys()) | set(local_values.keys()) | set(local_removed.keys())

        for key in keys:
            if is_meta_key(key):
                continue

            inc_val = values.get(key)
            inc_ver = int(versions.get(key, 0))
            inc_del = int(removed.get(key, 0))

            local_val = local_values.get(key)
            local_ver = self._local_version(key)
            local_del = self._local_deleted_version(key)

            winner = self._pick_winner(inc_val, inc_ver, inc_del, local_val, local_ver, local_del)
            if winner is None:
                continue

            source, state, value, ver = winner

            # Apply winner locally
            if state == "del":
                self.node.storage.delete(key, version=ver if ver > 0 else None)
                res_removed[key] = ver
            else:
                self.node.storage.put(key, value, version=ver if ver > 0 else None)
                res_values[key] = value
                res_versions[key] = ver

            # If local was newer and we won, ensure caller keeps our version
            if source == "local" and state == "del":
                res_removed[key] = ver
            elif source == "local" and state == "val":
                res_versions[key] = ver

        return res_values, res_versions, res_removed

    # ---------------- Client-side helpers using RPC ----------------

    def replicate_all_data(self, node):
        """Send our whole dataset (values+versions+tombstones) to another node via SetPartition RPC."""
        if not node or not node.address or node.address == self.node.address:
            return

        values = self.node.storage.base_items()
        removed = self.node.storage.deleted_items()
        versions: Dict[str, int] = {}
        for key in values.keys():
            versions[key] = self._local_version(key)

        try:
            ok = self._remote_set_partition(node, values, versions, removed)
            if not ok:
                self.logger.warning(f"replicate_all_data: SetPartition failed to {node.address}")
        except Exception as e:
            self.logger.warning(f"replicate_all_data error to {node.address}: {e}")

    def delegate_to_predecessor(self, predecessor):
        """Resolve conflicts with predecessor (RPC ResolveData) and keep only what predecessor tells us."""
        if not predecessor or not predecessor.address or predecessor.address == self.node.address:
            return

        values = self.node.storage.base_items()
        removed = self.node.storage.deleted_items()
        versions: Dict[str, int] = {k: self._local_version(k) for k in values.keys()}

        ok, res_values, res_versions, res_removed = self._remote_resolve_data(predecessor, values, versions, removed)
        if not ok:
            return

        # Apply returned partition locally (these are the items we should keep)
        self.set_partition(res_values, res_versions, res_removed)

    def initial_sync(self):
        """Aggressively fetch all relevant data when joining the network"""
        self.logger.info("Starting initial replication sync...")
        
        # Collect all unique nodes in the ring to fetch from
        nodes_to_fetch = set()
        
        # Get alive successors
        successors = self.get_successor_list(REPLICATION_K, alive_only=True)
        for succ in successors:
            if succ and succ.address != self.node.address:
                nodes_to_fetch.add(succ.address)
        
        # Get predecessor
        with self.node.lock:
            predecessor = self.node.predecessor
        if predecessor and predecessor.address != self.node.address:
            nodes_to_fetch.add(predecessor.address)
        
        # Try to discover other nodes in the ring by querying successors
        for succ in successors[:2]:  # Query first 2 successors for their predecessors
            if not succ or succ.address == self.node.address:
                continue
            try:
                channel = create_channel(succ.address)
                try:
                    stub = ChordServiceStub(channel)
                    pred = stub.GetPredecessor(Empty(), timeout=TIMEOUT)
                    if pred and pred.address != self.node.address and pred.address != succ.address:
                        nodes_to_fetch.add(pred.address)
                finally:
                    channel.close()
            except Exception as e:
                self.logger.debug(f"Could not get predecessor from {succ.address}: {e}")
        
        self.logger.info(f"Fetching data from {len(nodes_to_fetch)} nodes: {nodes_to_fetch}")
        
        # Fetch from all discovered nodes
        all_incoming_values: Dict[str, str] = {}
        all_incoming_versions: Dict[str, int] = {}
        all_incoming_removed: Dict[str, int] = {}
        
        for node_addr in nodes_to_fetch:
            try:
                channel = create_channel(node_addr)
                try:
                    stub = ChordServiceStub(channel)
                    response = stub.GetAllKeys(Empty(), timeout=TIMEOUT)
                finally:
                    channel.close()
                
                if response and response.items:
                    payload = {kv.key: kv.value for kv in response.items}
                    
                    for k, v in payload.items():
                        if k.startswith("__meta_ver__"):
                            bk = base_key_from_meta(k)
                            try:
                                ver = int(v) if v else 0
                                # Keep highest version
                                if bk not in all_incoming_versions or ver > all_incoming_versions[bk]:
                                    all_incoming_versions[bk] = ver
                            except Exception:
                                pass
                        elif k.startswith("__meta_del__"):
                            bk = base_key_from_meta(k)
                            try:
                                ver = int(v) if v else 0
                                # Keep highest deleted version
                                if bk not in all_incoming_removed or ver > all_incoming_removed[bk]:
                                    all_incoming_removed[bk] = ver
                            except Exception:
                                pass
                        else:
                            # For values, keep the one with highest version
                            if k not in all_incoming_values:
                                all_incoming_values[k] = v
                            else:
                                # Compare versions
                                current_ver = all_incoming_versions.get(k, 0)
                                try:
                                    # Get version for this value from the same payload
                                    new_ver_key = meta_ver_key(k)
                                    if new_ver_key in payload:
                                        new_ver = int(payload[new_ver_key])
                                        if new_ver > current_ver:
                                            all_incoming_values[k] = v
                                except Exception:
                                    pass
                    
                    self.logger.info(f"Fetched {len([k for k in payload.keys() if not is_meta_key(k)])} keys from {node_addr}")
                        
            except Exception as e:
                self.logger.error(f"Failed to fetch from {node_addr}: {e}")
        
        # Now filter: only keep keys we're responsible for OR should replicate
        filtered_values = {}
        filtered_versions = {}
        filtered_removed = {}
        
        for key in all_incoming_values.keys():
            try:
                key_hash = hash_key(key, self.node.m_bits)
                responsible = self.node.find_successor(key_hash)
                
                # Keep if we're responsible OR if we're a replica holder
                should_keep = False
                if responsible.address == self.node.address:
                    should_keep = True
                else:
                    # Check if we're in the first K successors (replica holder)
                    try:
                        channel = create_channel(responsible.address)
                        try:
                            stub = ChordServiceStub(channel)
                            current = responsible
                            for _ in range(REPLICATION_K - 1):
                                next_resp = stub.FindSuccessor(ID(id=(current.id + 1) % (2 ** self.node.m_bits)), timeout=TIMEOUT)
                                if next_resp and next_resp.address == self.node.address:
                                    should_keep = True
                                    break
                                if not next_resp or next_resp.address == responsible.address:
                                    break
                                current = next_resp
                        finally:
                            channel.close()
                    except Exception:
                        pass
                
                if should_keep:
                    filtered_values[key] = all_incoming_values[key]
                    if key in all_incoming_versions:
                        filtered_versions[key] = all_incoming_versions[key]
                        
            except Exception as e:
                self.logger.warning(f"Error determining responsibility for key {key}: {e}")
        
        for key in all_incoming_removed.keys():
            try:
                key_hash = hash_key(key, self.node.m_bits)
                responsible = self.node.find_successor(key_hash)
                if responsible.address == self.node.address:
                    filtered_removed[key] = all_incoming_removed[key]
            except Exception as e:
                self.logger.warning(f"Error determining responsibility for deleted key {key}: {e}")
        
        if filtered_values or filtered_removed:
            self.set_partition(filtered_values, filtered_versions, filtered_removed)
            self.logger.info(f"Acquired {len(filtered_values)} keys and {len(filtered_removed)} tombstones from network")
        else:
            self.logger.info("No keys acquired during initial sync")
        
        self.logger.info("Initial sync completed")

    def run(self):
        """Main loop for periodic replication and resolution"""
        self.logger.info("Replicator thread started")

        # Initial delay to let the node stabilize
        time.sleep(self.interval)
        
        # Perform aggressive initial sync
        try:
            self.initial_sync()
        except Exception as e:
            self.logger.error(f"Error during initial sync: {e}")

        cycle_count = 0
        resolve_every = 5  # Resolve replicas every 5 cycles

        while True:
            try:
                self.replicate_data()

                cycle_count += 1
                if cycle_count >= resolve_every:
                    self.resolve_replicas()
                    cycle_count = 0
            
            except Exception as e:
                self.logger.error(f"Error during replication cycle: {e}")
            
            time.sleep(self.interval)

    # ---------------- Merge helper ----------------

    def _pick_winner(self, inc_val: Optional[str], inc_ver: int, inc_del: int,
                     local_val: Optional[str], local_ver: int, local_del: int):
        """Return (source, state, value, version) for the winning state.

        source: "inc" | "local"
        state:  "val" | "del"
        version: the timestamp chosen for the winning state (>=0)
        value: only set when state == "val"
        """

        inc_state = None
        local_state = None

        # Determine incoming state and timestamp
        if inc_del >= inc_ver and inc_del > 0:
            inc_state = ("del", inc_del)
        elif inc_val is not None or inc_ver > 0:
            inc_state = ("val", inc_ver)
        elif inc_del > 0:
            inc_state = ("del", inc_del)

        # Determine local state and timestamp
        if local_del >= local_ver and local_del > 0:
            local_state = ("del", local_del)
        elif local_val is not None or local_ver > 0:
            local_state = ("val", local_ver)
        elif local_del > 0:
            local_state = ("del", local_del)

        if not inc_state and not local_state:
            return None

        # Fill missing timestamps with 0 so comparisons work
        inc_ts = inc_state[1] if inc_state else 0
        local_ts = local_state[1] if local_state else 0

        # Decide winner
        if inc_state and (inc_ts > local_ts):
            win_source, win_state, win_ver = "inc", inc_state[0], inc_ts
        elif local_state and (local_ts > inc_ts):
            win_source, win_state, win_ver = "local", local_state[0], local_ts
        else:
            # Tie: prefer value over delete, and prefer incoming to break ties if both are same state
            if inc_state and local_state:
                if inc_state[0] == "val" and local_state[0] == "del":
                    win_source, win_state, win_ver = "inc", "val", inc_ts
                elif inc_state[0] == "del" and local_state[0] == "val":
                    win_source, win_state, win_ver = "local", "val", local_ts
                else:
                    # Same state, default to local to reduce churn
                    win_source, win_state, win_ver = "local", local_state[0], local_ts
            else:
                # Only one side present (timestamps equal and zero)
                if inc_state:
                    win_source, win_state, win_ver = "inc", inc_state[0], inc_ts
                else:
                    win_source, win_state, win_ver = "local", local_state[0], local_ts

        win_val = inc_val if win_source == "inc" else local_val
        if win_state == "del":
            win_val = None

        # Ensure non-zero version for writes
        if win_ver <= 0:
            win_ver = self.node.now_version()

        return win_source, win_state, win_val, win_ver