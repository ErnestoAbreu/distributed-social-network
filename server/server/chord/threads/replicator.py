import logging
import time
import threading
import grpc

from typing import Dict, Tuple, Optional

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
            
        channel = grpc.insecure_channel(node.address)
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
        channel = grpc.insecure_channel(node.address)
        try:
            stub = ChordServiceStub(channel)
            stub.Put(KeyValue(key=key, value=value), timeout=TIMEOUT)
        finally:
            channel.close()

    def _replicate_delete(self, node, key: str):
        channel = grpc.insecure_channel(node.address)
        try:
            stub = ChordServiceStub(channel)
            stub.Delete(Key(key=key), timeout=TIMEOUT)
        finally:
            channel.close()

    # ---------------- Partition RPC helpers ----------------

    def _remote_set_partition(self, node, values: Dict[str, str], versions: Dict[str, int], removed: Dict[str, int]) -> bool:
        channel = grpc.insecure_channel(node.address)
        try:
            stub = ChordServiceStub(channel)
            resp = stub.SetPartition(Partition(values=values, versions=versions, removed=removed), timeout=TIMEOUT)
            return bool(getattr(resp, 'ok', False))
        finally:
            channel.close()

    def _remote_resolve_data(self, node, values: Dict[str, str], versions: Dict[str, int], removed: Dict[str, int]) -> Tuple[bool, Dict[str, str], Dict[str, int], Dict[str, int]]:
        channel = grpc.insecure_channel(node.address)
        try:
            stub = ChordServiceStub(channel)
            resp = stub.ResolveData(Partition(values=values, versions=versions, removed=removed), timeout=TIMEOUT)

            if not resp or not getattr(resp, 'ok', False) or not getattr(resp, 'partition', None):
                return False, {}, {}, {}

            part = resp.partition
            return True, dict(part.values), dict(part.versions), dict(part.removed)
        finally:
            channel.close()

    def get_successor_list(self, count=REPLICATION_K, alive_only=False):
        """Get a list of the next 'count' successor nodes for replication.
        Continues trying even if some nodes fail to maximize number of replicas.
        If alive_only=True, only returns alive nodes and continues searching until 'count' alive nodes are found."""
        successors = []

        with self.node.lock:
            current = self.node.finger[0]

        if not current or current.address == self.node.address:
            return successors
        
        # Start with immediate successor
        if alive_only:
            if self.ping_node(current):
                successors.append(current)
        else:
            successors.append(current)

        # Get additional successors by querying each node for its successor
        # Try multiple times even if some nodes fail
        # When alive_only is True, we need to try more attempts to get enough alive nodes
        max_attempts = count * 4 if alive_only else count * 2
        
        count_alive = 1
        for attempt in range(max_attempts):
            if count_alive >= count:
                break
                
            try:
                if not current or current.address == self.node.address:
                    break

                channel = grpc.insecure_channel(current.address)
                try:
                    stub = ChordServiceStub(channel)
                    next_node = stub.FindSuccessor(ID(id=(current.id + 1) % (2 ** self.node.m_bits)), timeout=TIMEOUT)
                finally:
                    channel.close()

                if next_node and next_node.address != self.node.address:
                    # Avoid duplicates
                    if not any(s.address == next_node.address for s in successors):
                        if alive_only:
                            if self.ping_node(next_node):
                                count_alive += 1
                                successors.append(next_node)
                        else:
                            if self.ping_node(next_node):
                                count_alive += 1
                            successors.append(next_node)
                    current = next_node
                else:
                    break

            except Exception as e:
                self.logger.debug(f"Failed to get successor from {current.address}: {e}. Trying to continue...")
                # Try to continue with the next node in the chain even if one fails
                current = next_node if 'next_node' in locals() else None
        return successors
    
    def ping_node(self, node):
        """Check if a node is alive with a short timeout. Retries once on failure."""
        if not node or not node.address:
            return False
        try:
            channel = grpc.insecure_channel(node.address)
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
        alive_successors = self.get_successor_list(REPLICATION_K - 1, alive_only=True)

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
                    self.logger.error(f"Failed to replicate set {key} to {successor.address}: {e}")
                    if successor.address not in failed_replications:
                        failed_replications[successor.address] = []
                    failed_replications[successor.address].append(('set', key))
                    fail_count += 1

            for key in deleted_items.keys():
                try:
                    self._replicate_remove_to_node(successor, key)
                    success_count += 1
                except Exception as e:
                    self.logger.error(f"Failed to replicate delete {key} to {successor.address}: {e}")
                    if successor.address not in failed_replications:
                        failed_replications[successor.address] = []
                    failed_replications[successor.address].append(('remove', key))
                    fail_count += 1
            
            if fail_count == 0:
                self.logger.info(f"Successfully replicated {success_count} items to {successor.address}")
                successful_replications[successor.address] = success_count
            else:
                self.logger.warning(f"Replicated {success_count} items to {successor.address} with {fail_count} failures")
        
        if failed_replications:
            self.logger.warning(f"Replication failures: {failed_replications}")
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

            key_hash = hash_key(key, self.node.id.bit_length())

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
                channel = grpc.insecure_channel(responsible_node.address)
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
                channel = grpc.insecure_channel(successor.address)
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
        """Apply a partition update using LWW semantics."""
        try:
            # Apply deletes first
            for key, del_ver in removed.items():
                local_ver = self._local_version(key)
                local_del = self._local_deleted_version(key)
                if del_ver > local_del and del_ver >= local_ver:
                    self.node.storage.delete(key, version=int(del_ver))

            # Apply sets
            for key, val in values.items():
                inc_ver = int(versions.get(key, 0))
                local_ver = self._local_version(key)
                local_del = self._local_deleted_version(key)
                if inc_ver > local_ver and local_del < inc_ver:
                    self.node.storage.put(key, val, version=inc_ver if inc_ver > 0 else None)

            return True
        except Exception as e:
            self.logger.error(f"set_partition error: {e}")
            return False

    def resolve_data(self, values: Dict[str, str], versions: Dict[str, int], removed: Dict[str, int]) -> Tuple[Dict[str, str], Dict[str, int], Dict[str, int]]:
        """Resolve conflicts against our local storage and return what the caller should keep."""
        res_values: Dict[str, str] = {}
        res_versions: Dict[str, int] = {}
        res_removed: Dict[str, int] = {}

        # Resolve sets
        for key, incoming_val in values.items():
            inc_ver = int(versions.get(key, 0))
            local_ver = self._local_version(key)
            local_del = self._local_deleted_version(key)

            # Local delete wins
            if local_del >= local_ver and local_del > 0:
                if local_del > inc_ver:
                    res_removed[key] = local_del
                else:
                    self.node.storage.put(key, incoming_val, version=inc_ver if inc_ver > 0 else None)
                continue

            if local_ver > inc_ver:
                local_val = self.node.storage.get(key) or ""
                res_values[key] = local_val
                res_versions[key] = local_ver
            else:
                self.node.storage.put(key, incoming_val, version=inc_ver if inc_ver > 0 else None)

        # Resolve deletes
        for key, inc_del in removed.items():
            inc_del = int(inc_del)
            local_ver = self._local_version(key)
            local_del = self._local_deleted_version(key)

            if local_del > inc_del:
                res_removed[key] = local_del
                continue

            if local_ver > inc_del:
                local_val = self.node.storage.get(key) or ""
                res_values[key] = local_val
                res_versions[key] = local_ver
                continue

            self.node.storage.delete(key, version=inc_del)

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
                channel = grpc.insecure_channel(succ.address)
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
                channel = grpc.insecure_channel(node_addr)
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
                key_hash = hash_key(key, self.node.id.bit_length())
                responsible = self.node.find_successor(key_hash)
                
                # Keep if we're responsible OR if we're a replica holder
                should_keep = False
                if responsible.address == self.node.address:
                    should_keep = True
                else:
                    # Check if we're in the first K successors (replica holder)
                    try:
                        channel = grpc.insecure_channel(responsible.address)
                        try:
                            stub = ChordServiceStub(channel)
                            current = responsible
                            for _ in range(REPLICATION_K - 1):
                                next_resp = stub.FindSuccessor(ID(id=(current.id + 1) % (2 ** self.node.id.bit_length())), timeout=TIMEOUT)
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
                key_hash = hash_key(key, self.node.id.bit_length())
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