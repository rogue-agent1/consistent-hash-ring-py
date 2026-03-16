#!/usr/bin/env python3
"""Consistent hash ring with virtual nodes. #500 MILESTONE 🏆"""
import hashlib, bisect, sys

class ConsistentHashRing:
    def __init__(self, vnodes=150):
        self.vnodes = vnodes; self.ring = []; self.node_map = {}; self.nodes = set()
    def _hash(self, key):
        return int(hashlib.sha256(key.encode()).hexdigest(), 16)
    def add_node(self, node):
        self.nodes.add(node)
        for i in range(self.vnodes):
            h = self._hash(f"{node}:{i}")
            bisect.insort(self.ring, h); self.node_map[h] = node
    def remove_node(self, node):
        self.nodes.discard(node)
        self.ring = [h for h in self.ring if self.node_map.get(h) != node]
        self.node_map = {h: n for h, n in self.node_map.items() if n != node}
    def get_node(self, key):
        if not self.ring: return None
        h = self._hash(key); idx = bisect.bisect_right(self.ring, h) % len(self.ring)
        return self.node_map[self.ring[idx]]
    def get_nodes(self, key, n=3):
        if not self.ring: return []
        h = self._hash(key); idx = bisect.bisect_right(self.ring, h)
        result = []; seen = set()
        for i in range(len(self.ring)):
            node = self.node_map[self.ring[(idx + i) % len(self.ring)]]
            if node not in seen: seen.add(node); result.append(node)
            if len(result) >= n: break
        return result

if __name__ == "__main__":
    ring = ConsistentHashRing()
    for s in ["server-1", "server-2", "server-3"]: ring.add_node(s)
    keys = sys.argv[1:] or ["user:alice", "user:bob", "session:xyz", "cache:page1"]
    for k in keys: print(f"{k} → {ring.get_node(k)} (replicas: {ring.get_nodes(k, 2)})")
    print("\n--- Remove server-2 ---")
    ring.remove_node("server-2")
    for k in keys: print(f"{k} → {ring.get_node(k)}")
