#![allow(dead_code)]
//! # Gossip Sub
//!
//! A library implementing gossip-based message dissemination for distributed
//! systems, with membership management, fan-out routing, and anti-entropy
//! synchronization for eventual consistency.
//!
//! ## Overview
//!
//! Gossip protocols (also called epidemic protocols) spread information through
//! a network much like a rumor spreads through a crowd. Each node periodically
//! selects a random subset of peers and shares state. This provides:
//!
//! - **Scalability**: O(log N) rounds to reach all N nodes
//! - **Fault tolerance**: Works despite node failures and message loss
//! - **Decentralization**: No single point of failure
//!
//! ## Example
//!
//! ```
//! use gossip_sub::{GossipMessage, PeerState, GossipRouter, MembershipProtocol, AntiEntropy};
//!
//! let mut router = GossipRouter::new("node-1", 3);
//! let mut membership = MembershipProtocol::new("node-1", 5000);
//!
//! membership.add_peer("node-2");
//! membership.add_peer("node-3");
//!
//! let msg = GossipMessage::new("node-1", b"hello".to_vec(), 5);
//! router.broadcast(&msg);
//! ```

use std::collections::{HashMap, HashSet, VecDeque};

/// A gossip message propagating through the network.
///
/// Each message carries its origin, payload, time-to-live (hop limit),
/// and a monotonically increasing sequence number for ordering.
#[derive(Debug, Clone, PartialEq)]
pub struct GossipMessage {
    /// Origin node that created this message.
    pub origin: String,
    /// Message payload bytes.
    pub payload: Vec<u8>,
    /// Time-to-live: decremented at each hop, dropped when 0.
    pub ttl: u32,
    /// Monotonic sequence number from the origin.
    pub sequence: u64,
}

impl GossipMessage {
    /// Create a new gossip message.
    pub fn new(origin: &str, payload: Vec<u8>, ttl: u32) -> Self {
        static mut COUNTER: u64 = 0;
        let seq = unsafe {
            COUNTER += 1;
            COUNTER
        };
        Self {
            origin: origin.to_string(),
            payload,
            ttl,
            sequence: seq,
        }
    }

    /// Create with explicit sequence number (for testing/deserialization).
    pub fn with_sequence(origin: &str, payload: Vec<u8>, ttl: u32, sequence: u64) -> Self {
        Self {
            origin: origin.to_string(),
            payload,
            ttl,
            sequence,
        }
    }

    /// Decrement TTL. Returns false if the message has expired (TTL was 0).
    pub fn hop(&mut self) -> bool {
        if self.ttl == 0 {
            return false;
        }
        self.ttl -= 1;
        true
    }

    /// Check if the message has remaining hops.
    pub fn is_alive(&self) -> bool {
        self.ttl > 0
    }
}

/// Tracks the state of a peer in the gossip network.
///
/// Maintains the peer's subscriptions, last seen time, and reliability
/// metrics used for routing decisions.
#[derive(Debug, Clone)]
pub struct PeerState {
    /// Connected peers and their subscription topics.
    peers: HashMap<String, HashSet<String>>,
    /// Last known heartbeat time for each peer.
    last_seen: HashMap<String, u64>,
    /// Current logical time (incremented on events).
    clock: u64,
}

impl PeerState {
    /// Create an empty peer state tracker.
    pub fn new() -> Self {
        Self {
            peers: HashMap::new(),
            last_seen: HashMap::new(),
            clock: 0,
        }
    }

    /// Add a peer with optional initial subscriptions.
    pub fn add_peer(&mut self, peer_id: &str) {
        self.peers.insert(peer_id.to_string(), HashSet::new());
        self.touch(peer_id);
    }

    /// Remove a peer entirely.
    pub fn remove_peer(&mut self, peer_id: &str) -> bool {
        self.last_seen.remove(peer_id);
        self.peers.remove(peer_id).is_some()
    }

    /// Subscribe a peer to a topic.
    pub fn subscribe(&mut self, peer_id: &str, topic: &str) -> bool {
        self.touch(peer_id);
        if let Some(subs) = self.peers.get_mut(peer_id) {
            subs.insert(topic.to_string())
        } else {
            false
        }
    }

    /// Unsubscribe a peer from a topic.
    pub fn unsubscribe(&mut self, peer_id: &str, topic: &str) -> bool {
        if let Some(subs) = self.peers.get_mut(peer_id) {
            subs.remove(topic)
        } else {
            false
        }
    }

    /// Get all peers subscribed to a topic.
    pub fn subscribers(&self, topic: &str) -> Vec<String> {
        self.peers
            .iter()
            .filter(|(_, subs)| subs.contains(topic))
            .map(|(id, _)| id.clone())
            .collect()
    }

    /// Get the set of topics a peer is subscribed to.
    pub fn topics(&self, peer_id: &str) -> HashSet<String> {
        self.peers.get(peer_id).cloned().unwrap_or_default()
    }

    /// Get all known peer IDs.
    pub fn peer_ids(&self) -> Vec<String> {
        self.peers.keys().cloned().collect()
    }

    /// Number of known peers.
    pub fn peer_count(&self) -> usize {
        self.peers.len()
    }

    /// Update last-seen timestamp for a peer.
    pub fn touch(&mut self, peer_id: &str) {
        self.clock += 1;
        self.last_seen.insert(peer_id.to_string(), self.clock);
    }

    /// Get last-seen time for a peer.
    pub fn last_seen(&self, peer_id: &str) -> Option<u64> {
        self.last_seen.get(peer_id).copied()
    }

    /// Check if a peer is known.
    pub fn has_peer(&self, peer_id: &str) -> bool {
        self.peers.contains_key(peer_id)
    }
}

impl Default for PeerState {
    fn default() -> Self {
        Self::new()
    }
}

/// Gossip router implementing fan-out message delivery.
///
/// When broadcasting, the router selects `fanout` random peers and forwards
/// the message to them. This creates an epidemic-style spreading pattern
/// where the message reaches all nodes in O(log N) rounds with high probability.
#[derive(Debug)]
pub struct GossipRouter {
    /// Local node ID.
    node_id: String,
    /// Fan-out degree: how many peers to forward to.
    fanout: usize,
    /// Messages we've already seen (deduplication by origin + sequence).
    seen: HashSet<(String, u64)>,
    /// Outbound message queue.
    pending: VecDeque<GossipMessage>,
    /// Total messages forwarded.
    forwarded_count: u64,
}

impl GossipRouter {
    /// Create a new router for the given node with specified fan-out.
    pub fn new(node_id: &str, fanout: usize) -> Self {
        Self {
            node_id: node_id.to_string(),
            fanout: fanout.max(1),
            seen: HashSet::new(),
            pending: VecDeque::new(),
            forwarded_count: 0,
        }
    }

    /// Check if we've already seen a message.
    pub fn is_seen(&self, msg: &GossipMessage) -> bool {
        self.seen.contains(&(msg.origin.clone(), msg.sequence))
    }

    /// Mark a message as seen.
    fn mark_seen(&mut self, msg: &GossipMessage) {
        self.seen.insert((msg.origin.clone(), msg.sequence));
    }

    /// Broadcast a message to the network. Marks it as seen and queues it.
    pub fn broadcast(&mut self, msg: &GossipMessage) -> usize {
        if self.is_seen(msg) {
            return 0;
        }
        self.mark_seen(msg);
        let count = self.fanout.min(self.seen.len().saturating_sub(1)).max(1);
        self.pending.push_back(msg.clone());
        self.forwarded_count += 1;
        count
    }

    /// Receive a message from a peer. Returns true if it's new.
    pub fn receive(&mut self, msg: &mut GossipMessage) -> bool {
        if self.is_seen(msg) {
            return false;
        }
        self.mark_seen(msg);
        if msg.hop() {
            self.pending.push_back(msg.clone());
            self.forwarded_count += 1;
        }
        true
    }

    /// Drain pending outbound messages.
    pub fn drain_pending(&mut self) -> Vec<GossipMessage> {
        self.pending.drain(..).collect()
    }

    /// Get number of seen messages.
    pub fn seen_count(&self) -> usize {
        self.seen.len()
    }

    /// Get total forwarded count.
    pub fn forwarded_count(&self) -> u64 {
        self.forwarded_count
    }

    /// Get fanout degree.
    pub fn fanout(&self) -> usize {
        self.fanout
    }
}

/// Membership protocol managing join/leave and failure detection via heartbeats.
///
/// Each node periodically sends heartbeats. If a peer isn't heard from within
/// the failure timeout, it's marked as suspected and eventually removed.
#[derive(Debug)]
pub struct MembershipProtocol {
    /// This node's ID.
    node_id: String,
    /// Heartbeat interval in milliseconds.
    heartbeat_interval_ms: u64,
    /// Failure timeout in milliseconds (typically 3× heartbeat interval).
    failure_timeout_ms: u64,
    /// Known members and their last heartbeat time.
    members: HashMap<String, u64>,
    /// Suspicion levels: how many timeouts since last heartbeat.
    suspicion: HashMap<String, u32>,
    /// Current logical time.
    time: u64,
}

impl MembershipProtocol {
    /// Create a new membership protocol instance.
    pub fn new(node_id: &str, failure_timeout_ms: u64) -> Self {
        Self {
            node_id: node_id.to_string(),
            heartbeat_interval_ms: failure_timeout_ms / 3,
            failure_timeout_ms,
            members: HashMap::new(),
            suspicion: HashMap::new(),
            time: 0,
        }
    }

    /// Add a peer to the membership list.
    pub fn add_peer(&mut self, peer_id: &str) {
        self.time += 1;
        self.members.insert(peer_id.to_string(), self.time);
        self.suspicion.remove(peer_id);
    }

    /// Remove a peer from the membership list.
    pub fn remove_peer(&mut self, peer_id: &str) -> bool {
        self.suspicion.remove(peer_id);
        self.members.remove(peer_id).is_some()
    }

    /// Record a heartbeat from a peer.
    pub fn receive_heartbeat(&mut self, peer_id: &str) {
        if self.members.contains_key(peer_id) {
            self.time += 1;
            self.members.insert(peer_id.to_string(), self.time);
            self.suspicion.remove(peer_id);
        }
    }

    /// Tick: advance time and check for failures.
    /// Returns list of peers that became suspected or removed.
    pub fn tick(&mut self) -> Vec<String> {
        self.time += 1;
        let timeout_threshold = self.time.saturating_sub(
            (self.failure_timeout_ms / self.heartbeat_interval_ms.max(1)).max(3),
        );
        let mut failed = Vec::new();
        let threshold = if timeout_threshold > 3 { timeout_threshold - 3 } else { 0 };

        for (peer, &last_time) in &self.members {
            if last_time < threshold {
                *self.suspicion.entry(peer.clone()).or_insert(0) += 1;
                if self.suspicion[peer] >= 3 {
                    failed.push(peer.clone());
                }
            }
        }
        for peer in &failed {
            self.members.remove(peer);
            self.suspicion.remove(peer);
        }
        failed
    }

    /// Get the list of live members.
    pub fn members(&self) -> Vec<String> {
        self.members.keys().cloned().collect()
    }

    /// Get number of live members.
    pub fn member_count(&self) -> usize {
        self.members.len()
    }

    /// Check if a peer is alive.
    pub fn is_alive(&self, peer_id: &str) -> bool {
        self.members.contains_key(peer_id)
    }

    /// Get suspicion level for a peer.
    pub fn suspicion_level(&self, peer_id: &str) -> u32 {
        self.suspicion.get(peer_id).copied().unwrap_or(0)
    }

    /// Generate a heartbeat payload to send to peers.
    pub fn heartbeat(&self) -> (&str, u64) {
        (&self.node_id, self.time)
    }
}

/// Anti-entropy mechanism for healing missed messages via periodic sync.
///
/// Nodes periodically exchange digests (summaries of what they've seen) and
/// reconcile differences. This ensures eventual delivery even when gossip
/// fan-out misses some nodes.
#[derive(Debug)]
pub struct AntiEntropy {
    /// Messages known to this node: (origin, sequence) → payload hash.
    known: HashMap<(String, u64), u64>,
    /// Pending messages to request from peers.
    missing: HashSet<(String, u64)>,
    /// Sync generation counter.
    generation: u64,
}

impl AntiEntropy {
    /// Create a new anti-entropy tracker.
    pub fn new() -> Self {
        Self {
            known: HashMap::new(),
            missing: HashSet::new(),
            generation: 0,
        }
    }

    /// Record that we've seen a message.
    pub fn record(&mut self, origin: &str, sequence: u64) {
        self.generation += 1;
        self.known.insert((origin.to_string(), sequence), self.generation);
    }

    /// Get a digest of all known messages for exchange.
    pub fn digest(&self) -> Vec<(String, u64)> {
        self.known.keys().map(|(o, s)| (o.clone(), *s)).collect()
    }

    /// Compute what we're missing compared to a peer's digest.
    /// Returns the set of (origin, sequence) pairs we don't have.
    pub fn diff(&self, peer_digest: &[(String, u64)]) -> Vec<(String, u64)> {
        peer_digest
            .iter()
            .filter(|k| !self.known.contains_key(k))
            .cloned()
            .collect()
    }

    /// Request missing messages.
    pub fn request_missing(&mut self, missing: Vec<(String, u64)>) {
        for m in missing {
            self.missing.insert(m);
        }
    }

    /// Drain the list of missing messages we need to request.
    pub fn drain_missing(&mut self) -> Vec<(String, u64)> {
        self.missing.drain().collect()
    }

    /// Get the current generation number.
    pub fn generation(&self) -> u64 {
        self.generation
    }

    /// Number of known messages.
    pub fn known_count(&self) -> usize {
        self.known.len()
    }

    /// Number of pending missing requests.
    pub fn missing_count(&self) -> usize {
        self.missing.len()
    }
}

impl Default for AntiEntropy {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_gossip_message_creation() {
        let msg = GossipMessage::new("n1", vec![1, 2, 3], 5);
        assert_eq!(msg.origin, "n1");
        assert_eq!(msg.payload, vec![1, 2, 3]);
        assert_eq!(msg.ttl, 5);
    }

    #[test]
    fn test_message_hop() {
        let mut msg = GossipMessage::with_sequence("n1", vec![], 2, 1);
        assert!(msg.hop()); // ttl: 2 → 1
        assert!(msg.is_alive());
        assert!(msg.hop()); // ttl: 1 → 0
        assert!(!msg.is_alive());
        assert!(!msg.hop()); // already 0
    }

    #[test]
    fn test_peer_state_add_remove() {
        let mut ps = PeerState::new();
        ps.add_peer("n1");
        ps.add_peer("n2");
        assert_eq!(ps.peer_count(), 2);
        assert!(ps.has_peer("n1"));
        assert!(ps.remove_peer("n1"));
        assert!(!ps.has_peer("n1"));
        assert_eq!(ps.peer_count(), 1);
    }

    #[test]
    fn test_peer_state_subscribe() {
        let mut ps = PeerState::new();
        ps.add_peer("n1");
        ps.add_peer("n2");
        ps.subscribe("n1", "topic-a");
        ps.subscribe("n1", "topic-b");
        ps.subscribe("n2", "topic-a");
        let subs = ps.subscribers("topic-a");
        assert_eq!(subs.len(), 2);
        let topics = ps.topics("n1");
        assert_eq!(topics.len(), 2);
    }

    #[test]
    fn test_peer_state_unsubscribe() {
        let mut ps = PeerState::new();
        ps.add_peer("n1");
        ps.subscribe("n1", "topic-a");
        ps.unsubscribe("n1", "topic-a");
        assert!(ps.subscribers("topic-a").is_empty());
    }

    #[test]
    fn test_peer_state_last_seen() {
        let mut ps = PeerState::new();
        ps.add_peer("n1");
        let t1 = ps.last_seen("n1").unwrap();
        ps.touch("n1");
        let t2 = ps.last_seen("n1").unwrap();
        assert!(t2 > t1);
    }

    #[test]
    fn test_router_broadcast() {
        let mut router = GossipRouter::new("n1", 3);
        let msg = GossipMessage::with_sequence("n1", vec![1], 5, 1);
        router.broadcast(&msg);
        assert_eq!(router.seen_count(), 1);
        assert_eq!(router.drain_pending().len(), 1);
    }

    #[test]
    fn test_router_dedup() {
        let mut router = GossipRouter::new("n1", 3);
        let msg = GossipMessage::with_sequence("n1", vec![1], 5, 1);
        router.broadcast(&msg);
        router.broadcast(&msg); // duplicate
        assert_eq!(router.seen_count(), 1);
    }

    #[test]
    fn test_router_receive_new() {
        let mut router = GossipRouter::new("n1", 3);
        let mut msg = GossipMessage::with_sequence("n2", vec![2], 5, 1);
        assert!(router.receive(&mut msg));
        assert_eq!(router.seen_count(), 1);
    }

    #[test]
    fn test_router_receive_duplicate() {
        let mut router = GossipRouter::new("n1", 3);
        let mut msg = GossipMessage::with_sequence("n2", vec![2], 5, 1);
        router.receive(&mut msg);
        let mut msg2 = GossipMessage::with_sequence("n2", vec![2], 5, 1);
        assert!(!router.receive(&mut msg2));
    }

    #[test]
    fn test_membership_add_remove() {
        let mut m = MembershipProtocol::new("n1", 5000);
        m.add_peer("n2");
        m.add_peer("n3");
        assert_eq!(m.member_count(), 2);
        assert!(m.is_alive("n2"));
        m.remove_peer("n2");
        assert!(!m.is_alive("n2"));
    }

    #[test]
    fn test_membership_heartbeat() {
        let mut m = MembershipProtocol::new("n1", 5000);
        m.add_peer("n2");
        m.receive_heartbeat("n2");
        assert!(m.is_alive("n2"));
    }

    #[test]
    fn test_membership_tick_failure() {
        let mut m = MembershipProtocol::new("n1", 5000);
        m.add_peer("n2");
        // Tick many times without heartbeat
        for _ in 0..20 {
            m.tick();
        }
        assert!(!m.is_alive("n2"));
    }

    #[test]
    fn test_anti_entropy_record_and_digest() {
        let mut ae = AntiEntropy::new();
        ae.record("n1", 1);
        ae.record("n1", 2);
        ae.record("n2", 1);
        assert_eq!(ae.known_count(), 3);
        let digest = ae.digest();
        assert_eq!(digest.len(), 3);
    }

    #[test]
    fn test_anti_entropy_diff() {
        let mut ae1 = AntiEntropy::new();
        ae1.record("n1", 1);
        ae1.record("n1", 2);
        let mut ae2 = AntiEntropy::new();
        ae2.record("n1", 1);
        let digest = ae1.digest();
        let diff = ae2.diff(&digest);
        assert_eq!(diff.len(), 1);
        assert_eq!(diff[0], ("n1".to_string(), 2));
    }

    #[test]
    fn test_anti_entropy_missing() {
        let mut ae = AntiEntropy::new();
        ae.request_missing(vec![("n2".to_string(), 5), ("n3".to_string(), 1)]);
        assert_eq!(ae.missing_count(), 2);
        let drained = ae.drain_missing();
        assert_eq!(drained.len(), 2);
        assert_eq!(ae.missing_count(), 0);
    }
}
