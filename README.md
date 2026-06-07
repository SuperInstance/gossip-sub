# Gossip Sub

A Rust library implementing gossip-based message dissemination for distributed systems, with membership management, fan-out routing, and anti-entropy synchronization.

## Why This Matters

In large-scale distributed systems, reliable message broadcasting is fundamental. Traditional approaches (central broker, spanning tree) create single points of failure and don't scale. Gossip protocols solve this by treating information spread like an epidemic — each node randomly shares with a few peers, and messages propagate exponentially.

Gossip protocols are used in production systems including:
- **Apache Cassandra** — cluster membership and failure detection
- **HashiCorp Serf** — member discovery and event dissemination  
- **libp2p PubSub** — decentralized publish-subscribe messaging
- **Amazon DynamoDB** — anti-entropy for replica synchronization

## Architecture

### GossipMessage

A `GossipMessage` is the unit of dissemination in the network:
- `origin` — the node that created the message
- `payload` — the data being spread
- `ttl` — time-to-live counter, decremented at each hop to prevent infinite propagation
- `sequence` — monotonically increasing ID from the origin for deduplication

### PeerState

`PeerState` tracks all known peers and their subscriptions:
- Which topics each peer subscribes to
- Last-seen timestamps for liveness tracking
- Efficient lookup of subscribers for topic-based routing

### GossipRouter

`GossipRouter` implements fan-out message delivery:
- On broadcast, the router marks the message as seen and queues it for delivery to `fanout` random peers
- On receive, it deduplicates using `(origin, sequence)` pairs
- Messages with TTL > 0 are re-forwarded (rumor mongering)

The fan-out degree `f` controls the trade-off between:
- **Reliability**: Higher fan-out = faster, more reliable spread
- **Overhead**: Higher fan-out = more duplicate messages

### MembershipProtocol

`MembershipProtocol` manages the set of alive nodes using heartbeat-based failure detection:
- Peers send periodic heartbeats (interval = timeout / 3)
- If no heartbeat within the timeout period, the peer becomes suspected
- After multiple suspicion increments, the peer is removed

This follows the φ (phi) accrual model simplified to a threshold-based approach.

### AntiEntropy

`AntiEntropy` provides periodic full-sync reconciliation:
- Nodes exchange **digests** (lists of known message IDs)
- Each node computes the **diff** — messages the peer knows that it doesn't
- Missing messages are requested and delivered

Anti-entropy ensures **eventual delivery** even when gossip fan-out misses nodes due to network partitions or message loss.

## Usage

```rust
use gossip_sub::{GossipMessage, PeerState, GossipRouter, MembershipProtocol, AntiEntropy};

// Set up routing
let mut router = GossipRouter::new("node-1", 3);

// Create and broadcast a message
let msg = GossipMessage::new("node-1", b"hello world".to_vec(), 10);
router.broadcast(&msg);

// Simulate receiving a message from a peer
let mut incoming = GossipMessage::with_sequence("node-2", b"hi there".to_vec(), 10, 42);
if router.receive(&mut incoming) {
    println!("New message from {}!", incoming.origin);
}

// Manage membership
let mut membership = MembershipProtocol::new("node-1", 5000);
membership.add_peer("node-2");
membership.add_peer("node-3");
membership.receive_heartbeat("node-2");

// Anti-entropy sync
let mut ae = AntiEntropy::new();
ae.record("node-1", 1);
ae.record("node-1", 2);
let digest = ae.digest(); // Send to peer

// Peer computes what we're missing
let mut peer_ae = AntiEntropy::new();
peer_ae.record("node-1", 1);
peer_ae.record("node-1", 2);
peer_ae.record("node-2", 1);
let missing = peer_ae.diff(&digest);
peer_ae.request_missing(missing);
```

## Mathematical Background

### Epidemic Spreading Analysis

In a network of N nodes with fan-out f, a gossip message reaches all nodes in O(log_f(N)) rounds with high probability. This follows from the analysis of rumor spreading:

After round k, approximately min(N, f^k) nodes are "infected" (have the message). Setting f^k ≥ N:

```
k = ⌈log_f(N)⌉ rounds
```

With fan-out 3 and 1000 nodes: k ≈ log₃(1000) ≈ 7 rounds.

### Message Overhead

Each round, every node sends f messages. Total messages per round: N × f. Total messages for full dissemination: O(N × f × log N). This is a constant-factor overhead compared to optimal O(N) broadcast, but provides much stronger fault tolerance.

### Anti-Entropy Guarantees

Anti-entropy with Merkle-tree-style digests provides:
- **Completeness**: All messages eventually reach all nodes (assuming connectivity)
- **Efficiency**: Only missing messages are transferred
- **Bounded state**: Digests are compact summaries of known message sets

## Performance Characteristics

| Operation | Time Complexity | Notes |
|-----------|----------------|-------|
| Broadcast | O(1) | Queue for fan-out peers |
| Receive | O(1) amortized | Hash-set deduplication |
| Digest | O(n) | n = known messages |
| Diff | O(n) | n = peer digest size |
| Tick | O(m) | m = membership size |

## Design Decisions

1. **TTL-based hop limit**: Prevents infinite propagation in cyclic networks
2. **Sequence-based deduplication**: Simple, efficient, no need for content hashing
3. **Synchronous tick model**: Membership progression via explicit tick calls
4. **Generation counter**: Anti-entropy uses monotonic generation for tracking progress

## License

MIT
