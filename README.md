# Position Distributor

A high-performance position distribution system using shared memory IPC for ultra-low latency communication between trading applications on the same box.

## Overview

This system implements a **Single Producer Multiple Consumer (SPMC)** architecture where each strategy acts as a single producer publishing position updates for an exchange to multiple subscribers. The communication is based on lock-free shared memory ring buffers, designed for minimal latency and maximum throughput.

## High-Level Architecture

```text
    ┌─────────────────┐         ┌──────────────────────────────────┐         ┌─────────────────┐
    │   Process 1     │         │         Shared Memory            │         │   Process 2     │
    │  (BINANCE)      │ Publish │                                  │ Publish │  (COINBASE)     │
    │                 ├────────▶│  Topic: position_update.BINANCE  │◀────────┤                 │
    │ PositionClient  │         │  Topic: position_update.COINBASE │         │ PositionClient  │
    │                 │Subscribe│                                  │Subscribe│                 │
    │                 │◀────────┤                                  ├────────▶│                 │
    └─────────────────┘         └──────────────────────────────────┘         └─────────────────┘

    • Process 1: Publishes BINANCE positions, subscribes to COINBASE positions
    • Process 2: Publishes COINBASE positions, subscribes to BINANCE positions  
```

## Shared Memory Layout

Each topic has its own shared memory region with the following structure:

```text
┌─────────────────────────────────────────────────────────────────────────────┐
│                          SharedMemoryHeader                                │
├─────────────────┬─────────────────┬─────────────────┬─────────────────────┤
│ magic_number    │ version         │ term_length     │ term_count          │
│ (0xAE70A1D0)    │ (schema v2)     │ (1MB default)   │ (3 buffers)         │
├─────────────────┴─────────────────┼─────────────────┴─────────────────────┤
│ producer_pos                      │ min_consumer_pos                      │
│ (absolute write position)         │ (cached min read pos)                 │
├───────────────────────────────────┼───────────────────────────────────────┤
│ active_term_id                    │ producer_heartbeat_ns                 │
│ (current term buffer)             │ (producer liveness)                   │
├───────────────────────────────────┴───────────────────────────────────────┤
│                      SubscriberSlots[64] Array                            │
├───────────┬───────────┬───────────┬───────────┬─────────────────────────┤
│  Slot 0   │  Slot 1   │  Slot 2   │  Slot 3   │         ...             │
├───────────┼───────────┼───────────┼───────────┼─────────────────────────┤
│ cursor    │ cursor    │ cursor    │ cursor    │ Each slot tracks:      │
│ (pos)     │ (pos)     │ (pos)     │ (pos)     │ • Read position        │
│ heartbeat │ heartbeat │ heartbeat │ heartbeat │ • Heartbeat (ns)       │
│ (ns)      │ (ns)      │ (ns)      │ (ns)      │ • Active flag (0/1)    │
│ active    │ active    │ active    │ active    │ • Process ID           │
│ (0/1)     │ (0/1)     │ (0/1)     │ (0/1)     │ • Generation counter   │
│           │           │           │           │                         │
└───────────┴───────────┴───────────┴───────────┴─────────────────────────┘
├─────────────────────────────────────────────────────────────────────────────┤
│                         Term Buffer 0 (1MB)                                │
├─────────────────────────────────────────────────────────────────────────────┤
│                          MessageFrame 1                                    │
├─────────────┬─────────────┬─────────────┬─────────┬───────────────────────┤
│frame_length │ frame_type  │ reserved    │sess_id  │term_id│term_offset    │
│  (atomic)   │(DATA/PADD)  │             │(produc) │       │               │
├─────────────┴─────────────┴─────────────┴─────────┴───────────────────────┤
│                       Payload Data                                        │
│                  (PositionUpdate encoded)                                 │
├─────────────────────────────────────────────────────────────────────────────┤
│                          MessageFrame 2                                    │
│                       (Similar structure...)                               │
├─────────────────────────────────────────────────────────────────────────────┤
│                         Term Buffer 1 (1MB)                                │
│                       (Similar structure...)                               │
├─────────────────────────────────────────────────────────────────────────────┤
│                         Term Buffer 2 (1MB)                                │
│                       (Similar structure...)                               │
└─────────────────────────────────────────────────────────────────────────────┘
```

### Key Fields Explained

**SharedMemoryHeader:**
- `producer_pos`: Absolute byte position where producer will write next message
- `min_consumer_pos`: Cached minimum position across all active consumers ("head")
- `active_term_id`: Which term buffer (0, 1, 2) is currently active for writing ("tail")
- `producer_heartbeat_ns`: Nanosecond timestamp updated by producer for liveness detection

**SubscriberSlots:**
- `cursor`: Consumer's last-read absolute position (independent per consumer)
- `last_heartbeat_ns`: Consumer's heartbeat timestamp for liveness detection
- `active`: Whether this slot is claimed by an active consumer (atomic flag)
- `generation`: Incremented when slot is reused to detect stale references

**MessageFrame:**
- `frame_length`: Atomic field, 0 during write, set last to "publish" the message
- `session_id`: Unique ID per producer instance (detects producer restarts)
- `term_id` + `term_offset`: Position within the term buffer for this message

### Key Design Features

1. **Shared Memory IPC**: Lock-free ring buffers for same-box communication
2. **SPMC Pattern**: Single producer per topic, multiple consumers supported
3. **Topic-based Routing**: Each exchange publishes to dedicated `position_update.<EXCHANGE>` topics
4. **Atomic Operations**: Lock-free publishing with memory barriers for consistency
5. **Sequence Numbering**: Per-strategy message ordering (producer monotonically increase seq number, consumer also check for ordering for safety)
6. **Heartbeat Mechanism**: Producer/consumer liveness tracking with timeouts
7. **Multi-subscriber Support**: Up to 64 concurrent subscribers per topic with independent cursors

## Build Instructions

### Prerequisites

- **C++17** compatible compiler (GCC 7+, Clang 6+)
- **CMake 3.16+**

### Compilation

```bash
# Create build directory
mkdir build
cd build

# Configure with CMake
cmake ..

# Build the project
make -j$(nproc)
```

This will create:
- `libposition_shm.a` - Static library with core functionality
- `exchange_client_example` - Example executable

### Running 

Example 1: Single producer, multiple consumer
```bash
# Terminal 1: Start BINANCE publisher + subscribe to COINBASE
./exchange_client_example -p BINANCE -s COINBASE

# Terminal 2: Start COINBASE publisher + subscribe to BINANCE  
./exchange_client_example -p COINBASE -s BINANCE

# Terminal 3: Subscribe-only mode (monitor multiple exchanges)
./exchange_client_example -s BINANCE -s COINBASE -s KRAKEN

# Terminal 4: Publisher-only mode
./exchange_client_example -p KRAKEN
```

Example 2: Producer Crash
```bash
# Terminal 1
./exchange_client_example -p BINANCE

# Terimanl 2
./exchange_client_example -s BINANCE

# Kill Terminal 1 process

# Expect Terminal 2 to see logs like "BINANCE PUBLISHER DISCONNECTED"

# Start BINANCE producer agian
./exchange_client_example -p BINANCE

# Expect Terminal 2 to detect reconnection with logs like: "Producer session change detected"
```

Feel free to try other scenarios too

### Command Line Options

```
Options:
  -p <exchange>    Publisher exchange (e.g., BINANCE, COINBASE)
  -s <exchange>    Subscribe to exchange (can be used multiple times)
  --clear-subs     Clear all subscriber slots on startup (dev mode)
  -h               Show help
```
## FAQ
1. Why shared memory over UDP / TCP?
- Since shared memory was listed in assignment, assume that the context is of same-box IPC
- Better latency / throughput: 
    - **Zero kernel involvement**: Shared memory bypasses the entire kernel networking stack (sockets, TCP/UDP headers, IP routing, etc.)
    - **Zero copy**: Direct memory access vs copying data through socket buffers → network buffers → application buffers
    - **No system calls**: Direct memory read/write vs send()/recv() system calls that require kernel mode switching
- Also to try my hand at lock-free programming in C++

2. Why separate "topic" / shared memory for each exchange?
- No requirement for total ordering between exchanges
- Each strategy can subscribe to what they are interested in (e.g. A-B cross-exchange arb maybe not interested in exchange C positions?)

3. What happens if publisher crashes?
- PositionPublisher periodically writes timestamp ns to SharedMemoryHeader
- PositionSubscriber periodicaly checks this field, and when the it breaches a configurable limit, invokes disconnect_callback
- PositionSubscriber will stil continue polling, and when the PositionPublisher comes up again with a different session_id, will reset its sequence number will reset sequence numbering and continue reading gracefully

4. What about subscriber crashing?
- Unlike a order gateway / OMS, subscriber disconnecting shouldn't be a big deal to publishers
- Currently in the publisher-side we periodically check for crashed procceses in checkSubscriberHeartbeats and release their SubscriberSlot
- BUT, when the subscriber comes up again, it needs the latest full snapshot from the publisher (which we have omitted - but the full implementation of a position gateway should cache in-memory the latest position values and either periodically publish full snapshots OR listen for new subscriber joining)

5. What happens if payload size exceeds term_length?
- We have not implemented message fragmentation and reassembly (good next step)
- Now we simply not publish

6. Why do we need PositionUpdate? Can't we stick with SymbolPosition
- Batching

7. How are we handling backpressure now?
- **Producer-side**:
  - Before writing, checks `producerCanWrite()` → verifies `(producer_pos + msg_size) - min_consumer_pos <= ring_capacity`
  - Uses cached `min_consumer_pos` for fast path, recomputes actual minimum across all active subscribers when needed
  - If ring buffer full → **drops message** and logs warning (no blocking to maintain low latency)
  - Periodically cleans up dead/slow subscriber slots via heartbeat monitoring to prevent them from blocking the ring
- **Consumer-side**:
  - Each consumer has **independent cursor** → fast consumers aren't blocked by slow ones
  - **Overrun detection**: if `(producer_pos - cursor) > ring_capacity` → consumer fell too far behind
  - When overrun detected → consumer **jumps to safe position** (typically `producer_pos - ring_capacity/2`) and loses messages


## Requirements Fulfilled

### 1. Correctness ✅

- **Atomic publishing**: Messages are published atomically using `frame_length` as publication barrier
- **Memory consistency**: Atomic operations with appropriate memory ordering prevent data races
- **Session tracking**: Producer session IDs help detect publisher restarts
- **Slot management**: Subscriber slots use atomic flags for safe concurrent access
- **Basic validation**: Magic numbers and version checks for shared memory integrity

### 2. Order Preservation ✅

- **Per-strategy/Exchange sequencing**: Each strategy has monotonically increasing sequence numbers
- **FIFO delivery**: Ring buffer structure maintains message ordering within topics
- **Independent cursors**: Each subscriber tracks its own position independently

### 3. Resilience ✅

- **Heartbeat tracking**: Producer and subscriber heartbeats with nanosecond timestamps
- **Timeout detection**: Configurable timeouts for liveness monitoring
- **Producer restart handling**: Session ID changes indicate publisher restarts
- **Subscriber cleanup**: Inactive subscriber slots are detected and can be reclaimed
- **Error callbacks**: Application-level error handling for connection issues

## Assumptions

### Same-Box Deployment
- **Shared Memory Requirement**: All components must run on the same physical machine
- **Process Isolation**: Different exchanges run as separate processes
- **Memory Sharing**: All processes share the same virtual memory space for topics

### SPMC Communication Pattern
- **Single Producer**: For each exchange only-one publisher
- **Multiple Consumers**: Can be in different processes or multiple subscription of same topic on same box
- **Topic Isolation**: Each exchange has its dedicated topic namespace (e.g. position_update.BINANCE)

## Future Improvements

### 1. Use better Logging Framework
**Current State**: Basic console logging with custom Logger class
```cpp
// Current approach
LOG_INFO("Position update: " + update.toString());
```

**Improvement**: Integrate industry-standard logging (spdlog, glog)
```cpp
// Proposed approach  
SPDLOG_INFO("Position update: strategy={}, seq={}, positions={}", 
           update.strategy_id, update.sequence_number, update.positions.size());
```

**Benefits**:
- Structured logging with configurable formats
- High-performance asynchronous logging
- Log rotation and archival
- Multiple output targets (console, files, network)

### 2. Professional Codec Libraries
**Current State**: Custom binary serialization
```cpp
// Current encoding
encodedSize = encodePositionUpdate(update, buffer);
```

**Improvement**: Simple Binary Encoding (SBE) integration
```cpp
// Proposed SBE approach
PositionUpdateEncoder encoder;
encoder.wrap(buffer, 0, bufferLength)
       .strategyId(update.strategy_id)
       .sequenceNumber(update.sequence_number);
```

**Benefits**:
- Schema evolution support
- Cross-language compatibility
- Standardized wire format


### 3. Cross-Box Support with Reliable UDP
**Current State**: Single-machine shared memory only

**Improvement**: Separate process which takes care of  cross-machine replication
```cpp
// Proposed cross-box architecture
class CrossBoxReplicator {
    // Replicate local shared memory to remote boxes
    void replicateToRemote(const std::string& remote_endpoint);
    
    // Receive and apply remote updates locally  
    void receiveFromRemote(const std::string& topic, 
                          const uint8_t* data, uint32_t length);
};
```

## API Usage Examples

### Basic Publisher
```cpp
PositionClientConfig config("BINANCE");
PositionClient client(config);
client.connect();

std::vector<SymbolPosition> positions = {
    {"BTCUSDT", 1000.5},
    {"ETHUSDT", -250.0}
};

client.publishPositions("BINANCE_STRATEGY_1", positions);
```

### Basic Subscriber
```cpp
PositionClientConfig config;
config.subscribed_exchanges = {"COINBASE", "KRAKEN"};
PositionClient client(config);

client.subscribeToExchange("COINBASE", 
    [](const PositionUpdate& update) {
        std::cout << "Received: " << update.toString() << std::endl;
    });

client.connect();
while (client.isConnected()) {
    client.pollMessages();
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
}
```

## References
1. Cursor (claude)
2. Aeron https://github.com/aeron-io/aeron?tab=readme-ov-file
