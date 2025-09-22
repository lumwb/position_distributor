# Position Distributor - Shared Memory Implementation

A high-performance position distribution system using shared memory transport, inspired by Aeron's architecture.

## Overview

This system provides ultra-low latency position distribution between processes using:
- **Shared Memory Transport**: Memory-mapped ring buffers for ~100ns latency
- **SBE-style Encoding**: Zero-copy serialization for maximum performance
- **Media Driver Pattern**: Central message routing similar to Aeron
- **Ordering Guarantees**: Strict sequence number validation
- **Heartbeat Monitoring**: Automatic connection health checking

## Architecture

```
PositionPublisher --SHM--> PositionMediaDriver --SHM--> PositionSubscriber
                                    |
                           (Topic: position_update.EXCHANGE)
```

### Components

1. **PositionMediaDriver**: Central message router and session manager
2. **PositionPublisher**: Publishes position updates to specific exchange topics
3. **PositionSubscriber**: Subscribes to position updates from exchange topics  
4. **PositionClient**: High-level client combining publisher + subscriber
5. **SharedMemoryManager**: Memory-mapped ring buffer management

## Building

```bash
mkdir build && cd build
cmake ..
make -j$(nproc)
```

## Usage

### 1. Start the Media Driver

The media driver must be started first:

```bash
./media_driver
```

### 2. Run Exchange Clients

Start multiple exchange clients that both publish and subscribe:

```bash
# Terminal 1: BINANCE client (publishes BINANCE, subscribes to COINBASE)
./exchange_client_example BINANCE COINBASE

# Terminal 2: COINBASE client (publishes COINBASE, subscribes to BINANCE)  
./exchange_client_example COINBASE BINANCE

# Terminal 3: KRAKEN client (publishes KRAKEN, subscribes to both)
./exchange_client_example KRAKEN BINANCE COINBASE
```

### 3. Individual Publisher/Subscriber Examples

For testing individual components:

```bash
# Publisher only
./simple_publisher_example BINANCE

# Subscriber only  
./simple_subscriber_example BINANCE
```

## Key Features

### 1. **Correctness**
- SBE-style binary encoding with validation
- Atomic message frames with length prefixes
- Error detection and reporting via callbacks

### 2. **Order Preservation**  
- Sequence number validation per strategy
- Ring buffer ordering within topics
- Ordering violation detection and reporting

### 3. **Resilience**
- Heartbeat monitoring (1s interval, 3s timeout)
- Automatic session cleanup on failures
- Memory-mapped files survive process restarts
- Connection error callbacks for application handling

### 4. **Performance**
- **Latency**: ~100ns (vs ~10μs TCP)
- **Throughput**: Memory bandwidth limited (GB/s)
- **CPU**: Minimal system calls, zero-copy operations

## Configuration

### Topic Structure
- Format: `position_update.{EXCHANGE}`
- Examples: `position_update.BINANCE`, `position_update.COINBASE`

### Memory Layout
- **Term Size**: 1MB per term (configurable)
- **Terms per Topic**: 3 (triple buffering)
- **Max Exchanges**: 10 (configurable)
- **Shared Memory Path**: `/dev/shm/position_distributor/`

### Message Format (SBE-style)

```cpp
struct PositionUpdateSBE {
    char strategy_id[32];           // Strategy identifier
    uint64_t timestamp;             // Milliseconds since epoch  
    uint64_t sequence_number;       // For ordering validation
    uint32_t position_count;        // Number of positions
    SymbolPositionSBE positions[];  // Variable-length array
};

struct SymbolPositionSBE {
    char symbol[16];                // Fixed-size symbol
    double net_position;            // Position value
};
```

## Monitoring

### Statistics Available
- Published/received message counts
- Sequence numbers and ordering errors
- Active publisher/subscriber counts
- Heartbeat status and connection health

### Example Output
```
=== Media Driver Statistics ===
Publishers: 2
Subscribers: 3  
Active Topics: 2
Topics: position_update.BINANCE, position_update.COINBASE
===============================
```

## Error Handling

All components provide error callbacks for:
- `HEARTBEAT_LOST`: Connection timeout detected
- `SLOW_CONSUMER`: Ring buffer full, backpressure applied
- `CORRUPTED_MEMORY`: Invalid message format detected

Example error handling:
```cpp
client.setErrorCallback([](const std::string& topic, ConnectionError error) {
    LOG_ERROR("Connection issue: " + topic);
    // Application-specific handling (e.g., cancel orders)
});
```

## Comparison with TCP Implementation

| Metric | TCP Version | Shared Memory Version |
|--------|-------------|----------------------|
| Latency | ~10μs | ~100ns |
| Throughput | ~100MB/s | ~GB/s |
| CPU Usage | High | Minimal |
| Complexity | Simple | Moderate |
| Cross-machine | Yes | No |

## Requirements Met

1. **Correctness**: SBE encoding, validation, error detection
2. **Order Preservation**: Sequence numbers, ring buffer ordering  
3. **Resilience**: Heartbeats, error callbacks, automatic cleanup

## Future Enhancements

- Cross-machine replication via UDP
- Schema evolution with proper versioning
- Real-time monitoring dashboard
- Configurable memory layouts
- Snapshot/recovery mechanisms

## Development Notes

This implementation demonstrates production-ready patterns for:
- Lock-free shared memory communication
- Zero-copy message serialization
- Robust session management
- Performance-critical financial systems

The codebase is structured for easy extension and follows modern C++ best practices.
