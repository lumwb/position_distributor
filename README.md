# Position Distributor

A high-performance C++ position distribution system for market makers trading on multiple exchanges. This system ensures consistent position views across different trading strategies while maintaining order preservation and resilience.

## Overview

The Position Distributor is designed to solve the core problem of maintaining consistent position information across multiple trading strategies (clients) that operate as separate processes. It provides:

- **Correctness**: All strategies have a consistent view of positions from different strategies
- **Order Preservation**: Position information is processed in the order it was sent
- **Resilience**: Handles connection drops and process crashes gracefully
- **Low Latency**: Optimized for high-frequency trading environments

## Architecture

The system uses a centralized server architecture with TCP-based inter-process communication:

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Strategy A    │    │   Strategy B    │    │   Strategy C    │
│  (Position      │    │  (Position      │    │  (Position      │
│   Client)       │    │   Client)       │    │   Client)       │
└─────────┬───────┘    └─────────┬───────┘    └─────────┬───────┘
          │                      │                      │
          │ TCP                  │ TCP                  │ TCP
          │                      │                      │
          └──────────────────────┼──────────────────────┘
                                 │
                    ┌─────────────▼─────────────┐
                    │    Position Server        │
                    │  (Central Distributor)    │
                    └───────────────────────────┘
```

## Features

### Core Requirements
- ✅ **Correctness**: All clients maintain consistent position views
- ✅ **Order Preservation**: Sequence numbers ensure message ordering
- ✅ **Resilience**: Automatic reconnection and heartbeat monitoring
- ✅ **Low Latency**: Optimized TCP communication with minimal overhead

### Technical Features
- **Custom Protocol**: Lightweight message serialization
- **Thread Safety**: Concurrent client handling with proper synchronization
- **Heartbeat System**: Connection health monitoring
- **Logging**: Comprehensive logging for debugging and monitoring
- **Cross-Platform**: Works on macOS, Linux, and Windows

## Project Structure

```
position_distributor/
├── CMakeLists.txt                 # Build configuration
├── README.md                      # This file
├── include/position_distributor/  # Header files
│   ├── position.h                 # Core data structures
│   ├── logger.h                   # Logging utilities
│   ├── simple_network.h           # TCP networking
│   ├── simple_position_server.h   # Position server
│   └── simple_position_client.h   # Position client
├── src/
│   ├── common/                    # Shared implementation
│   │   ├── position.cpp
│   │   ├── logger.cpp
│   │   └── simple_network.cpp
│   ├── server/                    # Server implementation
│   │   ├── simple_position_server.cpp
│   │   └── simple_position_server_main.cpp
│   └── client/                    # Client implementation
│       ├── simple_position_client.cpp
│       └── simple_position_client_main.cpp
├── examples/
│   └── simple_test.cpp            # Example usage
└── build/                         # Build output (generated)
```

## Dependencies

- **C++17** or later
- **CMake** 3.16 or later
- **pthread** (for threading support)

No external libraries required - uses only standard C++ libraries for maximum portability.

## Building

### Prerequisites

Ensure you have the following installed:
- C++ compiler with C++17 support (Clang++, GCC, or MSVC)
- CMake 3.16 or later

### Build Steps

1. **Clone and navigate to the project:**
   ```bash
   git clone <repository-url>
   cd position_distributor
   ```

2. **Create build directory:**
   ```bash
   mkdir build && cd build
   ```

3. **Configure and build:**
   ```bash
   cmake ..
   make
   ```

4. **Install (optional):**
   ```bash
   make install
   ```

### Build Output

The build process creates three executables:
- `position_server` - The central position distributor server
- `position_client` - A strategy client for sending/receiving positions
- `simple_test` - Example demonstrating server-client interaction

## Usage

### Starting the Server

```bash
# Start server on default port 8080
./position_server

# Start server on custom port
./position_server 8081
```

### Running Strategy Clients

```bash
# Connect a strategy client
./position_client <strategy_id> [server_host] [server_port]

# Examples:
./position_client BINANCE 127.0.0.1 8080
./position_client HUOBI 127.0.0.1 8080
./position_client COINBASE 127.0.0.1 8080
```

### Running the Example

```bash
# Run the integrated test
./simple_test
```

## API Reference

### Data Structures

#### SymbolPosition
```cpp
struct SymbolPosition {
    std::string symbol;      // Trading symbol (e.g., "BTCUSDT")
    double net_position;     // Net position value
};
```

#### PositionUpdate
```cpp
struct PositionUpdate {
    std::string strategy_id;                    // Strategy identifier
    std::vector<SymbolPosition> positions;      // Position data
    std::chrono::system_clock::time_point timestamp;
    uint64_t sequence_number;                   // For order preservation
};
```

### Server API

#### SimplePositionServer
```cpp
class SimplePositionServer {
public:
    SimplePositionServer(uint16_t port);
    bool start();
    void stop();
    
    // Get current positions
    std::unordered_map<std::string, std::vector<SymbolPosition>> getAllPositions();
    std::vector<SymbolPosition> getStrategyPositions(const std::string& strategy_id);
    
    // Statistics
    size_t getClientCount();
    size_t getTotalUpdates() const;
};
```

### Client API

#### SimplePositionClient
```cpp
class SimplePositionClient {
public:
    SimplePositionClient(const std::string& server_host, uint16_t server_port, 
                        const std::string& strategy_id);
    
    bool connect();
    void disconnect();
    bool sendPositionUpdate(const std::vector<SymbolPosition>& positions);
    
    // Callbacks
    void setPositionUpdateCallback(PositionUpdateCallback callback);
    void setConnectionCallback(ConnectionCallback callback);
    
    // State
    bool isConnected() const;
    const std::string& getStrategyId() const;
};
```

## Message Protocol

The system uses a simple text-based protocol over TCP:

### Position Update Message
```
POS_UPDATE|<strategy_id>|<sequence_number>|<positions>
```

Where positions are formatted as:
```
<symbol1>:<position1>,<symbol2>:<position2>,...
```

### Heartbeat Message
```
HEARTBEAT|<timestamp>
```

### Acknowledge Message
```
ACK|<sequence_number>
```

## Order Preservation

The system ensures message ordering through:

1. **Sequence Numbers**: Each position update includes a monotonically increasing sequence number
2. **Server Validation**: The server rejects out-of-order messages
3. **Client Tracking**: Clients maintain their own sequence numbers

## Resilience Features

### Connection Management
- **Automatic Reconnection**: Clients automatically reconnect on connection loss
- **Heartbeat Monitoring**: Regular heartbeat messages detect dead connections
- **Graceful Shutdown**: Proper cleanup on process termination

### Error Handling
- **Message Validation**: Invalid messages are logged and discarded
- **Connection Recovery**: Failed connections are automatically retried
- **Resource Cleanup**: Proper cleanup of sockets and threads

## Performance Considerations

### Latency Optimization
- **Minimal Serialization**: Simple text-based protocol reduces overhead
- **Direct TCP**: No additional protocol layers
- **Efficient Broadcasting**: Single server-to-all-clients distribution

### Memory Management
- **RAII**: Automatic resource management
- **Move Semantics**: Efficient data transfer
- **Lock-Free Operations**: Where possible, atomic operations are used

## Testing

### Unit Tests
```bash
# Run unit tests (if implemented)
make test
```

### Integration Testing
```bash
# Run the example test
./simple_test
```

### Manual Testing
1. Start the server: `./position_server 8080`
2. Start multiple clients: `./position_client STRATEGY1 127.0.0.1 8080`
3. Observe position updates in the logs

## Logging

The system provides comprehensive logging with different levels:

- **DEBUG**: Detailed debugging information
- **INFO**: General operational information
- **WARN**: Warning messages
- **ERROR**: Error conditions

Log format:
```
HH:MM:SS.mmm [LEVEL] message
```

## Troubleshooting

### Common Issues

1. **Port Already in Use**
   ```
   Error: Failed to bind socket to port 8080
   ```
   Solution: Use a different port or kill the existing process

2. **Connection Refused**
   ```
   Error: Failed to connect to server
   ```
   Solution: Ensure the server is running and accessible

3. **Out-of-Order Messages**
   ```
   Warning: Received out-of-order update
   ```
   Solution: Check client sequence number generation

### Debug Mode

Enable debug logging:
```cpp
Logger::instance().setLevel(LogLevel::DEBUG);
```

## Future Enhancements

### Potential Improvements
- **Binary Protocol**: More efficient serialization
- **Message Queuing**: Persistent message storage
- **Load Balancing**: Multiple server instances
- **Metrics**: Performance monitoring and statistics
- **Configuration**: External configuration files
- **Security**: Authentication and encryption

### Scalability Considerations
- **Connection Pooling**: Reuse connections for multiple strategies
- **Message Batching**: Group multiple updates
- **Partitioning**: Distribute load across multiple servers

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests if applicable
5. Submit a pull request

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Contact

For questions or support, please contact the development team.
