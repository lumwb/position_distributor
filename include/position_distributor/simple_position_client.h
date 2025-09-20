#pragma once

#include "simple_network.h"
#include "position.h"
#include <unordered_map>
#include <mutex>
#include <atomic>
#include <functional>
#include <thread>
#include <chrono>

namespace position_distributor
{

    class SimplePositionClient
    {
    public:
        using PositionUpdateCallback = std::function<void(const PositionUpdate &)>;
        using ConnectionCallback = std::function<void(bool connected)>;

        SimplePositionClient(const std::string &server_host, uint16_t server_port,
                             const std::string &strategy_id);
        ~SimplePositionClient();

        bool connect();
        void disconnect();

        // Send position update
        bool sendPositionUpdate(const std::vector<SymbolPosition> &positions);

        // Get current positions for all strategies
        std::unordered_map<std::string, std::vector<SymbolPosition>> getAllPositions();

        // Get positions for a specific strategy
        std::vector<SymbolPosition> getStrategyPositions(const std::string &strategy_id);

        // Set callbacks
        void setPositionUpdateCallback(PositionUpdateCallback callback);
        void setConnectionCallback(ConnectionCallback callback);

        // Client state
        bool isConnected() const;
        const std::string &getStrategyId() const { return strategy_id_; }

        // Statistics
        size_t getReceivedUpdates() const;
        size_t getSentUpdates() const;

    private:
        void handleServerData(const std::string &data);
        void sendHeartbeat();
        void heartbeatLoop();

        SimpleTcpClient client_;
        std::string strategy_id_;
        uint64_t next_sequence_number_;

        std::unordered_map<std::string, std::vector<SymbolPosition>> strategy_positions_;
        std::mutex positions_mutex_;

        PositionUpdateCallback position_callback_;
        ConnectionCallback connection_callback_;

        std::atomic<size_t> received_updates_;
        std::atomic<size_t> sent_updates_;
        std::atomic<bool> running_;
        std::thread heartbeat_thread_;

        static constexpr uint16_t HEARTBEAT_INTERVAL_MS = 1000;
    };

} // namespace position_distributor
