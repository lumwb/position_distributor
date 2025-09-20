#pragma once

#include "simple_network.h"
#include "position.h"
#include <unordered_map>
#include <mutex>
#include <atomic>
#include <thread>
#include <chrono>

namespace position_distributor
{

    class SimplePositionServer
    {
    public:
        SimplePositionServer(uint16_t port);
        ~SimplePositionServer();

        bool start();
        void stop();

        // Get current positions for all strategies
        std::unordered_map<std::string, std::vector<SymbolPosition>> getAllPositions();

        // Get positions for a specific strategy
        std::vector<SymbolPosition> getStrategyPositions(const std::string &strategy_id);

        // Get server statistics
        size_t getClientCount();
        size_t getTotalUpdates() const;

    private:
        void handleClientMessage(int client_fd, const std::string &data);
        void broadcastPositionUpdate(const PositionUpdate &update);
        void sendHeartbeat();
        void heartbeatLoop();

        SimpleTcpServer server_;
        std::unordered_map<std::string, std::vector<SymbolPosition>> strategy_positions_;
        std::unordered_map<std::string, uint64_t> last_sequence_numbers_;
        std::mutex positions_mutex_;

        std::atomic<size_t> total_updates_;
        std::atomic<bool> running_;
        std::thread heartbeat_thread_;

        static constexpr uint16_t HEARTBEAT_INTERVAL_MS = 1000;
    };

} // namespace position_distributor
