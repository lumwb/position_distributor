#pragma once

#include "position_distributor/message.h"
#include "position_distributor/position.h"
#include <boost/asio.hpp>
#include <string>
#include <memory>
#include <thread>
#include <atomic>
#include <functional>

namespace position_distributor
{

    class PositionClient : public MessageHandler
    {
    public:
        using PositionUpdateCallback = std::function<void(const PositionUpdate &)>;

        PositionClient(const std::string &strategy_id,
                       const std::string &server_host,
                       uint16_t server_port);
        ~PositionClient();

        bool connect();
        void disconnect();

        // Publish position updates
        void publishPositions(const std::vector<SymbolPosition> &positions);

        // Subscribe to position updates
        void setPositionUpdateCallback(PositionUpdateCallback callback);

        // MessageHandler interface
        void onPositionUpdate(const PositionUpdate &update) override;
        void onHeartbeat() override;
        void onAcknowledge(uint64_t sequence_number) override;
        void onError(const std::string &error) override;

        // Getters
        const std::string &getStrategyId() const { return strategy_id_; }
        bool isConnected() const { return connected_.load(); }
        uint64_t getNextSequenceNumber() { return ++sequence_number_; }

    private:
        void startHeartbeat();
        void sendHeartbeat();
        void handleConnectionError();

        std::string strategy_id_;
        std::string server_host_;
        uint16_t server_port_;

        boost::asio::io_context io_context_;
        std::shared_ptr<TcpConnection> connection_;
        std::thread io_thread_;

        std::atomic<bool> connected_{false};
        std::atomic<uint64_t> sequence_number_{0};

        PositionUpdateCallback position_callback_;
        std::mutex callback_mutex_;

        // Heartbeat
        boost::asio::steady_timer heartbeat_timer_;
        static constexpr int HEARTBEAT_INTERVAL_MS = 5000;
    };

} // namespace position_distributor
