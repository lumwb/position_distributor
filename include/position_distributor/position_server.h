#pragma once

#include "position_distributor/message.h"
#include "position_distributor/position.h"
#include <boost/asio.hpp>
#include <unordered_map>
#include <mutex>
#include <memory>
#include <thread>

namespace position_distributor
{

    class PositionServer : public MessageHandler, public std::enable_shared_from_this<PositionServer>
    {
    public:
        PositionServer(uint16_t port);
        ~PositionServer();

        void start();
        void stop();

        // MessageHandler interface
        void onPositionUpdate(const PositionUpdate &update) override;
        void onHeartbeat() override;
        void onAcknowledge(uint64_t sequence_number) override;
        void onError(const std::string &error) override;

        // Statistics
        size_t getClientCount() const;
        size_t getTotalMessages() const;

    private:
        void acceptConnections();
        void handleNewConnection(std::shared_ptr<TcpConnection> connection);
        void broadcastPositionUpdate(const PositionUpdate &update);
        void removeConnection(std::shared_ptr<TcpConnection> connection);

        boost::asio::io_context io_context_;
        boost::asio::ip::tcp::acceptor acceptor_;
        std::vector<std::shared_ptr<TcpConnection>> connections_;
        std::mutex connections_mutex_;

        // Order preservation: track sequence numbers per strategy
        std::unordered_map<std::string, uint64_t> strategy_sequences_;
        std::mutex sequences_mutex_;

        // Statistics
        std::atomic<size_t> total_messages_{0};
        std::atomic<bool> running_{false};

        std::thread server_thread_;
    };

} // namespace position_distributor
