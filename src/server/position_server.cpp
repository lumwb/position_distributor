#include "position_distributor/position_server.h"
#include "position_distributor/logger.h"
#include <iostream>

namespace position_distributor
{

    PositionServer::PositionServer(uint16_t port)
        : acceptor_(io_context_, boost::asio::ip::tcp::endpoint(boost::asio::ip::tcp::v4(), port))
    {
        LOG_INFO("Position server created on port " + std::to_string(port));
    }

    PositionServer::~PositionServer()
    {
        stop();
    }

    void PositionServer::start()
    {
        running_ = true;
        server_thread_ = std::thread([this]()
                                     {
        acceptConnections();
        io_context_.run(); });
        LOG_INFO("Position server started");
    }

    void PositionServer::stop()
    {
        if (running_)
        {
            running_ = false;
            io_context_.stop();
            if (server_thread_.joinable())
            {
                server_thread_.join();
            }
            LOG_INFO("Position server stopped");
        }
    }

    void PositionServer::acceptConnections()
    {
        auto connection = std::make_shared<TcpConnection>(io_context_, shared_from_this());

        acceptor_.async_accept(connection->socket(),
                               [this, connection](boost::system::error_code ec)
                               {
                                   if (!ec)
                                   {
                                       handleNewConnection(connection);
                                       acceptConnections(); // Continue accepting
                                   }
                                   else
                                   {
                                       LOG_ERROR("Error accepting connection: " + ec.message());
                                   }
                               });
    }

    void PositionServer::handleNewConnection(std::shared_ptr<TcpConnection> connection)
    {
        {
            std::lock_guard<std::mutex> lock(connections_mutex_);
            connections_.push_back(connection);
        }

        connection->start();
        LOG_INFO("New client connected. Total clients: " + std::to_string(getClientCount()));
    }

    void PositionServer::onPositionUpdate(const PositionUpdate &update)
    {
        total_messages_++;

        // Order preservation: check sequence number
        {
            std::lock_guard<std::mutex> lock(sequences_mutex_);
            auto it = strategy_sequences_.find(update.strategy_id);
            if (it != strategy_sequences_.end())
            {
                if (update.sequence_number <= it->second)
                {
                    LOG_WARN("Out-of-order message from " + update.strategy_id +
                             " (expected > " + std::to_string(it->second) +
                             ", got " + std::to_string(update.sequence_number) + ")");
                    return; // Drop out-of-order messages
                }
            }
            strategy_sequences_[update.strategy_id] = update.sequence_number;
        }

        LOG_INFO("Received position update: " + update.toString());
        broadcastPositionUpdate(update);
    }

    void PositionServer::onHeartbeat()
    {
        // Heartbeat received - connection is alive
        LOG_DEBUG("Heartbeat received");
    }

    void PositionServer::onAcknowledge(uint64_t sequence_number)
    {
        LOG_DEBUG("Acknowledge received for sequence " + std::to_string(sequence_number));
    }

    void PositionServer::onError(const std::string &error)
    {
        LOG_ERROR("Client error: " + error);
    }

    void PositionServer::broadcastPositionUpdate(const PositionUpdate &update)
    {
        Message message(MessageType::POSITION_UPDATE,
                        MessageSerializer::serialize(update));

        std::lock_guard<std::mutex> lock(connections_mutex_);

        // Send to all connected clients
        for (auto it = connections_.begin(); it != connections_.end();)
        {
            try
            {
                (*it)->sendMessage(message);
                ++it;
            }
            catch (const std::exception &e)
            {
                LOG_WARN("Failed to send to client, removing: " + std::string(e.what()));
                it = connections_.erase(it);
            }
        }

        LOG_DEBUG("Broadcasted position update to " + std::to_string(connections_.size()) + " clients");
    }

    void PositionServer::removeConnection(std::shared_ptr<TcpConnection> connection)
    {
        std::lock_guard<std::mutex> lock(connections_mutex_);
        connections_.erase(
            std::remove(connections_.begin(), connections_.end(), connection),
            connections_.end());
        LOG_INFO("Client disconnected. Total clients: " + std::to_string(getClientCount()));
    }

    size_t PositionServer::getClientCount() const
    {
        std::lock_guard<std::mutex> lock(connections_mutex_);
        return connections_.size();
    }

    size_t PositionServer::getTotalMessages() const
    {
        return total_messages_.load();
    }

} // namespace position_distributor
