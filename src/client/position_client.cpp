#include "position_distributor/position_client.h"
#include "position_distributor/logger.h"
#include <boost/asio.hpp>

namespace position_distributor
{

    PositionClient::PositionClient(const std::string &strategy_id,
                                   const std::string &server_host,
                                   uint16_t server_port)
        : strategy_id_(strategy_id), server_host_(server_host), server_port_(server_port), heartbeat_timer_(io_context_)
    {
        LOG_INFO("Position client created for strategy: " + strategy_id);
    }

    PositionClient::~PositionClient()
    {
        disconnect();
    }

    bool PositionClient::connect()
    {
        try
        {
            boost::asio::ip::tcp::resolver resolver(io_context_);
            auto endpoints = resolver.resolve(server_host_, std::to_string(server_port_));

            connection_ = std::make_shared<TcpConnection>(io_context_, shared_from_this());

            boost::asio::connect(connection_->socket(), endpoints);
            connection_->start();

            connected_ = true;

            // Start IO thread
            io_thread_ = std::thread([this]()
                                     { io_context_.run(); });

            // Start heartbeat
            startHeartbeat();

            LOG_INFO("Connected to position server at " + server_host_ + ":" + std::to_string(server_port_));
            return true;
        }
        catch (const std::exception &e)
        {
            LOG_ERROR("Failed to connect to position server: " + std::string(e.what()));
            return false;
        }
    }

    void PositionClient::disconnect()
    {
        if (connected_)
        {
            connected_ = false;

            if (connection_)
            {
                connection_->close();
            }

            io_context_.stop();
            if (io_thread_.joinable())
            {
                io_thread_.join();
            }

            LOG_INFO("Disconnected from position server");
        }
    }

    void PositionClient::publishPositions(const std::vector<SymbolPosition> &positions)
    {
        if (!connected_ || !connection_)
        {
            LOG_WARN("Cannot publish positions: not connected");
            return;
        }

        uint64_t seq_num = getNextSequenceNumber();
        PositionUpdate update(strategy_id_, positions, seq_num);

        Message message(MessageType::POSITION_UPDATE,
                        MessageSerializer::serialize(update));

        try
        {
            connection_->sendMessage(message);
            LOG_DEBUG("Published positions: " + update.toString());
        }
        catch (const std::exception &e)
        {
            LOG_ERROR("Failed to publish positions: " + std::string(e.what()));
            handleConnectionError();
        }
    }

    void PositionClient::setPositionUpdateCallback(PositionUpdateCallback callback)
    {
        std::lock_guard<std::mutex> lock(callback_mutex_);
        position_callback_ = callback;
    }

    void PositionClient::onPositionUpdate(const PositionUpdate &update)
    {
        LOG_DEBUG("Received position update: " + update.toString());

        std::lock_guard<std::mutex> lock(callback_mutex_);
        if (position_callback_)
        {
            position_callback_(update);
        }
    }

    void PositionClient::onHeartbeat()
    {
        LOG_DEBUG("Received heartbeat from server");
    }

    void PositionClient::onAcknowledge(uint64_t sequence_number)
    {
        LOG_DEBUG("Received acknowledge for sequence " + std::to_string(sequence_number));
    }

    void PositionClient::onError(const std::string &error)
    {
        LOG_ERROR("Server error: " + error);
    }

    void PositionClient::startHeartbeat()
    {
        heartbeat_timer_.expires_after(std::chrono::milliseconds(HEARTBEAT_INTERVAL_MS));
        heartbeat_timer_.async_wait([this](boost::system::error_code ec)
                                    {
        if (!ec && connected_) {
            sendHeartbeat();
            startHeartbeat(); // Schedule next heartbeat
        } });
    }

    void PositionClient::sendHeartbeat()
    {
        if (!connected_ || !connection_)
        {
            return;
        }

        try
        {
            Message message(MessageType::HEARTBEAT, MessageSerializer::createHeartbeat());
            connection_->sendMessage(message);
            LOG_DEBUG("Sent heartbeat");
        }
        catch (const std::exception &e)
        {
            LOG_ERROR("Failed to send heartbeat: " + std::string(e.what()));
            handleConnectionError();
        }
    }

    void PositionClient::handleConnectionError()
    {
        LOG_WARN("Connection error detected, attempting to reconnect...");
        connected_ = false;

        // In a production system, you might want to implement automatic reconnection
        // For now, we'll just log the error and let the user handle it
    }

} // namespace position_distributor
