#include "position_distributor/simple_position_client.h"
#include "position_distributor/logger.h"

namespace position_distributor
{

    SimplePositionClient::SimplePositionClient(const std::string &server_host, uint16_t server_port,
                                               const std::string &strategy_id)
        : client_(server_host, server_port), strategy_id_(strategy_id),
          next_sequence_number_(1), received_updates_(0), sent_updates_(0), running_(false)
    {
    }

    SimplePositionClient::~SimplePositionClient()
    {
        disconnect();
    }

    bool SimplePositionClient::connect()
    {
        client_.setDataHandler([this](const std::string &data)
                               { handleServerData(data); });

        if (!client_.connect())
        {
            return false;
        }

        running_ = true;
        heartbeat_thread_ = std::thread(&SimplePositionClient::heartbeatLoop, this);

        if (connection_callback_)
        {
            connection_callback_(true);
        }

        logInfo("Position client connected for strategy: " + strategy_id_);
        return true;
    }

    void SimplePositionClient::disconnect()
    {
        if (running_)
        {
            running_ = false;
            client_.disconnect();

            if (heartbeat_thread_.joinable())
            {
                heartbeat_thread_.join();
            }

            if (connection_callback_)
            {
                connection_callback_(false);
            }

            logInfo("Position client disconnected for strategy: " + strategy_id_);
        }
    }

    bool SimplePositionClient::sendPositionUpdate(const std::vector<SymbolPosition> &positions)
    {
        if (!isConnected())
        {
            logWarning("Cannot send position update: not connected");
            return false;
        }

        PositionUpdate update(strategy_id_, positions, next_sequence_number_++);
        std::string serialized = SimpleMessageProtocol::serializePositionUpdate(update);

        if (client_.send(serialized))
        {
            sent_updates_++;
            logInfo("Sent position update for strategy " + strategy_id_ +
                    " (seq: " + std::to_string(update.sequence_number) +
                    ", positions: " + std::to_string(positions.size()) + ")");
            return true;
        }
        else
        {
            logError("Failed to send position update for strategy " + strategy_id_);
            return false;
        }
    }

    void SimplePositionClient::handleServerData(const std::string &data)
    {
        if (SimpleMessageProtocol::isHeartbeat(data))
        {
            // Handle heartbeat - just acknowledge
            return;
        }

        try
        {
            PositionUpdate update = SimpleMessageProtocol::deserializePositionUpdate(data);

            // Update local positions
            {
                std::lock_guard<std::mutex> lock(positions_mutex_);
                strategy_positions_[update.strategy_id] = update.positions;
                received_updates_++;
            }

            // Notify callback
            if (position_callback_)
            {
                position_callback_(update);
            }

            logInfo("Received position update from strategy " + update.strategy_id +
                    " (seq: " + std::to_string(update.sequence_number) +
                    ", positions: " + std::to_string(update.positions.size()) + ")");
        }
        catch (const std::exception &e)
        {
            logError("Failed to process server data: " + std::string(e.what()));
        }
    }

    void SimplePositionClient::sendHeartbeat()
    {
        if (isConnected())
        {
            std::string heartbeat = SimpleMessageProtocol::createHeartbeat();
            client_.send(heartbeat);
        }
    }

    void SimplePositionClient::heartbeatLoop()
    {
        while (running_)
        {
            std::this_thread::sleep_for(std::chrono::milliseconds(HEARTBEAT_INTERVAL_MS));
            if (running_)
            {
                sendHeartbeat();
            }
        }
    }

    std::unordered_map<std::string, std::vector<SymbolPosition>> SimplePositionClient::getAllPositions()
    {
        std::lock_guard<std::mutex> lock(positions_mutex_);
        return strategy_positions_;
    }

    std::vector<SymbolPosition> SimplePositionClient::getStrategyPositions(const std::string &strategy_id)
    {
        std::lock_guard<std::mutex> lock(positions_mutex_);
        auto it = strategy_positions_.find(strategy_id);
        if (it != strategy_positions_.end())
        {
            return it->second;
        }
        return {};
    }

    void SimplePositionClient::setPositionUpdateCallback(PositionUpdateCallback callback)
    {
        position_callback_ = callback;
    }

    void SimplePositionClient::setConnectionCallback(ConnectionCallback callback)
    {
        connection_callback_ = callback;
    }

    bool SimplePositionClient::isConnected() const
    {
        return client_.isConnected();
    }

    size_t SimplePositionClient::getReceivedUpdates() const
    {
        return received_updates_.load();
    }

    size_t SimplePositionClient::getSentUpdates() const
    {
        return sent_updates_.load();
    }

} // namespace position_distributor
