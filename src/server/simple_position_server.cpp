#include "position_distributor/simple_position_server.h"
#include "position_distributor/logger.h"
#include <sstream>

namespace position_distributor
{

    SimplePositionServer::SimplePositionServer(uint16_t port)
        : server_(port), total_updates_(0), running_(false)
    {
    }

    SimplePositionServer::~SimplePositionServer()
    {
        stop();
    }

    bool SimplePositionServer::start()
    {
        if (!server_.start())
        {
            return false;
        }

        server_.setClientHandler([this](int client_fd, const std::string &data)
                                 { handleClientMessage(client_fd, data); });

        running_ = true;
        heartbeat_thread_ = std::thread(&SimplePositionServer::heartbeatLoop, this);

        logInfo("Position server started successfully");
        return true;
    }

    void SimplePositionServer::stop()
    {
        if (running_)
        {
            running_ = false;
            server_.stop();

            if (heartbeat_thread_.joinable())
            {
                heartbeat_thread_.join();
            }

            logInfo("Position server stopped");
        }
    }

    void SimplePositionServer::handleClientMessage(int client_fd, const std::string &data)
    {
        if (SimpleMessageProtocol::isHeartbeat(data))
        {
            // Handle heartbeat - just acknowledge
            return;
        }

        try
        {
            PositionUpdate update = SimpleMessageProtocol::deserializePositionUpdate(data);

            // Validate sequence number for order preservation
            {
                std::lock_guard<std::mutex> lock(positions_mutex_);
                auto it = last_sequence_numbers_.find(update.strategy_id);
                if (it != last_sequence_numbers_.end() && update.sequence_number <= it->second)
                {
                    logWarning("Received out-of-order update from strategy " + update.strategy_id +
                               " (seq: " + std::to_string(update.sequence_number) +
                               ", expected: > " + std::to_string(it->second) + ")");
                    return;
                }

                // Update positions
                strategy_positions_[update.strategy_id] = update.positions;
                last_sequence_numbers_[update.strategy_id] = update.sequence_number;
                total_updates_++;
            }

            // Broadcast to all clients
            broadcastPositionUpdate(update);

            logInfo("Processed position update from strategy " + update.strategy_id +
                    " (seq: " + std::to_string(update.sequence_number) +
                    ", positions: " + std::to_string(update.positions.size()) + ")");
        }
        catch (const std::exception &e)
        {
            logError("Failed to process client message: " + std::string(e.what()));
        }
    }

    void SimplePositionServer::broadcastPositionUpdate(const PositionUpdate &update)
    {
        std::string serialized = SimpleMessageProtocol::serializePositionUpdate(update);
        server_.broadcast(serialized);
    }

    void SimplePositionServer::sendHeartbeat()
    {
        std::string heartbeat = SimpleMessageProtocol::createHeartbeat();
        server_.broadcast(heartbeat);
    }

    void SimplePositionServer::heartbeatLoop()
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

    std::unordered_map<std::string, std::vector<SymbolPosition>> SimplePositionServer::getAllPositions()
    {
        std::lock_guard<std::mutex> lock(positions_mutex_);
        return strategy_positions_;
    }

    std::vector<SymbolPosition> SimplePositionServer::getStrategyPositions(const std::string &strategy_id)
    {
        std::lock_guard<std::mutex> lock(positions_mutex_);
        auto it = strategy_positions_.find(strategy_id);
        if (it != strategy_positions_.end())
        {
            return it->second;
        }
        return {};
    }

    size_t SimplePositionServer::getClientCount()
    {
        return server_.getClientCount();
    }

    size_t SimplePositionServer::getTotalUpdates() const
    {
        return total_updates_.load();
    }

} // namespace position_distributor
