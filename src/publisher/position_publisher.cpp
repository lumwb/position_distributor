#include "position_distributor/position_publisher.h"
#include "position_distributor/logger.h"
#include <stdexcept>
#include <functional>
#include <chrono>

namespace position_distributor
{

    PositionPublisher::PositionPublisher(const PublisherConfig &config)
        : config_(config), connected_(false), running_(false), cleanup_started_(false), session_id_(0), sequence_number_(0), published_count_(0), failed_count_(0)
    {
        if (config_.topic.empty())
        {
            throw std::invalid_argument("Topic cannot be empty");
        }

        LOG_INFO("Created position publisher for topic: " + config_.topic);
    }

    PositionPublisher::~PositionPublisher()
    {
        disconnect();
    }

    bool PositionPublisher::connect()
    {
        if (connected_.load())
        {
            LOG_WARN("Publisher already connected to topic: " + config_.topic);
            return true;
        }

        // Generate a unique session ID for this publisher
        session_id_ = std::hash<std::string>{}(config_.topic + std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()));

        LOG_INFO("Generated session ID: " + std::to_string(session_id_) + " for topic: " + config_.topic);

        // Try to create/access the shared memory topic
        auto &topic_registry = TopicRegistry::instance();
        auto topic_channel = topic_registry.getOrCreateTopic(config_.topic);
        if (!topic_channel)
        {
            LOG_ERROR("Failed to create/access shared memory topic: " + config_.topic);
            return false;
        }

        connected_.store(true);
        running_.store(true);

        // Start heartbeat thread if auto-heartbeat is enabled
        if (config_.auto_heartbeat)
        {
            heartbeat_thread_ = std::thread(&PositionPublisher::heartbeatLoop, this);
        }

        LOG_INFO("Publisher connected to topic '" + config_.topic + "' with session ID: " + std::to_string(session_id_));
        return true;
    }

    void PositionPublisher::disconnect()
    {
        // Prevent double cleanup with atomic flag
        bool expected = false;
        if (!cleanup_started_.compare_exchange_strong(expected, true))
        {
            LOG_DEBUG("Cleanup already in progress for topic: " + config_.topic);
            return;
        }

        LOG_INFO("Disconnecting publisher from topic: " + config_.topic);

        // Signal threads to stop
        running_.store(false);
        connected_.store(false);

        // Stop heartbeat thread
        if (heartbeat_thread_.joinable())
        {
            heartbeat_thread_.join();
        }

        // Clean up session
        session_id_ = 0;

        LOG_INFO("Publisher disconnected from topic: " + config_.topic);
    }

    bool PositionPublisher::publishPositions(const std::string &strategy_id,
                                             const std::vector<SymbolPosition> &positions)
    {
        if (!connected_.load())
        {
            LOG_WARN("Cannot publish: publisher not connected");
            updateStatistics(false);
            return false;
        }

        if (positions.empty())
        {
            LOG_WARN("Cannot publish: empty positions vector");
            updateStatistics(false);
            return false;
        }

        // Convert SymbolPosition to pairs for encoding
        std::vector<std::pair<std::string, double>> position_pairs;
        position_pairs.reserve(positions.size());

        for (const auto &pos : positions)
        {
            position_pairs.emplace_back(pos.symbol, pos.net_position);
        }

        return publishPositions(strategy_id, position_pairs);
    }

    bool PositionPublisher::publishPositions(const std::string &strategy_id,
                                             const std::vector<std::pair<std::string, double>> &positions)
    {
        if (!connected_.load())
        {
            LOG_WARN("Cannot publish: publisher not connected");
            updateStatistics(false);
            return false;
        }

        if (positions.empty())
        {
            LOG_WARN("Cannot publish: empty positions vector");
            updateStatistics(false);
            return false;
        }

        try
        {
            // Get next sequence number
            uint64_t seq_num = sequence_number_.fetch_add(1) + 1;

            // Calculate buffer size needed
            uint32_t buffer_size = PositionUpdateCodec::calculateBufferSize(static_cast<uint32_t>(positions.size()));

            // Allocate buffer
            std::vector<uint8_t> buffer(buffer_size);

            // Encode position update
            uint32_t encoded_size = PositionUpdateCodec::encode(
                buffer.data(), buffer_size, strategy_id, seq_num, positions);

            if (encoded_size == 0)
            {
                LOG_ERROR("Failed to encode position update");
                updateStatistics(false);
                return false;
            }

            // Publish to shared memory
            bool success = publishInternal(buffer.data(), encoded_size);
            updateStatistics(success);

            if (success)
            {
                LOG_DEBUG("Published position update for strategy '" + strategy_id +
                          "' with " + std::to_string(positions.size()) + " positions (seq: " +
                          std::to_string(seq_num) + ")");
            }

            return success;
        }
        catch (const std::exception &e)
        {
            LOG_ERROR("Exception during position publishing: " + std::string(e.what()));
            updateStatistics(false);
            handleError(ConnectionError::UNKNOWN_ERROR);
            return false;
        }
    }

    bool PositionPublisher::sendProdcuerHeartbeat()
    {
        if (!connected_.load())
        {
            return false;
        }

        // Update local heartbeat time (safely handle mutex failures during cleanup)
        try
        {
            std::lock_guard<std::mutex> lock(heartbeat_mutex_);
            last_heartbeat_ = std::chrono::steady_clock::now();
        }
        catch (const std::system_error &e)
        {
            LOG_DEBUG("Unable to acquire heartbeat mutex: " + std::string(e.what()));
            return false;
        }

        // Update shared memory producer heartbeat
        auto &topic_registry = TopicRegistry::instance();
        auto topic_channel = topic_registry.getOrCreateTopic(config_.topic);
        if (topic_channel)
        {
            topic_channel->sendProdcuerHeartbeat();
            LOG_DEBUG("Sent heartbeat for topic: " + config_.topic);
        }

        return true;
    }

    void PositionPublisher::setErrorCallback(ErrorCallback callback)
    {
        try
        {
            std::lock_guard<std::mutex> lock(callback_mutex_);
            error_callback_ = callback;
        }
        catch (const std::system_error &e)
        {
            LOG_DEBUG("Unable to acquire callback mutex: " + std::string(e.what()));
        }
    }

    void PositionPublisher::heartbeatLoop()
    {
        LOG_INFO("Heartbeat thread started for topic: " + config_.topic);

        while (running_.load())
        {
            std::this_thread::sleep_for(config_.heartbeat_interval);

            if (!running_.load())
                break;

            if (!sendProdcuerHeartbeat())
            {
                LOG_WARN("Heartbeat failed for topic: " + config_.topic);
                // Continue trying - don't break the loop on single failure
            }

            // Check for subscriber heartbeat timeouts
            auto &topic_registry = TopicRegistry::instance();
            auto topic_channel = topic_registry.getOrCreateTopic(config_.topic);
            if (topic_channel)
            {
                topic_channel->checkSubscriberHeartbeats();
            }
        }

        LOG_INFO("Heartbeat thread stopped for topic: " + config_.topic);
    }

    bool PositionPublisher::publishInternal(const uint8_t *data, uint32_t length)
    {
        if (!connected_.load() || session_id_ == 0)
        {
            return false;
        }

        // Publish directly to shared memory
        auto &topic_registry = TopicRegistry::instance();
        auto topic_channel = topic_registry.getOrCreateTopic(config_.topic);
        if (!topic_channel)
        {
            LOG_ERROR("Failed to get topic channel: " + config_.topic);
            return false;
        }

        bool success = topic_channel->publish(data, length, session_id_);

        if (!success)
        {
            LOG_WARN("Failed to publish message to topic: " + config_.topic);
            handleError(ConnectionError::SLOW_CONSUMER); // Might be frame data exceed term size, but we haven't implemented Message Fragmentation
        }

        return success;
    }

    void PositionPublisher::handleError(ConnectionError error)
    {
        LOG_ERROR("Publisher error for topic '" + config_.topic + "': " + std::to_string(static_cast<int>(error)));

        try
        {
            std::lock_guard<std::mutex> lock(callback_mutex_);
            if (error_callback_)
            {
                error_callback_(config_.topic, error);
            }
        }
        catch (const std::system_error &e)
        {
            LOG_DEBUG("Unable to acquire callback mutex for error callback: " + std::string(e.what()));
        }
    }

    void PositionPublisher::updateStatistics(bool success)
    {
        if (success)
        {
            published_count_.fetch_add(1);
        }
        else
        {
            failed_count_.fetch_add(1);
        }
    }

} // namespace position_distributor
