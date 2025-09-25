#include "position_distributor/position_subscriber.h"
#include "position_distributor/logger.h"
#include <stdexcept>
#include <functional>
#include <chrono>

namespace position_distributor
{

    PositionSubscriber::PositionSubscriber(const SubscriberConfig &config)
        : config_(config), connected_(false), running_(false), media_driver_(&MediaDriverManager::instance()), subscriber_id_(0), last_sequence_number_(0), received_count_(0), ordering_errors_(0)
    {
        if (config_.topic.empty())
        {
            throw std::invalid_argument("Topic cannot be empty");
        }

        LOG_INFO("Created position subscriber for topic: " + config_.topic);
    }

    PositionSubscriber::~PositionSubscriber()
    {
        disconnect();
    }

    bool PositionSubscriber::connect()
    {
        if (connected_.load())
        {
            LOG_WARN("Subscriber already connected to topic: " + config_.topic);
            return true;
        }

        // Generate a unique subscriber ID
        subscriber_id_ = std::hash<std::string>{}(config_.topic + "subscriber" + std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()));

        // Try to access the shared memory topic
        auto &shm_manager = SharedMemoryManager::instance();
        topic_channel_ = shm_manager.getOrCreateTopic(config_.topic);
        if (!topic_channel_)
        {
            LOG_ERROR("Failed to create/access shared memory topic: " + config_.topic);
            return false;
        }

        // Subscribe to topic channel messages
        auto message_handler = [this](const uint8_t *data, uint32_t length)
        {
            this->processMessage(data, length);
        };

        // Use legacy subscribe method for backward compatibility - call the bool version explicitly
        bool subscribed = topic_channel_->subscribe(message_handler);
        if (!subscribed)
        {
            LOG_ERROR("Failed to subscribe to topic channel: " + config_.topic);
            media_driver_->unregisterSubscriber(subscriber_id_);
            subscriber_id_ = 0;
            return false;
        }

        connected_.store(true);
        running_.store(true);

        // Start background threads
        message_thread_ = std::thread(&PositionSubscriber::messageLoop, this);
        heartbeat_thread_ = std::thread(&PositionSubscriber::heartbeatLoop, this);

        LOG_INFO("Subscriber connected to topic '" + config_.topic + "' with ID: " + std::to_string(subscriber_id_));
        return true;
    }

    void PositionSubscriber::disconnect()
    {
        if (!connected_.load())
        {
            return;
        }

        LOG_INFO("Disconnecting subscriber from topic: " + config_.topic);

        running_.store(false);
        connected_.store(false);

        // Stop background threads
        if (message_thread_.joinable())
        {
            message_thread_.join();
        }

        if (heartbeat_thread_.joinable())
        {
            heartbeat_thread_.join();
        }

        // Unsubscribe from topic channel
        if (topic_channel_)
        {
            topic_channel_->unsubscribe();
            topic_channel_.reset();
        }

        // Clean up subscriber ID
        subscriber_id_ = 0;

        LOG_INFO("Subscriber disconnected from topic: " + config_.topic);
    }

    void PositionSubscriber::setPositionUpdateCallback(PositionUpdateCallback callback)
    {
        std::lock_guard<std::mutex> lock(callback_mutex_);
        position_callback_ = callback;
    }

    void PositionSubscriber::setErrorCallback(ErrorCallback callback)
    {
        std::lock_guard<std::mutex> lock(callback_mutex_);
        error_callback_ = callback;
    }

    void PositionSubscriber::setPublisherDisconnectCallback(PublisherDisconnectCallback callback)
    {
        std::lock_guard<std::mutex> lock(callback_mutex_);
        publisher_disconnect_callback_ = callback;
    }

    bool PositionSubscriber::pollMessages()
    {
        if (!connected_.load() || !topic_channel_)
        {
            return false;
        }

        // Read messages from the ring buffer
        return topic_channel_->readMessages();
    }

    void PositionSubscriber::messageLoop()
    {
        LOG_INFO("Message processing thread started for topic: " + config_.topic);

        while (running_.load())
        {
            if (connected_.load() && topic_channel_)
            {
                pollMessages();
                
                // Check producer heartbeat
                if (!topic_channel_->isProducerAlive())
                {
                    LOG_WARN("Producer heartbeat lost for topic: " + config_.topic);
                    handleError(ConnectionError::PRODUCER_STALE);
                    
                    // Notify publisher disconnect callback
                    {
                        std::lock_guard<std::mutex> lock(callback_mutex_);
                        if (publisher_disconnect_callback_)
                        {
                            publisher_disconnect_callback_(config_.topic);
                        }
                    }
                }
            }

            std::this_thread::sleep_for(config_.poll_interval);
        }

        LOG_INFO("Message processing thread stopped for topic: " + config_.topic);
    }

    void PositionSubscriber::heartbeatLoop()
    {
        LOG_INFO("Heartbeat thread started for topic: " + config_.topic);

        while (running_.load())
        {
            std::this_thread::sleep_for(config_.activity_interval); // Reuse activity_interval for heartbeat frequency

            if (!running_.load())
                break;

            // Update subscriber heartbeat in shared memory
            if (connected_.load() && topic_channel_)
            {
                // The subscriber heartbeat is updated automatically when polling messages
                // But we need to ensure it's updated even when no messages are coming
                // We can do this by sending a dummy poll or accessing the ring buffer directly
                
                // For now, let's use a debug log to show the heartbeat is running
                // In production, we might want to access the shared memory directly
                LOG_DEBUG("Subscriber heartbeat for topic: " + config_.topic);
                
                // Force a poll to ensure subscriber heartbeat is updated
                pollMessages();
            }
        }

        LOG_INFO("Heartbeat thread stopped for topic: " + config_.topic);
    }

    void PositionSubscriber::processMessage(const uint8_t *data, uint32_t length)
    {
        if (!data || length == 0)
        {
            return;
        }

        try
        {
            // Decode the position update
            std::string strategy_id;
            uint64_t timestamp;
            uint64_t sequence_number;
            std::vector<std::pair<std::string, double>> position_pairs;

            bool success = PositionUpdateCodec::decode(data, length, strategy_id, timestamp,
                                                       sequence_number, position_pairs);

            if (!success)
            {
                LOG_WARN("Failed to decode position update message");
                return;
            }

            // Validate message ordering if enabled
            if (config_.enable_ordering_check)
            {
                if (!validateOrdering(strategy_id, sequence_number))
                {
                    ordering_errors_.fetch_add(1);
                    LOG_WARN("Ordering violation detected for strategy '" + strategy_id +
                             "' sequence: " + std::to_string(sequence_number));
                }
            }

            // Convert to SymbolPosition objects
            std::vector<SymbolPosition> positions;
            positions.reserve(position_pairs.size());

            for (const auto &[symbol, net_position] : position_pairs)
            {
                positions.emplace_back(symbol, net_position);
            }

            // Create position update object
            PositionUpdate update(strategy_id, timestamp, sequence_number, positions);

            // Update statistics
            received_count_.fetch_add(1);
            last_sequence_number_.store(sequence_number);

            // Invoke callback
            {
                std::lock_guard<std::mutex> lock(callback_mutex_);
                if (position_callback_)
                {
                    position_callback_(update);
                }
            }

            LOG_DEBUG("Processed position update: " + update.toString());
        }
        catch (const std::exception &e)
        {
            LOG_ERROR("Exception processing message: " + std::string(e.what()));
            handleError(ConnectionError::CORRUPTED_MEMORY);
        }
    }

    bool PositionSubscriber::validateOrdering(const std::string &strategy_id, uint64_t sequence_number)
    {
        std::lock_guard<std::mutex> lock(sequence_mutex_);

        auto it = strategy_sequence_map_.find(strategy_id);
        if (it == strategy_sequence_map_.end())
        {
            // First message from this strategy
            strategy_sequence_map_[strategy_id] = sequence_number;
            return true;
        }

        // Check if sequence number is greater than the last seen
        if (sequence_number > it->second)
        {
            it->second = sequence_number;
            return true;
        }

        // Ordering violation - sequence number is not greater than last seen
        return false;
    }

    void PositionSubscriber::handleError(ConnectionError error)
    {
        LOG_ERROR("Subscriber error for topic '" + config_.topic + "': " + std::to_string(static_cast<int>(error)));

        std::lock_guard<std::mutex> lock(callback_mutex_);
        if (error_callback_)
        {
            error_callback_(config_.topic, error);
        }
    }


} // namespace position_distributor
