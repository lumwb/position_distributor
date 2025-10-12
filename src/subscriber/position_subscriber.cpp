#include "position_distributor/position_subscriber.h"
#include "position_distributor/logger.h"
#include <stdexcept>
#include <functional>
#include <chrono>
#if defined(__x86_64__) || defined(_M_X64) || defined(__i386) || defined(_M_IX86)
#include <immintrin.h> // only on x86
#endif
#if defined(__aarch64__) || defined(__arm__)
#include <arm_acle.h> // optional, for __yield() intrinsic
#endif

inline void cpu_relax()
{
    {
#if defined(__x86_64__) || defined(_M_X64) || defined(__i386) || defined(_M_IX86)
        _mm_pause();
#elif defined(__aarch64__) || defined(__arm__)
        __yield();
#else
        std::this_thread::yield();
#endif
    }
}

namespace position_distributor
{

    PositionSubscriber::PositionSubscriber(const SubscriberConfig &config)
        : config_(config), connected_(false), running_(false), cleanup_started_(false), subscriber_id_(0), last_sequence_number_(0), received_count_(0), ordering_errors_(0), producer_heartbeat_lost_(false), current_session_id_(0)
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
        auto &topic_registry = TopicRegistry::instance();
        topic_channel_ = topic_registry.getOrCreateTopic(config_.topic);
        if (!topic_channel_)
        {
            LOG_ERROR("Failed to create/access shared memory topic: " + config_.topic);
            return false;
        }

        auto message_handler = [this](const uint8_t *data, uint32_t length, uint32_t session_id)
        {
            this->processMessage(data, length, session_id);
        };

        if (!topic_channel_->subscribe(message_handler, ("subscriber_" + std::to_string(subscriber_id_)).c_str()))
        {
            LOG_ERROR("Failed to subscribe to topic channel: " + config_.topic);
            subscriber_id_ = 0;
            return false;
        }

        connected_.store(true);
        running_.store(true);
        producer_heartbeat_lost_.store(false); // Reset heartbeat state on connect
        current_session_id_.store(0);          // Reset session tracking on connect

        // Start background threads
        message_thread_ = std::thread(&PositionSubscriber::messageLoop, this);
        heartbeat_thread_ = std::thread(&PositionSubscriber::heartbeatLoop, this);

        LOG_INFO("Subscriber connected to topic '" + config_.topic + "' with ID: " + std::to_string(subscriber_id_));
        return true;
    }

    void PositionSubscriber::disconnect()
    {
        // Prevent double cleanup with atomic flag
        bool expected = false;
        if (!cleanup_started_.compare_exchange_strong(expected, true))
        {
            LOG_DEBUG("Cleanup already in progress for topic: " + config_.topic);
            return;
        }

        LOG_INFO("Disconnecting subscriber from topic: " + config_.topic);

        // Signal threads to stop
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

        // Topic channel cleanup is handled in TopicChannel::cleanup()
        // Just reset our reference
        if (topic_channel_)
        {
            topic_channel_.reset();
        }

        // Clean up subscriber ID
        subscriber_id_ = 0;

        LOG_INFO("Subscriber disconnected from topic: " + config_.topic);
    }

    std::string PositionSubscriber::addPositionUpdateCallback(PositionUpdateCallback callback)
    {
        std::lock_guard<std::mutex> lock(callback_mutex_);
        std::string callback_id = "callback_" + std::to_string(std::chrono::steady_clock::now().time_since_epoch().count());
        position_callbacks_[callback_id] = callback;
        LOG_INFO("Added position callback: " + callback_id + " for topic: " + config_.topic);

        return callback_id;
    }

    bool PositionSubscriber::removePositionUpdateCallback(const std::string &callback_id)
    {
        std::lock_guard<std::mutex> lock(callback_mutex_);
        auto it = position_callbacks_.find(callback_id);
        if (it != position_callbacks_.end())
        {
            position_callbacks_.erase(it);
            LOG_INFO("Removed position callback: " + callback_id + " for topic: " + config_.topic);
            return true;
        }
        LOG_WARN("Callback not found: " + callback_id + " for topic: " + config_.topic);
        return false;
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

        // Use the new simple poll() method - no SubscriberHandle needed
        return topic_channel_->poll();
    }

    void PositionSubscriber::messageLoop()
    {
        LOG_INFO("Message processing thread started for topic: " + config_.topic +
                 (config_.use_busy_spin ? " (low latency mode)" : " (normal mode)"));

        // Normal mode: use sleep to yield to OS scheduler
        while (running_.load())
        {
            if (connected_.load() && topic_channel_)
            {
                pollMessages();
            }

            if (config_.use_busy_spin)
            {
                cpu_relax();
            }
            else
            {
                std::this_thread::sleep_for(config_.poll_interval);
            }
        }

        LOG_INFO("Message processing thread stopped for topic: " + config_.topic);
    }

    void PositionSubscriber::heartbeatLoop()
    {
        LOG_INFO("Heartbeat thread started for topic: " + config_.topic);

        while (running_.load())
        {
            std::this_thread::sleep_for(config_.activity_interval);

            if (!running_.load())
                break;

            // Update subscriber heartbeat in shared memory
            if (connected_.load() && topic_channel_)
            {
                // Check producer heartbeat
                bool producer_alive = topic_channel_->isProducerAlive();
                bool was_heartbeat_lost = producer_heartbeat_lost_.load();

                LOG_DEBUG("Heartbeat check - producer_alive: " + std::string(producer_alive ? "true" : "false") +
                          ", was_heartbeat_lost: " + std::string(was_heartbeat_lost ? "true" : "false") +
                          " for topic: " + config_.topic);

                if (!producer_alive && !was_heartbeat_lost)
                {
                    // Producer heartbeat just lost - transition from alive to lost
                    producer_heartbeat_lost_.store(true);

                    LOG_WARN("Producer heartbeat lost for topic: " + config_.topic);
                    handleError(ConnectionError::PRODUCER_STALE);

                    // Notify publisher disconnect callback (safely handle mutex failures)
                    try
                    {
                        std::lock_guard<std::mutex> lock(callback_mutex_);
                        if (publisher_disconnect_callback_)
                        {
                            publisher_disconnect_callback_(config_.topic);
                        }
                    }
                    catch (const std::system_error &e)
                    {
                        LOG_DEBUG("Unable to acquire callback mutex during heartbeat lost: " + std::string(e.what()));
                    }
                }
                else if (producer_alive && was_heartbeat_lost)
                {
                    // Producer heartbeat recovered - transition from lost to alive
                    producer_heartbeat_lost_.store(false);
                    LOG_INFO("Producer heartbeat recovered for topic: " + config_.topic);
                }
                // If (!producer_alive && was_heartbeat_lost) - heartbeat still lost, do nothing
                // If (producer_alive && !was_heartbeat_lost) - heartbeat still good, do nothing
            }
        }

        LOG_INFO("Heartbeat thread stopped for topic: " + config_.topic);
    }

    void PositionSubscriber::processMessage(const uint8_t *data, uint32_t length, uint32_t session_id)
    {
        if (!data || length == 0)
        {
            return;
        }

        // Check for session change (producer restart detection)
        uint32_t expected_session = current_session_id_.load();
        if (expected_session != session_id)
        {
            LOG_INFO("Producer session change detected (restart): " + std::to_string(expected_session) +
                     " -> " + std::to_string(session_id) + " for topic: " + config_.topic);

            // Reset sequence number tracking for all strategies
            // This allows the new producer to start with sequence number 1 again
            try
            {
                std::lock_guard<std::mutex> lock(sequence_mutex_);
                strategy_sequence_map_.clear();
                LOG_INFO("Reset sequence number tracking due to producer session change for topic: " + config_.topic);
            }
            catch (const std::system_error &e)
            {
                LOG_WARN("Unable to acquire sequence mutex during session change: " + std::string(e.what()));
            }

            current_session_id_.store(session_id);
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

            // Dispatch to all registered callbacks
            try
            {
                std::lock_guard<std::mutex> lock(callback_mutex_);
                for (const auto &[callback_id, callback] : position_callbacks_)
                {
                    if (callback)
                    {
                        callback(update);
                    }
                }
            }
            catch (const std::system_error &e)
            {
                LOG_DEBUG("Unable to acquire callback mutex for position update: " + std::string(e.what()));
                return; // Skip this update
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
} // namespace position_distributor
