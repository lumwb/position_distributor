#include "position_distributor/position_media_driver.h"
#include "position_distributor/logger.h"
#include <algorithm>
#include <random>

namespace position_distributor
{

    PositionMediaDriver::PositionMediaDriver()
        : running_(false), next_session_id_(1), next_subscriber_id_(1), shm_manager_(&SharedMemoryManager::instance())
    {
    }

    PositionMediaDriver::~PositionMediaDriver()
    {
        stop();
    }

    bool PositionMediaDriver::start()
    {
        if (running_.load())
        {
            LOG_WARN("Media driver already running");
            return true;
        }

        LOG_INFO("Starting Position Media Driver");

        // Clean up any existing shared memory
        shm_manager_->cleanup();

        running_.store(true);

        // Start background threads
        heartbeat_thread_ = std::thread(&PositionMediaDriver::heartbeatLoop, this);
        message_processing_thread_ = std::thread(&PositionMediaDriver::messageProcessingLoop, this);

        LOG_INFO("Position Media Driver started successfully");
        return true;
    }

    void PositionMediaDriver::stop()
    {
        if (!running_.load())
        {
            return;
        }

        LOG_INFO("Stopping Position Media Driver");

        running_.store(false);

        // Join background threads
        if (heartbeat_thread_.joinable())
        {
            heartbeat_thread_.join();
        }

        if (message_processing_thread_.joinable())
        {
            message_processing_thread_.join();
        }

        // Clean up sessions
        {
            std::lock_guard<std::mutex> lock(publishers_mutex_);
            publishers_.clear();
            topic_publishers_.clear();
        }

        {
            std::lock_guard<std::mutex> lock(subscribers_mutex_);
            subscribers_.clear();
            topic_subscribers_.clear();
        }

        // Clean up shared memory
        shm_manager_->cleanup();

        LOG_INFO("Position Media Driver stopped");
    }

    uint32_t PositionMediaDriver::registerPublisher(const std::string &topic)
    {
        std::lock_guard<std::mutex> lock(publishers_mutex_);

        uint32_t session_id = generateSessionId();
        auto session = std::make_unique<PublisherSession>(session_id, topic);

        publishers_[session_id] = std::move(session);
        topic_publishers_[topic].insert(session_id);

        // Ensure topic channel exists
        auto channel = shm_manager_->getOrCreateTopic(topic);
        if (!channel)
        {
            LOG_ERROR("Failed to create topic channel for: " + topic);
            publishers_.erase(session_id);
            topic_publishers_[topic].erase(session_id);
            return 0;
        }

        LOG_INFO("Registered publisher for topic '" + topic + "' with session ID: " + std::to_string(session_id));
        return session_id;
    }

    bool PositionMediaDriver::unregisterPublisher(uint32_t session_id)
    {
        std::lock_guard<std::mutex> lock(publishers_mutex_);

        auto it = publishers_.find(session_id);
        if (it == publishers_.end())
        {
            return false;
        }

        std::string topic = it->second->topic;
        publishers_.erase(it);

        auto topic_it = topic_publishers_.find(topic);
        if (topic_it != topic_publishers_.end())
        {
            topic_it->second.erase(session_id);
            if (topic_it->second.empty())
            {
                topic_publishers_.erase(topic_it);
                // Remove topic if no publishers or subscribers
                {
                    std::lock_guard<std::mutex> sub_lock(subscribers_mutex_);
                    if (topic_subscribers_.find(topic) == topic_subscribers_.end())
                    {
                        shm_manager_->removeTopic(topic);
                    }
                }
            }
        }

        LOG_INFO("Unregistered publisher with session ID: " + std::to_string(session_id));
        return true;
    }

    bool PositionMediaDriver::updatePublisherHeartbeat(uint32_t session_id)
    {
        std::lock_guard<std::mutex> lock(publishers_mutex_);

        auto it = publishers_.find(session_id);
        if (it != publishers_.end() && it->second->active)
        {
            it->second->last_heartbeat = std::chrono::steady_clock::now();
            return true;
        }

        return false;
    }

    uint32_t PositionMediaDriver::registerSubscriber(const std::string &topic,
                                                     std::function<void(const std::string &, ConnectionError)> error_callback)
    {
        std::lock_guard<std::mutex> lock(subscribers_mutex_);

        uint32_t subscriber_id = generateSubscriberId();
        auto session = std::make_unique<SubscriberSession>(subscriber_id, topic);
        session->error_callback = error_callback;

        subscribers_[subscriber_id] = std::move(session);
        topic_subscribers_[topic].insert(subscriber_id);

        // Ensure topic channel exists
        auto channel = shm_manager_->getOrCreateTopic(topic);
        if (!channel)
        {
            LOG_ERROR("Failed to create topic channel for: " + topic);
            subscribers_.erase(subscriber_id);
            topic_subscribers_[topic].erase(subscriber_id);
            return 0;
        }

        LOG_INFO("Registered subscriber for topic '" + topic + "' with ID: " + std::to_string(subscriber_id));
        return subscriber_id;
    }

    bool PositionMediaDriver::unregisterSubscriber(uint32_t subscriber_id)
    {
        std::lock_guard<std::mutex> lock(subscribers_mutex_);

        auto it = subscribers_.find(subscriber_id);
        if (it == subscribers_.end())
        {
            return false;
        }

        std::string topic = it->second->topic;
        subscribers_.erase(it);

        auto topic_it = topic_subscribers_.find(topic);
        if (topic_it != topic_subscribers_.end())
        {
            topic_it->second.erase(subscriber_id);
            if (topic_it->second.empty())
            {
                topic_subscribers_.erase(topic_it);
                // Remove topic if no publishers or subscribers
                {
                    std::lock_guard<std::mutex> pub_lock(publishers_mutex_);
                    if (topic_publishers_.find(topic) == topic_publishers_.end())
                    {
                        shm_manager_->removeTopic(topic);
                    }
                }
            }
        }

        LOG_INFO("Unregistered subscriber with ID: " + std::to_string(subscriber_id));
        return true;
    }

    bool PositionMediaDriver::updateSubscriberActivity(uint32_t subscriber_id)
    {
        std::lock_guard<std::mutex> lock(subscribers_mutex_);

        auto it = subscribers_.find(subscriber_id);
        if (it != subscribers_.end() && it->second->active)
        {
            it->second->last_activity = std::chrono::steady_clock::now();
            return true;
        }

        return false;
    }

    bool PositionMediaDriver::routeMessage(const std::string &topic, const uint8_t *data, uint32_t length, uint32_t session_id)
    {
        // Validate publisher session
        {
            std::lock_guard<std::mutex> lock(publishers_mutex_);
            auto it = publishers_.find(session_id);
            if (it == publishers_.end() || !it->second->active || it->second->topic != topic)
            {
                LOG_WARN("Invalid publisher session or topic mismatch: " + std::to_string(session_id));
                return false;
            }

            // Update sequence number
            it->second->sequence_number.fetch_add(1);
        }

        // Route message to topic channel
        auto channel = shm_manager_->getOrCreateTopic(topic);
        if (!channel)
        {
            LOG_ERROR("Failed to get topic channel: " + topic);
            return false;
        }

        bool success = channel->publish(data, length, session_id);
        if (!success)
        {
            LOG_WARN("Failed to publish message to topic: " + topic);
            handleConnectionError(topic, ConnectionError::SLOW_CONSUMER);
        }

        return success;
    }

    void PositionMediaDriver::heartbeatLoop()
    {
        LOG_INFO("Heartbeat monitoring thread started");

        while (running_.load())
        {
            std::this_thread::sleep_for(HEARTBEAT_INTERVAL);

            if (!running_.load())
                break;

            checkPublisherHeartbeats();
            checkSubscriberActivity();
            cleanupInactiveSessions();
        }

        LOG_INFO("Heartbeat monitoring thread stopped");
    }

    void PositionMediaDriver::messageProcessingLoop()
    {
        LOG_INFO("Message processing thread started");

        while (running_.load())
        {
            // Process messages from all topic channels
            auto topics = shm_manager_->getTopicList();

            bool processed_any = false;
            for (const auto &topic : topics)
            {
                auto channel = shm_manager_->getOrCreateTopic(topic);
                if (channel)
                {
                    // Process pending messages (this is a simplified version)
                    // In a real implementation, we'd have subscribers actively reading
                    processed_any = true;
                }
            }

            if (!processed_any)
            {
                std::this_thread::sleep_for(std::chrono::milliseconds(1));
            }
        }

        LOG_INFO("Message processing thread stopped");
    }

    void PositionMediaDriver::checkPublisherHeartbeats()
    {
        std::lock_guard<std::mutex> lock(publishers_mutex_);

        auto now = std::chrono::steady_clock::now();
        std::vector<uint32_t> inactive_publishers;

        for (auto &[session_id, session] : publishers_)
        {
            if (session->active)
            {
                auto elapsed = now - session->last_heartbeat;
                if (elapsed > HEARTBEAT_TIMEOUT)
                {
                    LOG_WARN("Publisher heartbeat timeout for session: " + std::to_string(session_id) +
                             " topic: " + session->topic);
                    session->active = false;
                    inactive_publishers.push_back(session_id);

                    handleConnectionError(session->topic, ConnectionError::HEARTBEAT_LOST);
                }
            }
        }

        // Clean up inactive publishers (done outside the main loop to avoid iterator invalidation)
        for (uint32_t session_id : inactive_publishers)
        {
            // Mark for cleanup but don't remove immediately to allow for reconnection
        }
    }

    void PositionMediaDriver::checkSubscriberActivity()
    {
        std::lock_guard<std::mutex> lock(subscribers_mutex_);

        auto now = std::chrono::steady_clock::now();

        for (auto &[subscriber_id, session] : subscribers_)
        {
            if (session->active)
            {
                auto elapsed = now - session->last_activity;
                if (elapsed > HEARTBEAT_TIMEOUT)
                {
                    LOG_WARN("Subscriber activity timeout for ID: " + std::to_string(subscriber_id) +
                             " topic: " + session->topic);
                    session->active = false;

                    if (session->error_callback)
                    {
                        session->error_callback(session->topic, ConnectionError::HEARTBEAT_LOST);
                    }
                }
            }
        }
    }

    void PositionMediaDriver::handleConnectionError(const std::string &topic, ConnectionError error)
    {
        LOG_ERROR("Connection error for topic '" + topic + "': " + std::to_string(static_cast<int>(error)));

        // Notify global error callback
        if (global_error_callback_)
        {
            global_error_callback_(topic, error);
        }

        // Notify all subscribers for this topic
        notifySubscribersError(topic, error);
    }

    void PositionMediaDriver::notifySubscribersError(const std::string &topic, ConnectionError error)
    {
        std::lock_guard<std::mutex> lock(subscribers_mutex_);

        auto topic_it = topic_subscribers_.find(topic);
        if (topic_it != topic_subscribers_.end())
        {
            for (uint32_t subscriber_id : topic_it->second)
            {
                auto sub_it = subscribers_.find(subscriber_id);
                if (sub_it != subscribers_.end() && sub_it->second->error_callback)
                {
                    sub_it->second->error_callback(topic, error);
                }
            }
        }
    }

    void PositionMediaDriver::cleanupInactiveSessions()
    {
        // Clean up inactive publishers
        {
            std::lock_guard<std::mutex> lock(publishers_mutex_);
            auto it = publishers_.begin();
            while (it != publishers_.end())
            {
                if (!it->second->active)
                {
                    auto elapsed = std::chrono::steady_clock::now() - it->second->last_heartbeat;
                    if (elapsed > std::chrono::seconds(30)) // Grace period for reconnection
                    {
                        LOG_INFO("Cleaning up inactive publisher session: " + std::to_string(it->first));

                        std::string topic = it->second->topic;
                        auto topic_it = topic_publishers_.find(topic);
                        if (topic_it != topic_publishers_.end())
                        {
                            topic_it->second.erase(it->first);
                            if (topic_it->second.empty())
                            {
                                topic_publishers_.erase(topic_it);
                            }
                        }

                        it = publishers_.erase(it);
                    }
                    else
                    {
                        ++it;
                    }
                }
                else
                {
                    ++it;
                }
            }
        }

        // Clean up inactive subscribers
        {
            std::lock_guard<std::mutex> lock(subscribers_mutex_);
            auto it = subscribers_.begin();
            while (it != subscribers_.end())
            {
                if (!it->second->active)
                {
                    auto elapsed = std::chrono::steady_clock::now() - it->second->last_activity;
                    if (elapsed > std::chrono::seconds(30)) // Grace period
                    {
                        LOG_INFO("Cleaning up inactive subscriber: " + std::to_string(it->first));

                        std::string topic = it->second->topic;
                        auto topic_it = topic_subscribers_.find(topic);
                        if (topic_it != topic_subscribers_.end())
                        {
                            topic_it->second.erase(it->first);
                            if (topic_it->second.empty())
                            {
                                topic_subscribers_.erase(topic_it);
                            }
                        }

                        it = subscribers_.erase(it);
                    }
                    else
                    {
                        ++it;
                    }
                }
                else
                {
                    ++it;
                }
            }
        }
    }

    uint32_t PositionMediaDriver::generateSessionId()
    {
        return next_session_id_.fetch_add(1);
    }

    uint32_t PositionMediaDriver::generateSubscriberId()
    {
        return next_subscriber_id_.fetch_add(1);
    }

    size_t PositionMediaDriver::getPublisherCount() const
    {
        std::lock_guard<std::mutex> lock(publishers_mutex_);
        return publishers_.size();
    }

    size_t PositionMediaDriver::getSubscriberCount() const
    {
        std::lock_guard<std::mutex> lock(subscribers_mutex_);
        return subscribers_.size();
    }

    size_t PositionMediaDriver::getTopicCount() const
    {
        return shm_manager_->getTopicCount();
    }

    std::vector<std::string> PositionMediaDriver::getActiveTopics() const
    {
        return shm_manager_->getTopicList();
    }

    void PositionMediaDriver::setGlobalErrorCallback(GlobalErrorCallback callback)
    {
        global_error_callback_ = callback;
    }

    // MediaDriverManager implementation
    std::unique_ptr<PositionMediaDriver> MediaDriverManager::instance_;
    std::once_flag MediaDriverManager::initialized_;

    PositionMediaDriver &MediaDriverManager::instance()
    {
        std::call_once(initialized_, []()
                       { instance_ = std::make_unique<PositionMediaDriver>(); });
        return *instance_;
    }

    bool MediaDriverManager::initialize()
    {
        return instance().start();
    }

    void MediaDriverManager::shutdown()
    {
        if (instance_)
        {
            instance_->stop();
            instance_.reset();
        }
    }

} // namespace position_distributor
