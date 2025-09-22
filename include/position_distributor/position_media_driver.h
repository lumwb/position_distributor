#pragma once

#include "position_distributor/shared_memory_manager.h"
#include <string>
#include <memory>
#include <unordered_map>
#include <unordered_set>
#include <thread>
#include <atomic>
#include <mutex>
#include <functional>
#include <chrono>

namespace position_distributor
{

    // Connection error types
    enum class ConnectionError
    {
        HEARTBEAT_LOST,
        SLOW_CONSUMER,
        CORRUPTED_MEMORY,
        UNKNOWN_ERROR
    };

    // Publisher session information
    struct PublisherSession
    {
        uint32_t session_id;
        std::string topic;
        std::chrono::steady_clock::time_point last_heartbeat;
        std::atomic<uint64_t> sequence_number;
        bool active;

        PublisherSession(uint32_t id, const std::string &t)
            : session_id(id), topic(t), last_heartbeat(std::chrono::steady_clock::now()), sequence_number(0), active(true)
        {
        }
    };

    // Subscriber session information
    struct SubscriberSession
    {
        uint32_t subscriber_id;
        std::string topic;
        std::chrono::steady_clock::time_point last_activity;
        std::function<void(const std::string &, ConnectionError)> error_callback;
        bool active;

        SubscriberSession(uint32_t id, const std::string &t)
            : subscriber_id(id), topic(t), last_activity(std::chrono::steady_clock::now()), active(true)
        {
        }
    };

    // Main media driver class - acts as message router and session manager
    class PositionMediaDriver
    {
    public:
        static constexpr std::chrono::milliseconds HEARTBEAT_INTERVAL{1000};
        static constexpr std::chrono::milliseconds HEARTBEAT_TIMEOUT{3000};

        PositionMediaDriver();
        ~PositionMediaDriver();

        // Lifecycle
        bool start();
        void stop();
        bool isRunning() const { return running_.load(); }

        // Publisher management
        uint32_t registerPublisher(const std::string &topic);
        bool unregisterPublisher(uint32_t session_id);
        bool updatePublisherHeartbeat(uint32_t session_id);

        // Subscriber management
        uint32_t registerSubscriber(const std::string &topic,
                                    std::function<void(const std::string &, ConnectionError)> error_callback);
        bool unregisterSubscriber(uint32_t subscriber_id);
        bool updateSubscriberActivity(uint32_t subscriber_id);

        // Message routing
        bool routeMessage(const std::string &topic, const uint8_t *data, uint32_t length, uint32_t session_id);

        // Statistics
        size_t getPublisherCount() const;
        size_t getSubscriberCount() const;
        size_t getTopicCount() const;
        std::vector<std::string> getActiveTopics() const;

        // Error handling
        using GlobalErrorCallback = std::function<void(const std::string &, ConnectionError)>;
        void setGlobalErrorCallback(GlobalErrorCallback callback);

    private:
        std::atomic<bool> running_;
        std::atomic<uint32_t> next_session_id_;
        std::atomic<uint32_t> next_subscriber_id_;

        // Session management
        std::unordered_map<uint32_t, std::unique_ptr<PublisherSession>> publishers_;
        std::unordered_map<uint32_t, std::unique_ptr<SubscriberSession>> subscribers_;
        std::unordered_map<std::string, std::unordered_set<uint32_t>> topic_publishers_;
        std::unordered_map<std::string, std::unordered_set<uint32_t>> topic_subscribers_;

        mutable std::mutex publishers_mutex_;
        mutable std::mutex subscribers_mutex_;

        // Shared memory management
        SharedMemoryManager *shm_manager_;

        // Heartbeat and monitoring
        std::thread heartbeat_thread_;
        std::thread message_processing_thread_;
        GlobalErrorCallback global_error_callback_;

        // Internal methods
        void heartbeatLoop();
        void messageProcessingLoop();
        void checkPublisherHeartbeats();
        void checkSubscriberActivity();
        void handleConnectionError(const std::string &topic, ConnectionError error);
        void notifySubscribersError(const std::string &topic, ConnectionError error);
        void cleanupInactiveSessions();

        uint32_t generateSessionId();
        uint32_t generateSubscriberId();

        // Disable copy/move
        PositionMediaDriver(const PositionMediaDriver &) = delete;
        PositionMediaDriver &operator=(const PositionMediaDriver &) = delete;
    };

    // Global media driver instance
    class MediaDriverManager
    {
    public:
        static PositionMediaDriver &instance();
        static bool initialize();
        static void shutdown();

    private:
        static std::unique_ptr<PositionMediaDriver> instance_;
        static std::once_flag initialized_;
    };

} // namespace position_distributor
