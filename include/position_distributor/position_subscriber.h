#pragma once

#include "position_distributor/topic_system.h"
#include "position_distributor/position_encoding.h"
#include "position_distributor/position.h"
#include <string>
#include <vector>
#include <atomic>
#include <thread>
#include <mutex>
#include <chrono>
#include <functional>
#include <unordered_map>

namespace position_distributor
{
    // Subscriber configuration
    struct SubscriberConfig
    {
        std::string topic;                           // Topic to subscribe to 
        std::chrono::milliseconds poll_interval;     // How often to poll for messages
        std::chrono::milliseconds activity_interval; // How often to report activity
        bool enable_ordering_check;                  // Check sequence numbers for ordering
        bool use_busy_spin;                          // Use busy spinning instead of sleep for low latency

        SubscriberConfig(const std::string &t = "")
            : topic(t), poll_interval(1), activity_interval(1000), enable_ordering_check(true), use_busy_spin(false)
        {
        }
    };

    // Position subscriber for shared memory transport
    class PositionSubscriber
    {
    public:
        using PositionUpdateCallback = std::function<void(const PositionUpdate &)>;
        using ErrorCallback = std::function<void(const std::string &, ConnectionError)>;
        using PublisherDisconnectCallback = std::function<void(const std::string &)>; // topic/exchange

        explicit PositionSubscriber(const SubscriberConfig &config);
        ~PositionSubscriber();

        // Lifecycle
        bool connect();
        void disconnect();
        bool isConnected() const { return connected_.load(); }

        // Multiple callback support - PositionSubscriber manages callbacks internally
        std::string addPositionUpdateCallback(PositionUpdateCallback callback);
        bool removePositionUpdateCallback(const std::string &callback_id);
        void setErrorCallback(ErrorCallback callback);
        void setPublisherDisconnectCallback(PublisherDisconnectCallback callback);

        // Configuration
        const SubscriberConfig &getConfig() const { return config_; }

        // Statistics
        uint64_t getReceivedCount() const { return received_count_.load(); }
        uint64_t getOrderingErrorCount() const { return ordering_errors_.load(); }
        uint64_t getLastSequenceNumber() const { return last_sequence_number_.load(); }

        // Manual polling (if needed)
        bool pollMessages();

    private:
        SubscriberConfig config_;
        std::atomic<bool> connected_;
        std::atomic<bool> running_;
        std::atomic<bool> cleanup_started_;

        uint32_t subscriber_id_;
        std::shared_ptr<TopicChannel> topic_channel_;

        // Message processing
        std::thread message_thread_;
        std::thread heartbeat_thread_;

        // Multiple callbacks support
        std::unordered_map<std::string, PositionUpdateCallback> position_callbacks_;
        ErrorCallback error_callback_;
        PublisherDisconnectCallback publisher_disconnect_callback_;
        std::mutex callback_mutex_;

        // Ordering and statistics
        std::atomic<uint64_t> last_sequence_number_;
        std::atomic<uint64_t> received_count_;
        std::atomic<uint64_t> ordering_errors_;
        std::unordered_map<std::string, uint64_t> strategy_sequence_map_;
        std::mutex sequence_mutex_;

        // Producer heartbeat state tracking
        std::atomic<bool> producer_heartbeat_lost_;

        // Session tracking for producer restart detection
        std::atomic<uint32_t> current_session_id_;

        // Internal methods
        void messageLoop();
        void heartbeatLoop();
        void processMessage(const uint8_t *data, uint32_t length, uint32_t session_id);
        bool validateOrdering(const std::string &strategy_id, uint64_t sequence_number);
        void handleError(ConnectionError error);

        // Disable copy/move
        PositionSubscriber(const PositionSubscriber &) = delete;
        PositionSubscriber &operator=(const PositionSubscriber &) = delete;
    };

} // namespace position_distributor
