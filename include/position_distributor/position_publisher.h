#pragma once

#include "position_distributor/position_media_driver.h"
#include "position_distributor/sbe_encoding.h"
#include "position_distributor/position.h"
#include <string>
#include <vector>
#include <atomic>
#include <thread>
#include <mutex>
#include <chrono>
#include <functional>

namespace position_distributor
{

    // Publisher configuration
    struct PublisherConfig
    {
        std::string topic;                            // Topic to publish to (e.g., "position_update.BINANCE")
        std::chrono::milliseconds heartbeat_interval; // Heartbeat frequency
        std::chrono::milliseconds publish_timeout;    // Timeout for publish operations
        bool auto_heartbeat;                          // Enable automatic heartbeat

        PublisherConfig(const std::string &t = "")
            : topic(t), heartbeat_interval(1000), publish_timeout(5000), auto_heartbeat(true)
        {
        }
    };

    // Position publisher for shared memory transport
    class PositionPublisher
    {
    public:
        using ErrorCallback = std::function<void(const std::string &, ConnectionError)>;

        explicit PositionPublisher(const PublisherConfig &config);
        ~PositionPublisher();

        // Lifecycle
        bool connect();
        void disconnect();
        bool isConnected() const { return connected_.load(); }

        // Publishing interface
        bool publishPositions(const std::string &strategy_id,
                              const std::vector<SymbolPosition> &positions);

        bool publishPositions(const std::string &strategy_id,
                              const std::vector<std::pair<std::string, double>> &positions);

        // Configuration
        void setErrorCallback(ErrorCallback callback);
        const PublisherConfig &getConfig() const { return config_; }

        // Statistics
        uint64_t getPublishedCount() const { return published_count_.load(); }
        uint64_t getFailedCount() const { return failed_count_.load(); }
        uint64_t getCurrentSequenceNumber() const { return sequence_number_.load(); }

        // Manual heartbeat (if auto_heartbeat is disabled)
        bool sendHeartbeat();

    private:
        PublisherConfig config_;
        std::atomic<bool> connected_;
        std::atomic<bool> running_;

        // Media driver integration
        PositionMediaDriver *media_driver_;
        uint32_t session_id_;

        // Sequence tracking for ordering
        std::atomic<uint64_t> sequence_number_;

        // Statistics
        std::atomic<uint64_t> published_count_;
        std::atomic<uint64_t> failed_count_;

        // Error handling
        ErrorCallback error_callback_;
        std::mutex callback_mutex_;

        // Heartbeat management
        std::thread heartbeat_thread_;
        std::chrono::steady_clock::time_point last_heartbeat_;
        std::mutex heartbeat_mutex_;

        // Internal methods
        void heartbeatLoop();
        bool publishInternal(const uint8_t *data, uint32_t length);
        void handleError(ConnectionError error);
        void updateStatistics(bool success);

        // Disable copy/move
        PositionPublisher(const PositionPublisher &) = delete;
        PositionPublisher &operator=(const PositionPublisher &) = delete;
    };

} // namespace position_distributor
