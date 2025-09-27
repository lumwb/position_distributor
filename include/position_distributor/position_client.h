#pragma once

#include "position_distributor/position_publisher.h"
#include "position_distributor/position_subscriber.h"
#include <string>
#include <vector>
#include <memory>
#include <unordered_set>
#include <mutex>
#include <functional>

namespace position_distributor
{

    // Client configuration for exchange-level operations
    struct PositionClientConfig
    {
        std::string exchange;                          // Exchange name (e.g., "BINANCE", "COINBASE")
        std::vector<std::string> subscribed_exchanges; // Other exchanges to subscribe to
        PublisherConfig publisher_config;              // Publisher configuration
        SubscriberConfig subscriber_config;            // Subscriber configuration (template)

        explicit PositionClientConfig(const std::string &exchange_name = "")
            : exchange(exchange_name)
        {
            if (!exchange.empty())
            {
                publisher_config.topic = "position_update." + exchange;
            }
        }
    };

    // Exchange-level position client combining publisher and subscriber functionality
    class PositionClient
    {
    public:
        using PositionUpdateCallback = std::function<void(const PositionUpdate &)>;
        using ErrorCallback = std::function<void(const std::string &, ConnectionError)>;
        using PublisherDisconnectCallback = std::function<void(const std::string &)>; // exchange (for per-exchange callbacks)

        explicit PositionClient(const PositionClientConfig &config);
        ~PositionClient();

        // Lifecycle
        bool connect();
        void disconnect();
        bool isConnected() const;

        // Publishing interface (for own exchange)
        bool publishPositions(const std::string &strategy_id,
                              const std::vector<SymbolPosition> &positions);

        bool publishPositions(const std::string &strategy_id,
                              const std::vector<std::pair<std::string, double>> &positions);

        // Subscription management (for other exchanges)
        bool subscribeToExchange(const std::string &exchange,
                                 PositionUpdateCallback position_callback,
                                 PublisherDisconnectCallback disconnect_callback = nullptr);
        bool unsubscribeFromExchange(const std::string &exchange);
        std::vector<std::string> getSubscribedExchanges() const;

        // Global error callback (for connection issues)
        void setErrorCallback(ErrorCallback callback);

        // Configuration and statistics
        const PositionClientConfig &getConfig() const { return config_; }
        const std::string &getExchange() const { return config_.exchange; }

        // Publisher statistics
        uint64_t getPublishedCount() const;
        uint64_t getPublishFailedCount() const;
        uint64_t getCurrentSequenceNumber() const;

        // Subscriber statistics
        uint64_t getReceivedCount() const;
        uint64_t getOrderingErrorCount() const;
        size_t getActiveSubscriptionCount() const;

        // Manual operations
        bool sendProdcuerHeartbeat();
        bool pollMessages();

    private:
        PositionClientConfig config_;

        // Core components
        std::unique_ptr<PositionPublisher> publisher_;
        std::unordered_map<std::string, std::unique_ptr<PositionSubscriber>> subscribers_;

        // Thread safety
        mutable std::mutex subscribers_mutex_;
        std::mutex callback_mutex_;

        // Callbacks
        ErrorCallback error_callback_;

        // Internal methods
        void handleError(const std::string &topic, ConnectionError error);
        std::string getTopicForExchange(const std::string &exchange) const;

        // Disable copy/move
        PositionClient(const PositionClient &) = delete;
        PositionClient &operator=(const PositionClient &) = delete;
    };

} // namespace position_distributor
