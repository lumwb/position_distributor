#pragma once

#include "position_distributor/position_publisher.h"
#include "position_distributor/position_subscriber.h"
#include <string>
#include <vector>
#include <memory>
#include <unordered_set>
#include <mutex>
#include <functional>
#include <shared_mutex>

namespace position_distributor
{

    // Client configuration for exchange-level operations
    struct PositionClientConfig
    {
        std::string exchange;                          // Exchange name (e.g., "BINANCE", "COINBASE")
        std::vector<std::string> subscribed_exchanges; // Other exchanges to subscribe to
        PublisherConfig publisher_config;              // Publisher configuration
        SubscriberConfig subscriber_config;            // Subscriber configuration (default same for each subscribed_exchange)
        bool should_cache_positions;                   // Whether to cache positions (default true)

        explicit PositionClientConfig(const std::string &exchange_name = "")
            : exchange(exchange_name), should_cache_positions(true)
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
        bool createAndConnectSubscriber(const std::string &exchange);
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
        bool sendProdcuerHeartbeat(); // Subscriber heartbeat is embedded in the polling
        bool pollMessages();

        std::optional<double> getPosition(const std::string &exchange, const std::string &symbol) const;

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

        // Cache of exchange -> symbol -> position
        std::unordered_map<std::string, std::unordered_map<std::string, double>> position_cache_;
        // Per-exchange mutexes
        mutable std::unordered_map<std::string, std::unique_ptr<std::shared_mutex>> exchange_shared_mutexes_;
        mutable std::mutex exchange_mutexes_mutex_; // Protects the mutex map itself

        // Get or create shared mutex for exchange
        std::shared_mutex &getExchangeSharedMutex(const std::string &exchange) const;

        // Methods to update cache
        void updatePositionCache(const std::string &exchange, const std::string &symbol, double position);
        void clearExchangeCache(const std::string &exchange);
    };

} // namespace position_distributor
