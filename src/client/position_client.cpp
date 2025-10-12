#include "position_distributor/position_client.h"
#include "position_distributor/logger.h"
#include <algorithm>

namespace position_distributor
{

    PositionClient::PositionClient(const PositionClientConfig &config)
        : config_(config)
    {
        // Create publisher for own exchange (only if topic is configured)
        if (!config_.publisher_config.topic.empty())
        {
            publisher_ = std::make_unique<PositionPublisher>(config_.publisher_config);

            // Set up publisher error callback
            auto publisher_error_callback = [this](const std::string &topic, ConnectionError error)
            {
                this->handleError(topic, error);
            };
            publisher_->setErrorCallback(publisher_error_callback);
        }

        LOG_INFO("Created position client for exchange: " + config_.exchange +
                 (publisher_ ? " (with publisher)" : " (subscriber-only)"));
    }

    PositionClient::~PositionClient()
    {
        disconnect();
    }

    bool PositionClient::connect()
    {
        // Connect publisher (only if enabled)
        if (publisher_)
        {
            if (!publisher_->connect())
            {
                LOG_ERROR("Failed to connect publisher for exchange: " + config_.exchange);
                return false;
            }
            LOG_INFO("Connected publisher for exchange: " + config_.exchange);
        }

        if (!config_.subscribed_exchanges.empty())
        {
            for (const std::string &exchange : config_.subscribed_exchanges)
            {
                createAndConnectSubscriber(exchange);
            }
        }

        return true;
    }

    bool PositionClient::createAndConnectSubscriber(const std::string &exchange)
    {
        std::lock_guard<std::mutex> lock(subscribers_mutex_);

        if (subscribers_.find(exchange) != subscribers_.end())
        {
            LOG_WARN("Already created subscriber for exchange: " + exchange);
            return true;
        }

        SubscriberConfig sub_config = config_.subscriber_config;
        sub_config.topic = getTopicForExchange(exchange);
        subscribers_[exchange] = std::make_unique<PositionSubscriber>(sub_config);
        if (!subscribers_[exchange]->connect())
        {
            LOG_ERROR("Failed to connect subscriber for exchange: " + exchange);
            return false;
        }
        LOG_INFO("Connected subscriber for exchange: " + exchange);
        return true;
    }

    void PositionClient::disconnect()
    {

        // Disconnect publisher
        if (publisher_)
        {
            LOG_INFO("Disconnecting publisher for exchange: " + config_.exchange);
            publisher_->disconnect();
        }

        // Disconnect all subscribers
        {
            std::lock_guard<std::mutex> lock(subscribers_mutex_);
            for (auto &[exchange, subscriber] : subscribers_)
            {
                LOG_INFO("Disconnecting subscriber for exchange: " + exchange);
                subscriber->disconnect();
            }
            subscribers_.clear();
        }
    }

    bool PositionClient::isConnected() const
    {
        // Check publisher connection (if enabled)
        if (publisher_ && !publisher_->isConnected())
        {
            return false;
        }

        // Check if at least one subscriber is connected (if any exist)
        std::lock_guard<std::mutex> lock(subscribers_mutex_);
        if (subscribers_.empty())
        {
            return publisher_ != nullptr; // If no subscribers, require publisher to be connected
        }

        // At least one subscriber should be connected
        for (const auto &[exchange, subscriber] : subscribers_)
        {
            if (subscriber->isConnected())
            {
                return true;
            }
        }

        return false;
    }

    bool PositionClient::publishPositions(const std::string &strategy_id,
                                          const std::vector<SymbolPosition> &positions)
    {
        if (!publisher_)
        {
            LOG_ERROR("Publisher not initialized");
            return false;
        }

        return publisher_->publishPositions(strategy_id, positions);
    }

    bool PositionClient::publishPositions(const std::string &strategy_id,
                                          const std::vector<std::pair<std::string, double>> &positions)
    {
        if (!publisher_)
        {
            LOG_ERROR("Publisher not initialized");
            return false;
        }

        return publisher_->publishPositions(strategy_id, positions);
    }

    bool PositionClient::subscribeToExchange(const std::string &exchange,
                                             PositionUpdateCallback position_callback,
                                             PublisherDisconnectCallback disconnect_callback)
    {
        if (exchange.empty())
        {
            LOG_ERROR("Exchange name cannot be empty");
            return false;
        }

        // Only check for self-subscription if we have a publisher
        if (publisher_ && exchange == config_.exchange)
        {
            LOG_WARN("Cannot subscribe to own exchange: " + exchange);
            return false;
        }

        std::lock_guard<std::mutex> lock(subscribers_mutex_);

        // Try fetch PositionSubscriber for exchange
        auto it = subscribers_.find(exchange);
        if (it == subscribers_.end())
        {
            // attempt to initialise one and continue
            if (!createAndConnectSubscriber(exchange))
            {
                LOG_ERROR("Failed to create and connect subscriber for exchange: " + exchange);
                return false;
            }
            it = subscribers_.find(exchange);
        }

        auto *subscriber = it->second.get(); // Returns PositionSubscriber* or nullptr
        if (!subscriber)
        {
            LOG_ERROR("Failed to get subscriber for exchange: " + exchange);
            return false;
        }

        // Set callbacks - use the provided position callback directly
        if (position_callback)
        {
            auto adjusted_position_callback = [this, exchange, position_callback](const PositionUpdate &update)
            {
                // Call user's callback
                position_callback(update);

                if (config_.should_cache_positions)
                {
                    for (const auto &pos : update.positions)
                    {
                        this->updatePositionCache(exchange, pos.symbol, pos.net_position);
                    }
                }
            };

            subscriber->addPositionUpdateCallback(adjusted_position_callback);
        }

        // Set error callback to use our internal error handler
        auto error_callback = [this](const std::string &topic, ConnectionError error)
        {
            this->handleError(topic, error);
        };
        subscriber->setErrorCallback(error_callback);

        // Set publisher disconnect callback if provided
        if (disconnect_callback)
        {
            auto publisher_disconnect = [disconnect_callback, exchange](const std::string &topic)
            {
                disconnect_callback(exchange); // Pass exchange name instead of topic
            };
            subscriber->setPublisherDisconnectCallback(publisher_disconnect);
        }

        return true;
    }

    bool PositionClient::unsubscribeFromExchange(const std::string &exchange)
    {
        std::lock_guard<std::mutex> lock(subscribers_mutex_);

        auto it = subscribers_.find(exchange);
        if (it == subscribers_.end())
        {
            LOG_WARN("Not subscribed to exchange: " + exchange);
            return false;
        }

        // Disconnect and remove subscriber
        it->second->disconnect();
        subscribers_.erase(it);

        LOG_INFO("Unsubscribed from exchange: " + exchange);
        return true;
    }

    std::vector<std::string> PositionClient::getSubscribedExchanges() const
    {
        std::lock_guard<std::mutex> lock(subscribers_mutex_);

        std::vector<std::string> exchanges;
        exchanges.reserve(subscribers_.size());

        for (const auto &[exchange, subscriber] : subscribers_)
        {
            exchanges.push_back(exchange);
        }

        return exchanges;
    }

    void PositionClient::setErrorCallback(ErrorCallback callback)
    {
        std::lock_guard<std::mutex> lock(callback_mutex_);
        error_callback_ = callback;
    }

    uint64_t PositionClient::getPublishedCount() const
    {
        return publisher_ ? publisher_->getPublishedCount() : 0;
    }

    uint64_t PositionClient::getPublishFailedCount() const
    {
        return publisher_ ? publisher_->getFailedCount() : 0;
    }

    uint64_t PositionClient::getCurrentSequenceNumber() const
    {
        return publisher_ ? publisher_->getCurrentSequenceNumber() : 0;
    }

    uint64_t PositionClient::getReceivedCount() const
    {
        std::lock_guard<std::mutex> lock(subscribers_mutex_);

        uint64_t total = 0;
        for (const auto &[exchange, subscriber] : subscribers_)
        {
            total += subscriber->getReceivedCount();
        }

        return total;
    }

    uint64_t PositionClient::getOrderingErrorCount() const
    {
        std::lock_guard<std::mutex> lock(subscribers_mutex_);

        uint64_t total = 0;
        for (const auto &[exchange, subscriber] : subscribers_)
        {
            total += subscriber->getOrderingErrorCount();
        }

        return total;
    }

    size_t PositionClient::getActiveSubscriptionCount() const
    {
        std::lock_guard<std::mutex> lock(subscribers_mutex_);

        size_t active = 0;
        for (const auto &[exchange, subscriber] : subscribers_)
        {
            if (subscriber->isConnected())
            {
                active++;
            }
        }

        return active;
    }

    bool PositionClient::sendProdcuerHeartbeat()
    {
        return publisher_ ? publisher_->sendProdcuerHeartbeat() : false;
    }

    bool PositionClient::pollMessages()
    {
        std::lock_guard<std::mutex> lock(subscribers_mutex_);

        bool polled_any = false;
        for (auto &[exchange, subscriber] : subscribers_)
        {
            if (subscriber->pollMessages())
            {
                polled_any = true;
            }
        }

        return polled_any;
    }

    void PositionClient::handleError(const std::string &topic, ConnectionError error)
    {
        LOG_ERROR("Position client error for topic '" + topic + "': " + std::to_string(static_cast<int>(error)));

        std::lock_guard<std::mutex> lock(callback_mutex_);
        if (error_callback_)
        {
            error_callback_(topic, error);
        }
    }

    std::string PositionClient::getTopicForExchange(const std::string &exchange) const
    {
        return "position_update." + exchange;
    }

    std::shared_mutex &PositionClient::getExchangeSharedMutex(const std::string &exchange) const
    {
        std::lock_guard<std::mutex> lock(exchange_mutexes_mutex_);
        auto it = exchange_shared_mutexes_.find(exchange);
        if (it == exchange_shared_mutexes_.end())
        {
            exchange_shared_mutexes_[exchange] = std::make_unique<std::shared_mutex>();
            return *exchange_shared_mutexes_[exchange];
        }
        return *it->second;
    }

    std::optional<double> PositionClient::getPosition(const std::string &exchange, const std::string &symbol) const
    {
        // Shared lock allows multiple concurrent readers
        std::shared_lock<std::shared_mutex> lock(getExchangeSharedMutex(exchange));

        auto exchange_it = position_cache_.find(exchange);
        if (exchange_it == position_cache_.end())
        {
            return std::nullopt;
        }

        auto symbol_it = exchange_it->second.find(symbol);
        if (symbol_it == exchange_it->second.end())
        {
            return std::nullopt;
        }

        return symbol_it->second; // Return by value for thread safety
    }

    void PositionClient::updatePositionCache(const std::string &exchange, const std::string &symbol, double position)
    {
        std::unique_lock<std::shared_mutex> lock(getExchangeSharedMutex(exchange));
        position_cache_[exchange][symbol] = position;
        LOG_INFO("Updated position cache for exchange: " + exchange + " symbol: " + symbol + " position: " + std::to_string(position));
    }

    void PositionClient::clearExchangeCache(const std::string &exchange)
    {
        std::unique_lock<std::shared_mutex> lock(getExchangeSharedMutex(exchange));
        position_cache_.erase(exchange);
    }
} // namespace position_distributor
