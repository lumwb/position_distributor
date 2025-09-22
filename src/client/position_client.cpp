#include "position_distributor/position_client.h"
#include "position_distributor/logger.h"
#include <algorithm>

namespace position_distributor
{

    PositionClient::PositionClient(const PositionClientConfig &config)
        : config_(config)
    {
        if (config_.exchange.empty())
        {
            throw std::invalid_argument("Exchange name cannot be empty");
        }

        // Create publisher for own exchange
        publisher_ = std::make_unique<PositionPublisher>(config_.publisher_config);

        // Set up publisher error callback
        auto publisher_error_callback = [this](const std::string &topic, ConnectionError error)
        {
            this->handleError(topic, error);
        };
        publisher_->setErrorCallback(publisher_error_callback);

        LOG_INFO("Created position client for exchange: " + config_.exchange);
    }

    PositionClient::~PositionClient()
    {
        disconnect();
    }

    bool PositionClient::connect()
    {
        LOG_INFO("Connecting position client for exchange: " + config_.exchange);

        // Connect publisher
        if (!publisher_->connect())
        {
            LOG_ERROR("Failed to connect publisher for exchange: " + config_.exchange);
            return false;
        }

        // Subscribe to configured exchanges
        for (const std::string &exchange : config_.subscribed_exchanges)
        {
            if (!subscribeToExchange(exchange))
            {
                LOG_WARN("Failed to subscribe to exchange: " + exchange);
                // Continue with other subscriptions
            }
        }

        LOG_INFO("Position client connected for exchange: " + config_.exchange);
        return true;
    }

    void PositionClient::disconnect()
    {
        LOG_INFO("Disconnecting position client for exchange: " + config_.exchange);

        // Disconnect publisher
        if (publisher_)
        {
            publisher_->disconnect();
        }

        // Disconnect all subscribers
        {
            std::lock_guard<std::mutex> lock(subscribers_mutex_);
            for (auto &[exchange, subscriber] : subscribers_)
            {
                subscriber->disconnect();
            }
            subscribers_.clear();
        }

        LOG_INFO("Position client disconnected for exchange: " + config_.exchange);
    }

    bool PositionClient::isConnected() const
    {
        if (!publisher_ || !publisher_->isConnected())
        {
            return false;
        }

        // Check if at least one subscriber is connected (if any exist)
        std::lock_guard<std::mutex> lock(subscribers_mutex_);
        if (subscribers_.empty())
        {
            return true; // No subscribers configured, publisher connection is sufficient
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

    bool PositionClient::subscribeToExchange(const std::string &exchange)
    {
        if (exchange.empty())
        {
            LOG_ERROR("Exchange name cannot be empty");
            return false;
        }

        if (exchange == config_.exchange)
        {
            LOG_WARN("Cannot subscribe to own exchange: " + exchange);
            return false;
        }

        std::lock_guard<std::mutex> lock(subscribers_mutex_);

        // Check if already subscribed
        if (subscribers_.find(exchange) != subscribers_.end())
        {
            LOG_WARN("Already subscribed to exchange: " + exchange);
            return true;
        }

        // Create subscriber configuration
        SubscriberConfig sub_config = config_.subscriber_config;
        sub_config.topic = getTopicForExchange(exchange);

        // Create subscriber
        auto subscriber = std::make_unique<PositionSubscriber>(sub_config);

        // Set callbacks
        auto position_callback = [this](const PositionUpdate &update)
        {
            this->handlePositionUpdate(update);
        };
        subscriber->setPositionUpdateCallback(position_callback);

        auto error_callback = [this](const std::string &topic, ConnectionError error)
        {
            this->handleError(topic, error);
        };
        subscriber->setErrorCallback(error_callback);

        // Connect subscriber
        if (!subscriber->connect())
        {
            LOG_ERROR("Failed to connect subscriber for exchange: " + exchange);
            return false;
        }

        // Store subscriber
        subscribers_[exchange] = std::move(subscriber);

        LOG_INFO("Subscribed to exchange: " + exchange);
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

    void PositionClient::setPositionUpdateCallback(PositionUpdateCallback callback)
    {
        std::lock_guard<std::mutex> lock(callback_mutex_);
        position_callback_ = callback;
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

    bool PositionClient::sendHeartbeat()
    {
        return publisher_ ? publisher_->sendHeartbeat() : false;
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

    void PositionClient::handlePositionUpdate(const PositionUpdate &update)
    {
        LOG_DEBUG("Received position update: " + update.toString());

        std::lock_guard<std::mutex> lock(callback_mutex_);
        if (position_callback_)
        {
            position_callback_(update);
        }
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

} // namespace position_distributor
