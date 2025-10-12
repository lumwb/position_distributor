#include "position_distributor/position_client.h"
#include "position_distributor/logger.h"
#include <iostream>
#include <random>
#include <thread>
#include <chrono>
#include <signal.h>
#include <unistd.h>
#include <unordered_set>

using namespace position_distributor;

std::unique_ptr<PositionClient> g_client;

const std::unordered_map<std::string, std::string> exchange_to_short_name = {
    {"BINANCE", "BN"},
    {"HUOBI", "HB"},
    {"KRAKEN", "KA"},
    {"COINBASE", "CB"}};

const std::vector<std::string> symbols = {
    "BTCUSDT", "ETHUSDT", "ADAUSDT", "DOTUSDT", "LINKUSDT",
    "BNBUSDT", "SOLUSDT", "MATICUSDT", "AVAXUSDT", "UNIUSDT"};

void signalHandler(int signal)
{
    // Use fprintf for signal-safe logging (avoid std::cout race conditions)
    fprintf(stderr, "\nReceived signal %d, shutting down client...\n", signal);

    if (g_client)
    {
        g_client->disconnect();
    }

    exit(0);
}

void onPositionUpdate(const PositionUpdate &update)
{
    LOG_INFO("RECEIVED: " + update.toString());
}

void onError(const std::string &topic, ConnectionError error)
{
    LOG_ERROR("ERROR - Topic: " + topic + ", Error: " + std::to_string(static_cast<int>(error)));
}

void printStatistics(const PositionClient &client)
{
    std::string stats = "\n=== Client Statistics ===\n";
    stats += "Exchange: " + client.getExchange() + "\n";
    stats += "Published: " + std::to_string(client.getPublishedCount()) + "\n";
    stats += "Publish Failures: " + std::to_string(client.getPublishFailedCount()) + "\n";
    stats += "Received: " + std::to_string(client.getReceivedCount()) + "\n";
    stats += "Ordering Errors: " + std::to_string(client.getOrderingErrorCount()) + "\n";
    stats += "Active Subscriptions: " + std::to_string(client.getActiveSubscriptionCount()) + "\n";
    stats += "Current Sequence: " + std::to_string(client.getCurrentSequenceNumber()) + "\n";

    auto subscriptions = client.getSubscribedExchanges();
    if (!subscriptions.empty())
    {
        stats += "Subscribed to: ";
        for (size_t i = 0; i < subscriptions.size(); ++i)
        {
            if (i > 0)
                stats += ", ";
            stats += subscriptions[i];
        }
        stats += "\n";
    }
    stats += "=========================";

    LOG_INFO(stats);
}

void printUsage(const char *program_name)
{
    std::cerr << "Usage: " << program_name << " [OPTIONS]" << std::endl;
    std::cerr << "Options:" << std::endl;
    std::cerr << "  -p <exchange>    Publisher exchange (optional)" << std::endl;
    std::cerr << "  -s <exchange>    Subscribe to exchange (can be used multiple times)" << std::endl;
    std::cerr << "  --clear-subs     Clear all subscriber slots on publisher startup (dev mode)" << std::endl;
    std::cerr << "  -h               Show this help" << std::endl;
    std::cerr << std::endl;
    std::cerr << "Examples:" << std::endl;
    std::cerr << "  " << program_name << " -p BINANCE -s COINBASE -s KRAKEN  # Publish BINANCE, subscribe to COINBASE & KRAKEN" << std::endl;
    std::cerr << "  " << program_name << " -p BINANCE                        # Publish BINANCE only" << std::endl;
    std::cerr << "  " << program_name << " -p BINANCE --clear-subs           # Publish BINANCE, clear stale subscribers" << std::endl;
    std::cerr << "  " << program_name << " -s BINANCE -s COINBASE            # Subscribe only (no publishing)" << std::endl;
}

std::string symbolWithShortName(const std::string &symbol, const std::string &exchange)
{
    if (exchange_to_short_name.find(exchange) == exchange_to_short_name.end())
    {
        // concat symbol + "." + exchange
        return symbol + "." + exchange;
    }

    return symbol + "." + exchange_to_short_name.at(exchange);
}

int main(int argc, char *argv[])
{
    // Set up signal handling
    signal(SIGINT, signalHandler);
    signal(SIGTERM, signalHandler);

    // Parse command line arguments
    std::string exchange = ""; // Publisher exchange
    std::vector<std::string> subscribe_to;
    bool clear_subscribers = false;

    for (int i = 1; i < argc; ++i)
    {
        std::string arg = argv[i];

        if (arg == "-h" || arg == "--help")
        {
            printUsage(argv[0]);
            return 0;
        }
        else if (arg == "-p")
        {
            if (i + 1 >= argc)
            {
                std::cerr << "Error: -p requires an exchange name" << std::endl;
                printUsage(argv[0]);
                return 1;
            }
            exchange = argv[++i];
        }
        else if (arg == "-s")
        {
            if (i + 1 >= argc)
            {
                std::cerr << "Error: -s requires an exchange name" << std::endl;
                printUsage(argv[0]);
                return 1;
            }
            subscribe_to.push_back(argv[++i]);
        }
        else if (arg == "--clear-subs")
        {
            clear_subscribers = true;
        }
        else
        {
            std::cerr << "Error: Unknown option '" << arg << "'" << std::endl;
            printUsage(argv[0]);
            return 1;
        }
    }

    // Validate arguments
    if (exchange.empty() && subscribe_to.empty())
    {
        std::cerr << "Error: Must specify at least one publisher (-p) or subscriber (-s)" << std::endl;
        printUsage(argv[0]);
        return 1;
    }

    // Set log level
    Logger::instance().setLevel(LogLevel::INFO);

    std::cout << "=== Exchange Position Client ===" << std::endl;
    if (!exchange.empty())
    {
        std::cout << "Publisher Exchange: " << exchange << std::endl;
    }
    else
    {
        std::cout << "Mode: Subscriber-only" << std::endl;
    }

    if (!subscribe_to.empty())
    {
        std::cout << "Subscribing to: ";
        for (size_t i = 0; i < subscribe_to.size(); ++i)
        {
            if (i > 0)
                std::cout << ", ";
            std::cout << subscribe_to[i];
        }
        std::cout << std::endl;
    }

    try
    {
        // Handle subscriber-only mode
        bool is_publisher_mode = !exchange.empty();

        // Create client configuration
        PositionClientConfig config(exchange); // If exchange is empty, publisher_config.topic will be empty too
        config.subscribed_exchanges = subscribe_to;

        if (!is_publisher_mode)
        {
            std::cout << "Subscriber-only mode: Publisher disabled" << std::endl;
        }

        // Create position client
        g_client = std::make_unique<PositionClient>(config);

        // Set error callback
        g_client->setErrorCallback(onError);

        if (!g_client->connect())
        {
            std::cerr << "Failed to connect" << std::endl;
            return 1;
        }

        // Set up per-exchange subscriptions with callbacks
        // (Note: connect() already subscribes to exchanges from config, but without callbacks)
        for (const std::string &exchange_to_sub : subscribe_to)
        {
            // Resubscribe with proper callbacks
            // g_client->unsubscribeFromExchange(exchange_to_sub); // First unregisterSubscriber the one without callbacks

            // Subscribe with position update and disconnect callbacks
            auto position_callback = [exchange_to_sub](const PositionUpdate &update)
            {
                LOG_INFO("[" + exchange_to_sub + "] RECEIVED: " + update.toString());
            };

            auto disconnect_callback = [exchange_to_sub](const std::string &exchange)
            {
                LOG_WARN("[" + exchange + "] PUBLISHER DISCONNECTED!");
            };

            if (!g_client->subscribeToExchange(exchange_to_sub, position_callback, disconnect_callback))
            {
                std::cerr << "Failed to registerSubscriber to exchange: " << exchange_to_sub << std::endl;
            }
        }

        // Clear subscriber slots if requested (only for publishers)
        if (is_publisher_mode && clear_subscribers)
        {
            std::cout << "Clearing all subscriber slots for publisher topics..." << std::endl;
            auto &topic_registry = TopicRegistry::instance();
            auto topic_channel = topic_registry.getOrCreateTopic("position_update." + exchange);
            if (topic_channel)
            {
                topic_channel->clearAllSubscribers();
                std::cout << "Cleared all subscriber slots for topic: position_update." << exchange << std::endl;
            }
        }

        // Random number generator for position simulation
        std::random_device rd;
        std::mt19937 gen(rd());
        std::uniform_real_distribution<> position_dist(-1000.0, 1000.0);
        std::uniform_int_distribution<> symbol_count_dist(1, 5);

        // Main publishing loop
        auto last_publish_time = std::chrono::steady_clock::now();
        auto last_stats_time = std::chrono::steady_clock::now();
        auto last_print_positions_time = std::chrono::steady_clock::now();
        const auto publish_interval = std::chrono::seconds(3);
        const auto stats_interval = std::chrono::seconds(15);
        const auto print_positions_interval = std::chrono::seconds(15);

        while (g_client->isConnected())
        {
            auto now = std::chrono::steady_clock::now();

            // Publish positions periodically (only in publisher mode)
            if (is_publisher_mode && now - last_publish_time >= publish_interval)
            {
                // Generate random positions
                int num_positions = symbol_count_dist(gen);
                std::vector<SymbolPosition> positions;
                std::unordered_set<std::string> used_symbols;

                for (int i = 0; i < num_positions; ++i)
                {
                    // Here we don't want to publish same symbol twice
                    std::string symbol = symbols[gen() % symbols.size()];
                    while (used_symbols.find(symbol) != used_symbols.end())
                    {
                        symbol = symbols[gen() % symbols.size()];
                    }
                    used_symbols.insert(symbol);
                    double position = position_dist(gen);
                    positions.emplace_back(symbolWithShortName(symbol, exchange), position);
                }

                // Publish positions
                // get PID
                std::string pid = std::to_string(getpid());
                std::string strategy_id = exchange + "_" + pid;
                if (g_client->publishPositions(strategy_id, positions))
                {
                    // Print all positions being published
                    std::string positions_log = "PUBLISHED " + std::to_string(positions.size()) + " positions for " + strategy_id + ":";
                    for (const auto &pos : positions)
                    {
                        positions_log += "\n  -> " + pos.toString();
                    }
                    LOG_INFO(positions_log);
                }
                else
                {
                    LOG_ERROR("FAILED to publish positions for " + strategy_id);
                }

                last_publish_time = now;
            }

            // Throttled printing of all positions in cache
            if (now - last_print_positions_time >= print_positions_interval)
            {
                for (const std::string &exchange : subscribe_to)
                {
                    LOG_INFO("[" + exchange + "] CACHED POSITIONS:");
                    for (const std::string &symbol : symbols)
                    {
                        auto position = g_client->getPosition(exchange, symbolWithShortName(symbol, exchange));
                        if (position)
                        {
                            LOG_INFO("[" + exchange + "] POSITION: " + symbolWithShortName(symbol, exchange) + " = " + std::to_string(*position));
                        }
                    }
                }
                last_print_positions_time = now;
            }

            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }
    }
    catch (const std::exception &e)
    {
        std::cerr << "Error: " << e.what() << std::endl;
        return 1;
    }

    LOG_INFO("Client stopped.");
    return 0;
}
