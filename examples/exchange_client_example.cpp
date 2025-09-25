#include "position_distributor/position_client.h"
#include "position_distributor/logger.h"
#include <iostream>
#include <random>
#include <thread>
#include <chrono>
#include <signal.h>

using namespace position_distributor;

std::unique_ptr<PositionClient> g_client;

void signalHandler(int signal)
{
    std::cout << "\nReceived signal " << signal << ", shutting down client..." << std::endl;

    if (g_client)
    {
        g_client->disconnect();
    }

    exit(0);
}

void onPositionUpdate(const PositionUpdate &update)
{
    std::cout << "RECEIVED: " << update.toString() << std::endl;
}

void onError(const std::string &topic, ConnectionError error)
{
    std::cout << "ERROR - Topic: " << topic
              << ", Error: " << static_cast<int>(error) << std::endl;
}

void printStatistics(const PositionClient &client)
{
    std::cout << "\n=== Client Statistics ===" << std::endl;
    std::cout << "Exchange: " << client.getExchange() << std::endl;
    std::cout << "Published: " << client.getPublishedCount() << std::endl;
    std::cout << "Publish Failures: " << client.getPublishFailedCount() << std::endl;
    std::cout << "Received: " << client.getReceivedCount() << std::endl;
    std::cout << "Ordering Errors: " << client.getOrderingErrorCount() << std::endl;
    std::cout << "Active Subscriptions: " << client.getActiveSubscriptionCount() << std::endl;
    std::cout << "Current Sequence: " << client.getCurrentSequenceNumber() << std::endl;

    auto subscriptions = client.getSubscribedExchanges();
    if (!subscriptions.empty())
    {
        std::cout << "Subscribed to: ";
        for (size_t i = 0; i < subscriptions.size(); ++i)
        {
            if (i > 0)
                std::cout << ", ";
            std::cout << subscriptions[i];
        }
        std::cout << std::endl;
    }
    std::cout << "=========================" << std::endl;
}

void printUsage(const char *program_name)
{
    std::cerr << "Usage: " << program_name << " [OPTIONS]" << std::endl;
    std::cerr << "Options:" << std::endl;
    std::cerr << "  -p <exchange>    Publisher exchange (optional)" << std::endl;
    std::cerr << "  -s <exchange>    Subscribe to exchange (can be used multiple times)" << std::endl;
    std::cerr << "  -h               Show this help" << std::endl;
    std::cerr << std::endl;
    std::cerr << "Examples:" << std::endl;
    std::cerr << "  " << program_name << " -p BINANCE -s COINBASE -s KRAKEN  # Publish BINANCE, subscribe to COINBASE & KRAKEN" << std::endl;
    std::cerr << "  " << program_name << " -p BINANCE                        # Publish BINANCE only" << std::endl;
    std::cerr << "  " << program_name << " -s BINANCE -s COINBASE            # Subscribe only (no publishing)" << std::endl;
}

int main(int argc, char *argv[])
{
    // Set up signal handling
    signal(SIGINT, signalHandler);
    signal(SIGTERM, signalHandler);

    // Parse command line arguments
    std::string exchange = ""; // Publisher exchange
    std::vector<std::string> subscribe_to;

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
    std::cout << "Exchange: " << exchange << std::endl;

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
        if (!is_publisher_mode)
        {
            exchange = "SUBSCRIBER_" + std::to_string(std::chrono::steady_clock::now().time_since_epoch().count());
            std::cout << "Subscriber-only mode: Using dummy publisher exchange: " << exchange << std::endl;
        }

        // Create client configuration
        PositionClientConfig config(exchange);
        config.subscribed_exchanges = subscribe_to;

        // Create position client
        g_client = std::make_unique<PositionClient>(config);

        // Set error callback
        g_client->setErrorCallback(onError);

        // Connect to media driver
        if (!g_client->connect())
        {
            std::cerr << "Failed to connect to media driver" << std::endl;
            return 1;
        }

        // Set up per-exchange subscriptions with callbacks
        // (Note: connect() already subscribes to exchanges from config, but without callbacks)
        for (const std::string &exchange_to_sub : subscribe_to)
        {
            // Resubscribe with proper callbacks
            g_client->unsubscribeFromExchange(exchange_to_sub); // First unsubscribe the one without callbacks

            // Subscribe with position update and disconnect callbacks
            auto position_callback = [exchange_to_sub](const PositionUpdate &update)
            {
                std::cout << "[" << exchange_to_sub << "] RECEIVED: " << update.toString() << std::endl;
            };

            auto disconnect_callback = [exchange_to_sub](const std::string &exchange)
            {
                std::cout << "[" << exchange << "] PUBLISHER DISCONNECTED!" << std::endl;
            };

            if (!g_client->subscribeToExchange(exchange_to_sub, position_callback, disconnect_callback))
            {
                std::cerr << "Failed to subscribe to exchange: " << exchange_to_sub << std::endl;
            }
        }

        if (is_publisher_mode)
        {
            std::cout << "Connected! Publishing random positions every 3 seconds..." << std::endl;
        }
        else
        {
            std::cout << "Connected! Subscriber-only mode - waiting for position updates..." << std::endl;
        }
        std::cout << "Press Ctrl+C to stop." << std::endl;

        // Random number generator for position simulation
        std::random_device rd;
        std::mt19937 gen(rd());
        std::uniform_real_distribution<> position_dist(-1000.0, 1000.0);
        std::uniform_int_distribution<> symbol_count_dist(1, 5);

        std::vector<std::string> symbols = {
            "BTCUSDT", "ETHUSDT", "ADAUSDT", "DOTUSDT", "LINKUSDT",
            "BNBUSDT", "SOLUSDT", "MATICUSDT", "AVAXUSDT", "UNIUSDT"};

        // Main publishing loop
        auto last_publish_time = std::chrono::steady_clock::now();
        auto last_stats_time = std::chrono::steady_clock::now();
        const auto publish_interval = std::chrono::seconds(3);
        const auto stats_interval = std::chrono::seconds(15);

        while (g_client->isConnected())
        {
            auto now = std::chrono::steady_clock::now();

            // Publish positions periodically (only in publisher mode)
            if (is_publisher_mode && now - last_publish_time >= publish_interval)
            {
                // Generate random positions
                int num_positions = symbol_count_dist(gen);
                std::vector<SymbolPosition> positions;

                for (int i = 0; i < num_positions; ++i)
                {
                    std::string symbol = symbols[gen() % symbols.size()];
                    double position = position_dist(gen);
                    positions.emplace_back(symbol, position);
                }

                // Publish positions
                std::string strategy_id = exchange + "_STRATEGY_1";
                if (g_client->publishPositions(strategy_id, positions))
                {
                    std::cout << "PUBLISHED: " << positions.size() << " positions for " << strategy_id << std::endl;
                }
                else
                {
                    std::cout << "FAILED to publish positions for " << strategy_id << std::endl;
                }

                last_publish_time = now;
            }

            // Print statistics periodically
            if (now - last_stats_time >= stats_interval)
            {
                printStatistics(*g_client);
                last_stats_time = now;
            }

            // Poll for incoming messages
            g_client->pollMessages();

            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }
    }
    catch (const std::exception &e)
    {
        std::cerr << "Error: " << e.what() << std::endl;
        return 1;
    }

    std::cout << "Client stopped." << std::endl;
    return 0;
}
