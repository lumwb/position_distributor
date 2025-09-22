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

int main(int argc, char *argv[])
{
    // Set up signal handling
    signal(SIGINT, signalHandler);
    signal(SIGTERM, signalHandler);

    // Parse command line arguments
    if (argc < 2)
    {
        std::cerr << "Usage: " << argv[0] << " <exchange> [subscribe_to_exchange1] [subscribe_to_exchange2] ..." << std::endl;
        std::cerr << "Example: " << argv[0] << " BINANCE COINBASE KRAKEN" << std::endl;
        return 1;
    }

    std::string exchange = argv[1];
    std::vector<std::string> subscribe_to;

    for (int i = 2; i < argc; ++i)
    {
        subscribe_to.push_back(argv[i]);
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
        // Create client configuration
        PositionClientConfig config(exchange);
        config.subscribed_exchanges = subscribe_to;

        // Create position client
        g_client = std::make_unique<PositionClient>(config);

        // Set callbacks
        g_client->setPositionUpdateCallback(onPositionUpdate);
        g_client->setErrorCallback(onError);

        // Connect to media driver
        if (!g_client->connect())
        {
            std::cerr << "Failed to connect to media driver" << std::endl;
            return 1;
        }

        std::cout << "Connected! Publishing random positions every 3 seconds..." << std::endl;
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

            // Publish positions periodically
            if (now - last_publish_time >= publish_interval)
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
