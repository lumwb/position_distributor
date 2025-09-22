#include "position_distributor/position_publisher.h"
#include "position_distributor/position_media_driver.h"
#include "position_distributor/logger.h"
#include <iostream>
#include <random>
#include <thread>
#include <chrono>
#include <signal.h>

using namespace position_distributor;

std::unique_ptr<PositionPublisher> g_publisher;

void signalHandler(int signal)
{
    std::cout << "\nReceived signal " << signal << ", shutting down publisher..." << std::endl;

    if (g_publisher)
    {
        g_publisher->disconnect();
    }

    exit(0);
}

void onError(const std::string &topic, ConnectionError error)
{
    std::cout << "PUBLISHER ERROR - Topic: " << topic
              << ", Error: " << static_cast<int>(error) << std::endl;
}

int main(int argc, char *argv[])
{
    // Set up signal handling
    signal(SIGINT, signalHandler);
    signal(SIGTERM, signalHandler);

    // Parse command line arguments
    if (argc < 2)
    {
        std::cerr << "Usage: " << argv[0] << " <exchange>" << std::endl;
        std::cerr << "Example: " << argv[0] << " BINANCE" << std::endl;
        return 1;
    }

    std::string exchange = argv[1];
    std::string topic = "position_update." + exchange;

    // Set log level
    Logger::instance().setLevel(LogLevel::INFO);

    std::cout << "=== Simple Position Publisher ===" << std::endl;
    std::cout << "Exchange: " << exchange << std::endl;
    std::cout << "Topic: " << topic << std::endl;

    try
    {
        // Ensure media driver is initialized (for testing)
        if (!MediaDriverManager::initialize())
        {
            std::cerr << "Failed to initialize media driver" << std::endl;
            return 1;
        }

        // Create publisher configuration
        PublisherConfig config(topic);
        config.heartbeat_interval = std::chrono::milliseconds(2000);

        // Create publisher
        g_publisher = std::make_unique<PositionPublisher>(config);
        g_publisher->setErrorCallback(onError);

        // Connect publisher
        if (!g_publisher->connect())
        {
            std::cerr << "Failed to connect publisher" << std::endl;
            return 1;
        }

        std::cout << "Publisher connected! Publishing positions every 2 seconds..." << std::endl;
        std::cout << "Press Ctrl+C to stop." << std::endl;

        // Random number generator
        std::random_device rd;
        std::mt19937 gen(rd());
        std::uniform_real_distribution<> position_dist(-500.0, 500.0);

        std::vector<std::string> symbols = {
            "BTCUSDT", "ETHUSDT", "ADAUSDT", "DOTUSDT", "LINKUSDT"};

        // Publishing loop
        int iteration = 0;
        while (g_publisher->isConnected())
        {
            std::this_thread::sleep_for(std::chrono::seconds(2));

            // Generate random positions
            std::vector<std::pair<std::string, double>> positions;
            for (const auto &symbol : symbols)
            {
                double position = position_dist(gen);
                positions.emplace_back(symbol, position);
            }

            // Publish positions
            std::string strategy_id = exchange + "_STRATEGY";
            bool success = g_publisher->publishPositions(strategy_id, positions);

            iteration++;
            std::cout << "[" << iteration << "] "
                      << (success ? "PUBLISHED" : "FAILED")
                      << ": " << positions.size() << " positions for " << strategy_id
                      << " (seq: " << g_publisher->getCurrentSequenceNumber() << ")" << std::endl;

            // Print statistics every 10 iterations
            if (iteration % 10 == 0)
            {
                std::cout << "\n=== Statistics ===" << std::endl;
                std::cout << "Published: " << g_publisher->getPublishedCount() << std::endl;
                std::cout << "Failed: " << g_publisher->getFailedCount() << std::endl;
                std::cout << "Current Sequence: " << g_publisher->getCurrentSequenceNumber() << std::endl;
                std::cout << "==================" << std::endl;
            }
        }
    }
    catch (const std::exception &e)
    {
        std::cerr << "Error: " << e.what() << std::endl;
        return 1;
    }

    std::cout << "Publisher stopped." << std::endl;
    return 0;
}
