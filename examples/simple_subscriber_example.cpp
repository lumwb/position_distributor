#include "position_distributor/position_subscriber.h"
#include "position_distributor/position_media_driver.h"
#include "position_distributor/logger.h"
#include <iostream>
#include <thread>
#include <chrono>
#include <signal.h>

using namespace position_distributor;

std::unique_ptr<PositionSubscriber> g_subscriber;

void signalHandler(int signal)
{
    std::cout << "\nReceived signal " << signal << ", shutting down subscriber..." << std::endl;

    if (g_subscriber)
    {
        g_subscriber->disconnect();
    }

    exit(0);
}

void onPositionUpdate(const PositionUpdate &update)
{
    std::cout << "RECEIVED: " << update.toString() << std::endl;
}

void onError(const std::string &topic, ConnectionError error)
{
    std::cout << "SUBSCRIBER ERROR - Topic: " << topic
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

    std::cout << "=== Simple Position Subscriber ===" << std::endl;
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

        // Create subscriber configuration
        SubscriberConfig config(topic);
        config.poll_interval = std::chrono::milliseconds(10);
        config.activity_interval = std::chrono::milliseconds(2000);

        // Create subscriber
        g_subscriber = std::make_unique<PositionSubscriber>(config);
        g_subscriber->setPositionUpdateCallback(onPositionUpdate);
        g_subscriber->setErrorCallback(onError);

        // Connect subscriber
        if (!g_subscriber->connect())
        {
            std::cerr << "Failed to connect subscriber" << std::endl;
            return 1;
        }

        std::cout << "Subscriber connected! Waiting for position updates..." << std::endl;
        std::cout << "Press Ctrl+C to stop." << std::endl;

        // Main monitoring loop
        auto last_stats_time = std::chrono::steady_clock::now();
        const auto stats_interval = std::chrono::seconds(15);

        while (g_subscriber->isConnected())
        {
            std::this_thread::sleep_for(std::chrono::seconds(1));

            // Print statistics periodically
            auto now = std::chrono::steady_clock::now();
            if (now - last_stats_time >= stats_interval)
            {
                std::cout << "\n=== Statistics ===" << std::endl;
                std::cout << "Received: " << g_subscriber->getReceivedCount() << std::endl;
                std::cout << "Ordering Errors: " << g_subscriber->getOrderingErrorCount() << std::endl;
                std::cout << "Last Sequence: " << g_subscriber->getLastSequenceNumber() << std::endl;
                std::cout << "==================" << std::endl;

                last_stats_time = now;
            }
        }
    }
    catch (const std::exception &e)
    {
        std::cerr << "Error: " << e.what() << std::endl;
        return 1;
    }

    std::cout << "Subscriber stopped." << std::endl;
    return 0;
}
