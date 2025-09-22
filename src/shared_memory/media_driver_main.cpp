#include "position_distributor/position_media_driver.h"
#include "position_distributor/logger.h"
#include <iostream>
#include <signal.h>
#include <chrono>
#include <thread>

using namespace position_distributor;

std::unique_ptr<PositionMediaDriver> g_media_driver;

void signalHandler(int signal)
{
    std::cout << "\nReceived signal " << signal << ", shutting down media driver..." << std::endl;

    if (g_media_driver)
    {
        g_media_driver->stop();
    }

    exit(0);
}

void printStatistics(const PositionMediaDriver &driver)
{
    std::cout << "\n=== Media Driver Statistics ===" << std::endl;
    std::cout << "Publishers: " << driver.getPublisherCount() << std::endl;
    std::cout << "Subscribers: " << driver.getSubscriberCount() << std::endl;
    std::cout << "Active Topics: " << driver.getTopicCount() << std::endl;

    auto topics = driver.getActiveTopics();
    if (!topics.empty())
    {
        std::cout << "Topics: ";
        for (size_t i = 0; i < topics.size(); ++i)
        {
            if (i > 0)
                std::cout << ", ";
            std::cout << topics[i];
        }
        std::cout << std::endl;
    }
    std::cout << "===============================" << std::endl;
}

int main(int argc, char *argv[])
{
    // Set up signal handling
    signal(SIGINT, signalHandler);
    signal(SIGTERM, signalHandler);

    // Set log level
    Logger::instance().setLevel(LogLevel::INFO);

    std::cout << "=== Position Media Driver ===" << std::endl;
    std::cout << "Starting position media driver..." << std::endl;

    try
    {
        // Create and start media driver
        g_media_driver = std::make_unique<PositionMediaDriver>();

        // Set global error callback for monitoring
        g_media_driver->setGlobalErrorCallback([](const std::string &topic, ConnectionError error)
                                               { std::cout << "GLOBAL ERROR - Topic: " << topic
                                                           << ", Error: " << static_cast<int>(error) << std::endl; });

        if (!g_media_driver->start())
        {
            std::cerr << "Failed to start media driver" << std::endl;
            return 1;
        }

        std::cout << "Media driver started successfully!" << std::endl;
        std::cout << "Press Ctrl+C to stop." << std::endl;
        std::cout << "\nMonitoring connections..." << std::endl;

        // Main monitoring loop
        auto last_stats_time = std::chrono::steady_clock::now();
        const auto stats_interval = std::chrono::seconds(10);

        while (g_media_driver->isRunning())
        {
            std::this_thread::sleep_for(std::chrono::seconds(1));

            // Print statistics periodically
            auto now = std::chrono::steady_clock::now();
            if (now - last_stats_time >= stats_interval)
            {
                printStatistics(*g_media_driver);
                last_stats_time = now;
            }
        }
    }
    catch (const std::exception &e)
    {
        std::cerr << "Error: " << e.what() << std::endl;
        return 1;
    }

    std::cout << "Media driver stopped." << std::endl;
    return 0;
}
