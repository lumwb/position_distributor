#include "position_distributor/position_subscriber.h"
#include "position_distributor/logger.h"
#include <iostream>
#include <thread>
#include <chrono>
#include <signal.h>
#include <atomic>

using namespace position_distributor;

std::unique_ptr<PositionSubscriber> g_subscriber;
std::atomic<int> g_message_count{0};

void signalHandler(int signal)
{
    std::cout << "\nShutting down subscriber..." << std::endl;
    std::cout << "Total messages received: " << g_message_count.load() << std::endl;
    if (g_subscriber)
    {
        g_subscriber->disconnect();
    }
    exit(0);
}

void onPositionUpdate(const PositionUpdate &update)
{
    g_message_count++;
    std::cout << "\n🔔 RECEIVED MESSAGE #" << g_message_count.load() << std::endl;
    std::cout << "Strategy: " << update.strategy_id << std::endl;
    std::cout << "Sequence: " << update.sequence_number << std::endl;
    std::cout << "Positions:" << std::endl;

    for (const auto &pos : update.positions)
    {
        std::cout << "  " << pos.symbol << ": " << pos.net_position << std::endl;
    }
    std::cout << "----------------------------------------" << std::endl;
}

void onError(const std::string &topic, ConnectionError error)
{
    std::cout << "❌ ERROR on topic " << topic << ": " << static_cast<int>(error) << std::endl;
}

int main(int argc, char *argv[])
{
    signal(SIGINT, signalHandler);

    if (argc < 2)
    {
        std::cerr << "Usage: " << argv[0] << " <exchange>" << std::endl;
        std::cerr << "Example: " << argv[0] << " BINANCE" << std::endl;
        return 1;
    }

    std::string exchange = argv[1];
    std::string topic = "position_update." + exchange;

    Logger::instance().setLevel(LogLevel::INFO);

    std::cout << "=== Position Subscriber Test ===" << std::endl;
    std::cout << "Exchange: " << exchange << std::endl;
    std::cout << "Topic: " << topic << std::endl;

    try
    {
        SubscriberConfig config(topic);
        config.poll_interval = std::chrono::milliseconds(10); // Poll frequently

        g_subscriber = std::make_unique<PositionSubscriber>(config);
        g_subscriber->setPositionUpdateCallback(onPositionUpdate);
        g_subscriber->setErrorCallback(onError);

        if (!g_subscriber->connect())
        {
            std::cerr << "Failed to connect subscriber" << std::endl;
            return 1;
        }

        std::cout << "Subscriber connected! Waiting for messages..." << std::endl;
        std::cout << "Press Ctrl+C to stop.\n"
                  << std::endl;

        // Keep the subscriber running
        while (true)
        {
            std::this_thread::sleep_for(std::chrono::seconds(5));

            // Print periodic statistics
            std::cout << "📊 Stats - Received: " << g_subscriber->getReceivedCount()
                      << ", Ordering Errors: " << g_subscriber->getOrderingErrorCount()
                      << ", Last Seq: " << g_subscriber->getLastSequenceNumber() << std::endl;
        }
    }
    catch (const std::exception &e)
    {
        std::cerr << "Error: " << e.what() << std::endl;
        return 1;
    }

    return 0;
}
