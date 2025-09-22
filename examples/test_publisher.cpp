#include "position_distributor/position_publisher.h"
#include "position_distributor/logger.h"
#include <iostream>
#include <thread>
#include <chrono>
#include <signal.h>

using namespace position_distributor;

std::unique_ptr<PositionPublisher> g_publisher;

void signalHandler(int signal)
{
    std::cout << "\nShutting down publisher..." << std::endl;
    if (g_publisher)
    {
        g_publisher->disconnect();
    }
    exit(0);
}

int main(int argc, char *argv[])
{
    signal(SIGINT, signalHandler);

    if (argc < 2)
    {
        std::cerr << "Usage: " << argv[0] << " <exchange> [strategy_id]" << std::endl;
        std::cerr << "Example: " << argv[0] << " BINANCE STRATEGY_1" << std::endl;
        return 1;
    }

    std::string exchange = argv[1];
    std::string strategy_id = argc > 2 ? argv[2] : (exchange + "_STRATEGY");
    std::string topic = "position_update." + exchange;

    Logger::instance().setLevel(LogLevel::INFO);

    std::cout << "=== Position Publisher Test ===" << std::endl;
    std::cout << "Exchange: " << exchange << std::endl;
    std::cout << "Strategy: " << strategy_id << std::endl;
    std::cout << "Topic: " << topic << std::endl;

    try
    {
        PublisherConfig config(topic);
        g_publisher = std::make_unique<PositionPublisher>(config);

        if (!g_publisher->connect())
        {
            std::cerr << "Failed to connect publisher" << std::endl;
            return 1;
        }

        std::cout << "Publisher connected! Publishing every 2 seconds..." << std::endl;
        std::cout << "Press Ctrl+C to stop.\n"
                  << std::endl;

        std::vector<std::pair<std::string, double>> positions = {
            {"BTCUSDT", 100.0},
            {"ETHUSDT", -50.0},
            {"ADAUSDT", 25.0}};

        int iteration = 0;
        while (true)
        {
            iteration++;

            // Update positions
            for (auto &[symbol, position] : positions)
            {
                position += (iteration % 2 == 0 ? 10.0 : -5.0);
            }

            bool success = g_publisher->publishPositions(strategy_id, positions);

            std::cout << "[" << iteration << "] Published " << positions.size()
                      << " positions: " << (success ? "SUCCESS" : "FAILED")
                      << " (seq: " << g_publisher->getCurrentSequenceNumber() << ")" << std::endl;

            // Print positions
            for (const auto &[symbol, position] : positions)
            {
                std::cout << "  " << symbol << ": " << position << std::endl;
            }
            std::cout << std::endl;

            std::this_thread::sleep_for(std::chrono::seconds(2));
        }
    }
    catch (const std::exception &e)
    {
        std::cerr << "Error: " << e.what() << std::endl;
        return 1;
    }

    return 0;
}
