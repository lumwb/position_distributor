#include "position_distributor/position_client.h"
#include "position_distributor/logger.h"
#include <iostream>
#include <thread>
#include <chrono>

using namespace position_distributor;

int main()
{
    // Set log level
    Logger::instance().setLevel(LogLevel::INFO);

    std::cout << "=== Position Distributor Demo ===" << std::endl;

    try
    {
        // Create BINANCE client (publishes BINANCE, subscribes to COINBASE)
        PositionClientConfig binance_config("BINANCE");
        binance_config.subscribed_exchanges = {"COINBASE"};

        PositionClient binance_client(binance_config);

        // Set up callback to receive position updates
        binance_client.setPositionUpdateCallback([](const PositionUpdate &update)
                                                 { std::cout << "BINANCE CLIENT RECEIVED: " << update.toString() << std::endl; });

        // Connect client
        if (!binance_client.connect())
        {
            std::cerr << "Failed to connect BINANCE client" << std::endl;
            return 1;
        }

        std::cout << "BINANCE client connected!" << std::endl;

        // Publish some test positions
        std::vector<SymbolPosition> positions = {
            {"BTCUSDT", 100.5},
            {"ETHUSDT", -50.25},
            {"ADAUSDT", 200.0}};

        for (int i = 0; i < 5; ++i)
        {
            std::cout << "\n--- Publishing iteration " << (i + 1) << " ---" << std::endl;

            bool success = binance_client.publishPositions("BINANCE_STRATEGY_1", positions);
            std::cout << "Published positions: " << (success ? "SUCCESS" : "FAILED") << std::endl;

            // Update positions for next iteration
            for (auto &pos : positions)
            {
                pos.net_position += 10.0;
            }

            std::this_thread::sleep_for(std::chrono::seconds(2));
        }

        // Print statistics
        std::cout << "\n=== Final Statistics ===" << std::endl;
        std::cout << "Published: " << binance_client.getPublishedCount() << std::endl;
        std::cout << "Failed: " << binance_client.getPublishFailedCount() << std::endl;
        std::cout << "Received: " << binance_client.getReceivedCount() << std::endl;
        std::cout << "Current Sequence: " << binance_client.getCurrentSequenceNumber() << std::endl;

        std::cout << "\nDemo completed successfully!" << std::endl;

        // Manual cleanup to avoid mutex issues
        binance_client.disconnect();
    }
    catch (const std::exception &e)
    {
        std::cerr << "Error: " << e.what() << std::endl;
        return 1;
    }

    return 0;
}
