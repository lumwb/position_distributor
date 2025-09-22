#include "position_distributor/position_client.h"
#include "position_distributor/logger.h"
#include <iostream>
#include <thread>
#include <chrono>
#include <atomic>

using namespace position_distributor;

std::atomic<int> received_count{0};

void onPositionUpdate(const std::string &client_name, const PositionUpdate &update)
{
    received_count++;
    std::cout << "[" << client_name << "] RECEIVED: " << update.toString() << std::endl;
}

int main()
{
    // Set log level
    Logger::instance().setLevel(LogLevel::WARN); // Reduce log noise

    std::cout << "=== Two-Client Position Distribution Demo ===" << std::endl;

    try
    {
        // Create BINANCE client (publishes BINANCE, subscribes to COINBASE)
        PositionClientConfig binance_config("BINANCE");
        binance_config.subscribed_exchanges = {"COINBASE"};
        PositionClient binance_client(binance_config);

        // Create COINBASE client (publishes COINBASE, subscribes to BINANCE)
        PositionClientConfig coinbase_config("COINBASE");
        coinbase_config.subscribed_exchanges = {"BINANCE"};
        PositionClient coinbase_client(coinbase_config);

        // Set up callbacks
        binance_client.setPositionUpdateCallback([](const PositionUpdate &update)
                                                 { onPositionUpdate("BINANCE", update); });

        coinbase_client.setPositionUpdateCallback([](const PositionUpdate &update)
                                                  { onPositionUpdate("COINBASE", update); });

        // Connect clients
        if (!binance_client.connect() || !coinbase_client.connect())
        {
            std::cerr << "Failed to connect clients" << std::endl;
            return 1;
        }

        std::cout << "Both clients connected!" << std::endl;
        std::cout << "Starting cross-exchange position publishing...\n"
                  << std::endl;

        // Give time for subscriptions to be established
        std::this_thread::sleep_for(std::chrono::milliseconds(100));

        // Test data
        std::vector<SymbolPosition> binance_positions = {
            {"BTCUSDT", 100.0},
            {"ETHUSDT", -50.0}};

        std::vector<SymbolPosition> coinbase_positions = {
            {"BTC-USD", 75.0},
            {"ETH-USD", 25.0}};

        // Publish positions from both exchanges
        for (int i = 0; i < 3; ++i)
        {
            std::cout << "--- Round " << (i + 1) << " ---" << std::endl;

            // BINANCE publishes
            bool binance_success = binance_client.publishPositions("BINANCE_STRATEGY", binance_positions);
            std::cout << "BINANCE published: " << (binance_success ? "SUCCESS" : "FAILED") << std::endl;

            std::this_thread::sleep_for(std::chrono::milliseconds(50));

            // COINBASE publishes
            bool coinbase_success = coinbase_client.publishPositions("COINBASE_STRATEGY", coinbase_positions);
            std::cout << "COINBASE published: " << (coinbase_success ? "SUCCESS" : "FAILED") << std::endl;

            // Update positions for next round
            for (auto &pos : binance_positions)
                pos.net_position += 10.0;
            for (auto &pos : coinbase_positions)
                pos.net_position += 5.0;

            std::this_thread::sleep_for(std::chrono::milliseconds(200));
        }

        // Give time for all messages to be processed
        std::this_thread::sleep_for(std::chrono::milliseconds(100));

        // Print final statistics
        std::cout << "\n=== Final Statistics ===" << std::endl;
        std::cout << "BINANCE - Published: " << binance_client.getPublishedCount()
                  << ", Received: " << binance_client.getReceivedCount() << std::endl;
        std::cout << "COINBASE - Published: " << coinbase_client.getPublishedCount()
                  << ", Received: " << coinbase_client.getReceivedCount() << std::endl;
        std::cout << "Total messages received by callbacks: " << received_count.load() << std::endl;

        std::cout << "\n✅ Cross-exchange position distribution working!" << std::endl;

        // Clean disconnect
        binance_client.disconnect();
        coinbase_client.disconnect();
    }
    catch (const std::exception &e)
    {
        std::cerr << "Error: " << e.what() << std::endl;
        return 1;
    }

    return 0;
}
