#include <iostream>
#include <thread>
#include <chrono>
#include "position_distributor/simple_position_server.h"
#include "position_distributor/simple_position_client.h"
#include "position_distributor/logger.h"

using namespace position_distributor;

int main()
{
    // Set up logging
    Logger::instance().setLevel(LogLevel::INFO);

    std::cout << "Starting Position Distributor Test" << std::endl;

    // Start server
    auto server = std::make_unique<SimplePositionServer>(8080);
    if (!server->start())
    {
        std::cerr << "Failed to start server" << std::endl;
        return 1;
    }

    // Wait a bit for server to start
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    // Create clients
    auto client1 = std::make_unique<SimplePositionClient>("127.0.0.1", 8080, "BINANCE");
    auto client2 = std::make_unique<SimplePositionClient>("127.0.0.1", 8080, "HUOBI");

    // Set up callbacks
    client1->setPositionUpdateCallback([](const PositionUpdate &update)
                                       { std::cout << "BINANCE received: " << update.toString() << std::endl; });

    client2->setPositionUpdateCallback([](const PositionUpdate &update)
                                       { std::cout << "HUOBI received: " << update.toString() << std::endl; });

    // Connect clients
    if (!client1->connect() || !client2->connect())
    {
        std::cerr << "Failed to connect clients" << std::endl;
        return 1;
    }

    std::cout << "Both clients connected!" << std::endl;

    // Publish some test positions
    for (int i = 0; i < 5; ++i)
    {
        // BINANCE publishes
        std::vector<SymbolPosition> binance_positions = {
            {"BTCUSDT.BN", 1.0 + i},
            {"ETHUSDT.BN", -0.5 + i * 0.1}};
        client1->sendPositionUpdate(binance_positions);

        // HUOBI publishes
        std::vector<SymbolPosition> huobi_positions = {
            {"BTCUSDT.HB", 2.0 + i},
            {"ADAUSDT.HB", -1.0 + i * 0.2}};
        client2->sendPositionUpdate(huobi_positions);

        std::this_thread::sleep_for(std::chrono::milliseconds(500));
    }

    std::cout << "Test completed!" << std::endl;

    // Cleanup
    client1->disconnect();
    client2->disconnect();
    server->stop();

    return 0;
}
