#include "position_distributor/simple_position_client.h"
#include "position_distributor/logger.h"
#include <iostream>
#include <signal.h>
#include <unistd.h>
#include <random>
#include <chrono>

std::unique_ptr<position_distributor::SimplePositionClient> g_client;

void signalHandler(int signal)
{
    if (g_client)
    {
        std::cout << "\nShutting down position client..." << std::endl;
        g_client->disconnect();
    }
    exit(0);
}

int main(int argc, char *argv[])
{
    // Set up signal handlers
    signal(SIGINT, signalHandler);
    signal(SIGTERM, signalHandler);

    if (argc < 2)
    {
        std::cerr << "Usage: " << argv[0] << " <strategy_id> [server_host] [server_port]" << std::endl;
        return 1;
    }

    std::string strategy_id = argv[1];
    std::string server_host = (argc > 2) ? argv[2] : "localhost";
    uint16_t server_port = (argc > 3) ? static_cast<uint16_t>(std::atoi(argv[3])) : 8080;

    std::cout << "Starting Position Client for strategy: " << strategy_id << std::endl;
    std::cout << "Connecting to server: " << server_host << ":" << server_port << std::endl;

    g_client = std::make_unique<position_distributor::SimplePositionClient>(
        server_host, server_port, strategy_id);

    // Set up callbacks
    g_client->setPositionUpdateCallback([](const position_distributor::PositionUpdate &update)
                                        {
        std::cout << "Received position update from " << update.strategy_id 
                  << " (seq: " << update.sequence_number << ")" << std::endl;
        for (const auto& pos : update.positions) {
            std::cout << "  " << pos.symbol << ": " << pos.net_position << std::endl;
        } });

    g_client->setConnectionCallback([](bool connected)
                                    {
        if (connected) {
            std::cout << "Connected to position server" << std::endl;
        } else {
            std::cout << "Disconnected from position server" << std::endl;
        } });

    if (!g_client->connect())
    {
        std::cerr << "Failed to connect to position server" << std::endl;
        return 1;
    }

    std::cout << "Position client is running. Press Ctrl+C to stop." << std::endl;

    // Simulate position updates
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_real_distribution<> price_dist(-100.0, 100.0);
    std::uniform_int_distribution<> symbol_dist(0, 2);

    std::vector<std::string> symbols = {"AAPL", "GOOGL", "MSFT"};

    int update_counter = 0;
    while (true)
    {
        std::this_thread::sleep_for(std::chrono::seconds(2));

        if (!g_client->isConnected())
        {
            std::cout << "Lost connection to server, attempting to reconnect..." << std::endl;
            if (!g_client->connect())
            {
                std::cout << "Failed to reconnect, retrying in 5 seconds..." << std::endl;
                std::this_thread::sleep_for(std::chrono::seconds(5));
                continue;
            }
        }

        // Generate random position update
        std::vector<position_distributor::SymbolPosition> positions;
        for (const auto &symbol : symbols)
        {
            double position = price_dist(gen);
            if (std::abs(position) > 0.1)
            { // Only include non-zero positions
                positions.emplace_back(symbol, position);
            }
        }

        if (!positions.empty())
        {
            g_client->sendPositionUpdate(positions);
            update_counter++;

            if (update_counter % 5 == 0)
            {
                std::cout << "Sent " << update_counter << " position updates" << std::endl;
            }
        }
    }

    return 0;
}
