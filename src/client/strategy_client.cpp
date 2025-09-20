#include "position_distributor/position_client.h"
#include "position_distributor/logger.h"
#include <iostream>
#include <random>
#include <thread>
#include <chrono>
#include <signal.h>

std::unique_ptr<position_distributor::PositionClient> g_client;

void signalHandler(int signal)
{
    if (g_client)
    {
        std::cout << "\nShutting down strategy client..." << std::endl;
        g_client->disconnect();
    }
    exit(0);
}

void onPositionUpdate(const position_distributor::PositionUpdate &update)
{
    std::cout << "RECEIVED: " << update.toString() << std::endl;
}

int main(int argc, char *argv[])
{
    // Set up signal handling
    signal(SIGINT, signalHandler);
    signal(SIGTERM, signalHandler);

    // Parse command line arguments
    if (argc < 2)
    {
        std::cerr << "Usage: " << argv[0] << " <strategy_id> [server_host] [server_port]" << std::endl;
        std::cerr << "Example: " << argv[0] << " BINANCE localhost 8080" << std::endl;
        return 1;
    }

    std::string strategy_id = argv[1];
    std::string server_host = argc > 2 ? argv[2] : "localhost";
    uint16_t server_port = argc > 3 ? std::stoi(argv[3]) : 8080;

    // Set log level
    position_distributor::Logger::instance().setLevel(position_distributor::LogLevel::INFO);

    std::cout << "Starting Strategy Client: " << strategy_id << std::endl;
    std::cout << "Connecting to: " << server_host << ":" << server_port << std::endl;

    try
    {
        g_client = std::make_unique<position_distributor::PositionClient>(
            strategy_id, server_host, server_port);

        // Set up position update callback
        g_client->setPositionUpdateCallback(onPositionUpdate);

        // Connect to server
        if (!g_client->connect())
        {
            std::cerr << "Failed to connect to position server" << std::endl;
            return 1;
        }

        std::cout << "Connected! Publishing random positions every 2 seconds..." << std::endl;
        std::cout << "Press Ctrl+C to stop." << std::endl;

        // Random number generator
        std::random_device rd;
        std::mt19937 gen(rd());
        std::uniform_real_distribution<> price_dist(-100.0, 100.0);
        std::uniform_int_distribution<> symbol_dist(0, 4);

        std::vector<std::string> symbols = {
            "BTCUSDT." + strategy_id.substr(0, 2),
            "ETHUSDT." + strategy_id.substr(0, 2),
            "ADAUSDT." + strategy_id.substr(0, 2),
            "DOTUSDT." + strategy_id.substr(0, 2),
            "LINKUSDT." + strategy_id.substr(0, 2)};

        // Publish random positions
        while (true)
        {
            std::this_thread::sleep_for(std::chrono::seconds(2));

            if (!g_client->isConnected())
            {
                std::cerr << "Lost connection to server" << std::endl;
                break;
            }

            // Generate random positions
            std::vector<position_distributor::SymbolPosition> positions;
            int num_positions = 1 + (gen() % 3); // 1-3 positions

            for (int i = 0; i < num_positions; ++i)
            {
                std::string symbol = symbols[symbol_dist(gen)];
                double position = price_dist(gen);
                positions.emplace_back(symbol, position);
            }

            g_client->publishPositions(positions);
        }
    }
    catch (const std::exception &e)
    {
        std::cerr << "Error: " << e.what() << std::endl;
        return 1;
    }

    return 0;
}
