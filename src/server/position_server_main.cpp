#include "position_distributor/position_server.h"
#include "position_distributor/logger.h"
#include <iostream>
#include <signal.h>

std::unique_ptr<position_distributor::PositionServer> g_server;

void signalHandler(int signal)
{
    if (g_server)
    {
        std::cout << "\nShutting down position server..." << std::endl;
        g_server->stop();
    }
    exit(0);
}

int main(int argc, char *argv[])
{
    // Set up signal handling
    signal(SIGINT, signalHandler);
    signal(SIGTERM, signalHandler);

    // Parse command line arguments
    uint16_t port = 8080;
    if (argc > 1)
    {
        try
        {
            port = std::stoi(argv[1]);
        }
        catch (const std::exception &e)
        {
            std::cerr << "Invalid port number: " << argv[1] << std::endl;
            return 1;
        }
    }

    // Set log level
    position_distributor::Logger::instance().setLevel(position_distributor::LogLevel::INFO);

    std::cout << "Starting Position Distributor Server on port " << port << std::endl;

    try
    {
        g_server = std::make_unique<position_distributor::PositionServer>(port);
        g_server->start();

        // Keep the server running
        std::cout << "Server is running. Press Ctrl+C to stop." << std::endl;
        while (true)
        {
            std::this_thread::sleep_for(std::chrono::seconds(1));

            // Print statistics every 10 seconds
            static int counter = 0;
            if (++counter >= 10)
            {
                std::cout << "Clients: " << g_server->getClientCount()
                          << ", Messages: " << g_server->getTotalMessages() << std::endl;
                counter = 0;
            }
        }
    }
    catch (const std::exception &e)
    {
        std::cerr << "Error: " << e.what() << std::endl;
        return 1;
    }

    return 0;
}
