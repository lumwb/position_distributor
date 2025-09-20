#include "position_distributor/simple_position_server.h"
#include "position_distributor/logger.h"
#include <iostream>
#include <signal.h>
#include <unistd.h>

std::unique_ptr<position_distributor::SimplePositionServer> g_server;

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
    // Set up signal handlers
    signal(SIGINT, signalHandler);
    signal(SIGTERM, signalHandler);

    uint16_t port = 8080;
    if (argc > 1)
    {
        port = static_cast<uint16_t>(std::atoi(argv[1]));
    }

    std::cout << "Starting Position Distributor Server on port " << port << std::endl;

    g_server = std::make_unique<position_distributor::SimplePositionServer>(port);

    if (!g_server->start())
    {
        std::cerr << "Failed to start position server" << std::endl;
        return 1;
    }

    std::cout << "Position server is running. Press Ctrl+C to stop." << std::endl;

    // Keep the server running
    while (true)
    {
        std::this_thread::sleep_for(std::chrono::seconds(1));

        // Print statistics every 10 seconds
        static int counter = 0;
        if (++counter >= 10)
        {
            counter = 0;
            std::cout << "Server stats - Clients: " << g_server->getClientCount()
                      << ", Updates: " << g_server->getTotalUpdates() << std::endl;
        }
    }

    return 0;
}
