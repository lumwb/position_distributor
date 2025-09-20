#pragma once

#include "position.h"
#include <string>
#include <vector>
#include <memory>
#include <functional>
#include <thread>
#include <atomic>
#include <mutex>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>

namespace position_distributor
{

    class SimpleTcpServer
    {
    public:
        using ClientHandler = std::function<void(int client_fd, const std::string &data)>;

        SimpleTcpServer(uint16_t port);
        ~SimpleTcpServer();

        bool start();
        void stop();
        void setClientHandler(ClientHandler handler);

        void broadcast(const std::string &data);
        size_t getClientCount();

    private:
        void acceptClients();
        void handleClient(int client_fd);

        uint16_t port_;
        int server_fd_;
        std::atomic<bool> running_;
        std::thread server_thread_;

        std::vector<int> clients_;
        std::mutex clients_mutex_;

        ClientHandler client_handler_;
    };

    class SimpleTcpClient
    {
    public:
        using DataHandler = std::function<void(const std::string &data)>;

        SimpleTcpClient(const std::string &host, uint16_t port);
        ~SimpleTcpClient();

        bool connect();
        void disconnect();
        bool send(const std::string &data);

        void setDataHandler(DataHandler handler);
        bool isConnected() const { return connected_; }

    private:
        void receiveLoop();

        std::string host_;
        uint16_t port_;
        int socket_fd_;
        std::atomic<bool> connected_;
        std::atomic<bool> running_;
        std::thread receive_thread_;

        DataHandler data_handler_;
    };

    // Simple message protocol
    class SimpleMessageProtocol
    {
    public:
        static std::string serializePositionUpdate(const PositionUpdate &update);
        static PositionUpdate deserializePositionUpdate(const std::string &data);

        static std::string createHeartbeat();
        static bool isHeartbeat(const std::string &data);

        static std::string createAcknowledge(uint64_t sequence_number);
        static uint64_t parseAcknowledge(const std::string &data);
    };

} // namespace position_distributor
