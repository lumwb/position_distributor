#include "position_distributor/simple_network.h"
#include "position_distributor/logger.h"
#include <sstream>
#include <cstring>
#include <algorithm>

namespace position_distributor
{

    // SimpleTcpServer implementation
    SimpleTcpServer::SimpleTcpServer(uint16_t port)
        : port_(port), server_fd_(-1), running_(false)
    {
    }

    SimpleTcpServer::~SimpleTcpServer()
    {
        stop();
    }

    bool SimpleTcpServer::start()
    {
        server_fd_ = socket(AF_INET, SOCK_STREAM, 0);
        if (server_fd_ < 0)
        {
            logError("Failed to create socket");
            return false;
        }

        int opt = 1;
        if (setsockopt(server_fd_, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt)) < 0)
        {
            logError("Failed to set socket options");
            close(server_fd_);
            return false;
        }

        sockaddr_in address;
        address.sin_family = AF_INET;
        address.sin_addr.s_addr = INADDR_ANY;
        address.sin_port = htons(port_);

        if (bind(server_fd_, (struct sockaddr *)&address, sizeof(address)) < 0)
        {
            logError("Failed to bind socket to port " + std::to_string(port_));
            close(server_fd_);
            return false;
        }

        if (listen(server_fd_, 10) < 0)
        {
            logError("Failed to listen on socket");
            close(server_fd_);
            return false;
        }

        running_ = true;
        server_thread_ = std::thread(&SimpleTcpServer::acceptClients, this);

        logInfo("TCP server started on port " + std::to_string(port_));
        return true;
    }

    void SimpleTcpServer::stop()
    {
        if (running_)
        {
            running_ = false;
            if (server_fd_ >= 0)
            {
                close(server_fd_);
                server_fd_ = -1;
            }
            if (server_thread_.joinable())
            {
                server_thread_.join();
            }

            std::lock_guard<std::mutex> lock(clients_mutex_);
            for (int client_fd : clients_)
            {
                close(client_fd);
            }
            clients_.clear();

            logInfo("TCP server stopped");
        }
    }

    void SimpleTcpServer::setClientHandler(ClientHandler handler)
    {
        client_handler_ = handler;
    }

    void SimpleTcpServer::broadcast(const std::string &data)
    {
        std::lock_guard<std::mutex> lock(clients_mutex_);
        auto it = clients_.begin();
        while (it != clients_.end())
        {
            if (send(*it, data.c_str(), data.length(), 0) < 0)
            {
                logWarning("Failed to send data to client, removing from list");
                close(*it);
                it = clients_.erase(it);
            }
            else
            {
                ++it;
            }
        }
    }

    size_t SimpleTcpServer::getClientCount()
    {
        std::lock_guard<std::mutex> lock(clients_mutex_);
        return clients_.size();
    }

    void SimpleTcpServer::acceptClients()
    {
        while (running_)
        {
            sockaddr_in client_address;
            socklen_t client_len = sizeof(client_address);

            int client_fd = accept(server_fd_, (struct sockaddr *)&client_address, &client_len);
            if (client_fd < 0)
            {
                if (running_)
                {
                    logError("Failed to accept client connection");
                }
                continue;
            }

            logInfo("New client connected");

            {
                std::lock_guard<std::mutex> lock(clients_mutex_);
                clients_.push_back(client_fd);
            }

            std::thread client_thread(&SimpleTcpServer::handleClient, this, client_fd);
            client_thread.detach();
        }
    }

    void SimpleTcpServer::handleClient(int client_fd)
    {
        char buffer[4096];
        std::string message_buffer;

        while (running_)
        {
            ssize_t bytes_received = recv(client_fd, buffer, sizeof(buffer) - 1, 0);
            if (bytes_received <= 0)
            {
                logInfo("Client disconnected");
                break;
            }

            buffer[bytes_received] = '\0';
            message_buffer += buffer;

            // Process complete messages (assuming newline as delimiter)
            size_t pos = 0;
            while ((pos = message_buffer.find('\n')) != std::string::npos)
            {
                std::string message = message_buffer.substr(0, pos);
                message_buffer.erase(0, pos + 1);

                if (client_handler_)
                {
                    client_handler_(client_fd, message);
                }
            }
        }

        close(client_fd);
        {
            std::lock_guard<std::mutex> lock(clients_mutex_);
            clients_.erase(std::remove(clients_.begin(), clients_.end(), client_fd), clients_.end());
        }
    }

    // SimpleTcpClient implementation
    SimpleTcpClient::SimpleTcpClient(const std::string &host, uint16_t port)
        : host_(host), port_(port), socket_fd_(-1), connected_(false), running_(false)
    {
    }

    SimpleTcpClient::~SimpleTcpClient()
    {
        disconnect();
    }

    bool SimpleTcpClient::connect()
    {
        socket_fd_ = socket(AF_INET, SOCK_STREAM, 0);
        if (socket_fd_ < 0)
        {
            logError("Failed to create client socket");
            return false;
        }

        sockaddr_in server_address;
        server_address.sin_family = AF_INET;
        server_address.sin_port = htons(port_);

        if (inet_pton(AF_INET, host_.c_str(), &server_address.sin_addr) <= 0)
        {
            logError("Invalid server address: " + host_);
            close(socket_fd_);
            return false;
        }

        if (::connect(socket_fd_, (struct sockaddr *)&server_address, sizeof(server_address)) < 0)
        {
            logError("Failed to connect to server " + host_ + ":" + std::to_string(port_));
            close(socket_fd_);
            return false;
        }

        connected_ = true;
        running_ = true;
        receive_thread_ = std::thread(&SimpleTcpClient::receiveLoop, this);

        logInfo("Connected to server " + host_ + ":" + std::to_string(port_));
        return true;
    }

    void SimpleTcpClient::disconnect()
    {
        if (connected_)
        {
            running_ = false;
            connected_ = false;

            if (socket_fd_ >= 0)
            {
                close(socket_fd_);
                socket_fd_ = -1;
            }

            if (receive_thread_.joinable())
            {
                receive_thread_.join();
            }

            logInfo("Disconnected from server");
        }
    }

    bool SimpleTcpClient::send(const std::string &data)
    {
        if (!connected_)
        {
            return false;
        }

        std::string message = data + "\n";
        ssize_t bytes_sent = ::send(socket_fd_, message.c_str(), message.length(), 0);
        return bytes_sent == static_cast<ssize_t>(message.length());
    }

    void SimpleTcpClient::setDataHandler(DataHandler handler)
    {
        data_handler_ = handler;
    }

    void SimpleTcpClient::receiveLoop()
    {
        char buffer[4096];
        std::string message_buffer;

        while (running_ && connected_)
        {
            ssize_t bytes_received = recv(socket_fd_, buffer, sizeof(buffer) - 1, 0);
            if (bytes_received <= 0)
            {
                logInfo("Server disconnected");
                connected_ = false;
                break;
            }

            buffer[bytes_received] = '\0';
            message_buffer += buffer;

            // Process complete messages
            size_t pos = 0;
            while ((pos = message_buffer.find('\n')) != std::string::npos)
            {
                std::string message = message_buffer.substr(0, pos);
                message_buffer.erase(0, pos + 1);

                if (data_handler_)
                {
                    data_handler_(message);
                }
            }
        }
    }

    // SimpleMessageProtocol implementation
    std::string SimpleMessageProtocol::serializePositionUpdate(const PositionUpdate &update)
    {
        std::ostringstream oss;
        oss << "POS_UPDATE|" << update.strategy_id << "|" << update.sequence_number << "|";

        for (size_t i = 0; i < update.positions.size(); ++i)
        {
            if (i > 0)
                oss << ",";
            oss << update.positions[i].symbol << ":" << update.positions[i].net_position;
        }

        return oss.str();
    }

    PositionUpdate SimpleMessageProtocol::deserializePositionUpdate(const std::string &data)
    {
        PositionUpdate update;

        std::istringstream iss(data);
        std::string token;
        std::vector<std::string> tokens;

        while (std::getline(iss, token, '|'))
        {
            tokens.push_back(token);
        }

        if (tokens.size() >= 3 && tokens[0] == "POS_UPDATE")
        {
            update.strategy_id = tokens[1];
            update.sequence_number = std::stoull(tokens[2]);

            if (tokens.size() > 3 && !tokens[3].empty())
            {
                std::istringstream pos_stream(tokens[3]);
                std::string pos_token;

                while (std::getline(pos_stream, pos_token, ','))
                {
                    size_t colon_pos = pos_token.find(':');
                    if (colon_pos != std::string::npos)
                    {
                        std::string symbol = pos_token.substr(0, colon_pos);
                        double position = std::stod(pos_token.substr(colon_pos + 1));
                        update.positions.emplace_back(symbol, position);
                    }
                }
            }
        }

        return update;
    }

    std::string SimpleMessageProtocol::createHeartbeat()
    {
        return "HEARTBEAT|" + std::to_string(std::chrono::duration_cast<std::chrono::milliseconds>(
                                                 std::chrono::system_clock::now().time_since_epoch())
                                                 .count());
    }

    bool SimpleMessageProtocol::isHeartbeat(const std::string &data)
    {
        return data.substr(0, 10) == "HEARTBEAT|";
    }

    std::string SimpleMessageProtocol::createAcknowledge(uint64_t sequence_number)
    {
        return "ACK|" + std::to_string(sequence_number);
    }

    uint64_t SimpleMessageProtocol::parseAcknowledge(const std::string &data)
    {
        if (data.substr(0, 4) == "ACK|")
        {
            return std::stoull(data.substr(4));
        }
        return 0;
    }

} // namespace position_distributor
