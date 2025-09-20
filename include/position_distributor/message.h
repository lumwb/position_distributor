#pragma once

#include "position.h"
#include <boost/asio.hpp>
#include <memory>

namespace position_distributor
{

    class MessageSerializer
    {
    public:
        static std::vector<uint8_t> serialize(const PositionUpdate &update);
        static PositionUpdate deserialize(const std::vector<uint8_t> &data);

        static std::vector<uint8_t> serialize(const Message &message);
        static Message deserialize(const std::vector<uint8_t> &data);

        static std::vector<uint8_t> createHeartbeat();
        static std::vector<uint8_t> createAcknowledge(uint64_t sequence_number);
        static std::vector<uint8_t> createError(const std::string &error_msg);
    };

    class MessageHandler
    {
    public:
        virtual ~MessageHandler() = default;
        virtual void onPositionUpdate(const PositionUpdate &update) = 0;
        virtual void onHeartbeat() = 0;
        virtual void onAcknowledge(uint64_t sequence_number) = 0;
        virtual void onError(const std::string &error) = 0;
    };

    // TCP connection wrapper
    class TcpConnection : public std::enable_shared_from_this<TcpConnection>
    {
    public:
        using tcp = boost::asio::ip::tcp;

        TcpConnection(boost::asio::io_context &io_context,
                      std::shared_ptr<MessageHandler> handler);

        tcp::socket &socket() { return socket_; }

        void start();
        void sendMessage(const Message &message);
        void close();

    private:
        void readHeader();
        void readPayload();
        void handleMessage(const Message &message);

        tcp::socket socket_;
        std::shared_ptr<MessageHandler> handler_;
        std::array<uint8_t, 5> header_buffer_; // type(1) + size(4)
        std::vector<uint8_t> payload_buffer_;
    };

} // namespace position_distributor
