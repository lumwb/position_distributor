#include "position_distributor/message.h"
#include "position_distributor/logger.h"
#include <cstring>
#include <stdexcept>

namespace position_distributor
{

    std::vector<uint8_t> MessageSerializer::serialize(const PositionUpdate &update)
    {
        std::vector<uint8_t> data;

        // Strategy ID length + data
        uint32_t strategy_len = update.strategy_id.length();
        data.insert(data.end(), reinterpret_cast<const uint8_t *>(&strategy_len),
                    reinterpret_cast<const uint8_t *>(&strategy_len) + sizeof(strategy_len));
        data.insert(data.end(), update.strategy_id.begin(), update.strategy_id.end());

        // Timestamp
        auto timestamp_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                                update.timestamp.time_since_epoch())
                                .count();
        data.insert(data.end(), reinterpret_cast<const uint8_t *>(&timestamp_ms),
                    reinterpret_cast<const uint8_t *>(&timestamp_ms) + sizeof(timestamp_ms));

        // Sequence number
        data.insert(data.end(), reinterpret_cast<const uint8_t *>(&update.sequence_number),
                    reinterpret_cast<const uint8_t *>(&update.sequence_number) + sizeof(update.sequence_number));

        // Number of positions
        uint32_t pos_count = update.positions.size();
        data.insert(data.end(), reinterpret_cast<const uint8_t *>(&pos_count),
                    reinterpret_cast<const uint8_t *>(&pos_count) + sizeof(pos_count));

        // Each position
        for (const auto &pos : update.positions)
        {
            // Symbol length + data
            uint32_t symbol_len = pos.symbol.length();
            data.insert(data.end(), reinterpret_cast<const uint8_t *>(&symbol_len),
                        reinterpret_cast<const uint8_t *>(&symbol_len) + sizeof(symbol_len));
            data.insert(data.end(), pos.symbol.begin(), pos.symbol.end());

            // Position value
            data.insert(data.end(), reinterpret_cast<const uint8_t *>(&pos.net_position),
                        reinterpret_cast<const uint8_t *>(&pos.net_position) + sizeof(pos.net_position));
        }

        return data;
    }

    PositionUpdate MessageSerializer::deserialize(const std::vector<uint8_t> &data)
    {
        if (data.size() < 16)
        { // Minimum size check
            throw std::runtime_error("Invalid position update data");
        }

        size_t offset = 0;
        PositionUpdate update;

        // Strategy ID
        uint32_t strategy_len;
        std::memcpy(&strategy_len, data.data() + offset, sizeof(strategy_len));
        offset += sizeof(strategy_len);

        if (offset + strategy_len > data.size())
        {
            throw std::runtime_error("Invalid strategy ID length");
        }

        update.strategy_id.assign(data.begin() + offset, data.begin() + offset + strategy_len);
        offset += strategy_len;

        // Timestamp
        int64_t timestamp_ms;
        std::memcpy(&timestamp_ms, data.data() + offset, sizeof(timestamp_ms));
        offset += sizeof(timestamp_ms);
        update.timestamp = std::chrono::system_clock::time_point(
            std::chrono::milliseconds(timestamp_ms));

        // Sequence number
        std::memcpy(&update.sequence_number, data.data() + offset, sizeof(update.sequence_number));
        offset += sizeof(update.sequence_number);

        // Number of positions
        uint32_t pos_count;
        std::memcpy(&pos_count, data.data() + offset, sizeof(pos_count));
        offset += sizeof(pos_count);

        // Each position
        update.positions.reserve(pos_count);
        for (uint32_t i = 0; i < pos_count; ++i)
        {
            SymbolPosition pos;

            // Symbol length + data
            uint32_t symbol_len;
            if (offset + sizeof(symbol_len) > data.size())
            {
                throw std::runtime_error("Invalid symbol length");
            }
            std::memcpy(&symbol_len, data.data() + offset, sizeof(symbol_len));
            offset += sizeof(symbol_len);

            if (offset + symbol_len + sizeof(pos.net_position) > data.size())
            {
                throw std::runtime_error("Invalid position data");
            }

            pos.symbol.assign(data.begin() + offset, data.begin() + offset + symbol_len);
            offset += symbol_len;

            // Position value
            std::memcpy(&pos.net_position, data.data() + offset, sizeof(pos.net_position));
            offset += sizeof(pos.net_position);

            update.positions.push_back(pos);
        }

        return update;
    }

    std::vector<uint8_t> MessageSerializer::serialize(const Message &message)
    {
        std::vector<uint8_t> data;

        // Message type
        data.push_back(static_cast<uint8_t>(message.type));

        // Payload size
        data.insert(data.end(), reinterpret_cast<const uint8_t *>(&message.payload_size),
                    reinterpret_cast<const uint8_t *>(&message.payload_size) + sizeof(message.payload_size));

        // Payload
        data.insert(data.end(), message.payload.begin(), message.payload.end());

        return data;
    }

    Message MessageSerializer::deserialize(const std::vector<uint8_t> &data)
    {
        if (data.size() < 5)
        { // type(1) + size(4)
            throw std::runtime_error("Invalid message data");
        }

        Message message;
        message.type = static_cast<MessageType>(data[0]);

        std::memcpy(&message.payload_size, data.data() + 1, sizeof(message.payload_size));

        if (data.size() < 5 + message.payload_size)
        {
            throw std::runtime_error("Incomplete message payload");
        }

        message.payload.assign(data.begin() + 5, data.begin() + 5 + message.payload_size);

        return message;
    }

    std::vector<uint8_t> MessageSerializer::createHeartbeat()
    {
        return std::vector<uint8_t>(); // Empty payload for heartbeat
    }

    std::vector<uint8_t> MessageSerializer::createAcknowledge(uint64_t sequence_number)
    {
        std::vector<uint8_t> data;
        data.insert(data.end(), reinterpret_cast<const uint8_t *>(&sequence_number),
                    reinterpret_cast<const uint8_t *>(&sequence_number) + sizeof(sequence_number));
        return data;
    }

    std::vector<uint8_t> MessageSerializer::createError(const std::string &error_msg)
    {
        std::vector<uint8_t> data;
        uint32_t msg_len = error_msg.length();
        data.insert(data.end(), reinterpret_cast<const uint8_t *>(&msg_len),
                    reinterpret_cast<const uint8_t *>(&msg_len) + sizeof(msg_len));
        data.insert(data.end(), error_msg.begin(), error_msg.end());
        return data;
    }

    TcpConnection::TcpConnection(boost::asio::io_context &io_context,
                                 std::shared_ptr<MessageHandler> handler)
        : socket_(io_context), handler_(handler)
    {
    }

    void TcpConnection::start()
    {
        readHeader();
    }

    void TcpConnection::readHeader()
    {
        auto self = shared_from_this();
        boost::asio::async_read(socket_, boost::asio::buffer(header_buffer_),
                                [this, self](boost::system::error_code ec, std::size_t length)
                                {
                                    if (!ec)
                                    {
                                        readPayload();
                                    }
                                    else
                                    {
                                        LOG_ERROR("Error reading header: " + ec.message());
                                    }
                                });
    }

    void TcpConnection::readPayload()
    {
        // Extract payload size from header
        uint32_t payload_size;
        std::memcpy(&payload_size, header_buffer_.data() + 1, sizeof(payload_size));

        payload_buffer_.resize(payload_size);

        auto self = shared_from_this();
        boost::asio::async_read(socket_, boost::asio::buffer(payload_buffer_),
                                [this, self](boost::system::error_code ec, std::size_t length)
                                {
                                    if (!ec)
                                    {
                                        Message message;
                                        message.type = static_cast<MessageType>(header_buffer_[0]);
                                        message.payload_size = payload_size;
                                        message.payload = payload_buffer_;
                                        handleMessage(message);
                                        readHeader(); // Continue reading
                                    }
                                    else
                                    {
                                        LOG_ERROR("Error reading payload: " + ec.message());
                                    }
                                });
    }

    void TcpConnection::handleMessage(const Message &message)
    {
        try
        {
            switch (message.type)
            {
            case MessageType::POSITION_UPDATE:
            {
                auto update = MessageSerializer::deserialize(message.payload);
                handler_->onPositionUpdate(update);
                break;
            }
            case MessageType::HEARTBEAT:
                handler_->onHeartbeat();
                break;
            case MessageType::ACKNOWLEDGE:
            {
                uint64_t seq_num;
                std::memcpy(&seq_num, message.payload.data(), sizeof(seq_num));
                handler_->onAcknowledge(seq_num);
                break;
            }
            case MessageType::ERROR:
            {
                uint32_t msg_len;
                std::memcpy(&msg_len, message.payload.data(), sizeof(msg_len));
                std::string error_msg(message.payload.begin() + sizeof(msg_len),
                                      message.payload.begin() + sizeof(msg_len) + msg_len);
                handler_->onError(error_msg);
                break;
            }
            default:
                LOG_WARN("Unknown message type: " + std::to_string(static_cast<int>(message.type)));
            }
        }
        catch (const std::exception &e)
        {
            LOG_ERROR("Error handling message: " + std::string(e.what()));
        }
    }

    void TcpConnection::sendMessage(const Message &message)
    {
        auto data = MessageSerializer::serialize(message);

        auto self = shared_from_this();
        boost::asio::async_write(socket_, boost::asio::buffer(data),
                                 [this, self](boost::system::error_code ec, std::size_t length)
                                 {
                                     if (ec)
                                     {
                                         LOG_ERROR("Error sending message: " + ec.message());
                                     }
                                 });
    }

    void TcpConnection::close()
    {
        boost::system::error_code ec;
        socket_.close(ec);
    }

} // namespace position_distributor
