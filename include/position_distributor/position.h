#pragma once

#include <string>
#include <vector>
#include <chrono>
#include <cstdint>

namespace position_distributor
{

    struct SymbolPosition
    {
        std::string symbol;
        double net_position;

        SymbolPosition() = default;
        SymbolPosition(const std::string &sym, double pos)
            : symbol(sym), net_position(pos) {}

        bool operator==(const SymbolPosition &other) const
        {
            return symbol == other.symbol && net_position == other.net_position;
        }

        std::string toString() const;
    };

    struct PositionUpdate
    {
        std::string strategy_id;
        std::vector<SymbolPosition> positions;
        std::chrono::system_clock::time_point timestamp;
        uint64_t sequence_number;

        PositionUpdate() = default;
        PositionUpdate(const std::string &strategy,
                       const std::vector<SymbolPosition> &pos,
                       uint64_t seq_num)
            : strategy_id(strategy), positions(pos),
              timestamp(std::chrono::system_clock::now()),
              sequence_number(seq_num) {}

        std::string toString() const;
    };

    // Message types for communication
    enum class MessageType : uint8_t
    {
        POSITION_UPDATE = 1,
        HEARTBEAT = 2,
        ACKNOWLEDGE = 3,
        ERROR = 4
    };

    // Protocol message structure
    struct Message
    {
        MessageType type;
        uint32_t payload_size;
        std::vector<uint8_t> payload;

        Message() = default;
        Message(MessageType t, const std::vector<uint8_t> &data)
            : type(t), payload_size(data.size()), payload(data) {}
    };

} // namespace position_distributor
