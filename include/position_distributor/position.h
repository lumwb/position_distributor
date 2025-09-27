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
        uint64_t timestamp;
        uint64_t sequence_number;
        std::vector<SymbolPosition> positions;

        PositionUpdate() = default;
        PositionUpdate(const std::string &strategy, uint64_t ts, uint64_t seq_num,
                       const std::vector<SymbolPosition> &pos)
            : strategy_id(strategy), timestamp(ts), sequence_number(seq_num), positions(pos)
        {
        }

        std::string toString() const
        {
            std::string result = "PositionUpdate{strategy=" + strategy_id +
                                 ", seq=" + std::to_string(sequence_number) +
                                 ", positions=[";
            for (size_t i = 0; i < positions.size(); ++i)
            {
                if (i > 0)
                    result += ", ";
                result += positions[i].toString();
            }
            result += "]}";
            return result;
        }
    };

} // namespace position_distributor
