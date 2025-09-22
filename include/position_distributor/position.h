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

} // namespace position_distributor
