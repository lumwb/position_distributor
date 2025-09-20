#include "position_distributor/position.h"
#include <sstream>

namespace position_distributor
{

    std::string SymbolPosition::toString() const
    {
        std::ostringstream oss;
        oss << symbol << ":" << net_position;
        return oss.str();
    }

    std::string PositionUpdate::toString() const
    {
        std::ostringstream oss;
        oss << "Strategy[" << strategy_id << "] Seq[" << sequence_number << "] ";
        for (const auto &pos : positions)
        {
            oss << pos.toString() << " ";
        }
        return oss.str();
    }

} // namespace position_distributor
