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

} // namespace position_distributor
