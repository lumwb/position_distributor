#include "position_distributor/position_encoding.h"
#include <stdexcept>
#include <algorithm>

namespace position_distributor
{

    uint32_t PositionUpdateCodec::encode(uint8_t *buffer, uint32_t buffer_size,
                                         const std::string &strategy_id,
                                         uint64_t sequence_number,
                                         const std::vector<std::pair<std::string, double>> &positions)
    {
        uint32_t required_size = calculateBufferSize(static_cast<uint32_t>(positions.size()));
        if (buffer_size < required_size)
        {
            throw std::runtime_error("Buffer too small for encoding position update");
        }

        // Create the header
        PositionUpdateSBE *header = reinterpret_cast<PositionUpdateSBE *>(buffer);
        *header = PositionUpdateSBE(strategy_id, sequence_number, static_cast<uint32_t>(positions.size()));

        // Encode positions
        SymbolPositionSBE *sbe_positions = header->getPositions();
        for (size_t i = 0; i < positions.size(); ++i)
        {
            sbe_positions[i] = SymbolPositionSBE(positions[i].first, positions[i].second);
        }

        return required_size;
    }

    bool PositionUpdateCodec::decode(const uint8_t *buffer, uint32_t buffer_size,
                                     std::string &strategy_id,
                                     uint64_t &timestamp,
                                     uint64_t &sequence_number,
                                     std::vector<std::pair<std::string, double>> &positions)
    {
        if (buffer_size < sizeof(PositionUpdateSBE))
        {
            return false;
        }

        const PositionUpdateSBE *header = reinterpret_cast<const PositionUpdateSBE *>(buffer);

        // Validate message size
        uint32_t expected_size = header->getMessageSize();
        if (buffer_size < expected_size)
        {
            return false;
        }

        // Extract header fields
        strategy_id = header->getStrategyId();
        timestamp = header->timestamp;
        sequence_number = header->sequence_number;

        // Extract positions
        positions.clear();
        positions.reserve(header->position_count);

        const SymbolPositionSBE *sbe_positions = header->getPositions();
        for (uint32_t i = 0; i < header->position_count; ++i)
        {
            positions.emplace_back(
                sbe_positions[i].getSymbol(),
                sbe_positions[i].net_position);
        }

        return true;
    }

    uint32_t PositionUpdateCodec::calculateBufferSize(uint32_t position_count)
    {
        return sizeof(PositionUpdateSBE) + (position_count * sizeof(SymbolPositionSBE));
    }

} // namespace position_distributor
