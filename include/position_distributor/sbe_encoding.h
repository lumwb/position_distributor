#pragma once

#include <cstdint>
#include <cstring>
#include <string>
#include <vector>
#include <chrono>

namespace position_distributor
{

// SBE-style fixed layout structures for zero-copy serialization
#pragma pack(push, 1)

    struct SymbolPositionSBE
    {
        char symbol[16];     // Fixed-size symbol string
        double net_position; // Position value

        SymbolPositionSBE() = default;
        SymbolPositionSBE(const std::string &sym, double pos) : net_position(pos)
        {
            std::memset(symbol, 0, sizeof(symbol));
            std::strncpy(symbol, sym.c_str(), sizeof(symbol) - 1);
        }

        std::string getSymbol() const
        {
            return std::string(symbol, strnlen(symbol, sizeof(symbol)));
        }
    };

    struct PositionUpdateSBE
    {
        char strategy_id[32];     // Fixed-size strategy ID
        uint64_t timestamp;       // Timestamp in milliseconds since epoch
        uint64_t sequence_number; // Sequence number for ordering
        uint32_t position_count;  // Number of positions in this update
        // SymbolPositionSBE positions[position_count] follows immediately after

        PositionUpdateSBE() = default;
        PositionUpdateSBE(const std::string &strategy, uint64_t seq_num, uint32_t pos_count)
            : sequence_number(seq_num), position_count(pos_count)
        {
            std::memset(strategy_id, 0, sizeof(strategy_id));
            std::strncpy(strategy_id, strategy.c_str(), sizeof(strategy_id) - 1);
            timestamp = std::chrono::duration_cast<std::chrono::milliseconds>(
                            std::chrono::system_clock::now().time_since_epoch())
                            .count();
        }

        std::string getStrategyId() const
        {
            return std::string(strategy_id, strnlen(strategy_id, sizeof(strategy_id)));
        }

        // Get pointer to positions array (immediately after this struct)
        SymbolPositionSBE *getPositions()
        {
            return reinterpret_cast<SymbolPositionSBE *>(
                reinterpret_cast<uint8_t *>(this) + sizeof(PositionUpdateSBE));
        }

        const SymbolPositionSBE *getPositions() const
        {
            return reinterpret_cast<const SymbolPositionSBE *>(
                reinterpret_cast<const uint8_t *>(this) + sizeof(PositionUpdateSBE));
        }

        // Calculate total message size
        static uint32_t calculateMessageSize(uint32_t position_count)
        {
            return sizeof(PositionUpdateSBE) + (position_count * sizeof(SymbolPositionSBE));
        }

        uint32_t getMessageSize() const
        {
            return calculateMessageSize(position_count);
        }
    };

    // Legacy MessageFrame - replaced by lock-free version in shared_memory_manager.h
    // This is kept for backward compatibility with existing SBE encoding
    struct LegacyMessageFrame
    {
        uint32_t frame_length; // Total frame size including header
        uint8_t frame_type;    // Message type (DATA, HEARTBEAT, etc.)
        uint8_t flags;         // Message flags
        uint16_t reserved;     // Reserved for future use
        uint32_t term_offset;  // Offset within current term
        uint32_t session_id;   // Publisher session ID
        uint32_t stream_id;    // Topic hash for routing
        uint32_t term_id;      // Current term ID
        // Message payload follows immediately after

        static constexpr uint8_t FRAME_TYPE_DATA = 1;
        static constexpr uint8_t FRAME_TYPE_HEARTBEAT = 2;
        static constexpr uint8_t FRAME_TYPE_PADDING = 3;

        static constexpr uint8_t FLAG_BEGIN_FRAG = 0x80;
        static constexpr uint8_t FLAG_END_FRAG = 0x40;

        LegacyMessageFrame() = default;
        LegacyMessageFrame(uint8_t type, uint32_t payload_size, uint32_t session, uint32_t stream, uint32_t term)
            : frame_length(sizeof(LegacyMessageFrame) + payload_size), frame_type(type), flags(FLAG_BEGIN_FRAG | FLAG_END_FRAG) // Single fragment for now
              ,
              reserved(0), term_offset(0) // Set by ring buffer
              ,
              session_id(session), stream_id(stream), term_id(term)
        {
        }

        uint8_t *getPayload()
        {
            return reinterpret_cast<uint8_t *>(this) + sizeof(LegacyMessageFrame);
        }

        const uint8_t *getPayload() const
        {
            return reinterpret_cast<const uint8_t *>(this) + sizeof(LegacyMessageFrame);
        }

        uint32_t getPayloadSize() const
        {
            return frame_length - sizeof(LegacyMessageFrame);
        }
    };

    // Note: MessageFrame is now defined in shared_memory_manager.h as the lock-free version
    // This LegacyMessageFrame is kept for SBE encoding compatibility only

#pragma pack(pop)

    // Helper class for encoding/decoding position updates
    class PositionUpdateCodec
    {
    public:
        // Encode position update into a buffer
        static uint32_t encode(uint8_t *buffer, uint32_t buffer_size,
                               const std::string &strategy_id,
                               uint64_t sequence_number,
                               const std::vector<std::pair<std::string, double>> &positions);

        // Decode position update from buffer
        static bool decode(const uint8_t *buffer, uint32_t buffer_size,
                           std::string &strategy_id,
                           uint64_t &timestamp,
                           uint64_t &sequence_number,
                           std::vector<std::pair<std::string, double>> &positions);

        // Calculate required buffer size for encoding
        static uint32_t calculateBufferSize(uint32_t position_count);
    };

} // namespace position_distributor
