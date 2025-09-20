#include <gtest/gtest.h>
#include "position_distributor/message.h"
#include "position_distributor/position.h"

using namespace position_distributor;

class MessageTest : public ::testing::Test
{
protected:
    void SetUp() override {}
    void TearDown() override {}
};

TEST_F(MessageTest, PositionUpdateSerialization)
{
    std::vector<SymbolPosition> positions = {
        {"BTCUSDT.BN", 1.5},
        {"ETHUSDT.BN", -2.0}};

    PositionUpdate original("BINANCE", positions, 123);

    // Serialize
    auto data = MessageSerializer::serialize(original);
    EXPECT_FALSE(data.empty());

    // Deserialize
    PositionUpdate deserialized = MessageSerializer::deserialize(data);

    EXPECT_EQ(deserialized.strategy_id, original.strategy_id);
    EXPECT_EQ(deserialized.sequence_number, original.sequence_number);
    EXPECT_EQ(deserialized.positions.size(), original.positions.size());

    for (size_t i = 0; i < original.positions.size(); ++i)
    {
        EXPECT_EQ(deserialized.positions[i].symbol, original.positions[i].symbol);
        EXPECT_EQ(deserialized.positions[i].net_position, original.positions[i].net_position);
    }
}

TEST_F(MessageTest, MessageSerialization)
{
    std::vector<uint8_t> payload = {1, 2, 3, 4, 5};
    Message original(MessageType::POSITION_UPDATE, payload);

    // Serialize
    auto data = MessageSerializer::serialize(original);
    EXPECT_FALSE(data.empty());

    // Deserialize
    Message deserialized = MessageSerializer::deserialize(data);

    EXPECT_EQ(deserialized.type, original.type);
    EXPECT_EQ(deserialized.payload_size, original.payload_size);
    EXPECT_EQ(deserialized.payload, original.payload);
}

TEST_F(MessageTest, HeartbeatCreation)
{
    auto heartbeat = MessageSerializer::createHeartbeat();
    EXPECT_TRUE(heartbeat.empty());
}

TEST_F(MessageTest, AcknowledgeCreation)
{
    uint64_t seq_num = 12345;
    auto ack = MessageSerializer::createAcknowledge(seq_num);

    EXPECT_EQ(ack.size(), sizeof(seq_num));

    uint64_t deserialized_seq;
    std::memcpy(&deserialized_seq, ack.data(), sizeof(deserialized_seq));
    EXPECT_EQ(deserialized_seq, seq_num);
}

TEST_F(MessageTest, ErrorCreation)
{
    std::string error_msg = "Test error message";
    auto error = MessageSerializer::createError(error_msg);

    EXPECT_GT(error.size(), sizeof(uint32_t));

    uint32_t msg_len;
    std::memcpy(&msg_len, error.data(), sizeof(msg_len));
    EXPECT_EQ(msg_len, error_msg.length());

    std::string deserialized_msg(error.begin() + sizeof(msg_len),
                                 error.begin() + sizeof(msg_len) + msg_len);
    EXPECT_EQ(deserialized_msg, error_msg);
}
