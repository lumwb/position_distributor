#include <gtest/gtest.h>
#include "position_distributor/position.h"

using namespace position_distributor;

class PositionTest : public ::testing::Test
{
protected:
    void SetUp() override {}
    void TearDown() override {}
};

TEST_F(PositionTest, SymbolPositionCreation)
{
    SymbolPosition pos("BTCUSDT", 1.5);
    EXPECT_EQ(pos.symbol, "BTCUSDT");
    EXPECT_EQ(pos.net_position, 1.5);
}

TEST_F(PositionTest, SymbolPositionEquality)
{
    SymbolPosition pos1("BTCUSDT", 1.5);
    SymbolPosition pos2("BTCUSDT", 1.5);
    SymbolPosition pos3("ETHUSDT", 1.5);

    EXPECT_EQ(pos1, pos2);
    EXPECT_NE(pos1, pos3);
}

TEST_F(PositionTest, PositionUpdateCreation)
{
    std::vector<SymbolPosition> positions = {
        {"BTCUSDT", 1.5},
        {"ETHUSDT", -2.0}};

    PositionUpdate update("BINANCE", positions, 123);

    EXPECT_EQ(update.strategy_id, "BINANCE");
    EXPECT_EQ(update.positions.size(), 2);
    EXPECT_EQ(update.sequence_number, 123);
    EXPECT_EQ(update.positions[0].symbol, "BTCUSDT");
    EXPECT_EQ(update.positions[1].net_position, -2.0);
}
