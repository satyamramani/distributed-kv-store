#include <gtest/gtest.h>

#include "kvstore/vector_clock.h"

class VectorClockTest : public ::testing::Test {};

// ── Increment ───────────────────────────────────────────────────────────────

TEST_F(VectorClockTest, IncrementCreatesEntry) {
    kvstore::VectorClock vc;
    vc.increment("S1");
    EXPECT_EQ(vc.getCounter("S1"), 1u);
    EXPECT_EQ(vc.size(), 1u);
}

TEST_F(VectorClockTest, IncrementExistingServer) {
    kvstore::VectorClock vc;
    vc.increment("S1");
    vc.increment("S1");
    vc.increment("S1");
    EXPECT_EQ(vc.getCounter("S1"), 3u);
}

TEST_F(VectorClockTest, IncrementMultipleServers) {
    kvstore::VectorClock vc;
    vc.increment("S1");
    vc.increment("S2");
    EXPECT_EQ(vc.getCounter("S1"), 1u);
    EXPECT_EQ(vc.getCounter("S2"), 1u);
    EXPECT_EQ(vc.size(), 2u);
}

TEST_F(VectorClockTest, GetCounterForUnknownServerReturnsZero) {
    kvstore::VectorClock vc;
    EXPECT_EQ(vc.getCounter("unknown"), 0u);
}

// ── Compare: EQUAL ──────────────────────────────────────────────────────────

TEST_F(VectorClockTest, EmptyClocksAreEqual) {
    kvstore::VectorClock a, b;
    EXPECT_EQ(a.compare(b), kvstore::ClockComparison::EQUAL);
}

TEST_F(VectorClockTest, IdenticalClocksAreEqual) {
    kvstore::VectorClock a, b;
    a.increment("S1");
    a.increment("S2");
    b.increment("S1");
    b.increment("S2");
    EXPECT_EQ(a.compare(b), kvstore::ClockComparison::EQUAL);
}

// ── Compare: BEFORE / AFTER ─────────────────────────────────────────────────

TEST_F(VectorClockTest, HappenedBefore) {
    // a = {S1:1}, b = {S1:2}  →  a BEFORE b
    kvstore::VectorClock a, b;
    a.increment("S1");       // {S1:1}
    b.increment("S1");
    b.increment("S1");       // {S1:2}

    EXPECT_EQ(a.compare(b), kvstore::ClockComparison::BEFORE);
    EXPECT_EQ(b.compare(a), kvstore::ClockComparison::AFTER);
}

TEST_F(VectorClockTest, HappenedBeforeSubset) {
    // a = {S1:1}, b = {S1:1, S2:1}  →  a BEFORE b
    kvstore::VectorClock a, b;
    a.increment("S1");
    b.increment("S1");
    b.increment("S2");

    EXPECT_EQ(a.compare(b), kvstore::ClockComparison::BEFORE);
}

// ── Compare: CONCURRENT (conflict!) ─────────────────────────────────────────

TEST_F(VectorClockTest, ConcurrentClocks) {
    // ByteByteGo example: D3({Sx:2, Sy:1}) vs D4({Sx:2, Sz:1})
    kvstore::VectorClock d3, d4;
    d3.increment("Sx");
    d3.increment("Sx");
    d3.increment("Sy");     // {Sx:2, Sy:1}

    d4.increment("Sx");
    d4.increment("Sx");
    d4.increment("Sz");     // {Sx:2, Sz:1}

    EXPECT_EQ(d3.compare(d4), kvstore::ClockComparison::CONCURRENT);
    EXPECT_EQ(d4.compare(d3), kvstore::ClockComparison::CONCURRENT);
}

TEST_F(VectorClockTest, ConcurrentSimple) {
    // a = {S1:1, S2:2}, b = {S1:2, S2:1}  →  concurrent
    kvstore::VectorClock a, b;
    a.increment("S1");
    a.increment("S2");
    a.increment("S2");

    b.increment("S1");
    b.increment("S1");
    b.increment("S2");

    EXPECT_EQ(a.compare(b), kvstore::ClockComparison::CONCURRENT);
}

// ── Descends ────────────────────────────────────────────────────────────────

TEST_F(VectorClockTest, DescendsFromAncestor) {
    kvstore::VectorClock ancestor, descendant;
    ancestor.increment("S1");          // {S1:1}
    descendant.increment("S1");
    descendant.increment("S1");
    descendant.increment("S2");        // {S1:2, S2:1}

    EXPECT_TRUE(descendant.descends(ancestor));
    EXPECT_FALSE(ancestor.descends(descendant));
}

TEST_F(VectorClockTest, DoesNotDescendFromConcurrent) {
    kvstore::VectorClock a, b;
    a.increment("S1");
    b.increment("S2");

    EXPECT_FALSE(a.descends(b));
    EXPECT_FALSE(b.descends(a));
}

// ── Merge ───────────────────────────────────────────────────────────────────

TEST_F(VectorClockTest, MergeTakesElementWiseMax) {
    kvstore::VectorClock a, b;
    a.increment("S1");
    a.increment("S1");     // {S1:2}
    a.increment("S2");     // {S1:2, S2:1}

    b.increment("S1");     // {S1:1}
    b.increment("S3");     // {S1:1, S3:1}

    a.merge(b);            // expected: {S1:2, S2:1, S3:1}
    EXPECT_EQ(a.getCounter("S1"), 2u);
    EXPECT_EQ(a.getCounter("S2"), 1u);
    EXPECT_EQ(a.getCounter("S3"), 1u);
}

TEST_F(VectorClockTest, MergeWithEmptyClock) {
    kvstore::VectorClock a, empty;
    a.increment("S1");
    a.merge(empty);
    EXPECT_EQ(a.getCounter("S1"), 1u);
}

// ── Prune ───────────────────────────────────────────────────────────────────

TEST_F(VectorClockTest, PruneReducesEntries) {
    kvstore::VectorClock vc;
    vc.increment("S1");           // counter=1
    vc.increment("S2");
    vc.increment("S2");           // counter=2
    vc.increment("S3");
    vc.increment("S3");
    vc.increment("S3");           // counter=3

    vc.prune(2);
    EXPECT_LE(vc.size(), 2u);

    // The two highest counters should survive (S3:3, S2:2).
    EXPECT_EQ(vc.getCounter("S3"), 3u);
    EXPECT_EQ(vc.getCounter("S2"), 2u);
}

TEST_F(VectorClockTest, PruneWithLargerLimitIsNoop) {
    kvstore::VectorClock vc;
    vc.increment("S1");
    vc.prune(10);
    EXPECT_EQ(vc.size(), 1u);
}

// ── Empty ───────────────────────────────────────────────────────────────────

TEST_F(VectorClockTest, NewClockIsEmpty) {
    kvstore::VectorClock vc;
    EXPECT_TRUE(vc.empty());
    EXPECT_EQ(vc.size(), 0u);
}

// ── ByteByteGo full scenario ────────────────────────────────────────────────

TEST_F(VectorClockTest, ByteByteGoScenario) {
    // Step 1: Client writes D1, handled by Sx → D1([Sx,1])
    kvstore::VectorClock d1;
    d1.increment("Sx");

    // Step 2: Another write handled by Sx → D2([Sx,2])
    kvstore::VectorClock d2 = d1;
    d2.increment("Sx");

    EXPECT_TRUE(d2.descends(d1));

    // Step 3: Write handled by Sy → D3([Sx,2],[Sy,1])
    kvstore::VectorClock d3 = d2;
    d3.increment("Sy");

    EXPECT_TRUE(d3.descends(d2));

    // Step 4: Another client reads D2, writes via Sz → D4([Sx,2],[Sz,1])
    kvstore::VectorClock d4 = d2;
    d4.increment("Sz");

    // D3 and D4 are concurrent (conflict!)
    EXPECT_EQ(d3.compare(d4), kvstore::ClockComparison::CONCURRENT);

    // Step 5: Reconcile D3 + D4, write via Sx → D5([Sx,3],[Sy,1],[Sz,1])
    kvstore::VectorClock d5 = d3;
    d5.merge(d4);
    d5.increment("Sx");

    EXPECT_TRUE(d5.descends(d3));
    EXPECT_TRUE(d5.descends(d4));
    EXPECT_EQ(d5.getCounter("Sx"), 3u);
    EXPECT_EQ(d5.getCounter("Sy"), 1u);
    EXPECT_EQ(d5.getCounter("Sz"), 1u);
}
