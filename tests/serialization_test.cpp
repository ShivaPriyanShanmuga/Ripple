#include <ripple/serialization.hpp>
#include <ripple/timestamp.hpp>
#include <ripple/window.hpp>

#include <gtest/gtest.h>

#include <cstddef>
#include <cstdint>
#include <optional>
#include <span>
#include <string>
#include <utility>
#include <vector>

namespace {

using ripple::ByteReader;
using ripple::ByteWriter;
using ripple::SerializationError;

template<typename T>
T round_trip(const T& value) {
    return ripple::deserialize<T>(ripple::serialize(value));
}

// ---------------------------------------------------------------------------
// Round trips
// ---------------------------------------------------------------------------

TEST(SerializationTest, RoundTripsArithmeticTypes) {
    EXPECT_EQ(round_trip<std::int32_t>(-42), -42);
    EXPECT_EQ(round_trip<std::int64_t>(1'700'000'000'000), 1'700'000'000'000);
    EXPECT_EQ(round_trip<std::uint8_t>(255), 255);
    EXPECT_DOUBLE_EQ(round_trip<double>(3.14159), 3.14159);
}

TEST(SerializationTest, RoundTripsBool) {
    EXPECT_TRUE(round_trip<bool>(true));
    EXPECT_FALSE(round_trip<bool>(false));
}

// Protects: bool occupies exactly one byte on the wire.
//
// `sizeof(bool)` is implementation-defined. Serializing it natively would work
// on every machine anyone tests on and then produce an unreadable state file on
// the one that does not agree.
TEST(SerializationTest, EncodesBoolAsExactlyOneByte) {
    EXPECT_EQ(ripple::serialize(true).size(), 1U);
}

TEST(SerializationTest, RoundTripsStringsIncludingEmptyAndEmbeddedNulls) {
    EXPECT_EQ(round_trip<std::string>("midtown"), "midtown");
    EXPECT_EQ(round_trip<std::string>(""), "");

    const std::string with_null("a\0b", 3);
    const std::string restored = round_trip(with_null);
    EXPECT_EQ(restored.size(), 3U);
    EXPECT_EQ(restored, with_null);
}

TEST(SerializationTest, RoundTripsContainersAndOptionals) {
    const std::vector<std::int32_t> numbers{1, -2, 3};
    EXPECT_EQ(round_trip(numbers), numbers);
    EXPECT_EQ(round_trip(std::vector<std::int32_t>{}), std::vector<std::int32_t>{});

    EXPECT_EQ(round_trip(std::optional<std::int32_t>{7}), std::optional<std::int32_t>{7});
    EXPECT_EQ(round_trip(std::optional<std::int32_t>{}), std::nullopt);

    const std::pair<std::string, std::int64_t> pair{"brooklyn", 99};
    EXPECT_EQ(round_trip(pair), pair);
}

TEST(SerializationTest, RoundTripsNestedContainers) {
    const std::vector<std::pair<std::string, std::vector<std::int32_t>>> nested{
        {"a", {1, 2}}, {"b", {}}, {"c", {3}}};
    EXPECT_EQ(round_trip(nested), nested);
}

TEST(SerializationTest, RoundTripsEngineTypes) {
    const ripple::Timestamp timestamp = ripple::timestamp_from_millis(-1'234);
    EXPECT_EQ(round_trip(timestamp), timestamp);

    const ripple::TimeWindow window{ripple::timestamp_from_millis(1'000),
                                    ripple::timestamp_from_millis(2'000)};
    EXPECT_EQ(round_trip(window), window);
}

// ---------------------------------------------------------------------------
// Format stability
// ---------------------------------------------------------------------------

// Protects: integers are written little-endian regardless of host byte order.
//
// Native order would work until a state file written on one machine is restored
// on another with the opposite endianness, at which point every integer is
// silently wrong -- not corrupt-looking, just wrong. Asserting the actual bytes
// is the only way to catch a regression here on a little-endian dev machine,
// where the buggy and correct versions are indistinguishable by round trip.
TEST(SerializationTest, EncodesIntegersLittleEndian) {
    const std::vector<std::byte> bytes = ripple::serialize<std::uint32_t>(0x01020304U);
    ASSERT_EQ(bytes.size(), 4U);
    EXPECT_EQ(bytes[0], std::byte{0x04});
    EXPECT_EQ(bytes[1], std::byte{0x03});
    EXPECT_EQ(bytes[2], std::byte{0x02});
    EXPECT_EQ(bytes[3], std::byte{0x01});
}

// Protects: serializing equal values produces byte-identical output.
//
// Checkpoints are compared, deduplicated and diffed. None of that works if the
// same logical state can serialize two different ways.
TEST(SerializationTest, IsDeterministic) {
    const std::vector<std::pair<std::string, std::int32_t>> value{{"a", 1}, {"b", 2}};
    EXPECT_EQ(ripple::serialize(value), ripple::serialize(value));
}

// ---------------------------------------------------------------------------
// Failure modes
// ---------------------------------------------------------------------------

// Protects: THE bug that hand-written serialization invites.
//
// `write` emitting more fields than `read` consumes corrupts everything after it
// in the stream, and is typically noticed -- if at all -- as inexplicably wrong
// data far from the cause. Requiring the reader to be fully consumed converts
// that entire class of bug into an exception at the point of the mistake.
TEST(SerializationTest, RejectsTrailingBytes) {
    std::vector<std::byte> bytes = ripple::serialize<std::int32_t>(7);
    bytes.push_back(std::byte{0xFF}); // as if `write` had emitted an extra field

    EXPECT_THROW((void)ripple::deserialize<std::int32_t>(bytes), SerializationError);
}

// Protects: a truncated stream throws rather than reading past the end.
TEST(SerializationTest, RejectsTruncatedInput) {
    std::vector<std::byte> bytes = ripple::serialize<std::int64_t>(7);
    bytes.resize(3);

    EXPECT_THROW((void)ripple::deserialize<std::int64_t>(bytes), SerializationError);
}

// Protects: a corrupt length prefix fails instead of attempting a huge
// allocation.
//
// Without the check against bytes remaining, a corrupted 4-byte length reading
// as 4 billion would make the reader try to allocate 4GB before discovering the
// input is 8 bytes long. Bad on a good day; a denial of service if the bytes
// ever come from somewhere untrusted.
TEST(SerializationTest, RejectsImplausibleLengthPrefix) {
    std::vector<std::byte> bytes = ripple::serialize<std::string>("hi");
    bytes[0] = std::byte{0xFF};
    bytes[1] = std::byte{0xFF};
    bytes[2] = std::byte{0xFF};
    bytes[3] = std::byte{0x7F};

    EXPECT_THROW((void)ripple::deserialize<std::string>(bytes), SerializationError);
}

TEST(SerializationTest, ReaderReportsExhaustion) {
    const std::vector<std::byte> bytes = ripple::serialize<std::int32_t>(1);
    ByteReader reader(bytes);
    EXPECT_FALSE(reader.exhausted());
    EXPECT_EQ(reader.read_fixed<std::int32_t>(), 1);
    EXPECT_TRUE(reader.exhausted());
}

TEST(SerializationTest, WriterAccumulates) {
    ByteWriter writer;
    writer.write_fixed<std::int32_t>(1);
    writer.write_fixed<std::int32_t>(2);
    EXPECT_EQ(writer.size(), 8U);
}

} // namespace
