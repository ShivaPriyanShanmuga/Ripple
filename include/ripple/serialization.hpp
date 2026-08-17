#pragma once

#include <ripple/timestamp.hpp>
#include <ripple/window.hpp>

#include <algorithm>
#include <array>
#include <bit>
#include <concepts>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <limits>
#include <optional>
#include <span>
#include <stdexcept>
#include <string>
#include <string_view>
#include <type_traits>
#include <utility>
#include <vector>

namespace ripple {

/// # Serialization
///
/// C++ has no reflection. There is no way to ask a type what fields it has, so
/// nothing can generate this for us and every conversion between an object and
/// bytes has to be written by hand. That is why this is a design task here and a
/// library call in Flink, and why it is designed in Stage 4 rather than
/// discovered in Stage 7: retrofitting a serialization scheme across every state
/// type after checkpointing exists is expensive.
///
/// ## The mechanism: a trait specialised per type
///
/// `Serializer<T>` is specialised for each supported type with a `write` and a
/// `read`. Specialisations are provided here for arithmetic types, `std::string`,
/// `std::vector`, `std::optional`, `std::pair`, and the engine's own types; user
/// types opt in by specialising it themselves.
///
/// **Rejected — a self-describing tagged format** (protobuf/msgpack style, where
/// every field carries a type tag and an id). It buys schema evolution across
/// versions, which we do not need since both ends of every conversion are
/// compiled together, and it charges for that on every field of every record.
///
/// **Rejected — macro-generated field visitors** (`RIPPLE_FIELDS(a, b, c)`). It
/// removes the duplication between `write` and `read`, which is a real benefit,
/// but macro-generated code is invisible to the debugger and produces error
/// messages that name the macro rather than the mistake.
///
/// **The duplication risk this leaves**, and how it is handled: writing three
/// fields and reading two is the classic hand-rolled-serialization bug, and it
/// corrupts silently rather than failing. `deserialize` therefore requires the
/// reader to be **fully consumed** and throws if it is not, which turns that
/// entire bug class into an immediate, loud failure.

/// Thrown when a byte stream is truncated, malformed, or not fully consumed.
/// Corrupt state must fail loudly: a checkpoint that restores subtly wrong state
/// is worse than one that refuses to restore at all.
class SerializationError : public std::runtime_error {
public:
    using std::runtime_error::runtime_error;
};

/// Append-only byte buffer.
class ByteWriter {
public:
    void write_raw(const void* data, std::size_t size) {
        const std::span<const std::byte> source{static_cast<const std::byte*>(data), size};
        buffer_.insert(buffer_.end(), source.begin(), source.end());
    }

    /// Writes a trivially-copyable value in **little-endian**, regardless of the
    /// host's byte order.
    ///
    /// Native order would be marginally faster and would work perfectly until
    /// the day a state file written on one machine is restored on another with
    /// the opposite endianness, at which point every integer is silently wrong.
    /// Fixing the byte order at the format level costs a compile-time-eliminated
    /// branch on every platform anyone actually uses.
    template<typename T>
        requires std::is_trivially_copyable_v<T>
    void write_fixed(T value) {
        std::array<std::byte, sizeof(T)> raw{};
        std::memcpy(raw.data(), &value, sizeof(T));
        if constexpr (std::endian::native == std::endian::big) {
            std::ranges::reverse(raw);
        }
        write_raw(raw.data(), raw.size());
    }

    [[nodiscard]] const std::vector<std::byte>& bytes() const noexcept { return buffer_; }

    [[nodiscard]] std::vector<std::byte> take() && noexcept { return std::move(buffer_); }

    [[nodiscard]] std::size_t size() const noexcept { return buffer_.size(); }

private:
    std::vector<std::byte> buffer_;
};

/// Sequential reader over a byte range.
///
/// Holds a non-owning view. The caller must keep the underlying bytes alive for
/// the reader's lifetime -- the usual reason a reader outlives its buffer is a
/// temporary passed directly into a constructor.
class ByteReader {
public:
    explicit ByteReader(std::span<const std::byte> data) noexcept : data_(data) {}

    void read_raw(void* out, std::size_t size) {
        if (remaining() < size) {
            throw SerializationError("byte stream truncated: wanted " + std::to_string(size) +
                                     " bytes, " + std::to_string(remaining()) + " remain");
        }
        std::memcpy(out, data_.subspan(position_, size).data(), size);
        position_ += size;
    }

    template<typename T>
        requires std::is_trivially_copyable_v<T>
    [[nodiscard]] T read_fixed() {
        std::array<std::byte, sizeof(T)> raw{};
        read_raw(raw.data(), raw.size());
        if constexpr (std::endian::native == std::endian::big) {
            std::ranges::reverse(raw);
        }
        T value{};
        std::memcpy(&value, raw.data(), sizeof(T));
        return value;
    }

    [[nodiscard]] std::size_t remaining() const noexcept { return data_.size() - position_; }

    [[nodiscard]] bool exhausted() const noexcept { return remaining() == 0; }

private:
    std::span<const std::byte> data_;
    std::size_t position_ = 0;
};

/// Specialise this for any type that needs to enter or leave a state backend.
/// Deliberately left undefined so that an unsupported type produces an error
/// naming the type rather than a link failure.
template<typename T>
struct Serializer;

template<typename T>
concept Serializable = requires(ByteWriter& writer, ByteReader& reader, const T& value) {
    { Serializer<T>::write(writer, value) } -> std::same_as<void>;
    { Serializer<T>::read(reader) } -> std::same_as<T>;
};

// --- Built-in specialisations ----------------------------------------------

template<typename T>
    requires std::is_arithmetic_v<T>
struct Serializer<T> {
    static void write(ByteWriter& writer, const T& value) { writer.write_fixed(value); }

    [[nodiscard]] static T read(ByteReader& reader) { return reader.read_fixed<T>(); }
};

/// Written as one byte rather than `sizeof(bool)`, which is
/// implementation-defined -- another value that would work everywhere until it
/// did not.
template<>
struct Serializer<bool> {
    static void write(ByteWriter& writer, const bool& value) {
        writer.write_fixed<std::uint8_t>(value ? 1U : 0U);
    }

    [[nodiscard]] static bool read(ByteReader& reader) {
        return reader.read_fixed<std::uint8_t>() != 0U;
    }
};

namespace detail {

/// Lengths are a fixed 32-bit prefix. Varints would be smaller for the common
/// short case, at the cost of a branchy decode on the hot path; 4 bytes of
/// overhead per string is not what limits this system.
inline void write_length(ByteWriter& writer, std::size_t length) {
    if (length > std::numeric_limits<std::uint32_t>::max()) {
        throw SerializationError("value too large to serialize");
    }
    writer.write_fixed(static_cast<std::uint32_t>(length));
}

[[nodiscard]] inline std::size_t read_length(ByteReader& reader) {
    const auto length = static_cast<std::size_t>(reader.read_fixed<std::uint32_t>());
    // A corrupt or hostile length must not cause a huge allocation before the
    // truncation is noticed, so it is validated against what is actually left.
    if (length > reader.remaining()) {
        throw SerializationError("declared length " + std::to_string(length) + " exceeds the " +
                                 std::to_string(reader.remaining()) + " bytes remaining");
    }
    return length;
}

} // namespace detail

template<>
struct Serializer<std::string> {
    static void write(ByteWriter& writer, const std::string& value) {
        detail::write_length(writer, value.size());
        writer.write_raw(value.data(), value.size());
    }

    [[nodiscard]] static std::string read(ByteReader& reader) {
        const std::size_t length = detail::read_length(reader);
        std::string value(length, '\0');
        reader.read_raw(value.data(), length);
        return value;
    }
};

template<Serializable T>
struct Serializer<std::vector<T>> {
    static void write(ByteWriter& writer, const std::vector<T>& value) {
        detail::write_length(writer, value.size());
        for (const T& element : value) {
            Serializer<T>::write(writer, element);
        }
    }

    [[nodiscard]] static std::vector<T> read(ByteReader& reader) {
        // Element count, not byte count -- so it cannot be validated against the
        // bytes remaining the way a string's length can. Elements are appended
        // one at a time and each read bounds-checks itself, so a corrupt count
        // fails on the first missing element rather than pre-allocating.
        const auto count = static_cast<std::size_t>(reader.read_fixed<std::uint32_t>());
        std::vector<T> value;
        for (std::size_t i = 0; i < count; ++i) {
            value.push_back(Serializer<T>::read(reader));
        }
        return value;
    }
};

/// Raw byte blobs, which is what every value in a state backend is. Needs its
/// own specialisation because `std::byte` is not arithmetic and so does not
/// satisfy `Serializable` on its own -- and writing the bytes in bulk is far
/// better than looping over them one at a time anyway.
template<>
struct Serializer<std::vector<std::byte>> {
    static void write(ByteWriter& writer, const std::vector<std::byte>& value) {
        detail::write_length(writer, value.size());
        writer.write_raw(value.data(), value.size());
    }

    [[nodiscard]] static std::vector<std::byte> read(ByteReader& reader) {
        const std::size_t length = detail::read_length(reader);
        std::vector<std::byte> value(length);
        reader.read_raw(value.data(), length);
        return value;
    }
};

template<Serializable T>
struct Serializer<std::optional<T>> {
    static void write(ByteWriter& writer, const std::optional<T>& value) {
        Serializer<bool>::write(writer, value.has_value());
        if (value.has_value()) {
            Serializer<T>::write(writer, *value);
        }
    }

    [[nodiscard]] static std::optional<T> read(ByteReader& reader) {
        if (!Serializer<bool>::read(reader)) {
            return std::nullopt;
        }
        return Serializer<T>::read(reader);
    }
};

template<Serializable A, Serializable B>
struct Serializer<std::pair<A, B>> {
    static void write(ByteWriter& writer, const std::pair<A, B>& value) {
        Serializer<A>::write(writer, value.first);
        Serializer<B>::write(writer, value.second);
    }

    [[nodiscard]] static std::pair<A, B> read(ByteReader& reader) {
        // Written as two statements: the order of evaluation of a braced pair's
        // arguments would be unspecified, and reading these two fields in the
        // wrong order silently swaps them.
        A first = Serializer<A>::read(reader);
        B second = Serializer<B>::read(reader);
        return std::pair<A, B>{std::move(first), std::move(second)};
    }
};

template<>
struct Serializer<Timestamp> {
    static void write(ByteWriter& writer, const Timestamp& value) {
        writer.write_fixed(millis_since_epoch(value));
    }

    [[nodiscard]] static Timestamp read(ByteReader& reader) {
        return timestamp_from_millis(reader.read_fixed<std::int64_t>());
    }
};

template<>
struct Serializer<TimeWindow> {
    static void write(ByteWriter& writer, const TimeWindow& value) {
        Serializer<Timestamp>::write(writer, value.start);
        Serializer<Timestamp>::write(writer, value.end);
    }

    [[nodiscard]] static TimeWindow read(ByteReader& reader) {
        const Timestamp start = Serializer<Timestamp>::read(reader);
        const Timestamp end = Serializer<Timestamp>::read(reader);
        return TimeWindow{start, end};
    }
};

// --- Convenience entry points ----------------------------------------------

template<Serializable T>
[[nodiscard]] std::vector<std::byte> serialize(const T& value) {
    ByteWriter writer;
    Serializer<T>::write(writer, value);
    return std::move(writer).take();
}

/// Requires the input to be **fully consumed**.
///
/// This is the guard against the defining bug of hand-written serialization:
/// `write` emitting three fields while `read` consumes two. Without the check
/// that mismatch corrupts every subsequent value in the stream and is noticed,
/// if at all, as inexplicably wrong data much later. With it, the mismatch
/// throws at the point of the mistake.
template<Serializable T>
[[nodiscard]] T deserialize(std::span<const std::byte> bytes) {
    ByteReader reader(bytes);
    T value = Serializer<T>::read(reader);
    if (!reader.exhausted()) {
        throw SerializationError(std::to_string(reader.remaining()) +
                                 " trailing bytes after deserialization -- write and read "
                                 "disagree about this type's layout");
    }
    return value;
}

} // namespace ripple
