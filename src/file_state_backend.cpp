#include <ripple/serialization.hpp>
#include <ripple/state/file_state_backend.hpp>
#include <ripple/state/state_backend.hpp>

#include <cstddef>
#include <filesystem>
#include <fstream>
#include <ios>
#include <iterator>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace ripple {

FileStateBackend::FileStateBackend(std::filesystem::path path) : path_(std::move(path)) {}

void FileStateBackend::set_current_key(StateKey key) {
    memory_.set_current_key(std::move(key));
}

const StateKey& FileStateBackend::current_key() const noexcept {
    return memory_.current_key();
}

std::optional<std::span<const std::byte>> FileStateBackend::get(std::string_view state_name) const {
    return memory_.get(state_name);
}

void FileStateBackend::put(std::string_view state_name, std::vector<std::byte> value) {
    memory_.put(state_name, std::move(value));
}

void FileStateBackend::remove(std::string_view state_name) {
    memory_.remove(state_name);
}

std::size_t FileStateBackend::key_count() const noexcept {
    return memory_.key_count();
}

void FileStateBackend::clear() {
    memory_.clear();
}

void FileStateBackend::write_snapshot(ByteWriter& writer) const {
    memory_.write_snapshot(writer);
}

void FileStateBackend::restore_snapshot(ByteReader& reader) {
    memory_.restore_snapshot(reader);
}

void FileStateBackend::flush() const {
    ByteWriter writer;
    memory_.write_snapshot(writer);

    // Write-then-rename. `rename` within a filesystem is atomic, so a crash
    // partway through leaves the previous complete file intact rather than a
    // truncated one. A truncated state file is the worse outcome by far: it
    // fails at restore time, which is precisely when you are already recovering
    // from something else and least want a second problem.
    std::filesystem::path temporary = path_;
    temporary += ".tmp";

    {
        std::ofstream out(temporary, std::ios::binary | std::ios::trunc);
        if (!out) {
            throw SerializationError("cannot open " + temporary.string() + " for writing");
        }
        const std::vector<std::byte>& bytes = writer.bytes();
        // std::ofstream deals in char; reinterpreting a byte buffer as chars is
        // the sanctioned way to write binary data and is explicitly permitted by
        // the strict-aliasing rules for char-like types.
        // NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast)
        out.write(reinterpret_cast<const char*>(bytes.data()),
                  static_cast<std::streamsize>(bytes.size()));
        if (!out) {
            throw SerializationError("write to " + temporary.string() + " failed");
        }
    } // closed here, so the rename below cannot see a partially flushed stream

    std::filesystem::rename(temporary, path_);
}

bool FileStateBackend::load() {
    std::ifstream in(path_, std::ios::binary);
    if (!in) {
        // Not an error: a job starting for the first time has no state file.
        return false;
    }

    const std::vector<char> raw{std::istreambuf_iterator<char>(in),
                                std::istreambuf_iterator<char>()};
    // NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast)
    const std::span<const std::byte> bytes{reinterpret_cast<const std::byte*>(raw.data()),
                                           raw.size()};
    ByteReader reader(bytes);
    memory_.restore_snapshot(reader);

    if (!reader.exhausted()) {
        throw SerializationError("state file " + path_.string() + " has " +
                                 std::to_string(reader.remaining()) +
                                 " trailing bytes; it is corrupt or was written by a "
                                 "different format version");
    }
    return true;
}

} // namespace ripple
