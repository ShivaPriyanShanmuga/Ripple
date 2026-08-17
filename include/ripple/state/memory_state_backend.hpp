#pragma once

#include <ripple/serialization.hpp>
#include <ripple/state/state_backend.hpp>

#include <cstddef>
#include <map>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace ripple {

/// Keeps all state in memory.
///
/// The default backend, and the one every test uses. Fast, simple, and bounded
/// only by RAM -- which is exactly the constraint a real job hits, and the reason
/// `key_count()` is worth watching.
class MemoryStateBackend final : public StateBackend {
public:
    void set_current_key(StateKey key) override { current_key_ = std::move(key); }

    [[nodiscard]] const StateKey& current_key() const noexcept override { return current_key_; }

    [[nodiscard]] std::optional<std::span<const std::byte>>
    get(std::string_view state_name) const override {
        const auto key_entry = state_.find(current_key_);
        if (key_entry == state_.end()) {
            return std::nullopt;
        }
        // `std::less<>` on the inner map makes this a heterogeneous lookup: the
        // string_view is compared directly rather than being materialised into a
        // temporary std::string on every single state read.
        const auto value_entry = key_entry->second.find(state_name);
        if (value_entry == key_entry->second.end()) {
            return std::nullopt;
        }
        return std::span<const std::byte>{value_entry->second};
    }

    void put(std::string_view state_name, std::vector<std::byte> value) override {
        state_[current_key_].insert_or_assign(std::string{state_name}, std::move(value));
    }

    void remove(std::string_view state_name) override {
        const auto key_entry = state_.find(current_key_);
        if (key_entry == state_.end()) {
            return;
        }
        key_entry->second.erase(std::string{state_name});

        // Drop the key entirely once its last state is gone. Without this,
        // clearing state would free the values but leak one map node per key
        // forever -- a slow leak proportional to key cardinality, which for
        // something like a user id is unbounded.
        if (key_entry->second.empty()) {
            state_.erase(key_entry);
        }
    }

    [[nodiscard]] std::size_t key_count() const noexcept override { return state_.size(); }

    void clear() override {
        state_.clear();
        current_key_.clear();
    }

    void write_snapshot(ByteWriter& writer) const override {
        detail::write_length(writer, state_.size());
        for (const auto& [key, entries] : state_) {
            Serializer<StateKey>::write(writer, key);
            detail::write_length(writer, entries.size());
            for (const auto& [name, value] : entries) {
                Serializer<std::string>::write(writer, name);
                Serializer<std::vector<std::byte>>::write(writer, value);
            }
        }
    }

    void restore_snapshot(ByteReader& reader) override {
        clear();
        const auto key_count = static_cast<std::size_t>(reader.read_fixed<std::uint32_t>());
        for (std::size_t i = 0; i < key_count; ++i) {
            StateKey key = Serializer<StateKey>::read(reader);
            const auto entry_count = static_cast<std::size_t>(reader.read_fixed<std::uint32_t>());
            std::map<std::string, std::vector<std::byte>, std::less<>> entries;
            for (std::size_t j = 0; j < entry_count; ++j) {
                std::string name = Serializer<std::string>::read(reader);
                std::vector<std::byte> value = Serializer<std::vector<std::byte>>::read(reader);
                entries.emplace(std::move(name), std::move(value));
            }
            state_.emplace(std::move(key), std::move(entries));
        }
    }

private:
    /// Both levels are ordered maps rather than hash maps, and that is a
    /// deliberate trade of a little lookup speed for a property checkpointing
    /// needs: **iteration order is deterministic**. Snapshotting identical state
    /// twice must produce identical bytes, or checkpoints cannot be compared,
    /// deduplicated, or diffed when something goes wrong. An unordered_map's
    /// iteration order depends on insertion history and bucket count.
    std::map<StateKey, std::map<std::string, std::vector<std::byte>, std::less<>>> state_;

    StateKey current_key_;
};

} // namespace ripple
