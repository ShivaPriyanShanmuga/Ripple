#pragma once

#include <ripple/record.hpp>
#include <ripple/watermark.hpp>

#include <cstddef>
#include <variant>

namespace ripple {

/// A watermark tagged with the channel it arrived on.
///
/// The tag exists because a fan-in queue carries traffic from several upstream
/// subtasks at once, and the consumer must take the **minimum** across them
/// (D-026). Without knowing which upstream a watermark came from, there is no
/// way to compute that minimum -- an untagged watermark would look like progress
/// on every channel simultaneously, which is exactly the "take the maximum"
/// mistake that silently discards on-time data from slower channels.
struct ChannelWatermark {
    std::size_t channel;
    Watermark watermark;
};

/// "Channel N will send nothing further."
///
/// Needed only on **fan-in** queues, and the asymmetry is worth stating: a queue
/// with a single producer can simply be `close()`d, and `pop()` returning
/// `nullopt` means the stream is over. A queue with N producers cannot -- the
/// first one to finish would close the queue out from under the others. So
/// end-of-stream on a shared queue must travel *as an element*, and the consumer
/// exits once it has counted one from every channel.
struct ChannelClosed {
    std::size_t channel;
};

/// What actually travels through the queues between threads.
///
/// A `std::variant`, and this is the case D-014 explicitly reserved variants for.
/// Payload types are an open set defined by users, so a variant over them would
/// be a central registry everyone has to edit. Stream elements are the opposite:
/// a **closed set fixed by the engine** -- data, time progress, end of channel --
/// which is precisely when a variant is the right tool. Stage 7 adds a fourth
/// alternative for checkpoint barriers and nothing else changes shape.
///
/// Note the layering. The *operator* interface never sees this type: operators
/// have `process` and `on_watermark` as separate virtual functions (D-021), so
/// map and filter still need no knowledge that watermarks exist. The variant
/// lives only in the transport, because a queue has to carry one concrete type.
/// Interface shape and transport shape are separate decisions.
template<typename T>
using StreamElement = std::variant<Record<T>, ChannelWatermark, ChannelClosed>;

} // namespace ripple
