#pragma once

#include <ripple/checkpoint/checkpoint_coordinator.hpp>
#include <ripple/record.hpp>
#include <ripple/watermark.hpp>

#include <cstddef>
#include <variant>

namespace ripple {

/// "This channel will send nothing further."
///
/// Needed only on **fan-in** queues, and the asymmetry is not arbitrary: a queue
/// with a single producer can simply be `close()`d, and `pop()` returning
/// `nullopt` means the stream is over. A queue with N producers cannot -- the
/// first producer to finish would close it out from under the others -- so
/// end-of-stream must travel as an element and the consumer exits once it has
/// counted one from every channel.
struct EndOfChannel {};

/// A Chandy-Lamport barrier: "snapshot your state at this point in the stream".
///
/// It carries nothing but an id because it needs nothing else. Its meaning comes
/// entirely from *where it is in the stream*, not from what it contains.
struct CheckpointBarrier {
    CheckpointId checkpoint_id = 0;
};

/// What travels through the queues between threads.
///
/// ## Why the channel sits outside the variant
///
/// Every element carries the index of the channel it arrived on. Watermarks need
/// it so a fan-in consumer can take the minimum across channels (D-049), and
/// **records need it for barrier alignment**: once a channel has delivered its
/// barrier, the consumer must set that channel's subsequent records aside, and
/// it cannot do that without knowing which channel each record came from.
///
/// Hoisting the channel out of the variant rather than tagging each alternative
/// separately means there is exactly one place it can be wrong, and the
/// dispatch below stays about *what* the element is rather than where it came
/// from.
///
/// ## Why a variant at all
///
/// This is the case D-014 reserved variants for. Payload types are an **open**
/// set defined by users, so a variant over them would be a central registry
/// everyone must edit. Stream elements are a **closed set fixed by the engine** --
/// data, time progress, end of channel, checkpoint barrier -- which is exactly
/// when a variant is right.
///
/// Note the layering: the *operator* interface never sees this type. Operators
/// have `process` and `on_watermark` as separate virtual functions (D-021), so
/// map and filter still need no knowledge that watermarks or barriers exist. The
/// variant lives only in the transport, because a queue has to carry one
/// concrete type.
template<typename T>
struct StreamElement {
    std::size_t channel = 0;
    std::variant<Record<T>, Watermark, EndOfChannel, CheckpointBarrier> payload;
};

} // namespace ripple
