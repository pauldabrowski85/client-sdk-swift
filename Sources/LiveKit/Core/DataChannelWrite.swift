/*
 * Copyright 2026 LiveKit
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import Foundation

internal import LiveKitWebRTC

// MARK: - Channel seam

/// The slice of a data channel that ``DataChannelDrain`` sends through — a seam so the queue and its
/// overflow policy stay unit-testable (`LKRTCDataChannel` cannot be constructed without a live peer
/// connection).
protocol DrainSendChannel: AnyObject, Sendable {
    var isOpen: Bool { get }
    func send(_ payload: Data) -> Bool
}

enum DrainSendAttempt {
    case sent
    case unavailable
    case rejected(Error)
    case failed
}

/// An admission decision whose final attempt owns the operation it permits. This makes revocation
/// and the irreversible `sendData` call one linearizable action instead of a check-then-send gap.
struct DataChannelSendAdmission: Sendable {
    let preflight: @Sendable () -> Error?
    let attempt: @Sendable (_ send: @Sendable () -> DrainSendAttempt) -> DrainSendAttempt

    init(
        preflight: @escaping @Sendable () -> Error?,
        attempt: @escaping @Sendable (_ send: @Sendable () -> DrainSendAttempt) -> DrainSendAttempt
    ) {
        self.preflight = preflight
        self.attempt = attempt
    }

    init(predicate: @escaping @Sendable () -> Bool) {
        preflight = {
            predicate() ? nil : LiveKitError(
                .cancelled,
                message: "Data channel send admission was revoked"
            )
        }
        attempt = { send in
            guard predicate() else {
                return .rejected(LiveKitError(
                    .cancelled,
                    message: "Data channel send admission was revoked"
                ))
            }
            return send()
        }
    }
}

/// Rejects stale work before it can park, then runs the final send decision while the owner holds
/// its generation/provenance lock. The attempt callback must invoke the send operation at most once.
struct DrainSendAdmission: Sendable {
    let preflight: @Sendable () -> Error?
    let attempt: @Sendable (
        _ channel: DrainSendChannel,
        _ send: @Sendable () -> Bool
    ) -> DrainSendAttempt
}

extension LKRTCDataChannel: DrainSendChannel {
    var isOpen: Bool { readyState == .open }

    func send(_ payload: Data) -> Bool {
        // The buffer is built here, at send time, not when the write was queued: the init memcpys
        // the payload into a CopyOnWriteBuffer, and on a drop-oldest channel a queued write is
        // routinely evicted before it ever gets this far — evicted bytes should cost nothing.
        sendData(RTC.createDataBuffer(data: payload))
    }
}

/// Hands a drain channel's teardown to ``RTC/park(_:)``; no-op for test channels not backed by WebRTC.
/// `close()` blocks on the signaling thread just like the destructor, so both go to the same place.
func parkChannelRelease(_ target: DrainSendChannel?, closing: Bool = false) {
    guard let channel = target as? LKRTCDataChannel else { return }
    if closing { RTC.park { channel.close() } } else { RTC.park(channel) }
}

// MARK: - Write phases

/// Serialized bytes and the sequence stamped on them: what a ``SendStage`` produces, before the
/// drain size-checks them and wraps them for the channel.
struct PreparedBytes {
    var bytes: Data
    var sequence: UInt32

    init(bytes: Data, sequence: UInt32 = 0) {
        self.bytes = bytes
        self.sequence = sequence
    }
}

/// The one-shot handle on a submitter's continuation.
///
/// Settlement is idempotent — the first outcome wins and any later attempt is a no-op — so no code
/// path can trap on a double resume, and a token released without ever being settled fails its
/// submitter from `deinit` rather than stranding it forever. Copies of a write share the one token,
/// which is what makes both properties hold across evict/drop/teardown paths.
///
/// Settlement can race the drain's single-consumer event loop with task cancellation, so the
/// continuation is lock-guarded and remains first-wins across both paths.
final class SendToken: @unchecked Sendable {
    private let continuation: StateSync<CheckedContinuation<Void, any Error>?>

    init(_ continuation: CheckedContinuation<Void, any Error>) {
        self.continuation = StateSync(continuation)
    }

    func settle(with result: Result<Void, any Error>) {
        let continuation = continuation.mutate { stored -> CheckedContinuation<Void, any Error>? in
            defer { stored = nil }
            return stored
        }
        continuation?.resume(with: result)
    }

    var isSettled: Bool { continuation.copy() == nil }

    /// Runs the final irreversible send decision while cancellation/settlement is excluded.
    /// Whichever acquires this token first wins: a cancellation that settles first prevents the
    /// send closure from running, while a send already admitted here has crossed the point where
    /// WebRTC can recall it.
    func attempt(
        settlingSubmission: Bool,
        _ operation: @Sendable () -> DrainSendAttempt
    ) -> DrainSendAttempt {
        let outcome = continuation.mutate { stored -> (
            attempt: DrainSendAttempt,
            continuation: CheckedContinuation<Void, any Error>?,
            result: Result<Void, any Error>?
        ) in
            guard stored != nil else {
                return (.rejected(LiveKitError(
                    .cancelled,
                    message: "Data channel submission was cancelled"
                )), nil, nil)
            }
            let attempt = operation()
            guard settlingSubmission, let result = attempt.submissionResult else {
                return (attempt, nil, nil)
            }
            let continuation = stored
            stored = nil
            return (attempt, continuation, result)
        }
        if let continuation = outcome.continuation, let result = outcome.result {
            continuation.resume(with: result)
        }
        return outcome.attempt
    }

    deinit {
        settle(with: .failure(LiveKitError(.cancelled, message: "Write dropped without settlement")))
    }
}

/// A write that has been serialized, stamped and size-checked, and so is ready for the channel.
/// Every write in an awaited group carries the same submitter token so cancellation gates each
/// irreversible send; only the last write settles the submission. A replayed write has no token.
struct ReadyWrite {
    let payload: Data
    let sequence: UInt32
    let admission: DrainSendAdmission?
    let submissionToken: SendToken?
    let settlesSubmission: Bool

    init(
        payload: Data,
        sequence: UInt32,
        admission: DrainSendAdmission? = nil,
        submissionToken: SendToken? = nil,
        settlesSubmission: Bool = false
    ) {
        self.payload = payload
        self.sequence = sequence
        self.admission = admission
        self.submissionToken = submissionToken
        self.settlesSubmission = settlesSubmission
    }

    var byteCount: Int { payload.count }

    /// Resolves or fails the submitter, if one is waiting. Safe on every path: see ``SendToken``.
    func settle(with result: Result<Void, any Error>) {
        guard settlesSubmission else { return }
        submissionToken?.settle(with: result)
    }

    func belongs(to token: SendToken) -> Bool { submissionToken === token }

    /// Couples the submitter's cancellation token to the same final attempt that enforces
    /// generation/deadline admission. This also covers ordinary writes with no extra admission.
    func attempt(on channel: DrainSendChannel) -> DrainSendAttempt {
        let send: @Sendable () -> Bool = {
            channel.send(payload)
        }
        let operation: @Sendable () -> DrainSendAttempt = {
            admission?.attempt(channel, send) ??
                (send() ? DrainSendAttempt.sent : .failed)
        }
        return submissionToken?.attempt(
            settlingSubmission: settlesSubmission,
            operation
        ) ?? operation()
    }
}

private extension DrainSendAttempt {
    var submissionResult: Result<Void, any Error>? {
        switch self {
        case .sent:
            .success(())
        case .unavailable:
            nil
        case let .rejected(error):
            .failure(error)
        case .failed:
            .failure(LiveKitError(.invalidState, message: "sendData failed"))
        }
    }
}

/// A dispatched write kept for SCTP-level replay on resume.
///
/// Has no token *field*, which is the point: replay hands the same bytes over again, so a retained
/// write that could still resume a submitter would resume it more than once.
struct RetainedWrite {
    let payload: Data
    let sequence: UInt32
    let admission: DrainSendAdmission?

    init(_ write: ReadyWrite) {
        payload = write.payload
        sequence = write.sequence
        admission = write.admission
    }

    /// Re-enters the queue with no waiter to resume.
    var replayed: ReadyWrite {
        ReadyWrite(payload: payload, sequence: sequence, admission: admission)
    }
}

/// Turns one group's prepared bytes into writes, rejecting any that exceeds the negotiated SCTP
/// max-message-size (`0` disables the check). Appends into `group`, a caller-reused scratch, so the
/// per-submit hot path allocates nothing.
///
/// Oversized writes are rejected here because sending more than the negotiated size makes libwebrtc
/// tear the channel down — `sendData` reports success and the channel then closes, breaking every
/// later send.
///
/// Every write carries `submissionToken`, but only the last is marked to settle it, so a submitter
/// resumes once its whole group has been handed over.
func makeWrites(
    from prepared: [PreparedBytes],
    into group: inout [ReadyWrite],
    submissionToken: SendToken?,
    admission: DrainSendAdmission? = nil,
    maxMessageSize: UInt64,
) throws {
    group.removeAll(keepingCapacity: true)
    group.reserveCapacity(prepared.count)

    for (index, bytes) in prepared.enumerated() {
        if maxMessageSize != 0, UInt64(bytes.bytes.count) > maxMessageSize {
            throw LiveKitError(
                .invalidParameter,
                message: "data packet size (\(bytes.bytes.count) bytes) exceeds the negotiated max-message-size (\(maxMessageSize) bytes)",
            )
        }
        group.append(ReadyWrite(
            payload: bytes.bytes,
            sequence: bytes.sequence,
            admission: admission,
            submissionToken: submissionToken,
            settlesSubmission: submissionToken != nil && index == prepared.count - 1,
        ))
    }
}

// MARK: - Stage

/// Per-channel send policy: what a submitted input turns into, what to remember about writes that
/// go out, and what out-of-band requests the channel accepts.
///
/// Driven by ``DataChannelDrain`` from its single event-loop consumer, in FIFO order, so state kept
/// here needs no synchronization of its own.
protocol SendStage: Sendable {
    /// What submitters hand over. One input becomes one group of writes, dispatched in order and
    /// never interleaved with another group's.
    associatedtype Input: Sendable

    /// Out-of-band requests, ordered with the writes. `Never` when the channel has none.
    associatedtype Command: Sendable = Never

    /// Serializes `input` into the bytes to send, appending them in order.
    ///
    /// Runs at submit time, inside the drain's single consumer, so a sequence stamped here matches
    /// the FIFO order in which writes reach `sendData` — which the SFU's per-publisher dedup gate
    /// requires. Throwing rejects the whole input; the drain fails its continuation.
    mutating func prepare(_ input: Input, into prepared: inout [PreparedBytes]) throws

    /// A write reached `sendData`.
    mutating func didDispatch(_ write: ReadyWrite)

    /// The channel reported draining `byteCount` bytes.
    mutating func didDrain(_ byteCount: UInt64)

    /// Handles an out-of-band request. `replay` re-queues a retained write ahead of new work.
    mutating func handle(_ command: Command, replay: (ReadyWrite) -> Void)

    /// The channel is gone: forget anything tied to it.
    mutating func reset()
}

extension SendStage {
    mutating func didDispatch(_: ReadyWrite) {}
    mutating func didDrain(_: UInt64) {}
    mutating func handle(_: Command, replay _: (ReadyWrite) -> Void) {}
    mutating func reset() {}
}

// MARK: - Queue

/// What happens when writes arrive faster than the channel drains.
enum SendOverflow: Sendable {
    /// Queue without bound; every submitter waits its turn, in order.
    case park
    /// Hold only the freshest group, evicting the one waiting before it. A channel swap discards
    /// what was queued for the previous channel: it was already stale.
    ///
    /// - Note: Capacity is fixed at one group, as in rust-sdks' `DataChannelSender`. Add a depth
    ///   when something wants more than "freshest wins".
    case dropOldest
}

/// A drain's outbound queue. Under ``SendOverflow/park`` `inFlight` is the whole queue; under
/// ``SendOverflow/dropOldest`` it is the group being handed over, and `pending` is the one waiting.
struct WriteQueue {
    /// Writes the channel takes next, in order.
    var inFlight: Deque<ReadyWrite> = []
    /// ``SendOverflow/dropOldest`` only: the freshest group, waiting for `inFlight` to empty.
    var pending: [ReadyWrite]?

    var next: ReadyWrite? { inFlight.first }

    mutating func append(_ write: ReadyWrite) {
        inFlight.append(write)
    }

    mutating func append(_ group: [ReadyWrite]) {
        for write in group {
            inFlight.append(write)
        }
    }

    /// Promotes the waiting group once the group being handed over is done, so a group's writes are
    /// never interleaved with another's.
    mutating func promoteIfIdle() {
        guard inFlight.isEmpty, let group = pending else { return }
        pending = nil
        append(group)
    }

    /// Replaces the waiting group, handing back the one it displaced so the caller can settle any
    /// continuations rather than strand them.
    mutating func evict(replacingWith group: [ReadyWrite]) -> [ReadyWrite] {
        let displaced = pending ?? []
        pending = group
        return displaced
    }

    mutating func advance() {
        if !inFlight.isEmpty { inFlight.removeFirst() }
    }

    /// Empties the queue, handing back every write so the caller can settle their continuations.
    mutating func removeAll() -> [ReadyWrite] {
        var removed = pending ?? []
        pending = nil
        while !inFlight.isEmpty {
            removed.append(inFlight.removeFirst())
        }
        return removed
    }

    mutating func remove(submission token: SendToken) -> [ReadyWrite] {
        var removed: [ReadyWrite] = []
        var retained: Deque<ReadyWrite> = []
        while !inFlight.isEmpty {
            let write = inFlight.removeFirst()
            if write.belongs(to: token) {
                removed.append(write)
            } else {
                retained.append(write)
            }
        }
        inFlight = retained
        if let pending {
            self.pending = pending.filter { write in
                if write.belongs(to: token) {
                    removed.append(write)
                    return false
                }
                return true
            }
            if self.pending?.isEmpty == true { self.pending = nil }
        }
        return removed
    }

    /// Empties the queue, settling every waiting submitter with `outcome`.
    mutating func settleAll(_ outcome: Result<Void, any Error>) {
        for write in removeAll() {
            write.settle(with: outcome)
        }
    }

    /// Drops the group being handed over, keeping whatever is waiting behind it, and hands the
    /// discarded writes back so the caller can settle their continuations.
    mutating func dropInFlight() -> [ReadyWrite] {
        var discarded: [ReadyWrite] = []
        while !inFlight.isEmpty {
            discarded.append(inFlight.removeFirst())
        }
        return discarded
    }
}
