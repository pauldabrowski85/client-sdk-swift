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

internal import LiveKitFFI

/// One admitted incoming stream, shared by ``IncomingStreamAdmission`` and the stream's reader.
///
/// The core reassembles the stream, but it names a sender by identity label only, keeps no
/// per-topic limits and does not hand over the trailer. This carries what admission recorded when
/// the header arrived, and it is how admission ends a stream the core would keep reading.
final class IncomingStreamSource: @unchecked Sendable {
    /// Unique to this stream. A stream ID is not: a sender may reuse one.
    let token: UInt64
    let streamID: String
    let topic: String
    let identity: Participant.Identity
    /// The SID on the header packet. `nil` when the SFU had not filled it in yet.
    let publisherParticipantSid: Participant.Sid?
    let dataPacketReceiveGeneration: UInt64
    let receivedAtContinuousTimeNanoseconds: UInt64
    let encryptionType: EncryptionType
    let attributes: [String: String]
    let limits: IncomingStreamLimits
    let onStreamRejected: IncomingStreamRejectionHandler?
    /// The trailer's attributes land here before the trailer reaches the core, so a reader that
    /// has seen the stream end always observes them.
    let trailer = StreamTrailerStorage()

    private struct State {
        var participantSid: Participant.Sid?
        var failure: Error?
        var finish: (@Sendable (Error) -> Void)?
        var pump: Task<Void, Never>?
    }

    private let state: StateSync<State>

    init(
        token: UInt64,
        streamID: String,
        topic: String,
        identity: Participant.Identity,
        publisherParticipantSid: Participant.Sid?,
        dataPacketReceiveGeneration: UInt64,
        receivedAtContinuousTimeNanoseconds: UInt64,
        encryptionType: EncryptionType,
        attributes: [String: String],
        limits: IncomingStreamLimits,
        onStreamRejected: IncomingStreamRejectionHandler?
    ) {
        self.token = token
        self.streamID = streamID
        self.topic = topic
        self.identity = identity
        self.publisherParticipantSid = publisherParticipantSid
        self.dataPacketReceiveGeneration = dataPacketReceiveGeneration
        self.receivedAtContinuousTimeNanoseconds = receivedAtContinuousTimeNanoseconds
        self.encryptionType = encryptionType
        self.attributes = attributes
        self.limits = limits
        self.onStreamRejected = onStreamRejected
        state = StateSync(State(participantSid: publisherParticipantSid))
    }

    /// The header's SID, or the first one a later packet of the stream carried.
    var participantSid: Participant.Sid? {
        state.read { $0.participantSid }
    }

    func learn(participantSid: Participant.Sid) {
        state.mutate { if $0.participantSid == nil { $0.participantSid = participantSid } }
    }

    /// Ends the reader with `error`. Chunks already buffered are still delivered first.
    ///
    /// - Returns: `false` when the stream had already been ended this way.
    @discardableResult
    func fail(_ error: Error) -> Bool {
        let ended = state.mutate { state -> (finish: (@Sendable (Error) -> Void)?, pump: Task<Void, Never>?)? in
            guard state.failure == nil else { return nil }
            state.failure = error
            return (state.finish, state.pump)
        }
        guard let ended else { return false }
        ended.finish?(error)
        ended.pump?.cancel()
        return true
    }

    /// Connects the reader's buffer.
    ///
    /// - Returns: The error, when admission ended the stream before its reader existed.
    func attach(finish: @escaping @Sendable (Error) -> Void, pump: Task<Void, Never>) -> Error? {
        state.mutate {
            if let failure = $0.failure { return failure }
            $0.finish = finish
            $0.pump = pump
            return nil
        }
    }
}

/// Drains the core's reader into a bounded buffer.
///
/// The core buffers a stream without limit until its reader takes it. Draining here bounds what a
/// handler that stopped reading can make the SDK hold, counts the bytes after decompression, and
/// lets admission end the reader without waiting for the core.
final class GatedStreamChunks<Chunk: Sendable>: @unchecked Sendable {
    private let stream: AsyncThrowingStream<Chunk, Error>

    init(
        source: IncomingStreamSource,
        admission: IncomingStreamAdmission,
        byteCount: @escaping @Sendable (Chunk) -> Int,
        drain: @escaping @Sendable () async throws -> Chunk?
    ) {
        let (stream, continuation) = AsyncThrowingStream<Chunk, Error>.makeStream(
            bufferingPolicy: .bufferingOldest(source.limits.maxBufferedChunks)
        )
        self.stream = stream
        // A reader dropped before its stream ended frees the stream at once, not at the next
        // chunk: a sender that went quiet would otherwise keep the slot.
        continuation.onTermination = { termination in
            if case .cancelled = termination { admission.cancel(source) }
        }
        let pump = Task.detached {
            defer { admission.coreDidEnd(source) }
            var delivered = 0
            do {
                while let chunk = try await drain() {
                    // Admission ended the stream; what the core still sends is not for the reader.
                    guard !Task.isCancelled else { return }
                    let sum = delivered.addingReportingOverflow(byteCount(chunk))
                    delivered = sum.partialValue
                    let maximum = source.limits.maxStreamBytes
                    if sum.overflow || maximum.map({ delivered > $0 }) == true {
                        admission.reject(source, error: .streamSizeExceeded(maximumBytes: maximum ?? Int.max))
                        return
                    }
                    switch continuation.yield(chunk) {
                    case .dropped:
                        admission.reject(source, error: .bufferOverflow)
                        return
                    case .terminated:
                        // The reader is gone, or admission already ended the stream.
                        admission.cancel(source)
                        return
                    default:
                        break
                    }
                }
                continuation.finish()
            } catch {
                continuation.finish(throwing: error)
            }
        }
        if let failure = source.attach(finish: { continuation.finish(throwing: $0) }, pump: pump) {
            continuation.finish(throwing: failure)
            pump.cancel()
        }
    }

    /// Reads are one at a time: the SDK's readers run each through their serial runner. They also
    /// run each read in its own task, which matters here: a cancelled task that awaits an
    /// `AsyncThrowingStream` ends the stream for everyone.
    func next() async throws -> Chunk? {
        var iterator = stream.makeAsyncIterator()
        return try await iterator.next()
    }
}

/// The core's text reader behind admission's bounded buffer.
final class GatedTextStreamReader: LiveKitFFI.TextStreamReaderProtocol, @unchecked Sendable {
    private let reader: any LiveKitFFI.TextStreamReaderProtocol
    private let chunks: GatedStreamChunks<String>

    init(_ reader: any LiveKitFFI.TextStreamReaderProtocol, source: IncomingStreamSource, admission: IncomingStreamAdmission) {
        self.reader = reader
        chunks = GatedStreamChunks(source: source, admission: admission, byteCount: { $0.utf8.count }) {
            try await reader.next()
        }
    }

    func info() -> LiveKitFFI.TextStreamInfo { reader.info() }

    func next() async throws -> String? { try await chunks.next() }

    func readAll() async throws -> String {
        var result = ""
        while let chunk = try await next() {
            result.append(chunk)
        }
        return result
    }
}

/// The core's byte reader behind admission's bounded buffer.
final class GatedByteStreamReader: LiveKitFFI.ByteStreamReaderProtocol, @unchecked Sendable {
    private let reader: any LiveKitFFI.ByteStreamReaderProtocol
    private let chunks: GatedStreamChunks<Data>

    init(_ reader: any LiveKitFFI.ByteStreamReaderProtocol, source: IncomingStreamSource, admission: IncomingStreamAdmission) {
        self.reader = reader
        chunks = GatedStreamChunks(source: source, admission: admission, byteCount: { $0.count }) {
            try await reader.next()
        }
    }

    func info() -> LiveKitFFI.ByteStreamInfo { reader.info() }

    func next() async throws -> Data? { try await chunks.next() }

    func readAll() async throws -> Data {
        var result = Data()
        while let chunk = try await next() {
            result.append(chunk)
        }
        return result
    }
}
