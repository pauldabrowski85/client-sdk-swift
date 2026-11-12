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

/// Decides which data stream packets reach the core's incoming manager.
///
/// The core reassembles streams, but it does not know which participant connection a packet came
/// from: it sees an identity label, which a replacement participant reuses, and it accepts a chunk
/// or trailer from anyone. It also has one payload cap for the whole connection. Every stream
/// packet passes here first, where the data packet still carries its sender SID and the receive
/// generation of the transport that delivered it. This is where a stream is bound to its sender,
/// where the per-topic ``IncomingStreamLimits`` apply, and where the trailer's attributes are read.
///
/// Packets are handed to the core while this object's lock is held. The core's entry point only
/// enqueues, and holding the lock keeps its queue in the order admission decided in.
final class IncomingStreamAdmission: @unchecked Sendable, Loggable {
    typealias Core = any LiveKitFFI.IncomingDataStreamManagerProtocol

    /// What a topic's handler registered with.
    struct Registration {
        let limits: IncomingStreamLimits
        let onStreamRejected: IncomingStreamRejectionHandler?
    }

    /// Stamped on every header that is forwarded, so the reader the core opens for it is matched
    /// to its admission exactly; a stream ID cannot do that, because a sender may reuse one. It is
    /// overwritten on the way in and removed from the stream info, so a sender cannot set it.
    static let tokenAttribute = "lk.swift.admission"

    private struct Owner: Equatable {
        let identity: Participant.Identity
        let participantSid: Participant.Sid?
        let dataPacketReceiveGeneration: UInt64
    }

    private struct State {
        var receiveGeneration: UInt64 = 0
        var overflowGeneration: UInt64?
        var nextToken: UInt64 = 0
        /// Streams still open on the wire, by the ID their sender chose.
        var open: [String: IncomingStreamSource] = [:]
        /// Admitted streams the core has not reported open yet.
        var pending: [UInt64: IncomingStreamSource] = [:]
        /// The core the pending and open streams were handed to. A connection has its own.
        /// Weak, not an identifier: the next core can get the address of one that is gone.
        weak var core: AnyObject?
        /// Streams whose handler has not returned. A handler keeps its stream's slot after the
        /// stream closes, so a flood of short streams cannot start handlers without bound.
        var handlers: [UInt64: IncomingStreamSource] = [:]
        /// Topics already reported as having no handler.
        var failedTopics: Set<String> = []
        /// Packets handed to the core, and how many of them it is known to have processed.
        var forwarded: UInt64 = 0
        var acknowledged: UInt64 = 0
    }

    private let state = StateSync(State())
    private let ingressCapacity: UInt64
    private let probeInterval: UInt64
    private let core: @Sendable () -> Core?
    private let onIngressOverflow: @Sendable (UInt64) -> Void

    /// - Parameters:
    ///   - ingressCapacity: How many packets the core may have been handed and not yet processed.
    ///   - core: The current connection's incoming manager, for ending a stream after the fact.
    ///   - onIngressOverflow: Called with the receive generation whose packets overflowed.
    init(
        ingressCapacity: Int = 1024,
        core: @escaping @Sendable () -> Core?,
        onIngressOverflow: @escaping @Sendable (UInt64) -> Void
    ) {
        precondition(ingressCapacity > 0)
        self.ingressCapacity = UInt64(ingressCapacity)
        probeInterval = max(1, UInt64(ingressCapacity) / 16)
        self.core = core
        self.onIngressOverflow = onIngressOverflow
    }

    // MARK: - Introspection

    var receiveGeneration: UInt64 { state.read { $0.receiveGeneration } }
    var openStreamCount: Int { state.read { $0.open.count } }
    /// Streams that hold a handler slot: the handler runs, or will once the core opens the stream.
    var activeHandlerCount: Int { state.read { $0.handlers.count + $0.pending.count } }
    var failedTopicDiagnosticCount: Int { state.read { $0.failedTopics.count } }
    var hasIngressOverflowed: Bool { state.read { $0.overflowGeneration != nil } }

    // MARK: - Packets

    /// Admits `packet`, and hands it to `core` if it may continue.
    ///
    /// - Parameter receiveGeneration: The generation of the transport that delivered the packet.
    ///   `nil` stands for the current one.
    // swiftlint:disable:next function_parameter_count
    func receive(
        _ packet: Livekit_DataPacket,
        serialized: Data?,
        encryptionType: EncryptionType,
        receiveGeneration: UInt64?,
        receivedAtContinuousTimeNanoseconds: UInt64,
        core: Core,
        registration: (Livekit_DataStream.Header) -> Registration?
    ) {
        let header: Livekit_DataStream.Header? = if case let .streamHeader(header) = packet.value { header } else { nil }
        // Looked up before the lock: the registry has its own.
        let registration = header.flatMap(registration)

        var effects: [() -> Void] = []
        state.mutate { state in
            let generation = receiveGeneration ?? state.receiveGeneration
            guard generation == state.receiveGeneration, state.overflowGeneration == nil else { return }

            let coreObject = core as AnyObject
            if state.core !== coreObject {
                // What an earlier core was handed and did not open, it never will.
                let undispatched = Set(state.pending.keys)
                let abandoned = Array(state.open.values) + state.pending.values.filter { state.open[$0.streamID] !== $0 }
                effects.append { Self.interrupt(abandoned, undispatched: undispatched) }
                state.open.removeAll()
                state.pending.removeAll()
                state.acknowledged = state.forwarded
                state.core = coreObject
            }

            guard state.forwarded &- state.acknowledged < ingressCapacity else {
                overflow(&state, droppedHeader: header, from: packet, registration: registration, core: core, effects: &effects)
                return
            }

            let sid = packet.participantSid.isEmpty ? nil : Participant.Sid(from: packet.participantSid)
            switch packet.value {
            case let .streamHeader(header):
                admit(
                    header,
                    in: packet,
                    identity: Participant.Identity(from: packet.participantIdentity),
                    participantSid: sid,
                    encryptionType: encryptionType,
                    receivedAtContinuousTimeNanoseconds: receivedAtContinuousTimeNanoseconds,
                    registration: registration,
                    core: core,
                    state: &state,
                    effects: &effects
                )
            case let .streamChunk(chunk):
                guard let source = state.open[chunk.streamID],
                      source.dataPacketReceiveGeneration == generation,
                      sender(of: packet, participantSid: sid, matches: source, state: &state, core: core, effects: &effects)
                else { return }
                if source.encryptionType != encryptionType {
                    // The core fails the stream on this packet; the reader gets its error.
                    state.open[chunk.streamID] = nil
                }
                forward(packet, serialized: serialized, encryptionType: encryptionType, to: core, state: &state, effects: &effects)
            case let .streamTrailer(trailer):
                guard let source = state.open[trailer.streamID],
                      source.dataPacketReceiveGeneration == generation,
                      sender(of: packet, participantSid: sid, matches: source, state: &state, core: core, effects: &effects)
                else { return }
                // Removed here, not when the core reports the close: a header that reuses the ID
                // may be the next packet.
                state.open[trailer.streamID] = nil
                // After the sender and encryption checks, since a trailer that fails either must
                // not inject attributes, and before the core sees the trailer and ends the stream.
                //
                // The token goes: the core merges a trailer's attributes into the info it reports
                // the stream with, where a sender could replace the one stamped on its header.
                var attributes = trailer.attributes
                let namesToken = attributes.removeValue(forKey: Self.tokenAttribute) != nil
                if source.encryptionType == encryptionType {
                    source.trailer.record(attributes)
                }
                if namesToken {
                    let strippedTrailer = trailer.modifying { $0.attributes = attributes }
                    let stripped = packet.modifying { $0.streamTrailer = strippedTrailer }
                    forward(stripped, serialized: nil, encryptionType: encryptionType, to: core, state: &state, effects: &effects)
                } else {
                    forward(packet, serialized: serialized, encryptionType: encryptionType, to: core, state: &state, effects: &effects)
                }
            default:
                return
            }
        }
        for effect in effects {
            effect()
        }
    }

    // swiftlint:disable:next function_parameter_count function_body_length cyclomatic_complexity
    private func admit(
        _ header: Livekit_DataStream.Header,
        in packet: Livekit_DataPacket,
        identity: Participant.Identity,
        participantSid: Participant.Sid?,
        encryptionType: EncryptionType,
        receivedAtContinuousTimeNanoseconds: UInt64,
        registration: Registration?,
        core: Core,
        state: inout State,
        effects: inout [() -> Void]
    ) {
        let generation = state.receiveGeneration
        let reject: (StreamError) -> () -> Void = { error in
            { registration?.onStreamRejected?(IncomingStreamRejection(
                streamID: header.streamID,
                topic: header.topic,
                participantIdentity: identity,
                publisherParticipantSid: participantSid,
                dataPacketReceiveGeneration: generation,
                attributes: header.attributes,
                handlerWasDispatched: false,
                error: error
            )) }
        }

        // The core drops these without opening a reader. Dropping them here too keeps a header
        // the core refuses from holding a stream slot.
        switch header.contentHeader {
        case .textHeader, .byteHeader: break
        default: return
        }
        guard header.compression == .none || header.compression == .deflateRaw else { return }

        let totalLength: Int?
        if header.hasTotalLength {
            guard let length = Int(exactly: header.totalLength) else {
                effects.append(reject(.invalidDeclaredLength))
                return
            }
            totalLength = length
        } else {
            totalLength = nil
        }

        let id = header.streamID
        let topic = header.topic
        guard state.open[id] == nil else {
            log("Ignoring stream \(id) from \(identity): a stream with this ID is already open", .warning)
            return
        }
        guard let registration else {
            if !state.failedTopics.contains(topic), state.failedTopics.count < Self.maximumFailedTopicDiagnostics {
                log("Unable to find handler for incoming stream: \(id), topic: \(topic), opened by: \(identity)", .warning)
                state.failedTopics.insert(topic)
            }
            return
        }
        let limits = registration.limits

        if let maximum = limits.maxStreamBytes {
            // An inline payload that is compressed is measured once the core has inflated it.
            let inlineLength = header.hasInlineContent && header.compression == .none ? header.inlineContent.count : 0
            guard max(totalLength ?? 0, inlineLength) <= maximum else {
                effects.append(reject(.streamSizeExceeded(maximumBytes: maximum)))
                return
            }
        }

        guard inFlightCount(in: state, topic: topic) < limits.maxConcurrentStreams else {
            effects.append(reject(.tooManyOpenStreams(maximum: limits.maxConcurrentStreams)))
            return
        }
        if let maximum = limits.maxConcurrentStreamsPerParticipantConnection {
            let owner = Owner(identity: identity, participantSid: participantSid, dataPacketReceiveGeneration: generation)
            guard inFlightCount(in: state, topic: topic, ownedBy: owner) < maximum else {
                effects.append(reject(.tooManyOpenStreams(maximum: maximum)))
                return
            }
        }

        let token = state.nextToken
        state.nextToken &+= 1
        var attributes = header.attributes
        attributes[Self.tokenAttribute] = String(token)
        let stampedHeader = header.modifying { $0.attributes = attributes }
        let stamped = packet.modifying { $0.streamHeader = stampedHeader }
        guard let data = try? stamped.serializedData() else { return }

        var publicAttributes = attributes
        publicAttributes[Self.tokenAttribute] = nil
        let source = IncomingStreamSource(
            token: token,
            streamID: id,
            topic: topic,
            identity: identity,
            publisherParticipantSid: participantSid,
            dataPacketReceiveGeneration: generation,
            receivedAtContinuousTimeNanoseconds: receivedAtContinuousTimeNanoseconds,
            encryptionType: encryptionType,
            attributes: publicAttributes,
            limits: limits,
            onStreamRejected: registration.onStreamRejected
        )
        state.pending[token] = source
        // A single-packet stream carries its whole payload in the header: it is never open.
        if !header.hasInlineContent {
            state.open[id] = source
        }
        forward(stamped, serialized: data, encryptionType: encryptionType, to: core, state: &state, effects: &effects)
    }

    /// Whether `packet` comes from the participant connection that opened `source`. A packet that
    /// does not ends the stream.
    ///
    /// The SID is the connection: when both packets carry one, it alone decides. Identities
    /// legitimately differ across one stream's packets — an agent publishes a transcription stream
    /// attributed to the transcribed participant's identity while other fragments of the same
    /// stream carry the agent's own identity (observed live on LiveKit Cloud, 2026-09-01). An
    /// absent SID on either side is "not yet known" — the SFU fills `participant_sid` lazily — and
    /// the identity is then the best remaining signal; once a SID is recorded a later different
    /// SID still rejects.
    // swiftlint:disable:next function_parameter_count
    private func sender(
        of packet: Livekit_DataPacket,
        participantSid: Participant.Sid?,
        matches source: IncomingStreamSource,
        state: inout State,
        core: Core,
        effects: inout [() -> Void]
    ) -> Bool {
        let recordedSid = source.participantSid
        let matches = if let recordedSid, let participantSid {
            recordedSid == participantSid
        } else {
            source.identity.stringValue == packet.participantIdentity
        }
        guard matches else {
            let generation = source.dataPacketReceiveGeneration
            end(source, with: .senderMismatch(
                expected: Self.senderDescription(source.identity.stringValue, recordedSid, generation),
                received: Self.senderDescription(packet.participantIdentity, participantSid, generation)
            ), reportsRejection: true, state: &state, core: core, effects: &effects)
            return false
        }
        if recordedSid == nil, let participantSid {
            source.learn(participantSid: participantSid)
        }
        return true
    }

    private func forward(
        _ packet: Livekit_DataPacket,
        serialized: Data?,
        encryptionType: EncryptionType,
        to core: Core,
        state: inout State,
        effects: inout [() -> Void]
    ) {
        guard let data = try? serialized ?? packet.serializedData() else { return }
        core.handlePacketReceived(packet: data, encryptionType: encryptionType.ffiValue)
        state.forwarded &+= 1
        guard state.forwarded % probeInterval == 0 else { return }
        // The core answers this query in order with the packets it was handed before it, so the
        // answer says they have all been processed. The core has no other way to report that.
        let mark = state.forwarded
        let generation = state.receiveGeneration
        effects.append { [weak self] in
            Task {
                _ = await core.openStreamCount()
                self?.acknowledge(mark, receiveGeneration: generation)
            }
        }
    }

    private func acknowledge(_ mark: UInt64, receiveGeneration: UInt64) {
        state.mutate {
            guard $0.receiveGeneration == receiveGeneration, mark > $0.acknowledged else { return }
            $0.acknowledged = mark
        }
    }

    // MARK: - Ending a stream

    /// Ends `source` because it broke a limit, and reports the rejection to its handler's owner.
    func reject(_ source: IncomingStreamSource, error: StreamError) {
        end(source, with: error, reportsRejection: true)
    }

    /// Ends `source` because its reader no longer wants it.
    func cancel(_ source: IncomingStreamSource) {
        end(source, with: .terminated, reportsRejection: false)
    }

    private func end(_ source: IncomingStreamSource, with error: StreamError, reportsRejection: Bool) {
        var effects: [() -> Void] = []
        state.mutate { state in
            end(source, with: error, reportsRejection: reportsRejection, state: &state, core: core(), effects: &effects)
        }
        for effect in effects {
            effect()
        }
    }

    // swiftlint:disable:next function_parameter_count
    private func end(
        _ source: IncomingStreamSource,
        with error: StreamError,
        reportsRejection: Bool,
        state: inout State,
        core: Core?,
        effects: inout [() -> Void]
    ) {
        // Only while this stream still owns its ID: once its trailer has passed, the ID may
        // belong to a later stream, and the core closes this one without help.
        if state.open[source.streamID] === source {
            state.open[source.streamID] = nil
            close(source, in: core)
        }
        effects.append {
            guard source.fail(error), reportsRejection else { return }
            source.onStreamRejected?(IncomingStreamRejection(
                streamID: source.streamID,
                topic: source.topic,
                participantIdentity: source.identity,
                publisherParticipantSid: source.participantSid,
                dataPacketReceiveGeneration: source.dataPacketReceiveGeneration,
                attributes: source.attributes,
                // A stream the core has not opened yet stays claimable: its handler still runs,
                // and its reader throws this error.
                handlerWasDispatched: true,
                error: error
            ))
        }
    }

    /// Makes the core drop a stream it still holds open. It has no call for that, but a trailer
    /// with a reason ends the stream there, and frees what the core buffered for it.
    private func close(_ source: IncomingStreamSource, in core: Core?) {
        let trailer = Livekit_DataPacket.with {
            $0.streamTrailer = Livekit_DataStream.Trailer.with {
                $0.streamID = source.streamID
                $0.reason = "ended by the receiver"
            }
        }
        guard let core, let data = try? trailer.serializedData() else { return }
        core.handlePacketReceived(packet: data, encryptionType: source.encryptionType.ffiValue)
    }

    /// The core's reader finished `source`, with or without an error.
    func coreDidEnd(_ source: IncomingStreamSource) {
        state.mutate {
            guard $0.open[source.streamID] === source else { return }
            $0.open[source.streamID] = nil
            // A reader can fail by itself (text that is not UTF-8) while the core keeps the stream
            // open. Left open there, the core drops the next header that uses this ID.
            close(source, in: core())
        }
    }

    /// Fails the open streams of one exact participant connection. Identity alone is not
    /// sufficient because a replacement reuses it.
    func closeStreams(
        from identity: Participant.Identity,
        participantSid: Participant.Sid,
        dataPacketReceiveGeneration: UInt64
    ) {
        let sources = state.mutate { state in
            let sources = state.open.values.filter {
                $0.identity == identity
                    && $0.participantSid == participantSid
                    && $0.dataPacketReceiveGeneration == dataPacketReceiveGeneration
            }
            let core = core()
            for source in sources {
                state.open[source.streamID] = nil
                close(source, in: core)
            }
            return sources
        }
        for source in sources {
            source.fail(StreamError.terminated)
        }
    }

    /// Fails every stream that is still open on the wire. Handler registrations are not kept
    /// here, so streams that arrive after a reconnect are still handled.
    ///
    /// - Parameter nextReceiveGeneration: The generation packets must carry from now on. An older
    ///   one than the current leaves everything as it is.
    func reset(to nextReceiveGeneration: UInt64? = nil) {
        let interrupted: (sources: [IncomingStreamSource], undispatched: Set<UInt64>)? = state.mutate { state in
            if let nextReceiveGeneration {
                guard nextReceiveGeneration >= state.receiveGeneration else { return nil }
                state.receiveGeneration = nextReceiveGeneration
                state.overflowGeneration = nil
                state.failedTopics.removeAll()
            }
            // Never back to zero: a probe still on its way would then acknowledge packets that
            // were not forwarded yet.
            state.acknowledged = state.forwarded
            // A stream whose last packet is already with the core is complete: the core still
            // opens it, and its handler reads it whole, with the generation it arrived in.
            let sources = Array(state.open.values)
            var undispatched: Set<UInt64> = []
            for source in sources where state.pending.removeValue(forKey: source.token) != nil {
                undispatched.insert(source.token)
            }
            state.open.removeAll()
            core()?.abortAllStreams()
            return (sources, undispatched)
        }
        if let interrupted {
            Self.interrupt(interrupted.sources, undispatched: interrupted.undispatched)
        }
    }

    /// Ends streams whose connection is gone. A stream whose handler never started is reported
    /// as rejected, since no reader exists to throw the error.
    private static func interrupt(_ sources: [IncomingStreamSource], undispatched: Set<UInt64>) {
        for source in sources where source.fail(StreamError.terminated) && undispatched.contains(source.token) {
            source.onStreamRejected?(IncomingStreamRejection(
                streamID: source.streamID,
                topic: source.topic,
                participantIdentity: source.identity,
                publisherParticipantSid: source.participantSid,
                dataPacketReceiveGeneration: source.dataPacketReceiveGeneration,
                attributes: source.attributes,
                handlerWasDispatched: false,
                error: .terminated
            ))
        }
    }

    private func overflow(
        _ state: inout State,
        droppedHeader: Livekit_DataStream.Header?,
        from packet: Livekit_DataPacket,
        registration: Registration?,
        core: Core,
        effects: inout [() -> Void]
    ) {
        let generation = state.receiveGeneration
        state.overflowGeneration = generation
        let sources = Array(state.open.values) + state.pending.values.filter { state.open[$0.streamID] !== $0 }
        let undispatched = Set(state.pending.keys)
        state.open.removeAll()
        state.pending.removeAll()
        core.abortAllStreams()
        let onIngressOverflow = onIngressOverflow
        effects.append {
            if let droppedHeader {
                registration?.onStreamRejected?(IncomingStreamRejection(
                    streamID: droppedHeader.streamID,
                    topic: droppedHeader.topic,
                    participantIdentity: Participant.Identity(from: packet.participantIdentity),
                    publisherParticipantSid: packet.participantSid.isEmpty ? nil : Participant.Sid(from: packet.participantSid),
                    dataPacketReceiveGeneration: generation,
                    attributes: droppedHeader.attributes,
                    handlerWasDispatched: false,
                    error: .ingressBufferOverflow
                ))
            }
            for source in sources where source.fail(StreamError.ingressBufferOverflow) {
                source.onStreamRejected?(IncomingStreamRejection(
                    streamID: source.streamID,
                    topic: source.topic,
                    participantIdentity: source.identity,
                    publisherParticipantSid: source.participantSid,
                    dataPacketReceiveGeneration: source.dataPacketReceiveGeneration,
                    attributes: source.attributes,
                    handlerWasDispatched: !undispatched.contains(source.token),
                    error: .ingressBufferOverflow
                ))
            }
            onIngressOverflow(generation)
        }
    }

    // MARK: - Readers and handlers

    /// The admission of the stream the core just opened, matched by the token stamped on its
    /// header. The stream holds a handler slot from here until ``handlerCompleted(_:)``.
    ///
    /// - Returns: `nil` for a stream that was reset before the core opened it.
    func claim(attributes: [String: String]) -> IncomingStreamSource? {
        guard let token = attributes[Self.tokenAttribute].flatMap(UInt64.init) else { return nil }
        return state.mutate { state in
            guard let source = state.pending.removeValue(forKey: token) else { return nil }
            state.handlers[token] = source
            return source
        }
    }

    func handlerCompleted(_ source: IncomingStreamSource) {
        state.mutate { $0.handlers[source.token] = nil }
    }

    /// Streams on `topic` that are open, waiting for the core to open them, or still being
    /// handled. A stream stays in flight until all three are over.
    private func inFlightCount(in state: State, topic: String, ownedBy owner: Owner? = nil) -> Int {
        var tokens: Set<UInt64> = []
        for source in [Array(state.open.values), Array(state.pending.values), Array(state.handlers.values)].joined()
            where source.topic == topic
        {
            if let owner, owner != Owner(
                identity: source.identity,
                participantSid: source.participantSid,
                dataPacketReceiveGeneration: source.dataPacketReceiveGeneration
            ) { continue }
            tokens.insert(source.token)
        }
        return tokens.count
    }

    private static func senderDescription(_ identity: String, _ sid: Participant.Sid?, _ generation: UInt64) -> String {
        "\(identity)/\(sid?.stringValue ?? "nil")@\(generation)"
    }

    private static let maximumFailedTopicDiagnostics = 64
}
