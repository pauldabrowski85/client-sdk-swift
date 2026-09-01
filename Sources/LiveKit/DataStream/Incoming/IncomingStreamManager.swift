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

// swiftlint:disable file_length

/// Manages state of incoming data streams.
actor IncomingStreamManager: Loggable {
    private struct IngressState {
        var receiveGeneration: UInt64 = 0
        var overflowToken: UInt64 = 0
        var overflowGeneration: UInt64?
    }

    /// Information about an open data stream.
    private struct Descriptor {
        /// Distinguishes this descriptor from others that reuse the same stream
        /// ID, so a stale cleanup can't remove a successor.
        let generation = UUID()
        let info: StreamInfo
        let identity: Participant.Identity
        /// Mutable: a SID learned from a later packet is locked in (see
        /// `senderSidMatches`).
        var participantSid: Participant.Sid?
        let dataPacketReceiveGeneration: UInt64
        let continuation: StreamReaderSource.Continuation
        let limits: IncomingStreamLimits
        let onStreamRejected: IncomingStreamRejectionHandler?
        var readLength = 0
    }

    private struct ByteHandlerRegistration {
        let handler: ByteStreamHandler
        let limits: IncomingStreamLimits
        let onStreamRejected: IncomingStreamRejectionHandler?
    }

    private struct TextHandlerRegistration {
        let handler: TextStreamHandler
        let limits: IncomingStreamLimits
        let onStreamRejected: IncomingStreamRejectionHandler?
    }

    private struct ResolvedHandler {
        let handler: AnyStreamHandler
        let limits: IncomingStreamLimits
        let onStreamRejected: IncomingStreamRejectionHandler?
    }

    private struct StreamOwner: Equatable {
        let identity: Participant.Identity
        let participantSid: Participant.Sid?
        let dataPacketReceiveGeneration: UInt64
    }

    /// Mapping between stream ID and descriptor for open streams.
    private var openStreams: [String: Descriptor] = [:]

    var openStreamCount: Int { openStreams.count }
    var activeHandlerCount: Int {
        activeHandlerOwners.values.reduce(into: 0) { $0 += $1.count }
    }
    var failedTopicDiagnosticCount: Int { failedToOpenStreams.count }
    nonisolated var hasIngressOverflowed: Bool { ingressState.overflowGeneration != nil }
    nonisolated var ingressOverflowTokenForTests: UInt64 { ingressState.overflowToken }
    /// Stream topics without a registered handler.
    private var failedToOpenStreams: Set<String> = []

    private var byteStreamHandlers: [String: ByteHandlerRegistration] = [:]
    private var textStreamHandlers: [String: TextHandlerRegistration] = [:]
    private let ingressState = StateSync(IngressState())

    /// Topics whose handlers preserve wire order (see `registerTextStreamHandler`).
    private var orderedTopics: Set<String> = []
    /// Handlers of streams that are still open on the wire, keyed by topic and
    /// descriptor generation. Open streams gate nothing.
    private var runningHandlers: [String: [UUID: Task<Void, Never>]] = [:]
    /// Handlers of streams already closed on the wire but still executing (e.g.
    /// draining buffered chunks or emitting a finalization). A new stream on the
    /// topic opened after these closed, so its handler must wait for them.
    private var finishingHandlers: [String: [UUID: Task<Void, Never>]] = [:]
    /// Every dispatched handler remains admitted until it returns, even after its
    /// descriptor closes. This prevents a fast header/trailer flood from creating
    /// an unbounded number of detached handler tasks.
    private var activeHandlerOwners: [String: [UUID: StreamOwner]] = [:]

    /// Events are processed in a serial (FIFO) order
    enum StreamEvent {
        case header(Livekit_DataStream.Header, String, Participant.Sid?, UInt64, EncryptionType, UInt64)
        case chunk(Livekit_DataStream.Chunk, String, Participant.Sid?, UInt64, EncryptionType)
        case trailer(Livekit_DataStream.Trailer, String, Participant.Sid?, UInt64, EncryptionType)

        var receiveGeneration: UInt64 {
            switch self {
            case let .header(_, _, _, receiveGeneration, _, _),
                 let .chunk(_, _, _, receiveGeneration, _),
                 let .trailer(_, _, _, receiveGeneration, _):
                receiveGeneration
            }
        }

        static func header(
            _ header: Livekit_DataStream.Header,
            _ identity: String,
            _ participantSid: Participant.Sid?,
            _ receiveGeneration: UInt64,
            _ encryptionType: EncryptionType
        ) -> StreamEvent {
            .header(
                header,
                identity,
                participantSid,
                receiveGeneration,
                encryptionType,
                RpcContinuousClock.nowNanoseconds()
            )
        }
    }

    private struct AdmittedStreamEvent {
        let event: StreamEvent

        var receiveGeneration: UInt64 { event.receiveGeneration }
    }

    private let eventContinuation: AsyncStream<AdmittedStreamEvent>.Continuation
    private var eventLoopTask: AnyTaskCancellable?
    private nonisolated let onIngressOverflow: (@Sendable (UInt64) -> Void)?
    private nonisolated let onEventAdmittedBeforeYield: (@Sendable () -> Void)?

    init(
        eventBufferCapacity: Int = 1_024,
        onIngressOverflow: (@Sendable (UInt64) -> Void)? = nil,
        onEventAdmittedBeforeYield: (@Sendable () -> Void)? = nil
    ) {
        precondition(eventBufferCapacity > 0)
        self.onIngressOverflow = onIngressOverflow
        self.onEventAdmittedBeforeYield = onEventAdmittedBeforeYield
        let (stream, continuation) = AsyncStream.makeStream(
            of: AdmittedStreamEvent.self,
            bufferingPolicy: .bufferingNewest(eventBufferCapacity)
        )
        eventContinuation = continuation

        Task {
            await observe(events: stream)
        }
    }

    private func observe(events stream: AsyncStream<AdmittedStreamEvent>) {
        eventLoopTask = stream.subscribe(self) { observer, event in
            await observer.process(event)
        }
    }

    nonisolated func handle(_ event: StreamEvent) {
        let admittedEvent = AdmittedStreamEvent(
            event: event
        )
        let receiveGeneration = admittedEvent.receiveGeneration
        let overflow = ingressState.mutate { state -> (token: UInt64, dropped: AdmittedStreamEvent)? in
            guard state.receiveGeneration == receiveGeneration,
                  state.overflowGeneration == nil
            else { return nil }

            // Admission and yield are one critical section. Without this, a
            // stale event can pass admission, pause while reset installs a new
            // generation, then evict a current event from the bounded stream.
            onEventAdmittedBeforeYield?()
            guard case let .dropped(droppedEvent) = eventContinuation.yield(admittedEvent) else {
                return nil
            }

            // A generation reset can leave old events in AsyncStream's fixed
            // buffer. A current event may displace one of those stale events;
            // only loss of an event owned by the currently installed
            // generation trips overflow.
            guard droppedEvent.receiveGeneration == state.receiveGeneration else {
                return nil
            }
            state.overflowToken &+= 1
            state.overflowGeneration = receiveGeneration
            return (state.overflowToken, droppedEvent)
        }
        guard let overflow else { return }
        onIngressOverflow?(receiveGeneration)
        Task {
            await failForIngressOverflow(
                receiveGeneration: receiveGeneration,
                overflowToken: overflow.token,
                droppedEvent: overflow.dropped.event
            )
        }
    }

    private func process(_ admittedEvent: AdmittedStreamEvent) {
        guard !hasIngressOverflowed else { return }
        switch admittedEvent.event {
        case let .header(
            header,
            identityString,
            participantSid,
            receiveGeneration,
            encryptionType,
            receivedAtContinuousTimeNanoseconds
        ):
            guard receiveGeneration == ingressState.receiveGeneration else { return }
            handle(
                header: header,
                from: identityString,
                participantSid: participantSid,
                dataPacketReceiveGeneration: receiveGeneration,
                encryptionType: encryptionType,
                receivedAtContinuousTimeNanoseconds: receivedAtContinuousTimeNanoseconds
            )
        case let .chunk(chunk, identity, participantSid, receiveGeneration, encryptionType):
            guard receiveGeneration == ingressState.receiveGeneration else { return }
            handle(
                chunk: chunk,
                from: identity,
                participantSid: participantSid,
                dataPacketReceiveGeneration: receiveGeneration,
                encryptionType: encryptionType
            )
        case let .trailer(trailer, identity, participantSid, receiveGeneration, encryptionType):
            guard receiveGeneration == ingressState.receiveGeneration else { return }
            handle(
                trailer: trailer,
                from: identity,
                participantSid: participantSid,
                dataPacketReceiveGeneration: receiveGeneration,
                encryptionType: encryptionType
            )
        }
    }

    // MARK: - Handler registration

    func registerByteStreamHandler(
        for topic: String,
        limits: IncomingStreamLimits = .default,
        onStreamRejected: IncomingStreamRejectionHandler? = nil,
        _ onNewStream: @escaping ByteStreamHandler
    ) throws {
        guard byteStreamHandlers[topic] == nil else {
            throw StreamError.handlerAlreadyRegistered
        }
        byteStreamHandlers[topic] = ByteHandlerRegistration(
            handler: onNewStream,
            limits: limits,
            onStreamRejected: onStreamRejected
        )
    }

    /// When `ordered` is true, handlers for streams that do not overlap on the
    /// wire run in wire order: a stream opened after another closed waits for the
    /// earlier handler to finish. Streams that are open concurrently are handled
    /// concurrently, so a still-open stream never delays later ones. Off by
    /// default: it would serialize consumers that want strict concurrency (e.g.
    /// RPC request handling).
    ///
    /// Contract: an ordered handler should return promptly once its reader ends —
    /// work it keeps doing after its stream closed delays every later
    /// non-overlapping stream on the topic.
    func registerTextStreamHandler(
        for topic: String,
        ordered: Bool = false,
        limits: IncomingStreamLimits = .default,
        onStreamRejected: IncomingStreamRejectionHandler? = nil,
        _ onNewStream: @escaping TextStreamHandler
    ) throws {
        guard textStreamHandlers[topic] == nil else {
            throw StreamError.handlerAlreadyRegistered
        }
        textStreamHandlers[topic] = TextHandlerRegistration(
            handler: onNewStream,
            limits: limits,
            onStreamRejected: onStreamRejected
        )
        if ordered { orderedTopics.insert(topic) }
    }

    /// SDK-internal: register `onNewStream` for `topic` if no handler is registered yet,
    /// otherwise no-op. Used by idempotent wiring paths (e.g. RPC v2 setup runs on every
    /// connect) that don't want the duplicate-registration throw from the public API.
    @discardableResult
    func registerTextStreamHandlerIfNeeded(
        for topic: String,
        limits: IncomingStreamLimits = .default,
        onStreamRejected: IncomingStreamRejectionHandler? = nil,
        _ onNewStream: @escaping TextStreamHandler
    ) -> Bool {
        guard textStreamHandlers[topic] == nil else { return false }
        textStreamHandlers[topic] = TextHandlerRegistration(
            handler: onNewStream,
            limits: limits,
            onStreamRejected: onStreamRejected
        )
        return true
    }

    func unregisterByteStreamHandler(for topic: String) {
        byteStreamHandlers[topic] = nil
    }

    func unregisterTextStreamHandler(for topic: String) {
        textStreamHandlers[topic] = nil
        orderedTopics.remove(topic)
    }

    // MARK: - Packet processing

    /// Handles a data stream header.
    private func handle(
        header: Livekit_DataStream.Header,
        from identityString: String,
        participantSid: Participant.Sid?,
        dataPacketReceiveGeneration: UInt64,
        encryptionType: EncryptionType,
        receivedAtContinuousTimeNanoseconds: UInt64
    ) {
        let identity = Participant.Identity(from: identityString)

        guard let streamInfo = Self.streamInfo(
            from: header,
            participantSid: participantSid,
            dataPacketReceiveGeneration: dataPacketReceiveGeneration,
            encryptionType: encryptionType,
            receivedAtContinuousTimeNanoseconds: receivedAtContinuousTimeNanoseconds
        ) else {
            rejectionHandler(for: header)?(IncomingStreamRejection(
                streamID: header.streamID,
                topic: header.topic,
                participantIdentity: identity,
                publisherParticipantSid: participantSid,
                dataPacketReceiveGeneration: dataPacketReceiveGeneration,
                attributes: header.attributes,
                handlerWasDispatched: false,
                error: .invalidDeclaredLength
            ))
            return
        }
        openStream(
            with: streamInfo,
            from: identity,
            participantSid: participantSid,
            dataPacketReceiveGeneration: dataPacketReceiveGeneration
        )
    }

    private func openStream(
        with info: StreamInfo,
        from identity: Participant.Identity,
        participantSid: Participant.Sid?,
        dataPacketReceiveGeneration: UInt64
    ) {
        guard openStreams[info.id] == nil else {
            log("Ignoring stream \(info.id) from \(identity): a stream with this ID is already open", .warning)
            return
        }
        guard let registration = handler(for: info) else {
            let topic = info.topic
            if !failedToOpenStreams.contains(topic) {
                if failedToOpenStreams.count < Self.maximumFailedTopicDiagnostics {
                    log("Unable to find handler for incoming stream: \(info.id), topic: \(topic), opened by: \(identity)", .warning)
                    failedToOpenStreams.insert(topic)
                }
            }
            return
        }

        if let totalLength = info.totalLength,
           let maxStreamBytes = registration.limits.maxStreamBytes,
           totalLength > maxStreamBytes
        {
            registration.onStreamRejected?(IncomingStreamRejection(
                streamID: info.id,
                topic: info.topic,
                participantIdentity: identity,
                publisherParticipantSid: participantSid,
                dataPacketReceiveGeneration: dataPacketReceiveGeneration,
                attributes: info.attributes,
                handlerWasDispatched: false,
                error: .streamSizeExceeded(maximumBytes: maxStreamBytes)
            ))
            return
        }

        guard inFlightStreamCount(for: info.topic) < registration.limits.maxConcurrentStreams else {
            registration.onStreamRejected?(IncomingStreamRejection(
                streamID: info.id,
                topic: info.topic,
                participantIdentity: identity,
                publisherParticipantSid: participantSid,
                dataPacketReceiveGeneration: dataPacketReceiveGeneration,
                attributes: info.attributes,
                handlerWasDispatched: false,
                error: .tooManyOpenStreams(maximum: registration.limits.maxConcurrentStreams)
            ))
            return
        }

        if let maximumForConnection = registration.limits.maxConcurrentStreamsPerParticipantConnection {
            let owner = StreamOwner(
                identity: identity,
                participantSid: participantSid,
                dataPacketReceiveGeneration: dataPacketReceiveGeneration
            )
            guard inFlightStreamCount(for: info.topic, ownedBy: owner) < maximumForConnection else {
                registration.onStreamRejected?(IncomingStreamRejection(
                    streamID: info.id,
                    topic: info.topic,
                    participantIdentity: identity,
                    publisherParticipantSid: participantSid,
                    dataPacketReceiveGeneration: dataPacketReceiveGeneration,
                    attributes: info.attributes,
                    handlerWasDispatched: false,
                    error: .tooManyOpenStreams(maximum: maximumForConnection)
                ))
                return
            }
        }

        var continuation: StreamReaderSource.Continuation!
        let source = StreamReaderSource(
            bufferingPolicy: .bufferingOldest(registration.limits.maxBufferedChunks)
        ) {
            continuation = $0
        }

        let descriptor = Descriptor(
            info: info,
            identity: identity,
            participantSid: participantSid,
            dataPacketReceiveGeneration: dataPacketReceiveGeneration,
            continuation: continuation,
            limits: registration.limits,
            onStreamRejected: registration.onStreamRejected,
        )
        openStreams[info.id] = descriptor

        // Set after the descriptor is stored: this task runs at an arbitrary
        // later point, and a sender may have reused the stream ID by then, so
        // it must only remove its own generation.
        continuation.onTermination = { @Sendable [weak self, generation = descriptor.generation] _ in
            guard let self else { return }
            Task { await self.closeStream(with: info.id, generation: generation) }
        }
        let cancelSource: @Sendable () async -> Void = { [weak self, generation = descriptor.generation] in
            await self?.cancelStream(with: info.id, generation: generation)
        }

        activeHandlerOwners[info.topic, default: [:]][descriptor.generation] = StreamOwner(
            identity: identity,
            participantSid: participantSid,
            dataPacketReceiveGeneration: dataPacketReceiveGeneration
        )

        // Detached: handler lifetime is not tied to the descriptor — abnormal stream
        // conditions are signalled through `source` throwing instead.
        if orderedTopics.contains(info.topic) {
            // Wire happens-before: this stream opened after `predecessors` closed,
            // so their handlers must finish first. Same-segment streams never
            // overlap (senders close one before opening the next), which is what
            // makes finalizations and stream-ID reuse race-free.
            let predecessors = Array((finishingHandlers[info.topic] ?? [:]).values)
            let topic = info.topic
            let generation = descriptor.generation
            let task = Task.detached { [weak self] in
                for predecessor in predecessors {
                    await predecessor.value
                }
                do {
                    try await registration.handler(source, identity, cancelSource)
                } catch {
                    self?.log("Text stream handler for topic '\(topic)' threw: \(error)", .warning)
                }
                await self?.handlerCompleted(topic: topic, generation: generation)
            }
            runningHandlers[topic, default: [:]][generation] = task
        } else {
            let topic = info.topic
            let generation = descriptor.generation
            Task.detached { [weak self] in
                do {
                    try await registration.handler(source, identity, cancelSource)
                } catch {
                    self?.log("Text stream handler for topic '\(topic)' threw: \(error)", .warning)
                }
                await self?.handlerCompleted(topic: topic, generation: generation)
            }
        }
    }

    /// Marks the stream's handler as gating later non-overlapping streams on the
    /// same ordered topic. Called wherever a stream is closed on the wire.
    private func streamDidClose(_ descriptor: Descriptor) {
        let topic = descriptor.info.topic
        if let task = runningHandlers[topic]?.removeValue(forKey: descriptor.generation) {
            finishingHandlers[topic, default: [:]][descriptor.generation] = task
        }
    }

    private func handlerCompleted(topic: String, generation: UUID) {
        runningHandlers[topic]?[generation] = nil
        finishingHandlers[topic]?[generation] = nil
        activeHandlerOwners[topic]?[generation] = nil
    }

    /// Counts the union of open descriptors and handlers that have not returned.
    /// A stream remains in flight until both resources are gone.
    private func inFlightStreamCount(
        for topic: String,
        ownedBy expectedOwner: StreamOwner? = nil
    ) -> Int {
        let handlers = activeHandlerOwners[topic] ?? [:]
        var count = handlers.values.lazy.filter { owner in
            expectedOwner == nil || owner == expectedOwner
        }.count
        count += openStreams.values.lazy.filter { descriptor in
            guard descriptor.info.topic == topic,
                  handlers[descriptor.generation] == nil
            else { return false }
            guard let expectedOwner else { return true }
            return descriptor.identity == expectedOwner.identity &&
                descriptor.participantSid == expectedOwner.participantSid &&
                descriptor.dataPacketReceiveGeneration == expectedOwner.dataPacketReceiveGeneration
        }.count
        return count
    }

    /// Close the stream with the given id, unless it has been superseded by a
    /// newer stream reusing the same id.
    private func closeStream(with id: String, generation: UUID) {
        guard openStreams[id]?.generation == generation else { return }
        openStreams[id] = nil
    }

    private func cancelStream(with id: String, generation: UUID) {
        guard let descriptor = openStreams[id], descriptor.generation == generation else { return }
        openStreams[id] = nil
        streamDidClose(descriptor)
        descriptor.continuation.finish(throwing: StreamError.terminated)
    }

    /// Fails open streams owned by one exact participant connection. Identity
    /// alone is not sufficient because it can be reused by a replacement.
    func closeStreams(
        from identity: Participant.Identity,
        participantSid: Participant.Sid,
        dataPacketReceiveGeneration: UInt64
    ) {
        for (id, descriptor) in openStreams
            where descriptor.identity == identity
                && descriptor.participantSid == participantSid
                && descriptor.dataPacketReceiveGeneration == dataPacketReceiveGeneration
        {
            openStreams[id] = nil
            streamDidClose(descriptor)
            descriptor.continuation.finish(throwing: StreamError.terminated)
        }
    }

    /// Fails all open streams. Handler registrations survive so streams arriving
    /// after a reconnect are still handled.
    func reset(to nextDataPacketReceiveGeneration: UInt64? = nil) {
        if let nextDataPacketReceiveGeneration {
            let didAdvance = ingressState.mutate { state -> Bool in
                guard nextDataPacketReceiveGeneration >= state.receiveGeneration else { return false }
                state.receiveGeneration = nextDataPacketReceiveGeneration
                state.overflowToken &+= 1
                state.overflowGeneration = nil
                return true
            }
            guard didAdvance else { return }
            failedToOpenStreams.removeAll()
        }
        for descriptor in openStreams.values {
            streamDidClose(descriptor)
            descriptor.continuation.finish(throwing: StreamError.terminated)
        }
        openStreams.removeAll()
    }

    private static func senderDescription(
        _ identity: String,
        _ sid: Participant.Sid?,
        _ generation: UInt64
    ) -> String {
        "\(identity)/\(sid?.stringValue ?? "nil")@\(generation)"
    }

    /// Whether a later fragment comes from the participant connection that
    /// opened the stream.
    ///
    /// The SID is the connection: when both packets carry one, it alone
    /// decides. Identities legitimately differ across one stream's packets —
    /// an agent publishes a transcription stream attributed to the transcribed
    /// participant's identity while other fragments of the same stream carry
    /// the agent's own identity (observed live on LiveKit Cloud, 2026-09-01).
    /// An absent SID on either side is "not yet known" — the SFU fills
    /// `participant_sid` lazily — and the identity is then the best remaining
    /// signal; once a SID is recorded a later different SID still rejects.
    private static func senderMatches(
        recordedIdentity: Participant.Identity,
        recordedSid: Participant.Sid?,
        identity: Participant.Identity,
        participantSid: Participant.Sid?
    ) -> Bool {
        if let recordedSid, let participantSid {
            return recordedSid == participantSid
        }
        return recordedIdentity == identity
    }

    /// Handles a data stream chunk.
    private func handle(
        chunk: Livekit_DataStream.Chunk,
        from identityString: String,
        participantSid: Participant.Sid?,
        dataPacketReceiveGeneration: UInt64,
        encryptionType: EncryptionType
    ) {
        guard !chunk.content.isEmpty,
              let descriptor = openStreams[chunk.streamID],
              descriptor.dataPacketReceiveGeneration == dataPacketReceiveGeneration else { return }

        guard Self.senderMatches(
            recordedIdentity: descriptor.identity,
            recordedSid: descriptor.participantSid,
            identity: Participant.Identity(from: identityString),
            participantSid: participantSid
        ) else {
            reject(descriptor, streamID: chunk.streamID, error: .senderMismatch(
                expected: Self.senderDescription(
                    descriptor.identity.stringValue,
                    descriptor.participantSid,
                    descriptor.dataPacketReceiveGeneration
                ),
                received: Self.senderDescription(identityString, participantSid, dataPacketReceiveGeneration)
            ))
            return
        }
        if descriptor.participantSid == nil, let participantSid {
            openStreams[chunk.streamID]?.participantSid = participantSid
        }

        // Error paths remove the descriptor synchronously for the same reason as
        // the trailer path: a header reusing this stream ID may be the next event.
        if descriptor.info.encryptionType != encryptionType {
            let error = StreamError.encryptionTypeMismatch(
                expected: descriptor.info.encryptionType,
                received: encryptionType,
            )
            openStreams[chunk.streamID] = nil
            streamDidClose(descriptor)
            descriptor.continuation.finish(throwing: error)
            return
        }

        let lengthAddition = descriptor.readLength.addingReportingOverflow(chunk.content.count)
        guard !lengthAddition.overflow else {
            reject(descriptor, streamID: chunk.streamID, error: .streamSizeExceeded(maximumBytes: Int.max))
            return
        }
        let readLength = lengthAddition.partialValue

        if let totalLength = descriptor.info.totalLength {
            guard readLength <= totalLength else {
                openStreams[chunk.streamID] = nil
                streamDidClose(descriptor)
                descriptor.continuation.finish(throwing: StreamError.lengthExceeded)
                return
            }
        }
        if let maxStreamBytes = descriptor.limits.maxStreamBytes,
           readLength > maxStreamBytes
        {
            reject(
                descriptor,
                streamID: chunk.streamID,
                error: .streamSizeExceeded(maximumBytes: maxStreamBytes)
            )
            return
        }
        openStreams[chunk.streamID]!.readLength = readLength
        if case .dropped = descriptor.continuation.yield(chunk.content) {
            reject(descriptor, streamID: chunk.streamID, error: .bufferOverflow)
        }
    }

    /// Handles a data stream trailer.
    private func handle(
        trailer: Livekit_DataStream.Trailer,
        from identityString: String,
        participantSid: Participant.Sid?,
        dataPacketReceiveGeneration: UInt64,
        encryptionType: EncryptionType
    ) {
        guard let descriptor = openStreams[trailer.streamID],
              descriptor.dataPacketReceiveGeneration == dataPacketReceiveGeneration else {
            return
        }

        guard Self.senderMatches(
            recordedIdentity: descriptor.identity,
            recordedSid: descriptor.participantSid,
            identity: Participant.Identity(from: identityString),
            participantSid: participantSid
        ) else {
            reject(descriptor, streamID: trailer.streamID, error: .senderMismatch(
                expected: Self.senderDescription(
                    descriptor.identity.stringValue,
                    descriptor.participantSid,
                    descriptor.dataPacketReceiveGeneration
                ),
                received: Self.senderDescription(identityString, participantSid, dataPacketReceiveGeneration)
            ))
            return
        }

        // Remove synchronously: senders may reuse a stream ID, and the reopening
        // header is processed by this same event loop right after the trailer.
        // The reader's `onTermination` cleanup runs in its own task and can lose
        // that race, making `openStream` silently drop the new stream.
        openStreams[trailer.streamID] = nil
        streamDidClose(descriptor)

        if descriptor.info.encryptionType != encryptionType {
            let error = StreamError.encryptionTypeMismatch(
                expected: descriptor.info.encryptionType,
                received: encryptionType,
            )
            descriptor.continuation.finish(throwing: error)
            return
        }

        if let totalLength = descriptor.info.totalLength {
            guard descriptor.readLength == totalLength else {
                descriptor.continuation.finish(throwing: StreamError.incomplete)
                return
            }
        }
        guard trailer.reason.isEmpty else {
            // According to protocol documentation, a non-empty reason string indicates an error
            let error = StreamError.abnormalEnd(reason: trailer.reason)
            descriptor.continuation.finish(throwing: error)
            return
        }
        descriptor.continuation.finish()
    }

    // MARK: - Handler resolution

    /// Type-erased stream handler.
    private typealias AnyStreamHandler = @Sendable (
        StreamReaderSource,
        Participant.Identity,
        @escaping @Sendable () async -> Void
    ) async throws -> Void

    /// Finds a registered handler suitable for handling the stream with the given info.
    private func handler(for info: StreamInfo) -> ResolvedHandler? {
        if let info = info as? ByteStreamInfo,
           let registration = byteStreamHandlers[info.topic]
        {
            return ResolvedHandler(
                handler: { source, identity, _ in
                    try await registration.handler(ByteStreamReader(info: info, source: source), identity)
                },
                limits: registration.limits,
                onStreamRejected: registration.onStreamRejected
            )
        }
        if let info = info as? TextStreamInfo,
           let registration = textStreamHandlers[info.topic]
        {
            return ResolvedHandler(
                handler: { source, identity, cancelSource in
                    try await registration.handler(
                        TextStreamReader(info: info, source: source, cancelSource: cancelSource),
                        identity
                    )
                },
                limits: registration.limits,
                onStreamRejected: registration.onStreamRejected
            )
        }
        return nil
    }

    private func reject(_ descriptor: Descriptor, streamID: String, error: StreamError) {
        guard openStreams[streamID]?.generation == descriptor.generation else { return }
        openStreams[streamID] = nil
        streamDidClose(descriptor)
        descriptor.onStreamRejected?(IncomingStreamRejection(
            streamID: descriptor.info.id,
            topic: descriptor.info.topic,
            participantIdentity: descriptor.identity,
            publisherParticipantSid: descriptor.participantSid,
            dataPacketReceiveGeneration: descriptor.dataPacketReceiveGeneration,
            attributes: descriptor.info.attributes,
            handlerWasDispatched: true,
            error: error
        ))
        descriptor.continuation.finish(throwing: error)
    }

    func failForIngressOverflow(
        receiveGeneration: UInt64,
        overflowToken: UInt64,
        droppedEvent: StreamEvent? = nil
    ) {
        guard ingressState.read({ state in
            state.receiveGeneration == receiveGeneration &&
                state.overflowGeneration == receiveGeneration &&
                state.overflowToken == overflowToken
        }) else { return }
        if case let .header(header, identityString, participantSid, _, _, _) = droppedEvent {
            rejectionHandler(for: header)?(IncomingStreamRejection(
                streamID: header.streamID,
                topic: header.topic,
                participantIdentity: Participant.Identity(from: identityString),
                publisherParticipantSid: participantSid,
                dataPacketReceiveGeneration: receiveGeneration,
                attributes: header.attributes,
                handlerWasDispatched: false,
                error: .ingressBufferOverflow
            ))
        }
        let descriptors = Array(openStreams.values)
        openStreams.removeAll()
        for descriptor in descriptors {
            streamDidClose(descriptor)
            descriptor.onStreamRejected?(IncomingStreamRejection(
                streamID: descriptor.info.id,
                topic: descriptor.info.topic,
                participantIdentity: descriptor.identity,
                publisherParticipantSid: descriptor.participantSid,
                dataPacketReceiveGeneration: descriptor.dataPacketReceiveGeneration,
                attributes: descriptor.info.attributes,
                handlerWasDispatched: true,
                error: .ingressBufferOverflow
            ))
            descriptor.continuation.finish(throwing: StreamError.ingressBufferOverflow)
        }
    }

    private func rejectionHandler(
        for header: Livekit_DataStream.Header
    ) -> IncomingStreamRejectionHandler? {
        return switch header.contentHeader {
        case .byteHeader:
            byteStreamHandlers[header.topic]?.onStreamRejected
        case .textHeader:
            textStreamHandlers[header.topic]?.onStreamRejected
        default:
            nil
        }
    }

    private static let maximumFailedTopicDiagnostics = 64

    // MARK: - Clean up

    deinit {
        eventContinuation.finish()
        guard !openStreams.isEmpty else { return }
        for descriptor in openStreams.values {
            descriptor.continuation.finish(throwing: StreamError.terminated)
        }
    }
}

// MARK: - Type aliases

/// Handler for incoming byte data streams.
public typealias ByteStreamHandler = @Sendable (ByteStreamReader, Participant.Identity) async throws -> Void

/// Handler for incoming text data streams.
public typealias TextStreamHandler = @Sendable (TextStreamReader, Participant.Identity) async throws -> Void

// MARK: - From protocol types

extension IncomingStreamManager {
    static func streamInfo(
        from header: Livekit_DataStream.Header,
        participantSid: Participant.Sid?,
        dataPacketReceiveGeneration: UInt64,
        encryptionType: EncryptionType,
        receivedAtContinuousTimeNanoseconds: UInt64
    ) -> StreamInfo? {
        if header.hasTotalLength,
           Int(exactly: header.totalLength) == nil
        {
            return nil
        }
        return switch header.contentHeader {
        case let .byteHeader(byteHeader): ByteStreamInfo(header, byteHeader, encryptionType)
        case let .textHeader(textHeader): TextStreamInfo(
            header,
            textHeader,
            participantSid,
            dataPacketReceiveGeneration,
            encryptionType,
            receivedAtContinuousTimeNanoseconds
        )
        default: nil
        }
    }
}

extension ByteStreamInfo {
    convenience init(
        _ header: Livekit_DataStream.Header,
        _ byteHeader: Livekit_DataStream.ByteHeader,
        _ encryptionType: EncryptionType,
    ) {
        self.init(
            id: header.streamID,
            topic: header.topic,
            timestamp: header.timestampDate,
            totalLength: header.hasTotalLength ? Int(exactly: header.totalLength) : nil,
            attributes: header.attributes,
            encryptionType: encryptionType,
            // ---
            mimeType: header.mimeType,
            name: byteHeader.name,
        )
    }
}

extension TextStreamInfo {
    convenience init(
        _ header: Livekit_DataStream.Header,
        _ textHeader: Livekit_DataStream.TextHeader,
        _ publisherParticipantSid: Participant.Sid?,
        _ dataPacketReceiveGeneration: UInt64,
        _ encryptionType: EncryptionType,
        _ receivedAtContinuousTimeNanoseconds: UInt64 = RpcContinuousClock.nowNanoseconds(),
    ) {
        self.init(
            id: header.streamID,
            topic: header.topic,
            timestamp: header.timestampDate,
            totalLength: header.hasTotalLength ? Int(exactly: header.totalLength) : nil,
            attributes: header.attributes,
            encryptionType: encryptionType,
            // ---
            operationType: TextStreamInfo.OperationType(textHeader.operationType),
            version: Int(textHeader.version),
            replyToStreamID: !textHeader.replyToStreamID.isEmpty ? textHeader.replyToStreamID : nil,
            attachedStreamIDs: textHeader.attachedStreamIds,
            generated: textHeader.generated,
            publisherParticipantSid: publisherParticipantSid,
            dataPacketReceiveGeneration: dataPacketReceiveGeneration,
            receivedAtContinuousTimeNanoseconds: receivedAtContinuousTimeNanoseconds,
        )
    }
}

extension TextStreamInfo.OperationType {
    init(_ operationType: Livekit_DataStream.OperationType) {
        self = Self(rawValue: operationType.rawValue) ?? .create
    }
}

// swiftlint:enable file_length
