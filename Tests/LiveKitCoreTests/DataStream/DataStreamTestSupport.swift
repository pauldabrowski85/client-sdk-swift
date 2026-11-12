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
@testable import LiveKit

/// One incoming data stream packet, with what the data channel knew about it: the sender's
/// identity and SID, the receive generation of the transport, and the encryption type.
enum IncomingStreamTestEvent {
    case header(Livekit_DataStream.Header, String, Participant.Sid?, UInt64, EncryptionType, UInt64)
    case chunk(Livekit_DataStream.Chunk, String, Participant.Sid?, UInt64, EncryptionType)
    case trailer(Livekit_DataStream.Trailer, String, Participant.Sid?, UInt64, EncryptionType)
}

extension IncomingStreamTestEvent {
    static func header(
        _ header: Livekit_DataStream.Header,
        _ identity: String,
        _ sid: Participant.Sid?,
        _ generation: UInt64,
        _ encryptionType: EncryptionType
    ) -> Self {
        .header(header, identity, sid, generation, encryptionType, RpcContinuousClock.nowNanoseconds())
    }
}

/// The incoming stream path of a room that never connects: admission in front of the core.
final class IncomingStreamManager: Sendable {
    let room = Room()

    init() {
        try? room.ensureDataStreamConnection()
    }

    private var admission: IncomingStreamAdmission { room.dataStreams.admission }

    func registerTextStreamHandler(
        for topic: String,
        ordered: Bool = false,
        limits: IncomingStreamLimits = .default,
        onStreamRejected: IncomingStreamRejectionHandler? = nil,
        _ onNewStream: @escaping TextStreamHandler
    ) async throws {
        try room.dataStreams.registerTextStreamHandler(
            for: topic,
            ordered: ordered,
            limits: limits,
            onStreamRejected: onStreamRejected,
            onNewStream
        )
    }

    func registerByteStreamHandler(
        for topic: String,
        limits: IncomingStreamLimits = .default,
        onStreamRejected: IncomingStreamRejectionHandler? = nil,
        _ onNewStream: @escaping ByteStreamHandler
    ) async throws {
        try room.dataStreams.registerByteStreamHandler(for: topic, limits: limits, onStreamRejected: onStreamRejected, onNewStream)
    }

    func unregisterTextStreamHandler(for topic: String) async {
        room.dataStreams.unregisterTextStreamHandler(for: topic)
    }

    func unregisterByteStreamHandler(for topic: String) async {
        room.dataStreams.unregisterByteStreamHandler(for: topic)
    }

    func handle(_ event: IncomingStreamTestEvent) {
        room.incomingStreamManager.handle(event)
    }

    var openStreamCount: Int { get async { admission.openStreamCount } }
    var activeHandlerCount: Int { get async { admission.activeHandlerCount } }
    var failedTopicDiagnosticCount: Int { get async { admission.failedTopicDiagnosticCount } }
    var hasIngressOverflowed: Bool { get async { admission.hasIngressOverflowed } }

    func reset(to nextReceiveGeneration: UInt64? = nil) async {
        room.dataStreams.reset(to: nextReceiveGeneration)
    }

    func closeStreams(
        from identity: Participant.Identity,
        participantSid: Participant.Sid,
        dataPacketReceiveGeneration: UInt64
    ) async {
        room.dataStreams.closeStreams(
            from: identity,
            participantSid: participantSid,
            dataPacketReceiveGeneration: dataPacketReceiveGeneration
        )
    }
}

/// Feeds stream packets into a room the way its data channel does, so a test needs no network.
struct IncomingStreamTestFeed {
    let room: Room

    private var admission: IncomingStreamAdmission { room.dataStreams.admission }

    func handle(_ event: IncomingStreamTestEvent) {
        try? room.ensureDataStreamConnection()
        var packet = Livekit_DataPacket()
        let sender: (identity: String, sid: Participant.Sid?)
        let generation: UInt64
        let encryptionType: EncryptionType
        var receivedAt = RpcContinuousClock.nowNanoseconds()
        switch event {
        case let .header(header, identity, sid, headerGeneration, headerEncryptionType, headerReceivedAt):
            packet = packet.modifying { $0.streamHeader = header }
            (sender, generation, encryptionType, receivedAt) = ((identity, sid), headerGeneration, headerEncryptionType, headerReceivedAt)
        case let .chunk(chunk, identity, sid, chunkGeneration, chunkEncryptionType):
            packet = packet.modifying { $0.streamChunk = chunk }
            (sender, generation, encryptionType) = ((identity, sid), chunkGeneration, chunkEncryptionType)
        case let .trailer(trailer, identity, sid, trailerGeneration, trailerEncryptionType):
            packet = packet.modifying { $0.streamTrailer = trailer }
            (sender, generation, encryptionType) = ((identity, sid), trailerGeneration, trailerEncryptionType)
        }
        packet = packet.modifying {
            $0.participantIdentity = sender.identity
            if let sid = sender.sid { $0.participantSid = sid.stringValue }
        }
        room.dataStreams.handleIncoming(
            packet,
            serialized: nil,
            encryptionType: encryptionType,
            receiveGeneration: generation,
            receivedAtContinuousTimeNanoseconds: receivedAt
        )
    }

    /// Streams whose trailer has not arrived.
    var openStreamCount: Int { get async { admission.openStreamCount } }

    /// Streams whose handler has not returned.
    var activeHandlerCount: Int { get async { admission.activeHandlerCount } }

    func reset(to nextReceiveGeneration: UInt64? = nil) async {
        room.dataStreams.reset(to: nextReceiveGeneration)
    }
}

/// What a test may ask about a room's outgoing streams.
struct OutgoingStreamTestProbe {
    let room: Room

    private var contexts: OutgoingStreamSendContexts? {
        room._state.stage.connection?.outgoingDataStreams.copy().delegate.contexts
    }

    /// Streams that may still send a packet.
    var openStreamCount: Int { get async { contexts?.count ?? 0 } }

    func setOperationObserver(_ observer: (@Sendable (OutgoingStreamOperationKind) -> Void)?) async {
        try? room.ensureDataStreamConnection()
        contexts?.operationObserver.mutate { $0 = observer }
    }
}

extension Room {
    var incomingStreamManager: IncomingStreamTestFeed { IncomingStreamTestFeed(room: self) }
    var outgoingStreamManager: OutgoingStreamTestProbe { OutgoingStreamTestProbe(room: self) }

    /// The data stream managers belong to a connection. A test that never connects gets one here.
    func ensureDataStreamConnection() throws {
        try _state.mutate {
            guard $0.stage.connection == nil else { return }
            try $0.stage.begin(ConnectionDependencies(idle: $0.stage.idle, room: self, roomOptions: $0.roomOptions))
        }
    }
}
