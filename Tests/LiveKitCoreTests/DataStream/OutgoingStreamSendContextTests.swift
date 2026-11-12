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
import Testing
#if canImport(LiveKitTestSupport)
import LiveKitTestSupport
#endif

/// The core builds an outgoing stream's packets, but it does not know which transport the stream
/// was opened on, who may still receive it, or that its caller gave up. Each packet leaves through
/// the send context opened with its stream, which does.
@Suite(.tags(.dataStream))
struct OutgoingStreamSendContextTests {
    private let sent = StateSync<[Livekit_DataPacket]>([])

    private var sentKinds: [String] {
        sent.copy().map {
            switch $0.value {
            case .streamHeader: "header"
            case .streamChunk: "chunk"
            case .streamTrailer: "trailer"
            default: "other"
            }
        }
    }

    private func makeRoom(channel: (@escaping (Livekit_DataPacket) -> Void) -> MockDataChannelPair = { MockDataChannelPair(packetHandler: $0) }) throws -> Room {
        let room = Room()
        room._state.mutate { $0.connectionState = .connected }
        try room.ensureDataStreamConnection()
        room.publisherDataChannel = channel { [sent] packet in sent.mutate { $0.append(packet) } }
        return room
    }

    /// A reconnect replaces the data channel. A stream whose header went out on the old one must
    /// not put its next chunk on the new one, where no receiver saw the header.
    @Test func streamCannotCrossDataChannelSendGenerationAfterHeader() async throws {
        let room = try makeRoom()
        let writer = try await room.localParticipant.streamText(for: "generation-bound")
        #expect(sentKinds == ["header"])

        room.publisherDataChannel.reset()

        await #expect(throws: LiveKitError.self) {
            try await writer.write("must-not-cross")
        }
        #expect(sentKinds == ["header"])
        #expect(await room.outgoingStreamManager.openStreamCount == 0)
        await #expect(throws: (any Error).self) {
            try await writer.close()
        }
        #expect(sentKinds == ["header"])
    }

    /// A header still waiting in the channel when the transport changes is not sent afterwards.
    @Test func headerHeldAcrossASendGenerationChangeIsNotSent() async throws {
        let gate = SendGate()
        let room = try makeRoom { GatedDataChannelPair(gate: gate, packetHandler: $0) }
        let open = Task { try await room.localParticipant.streamText(for: "pending-open") }
        try await gate.entered.wait()

        room.publisherDataChannel.reset()
        gate.release.resume(returning: ())

        await #expect(throws: LiveKitError.self) {
            _ = try await open.value
        }
        #expect(sentKinds.isEmpty)
        #expect(await room.outgoingStreamManager.openStreamCount == 0)
    }

    /// The caller of a one-shot send learns why its last packet failed, and the stream holds
    /// nothing afterwards.
    @Test func sendTextPropagatesTrailerFailureAndClosesTheStream() async throws {
        let room = try makeRoom { FailingTrailerDataChannelPair(packetHandler: $0) }
        // A receiver that takes no single-packet streams, so the send has a trailer.
        let legacy = Livekit_ParticipantInfo.with {
            $0.identity = "legacy"
            $0.sid = "PA_legacy"
        }
        room._state.mutate {
            $0.remoteParticipants[Participant.Identity(from: "legacy")] = RemoteParticipant(info: legacy, room: room, connectionState: .connected)
        }

        let error = await #expect(throws: LiveKitError.self) {
            _ = try await room.localParticipant.sendText("complete-payload", for: "trailer-failure")
        }
        #expect(error?.message == FailingTrailerDataChannelPair.message)
        #expect(sentKinds == ["header", "chunk"])
        #expect(await room.outgoingStreamManager.openStreamCount == 0)
    }

    /// A stream whose caller withdrew it sends nothing more, not even the trailer that would tell
    /// the receiver a complete stream had arrived.
    @Test func cancelledStreamSendsNothingMore() async throws {
        let room = try makeRoom()
        let cancellation = StateSync<OutgoingStreamCancellation?>(nil)
        let writer = try await room.dataStreams.streamText(
            options: StreamTextOptions(topic: "cancelled"),
            onReservation: { reserved in
                cancellation.mutate { $0 = reserved }
                return true
            }
        )
        #expect(sentKinds == ["header"])

        await cancellation.copy()?.cancel()

        await #expect(throws: (any Error).self) {
            try await writer.write("after-cancel")
        }
        await #expect(throws: (any Error).self) {
            try await writer.close()
        }
        #expect(sentKinds == ["header"])
        #expect(await room.outgoingStreamManager.openStreamCount == 0)
    }

    /// The core still holds a cancelled open that waits behind another stream. Its ID stays taken
    /// until the core is done with it, so its header cannot leave with a later stream's context.
    @Test func cancelledOpenKeepsItsIDUntilTheCoreIsDoneWithIt() async throws {
        let gate = SendGate()
        let room = try makeRoom { GatedDataChannelPair(gate: gate, packetHandler: $0) }
        let blocker = Task { try await room.localParticipant.streamText(for: "blocker") }
        try await gate.entered.wait()

        let cancellation = StateSync<OutgoingStreamCancellation?>(nil)
        let reserved = AsyncCompleter<Void>(label: "Reserved", defaultTimeout: 5)
        let queued = Task {
            try await room.dataStreams.streamText(
                options: StreamTextOptions(topic: "reused", id: "reused-id"),
                onReservation: { reservation in
                    cancellation.mutate { $0 = reservation }
                    reserved.resume(returning: ())
                    return true
                }
            )
        }
        try await reserved.wait()
        await cancellation.copy()?.cancel()

        await #expect(throws: StreamError.alreadyOpened) {
            _ = try await room.dataStreams.streamText(options: StreamTextOptions(topic: "reused", id: "reused-id"))
        }

        gate.release.resume(returning: ())
        _ = try await blocker.value
        await #expect(throws: (any Error).self) { _ = try await queued.value }

        let writer = try await room.dataStreams.streamText(options: StreamTextOptions(topic: "reused", id: "reused-id"))
        try await writer.close()
        let reusedHeaders = sent.copy().filter {
            if case let .streamHeader(header) = $0.value { header.streamID == "reused-id" } else { false }
        }
        #expect(reusedHeaders.count == 1)
        #expect(await room.outgoingStreamManager.openStreamCount == 1)
    }

    /// A caller that is already gone when the stream is reserved gets no stream and sends nothing.
    @Test func refusedReservationSendsNothing() async throws {
        let room = try makeRoom()

        await #expect(throws: StreamError.terminated) {
            _ = try await room.dataStreams.streamText(
                options: StreamTextOptions(topic: "refused"),
                onReservation: { _ in false }
            )
        }
        #expect(sentKinds.isEmpty)
        #expect(await room.outgoingStreamManager.openStreamCount == 0)
    }

    /// The admission a stream was opened with is asked again for every packet, so a stream to a
    /// participant connection that has gone stops there.
    @Test func revokedAdmissionStopsTheStream() async throws {
        let room = try makeRoom()
        let admitted = StateSync(true)
        let writer = try await room.dataStreams.streamText(
            options: StreamTextOptions(topic: "admission-bound"),
            admission: DataChannelSendAdmission(predicate: { admitted.copy() })
        )
        #expect(sentKinds == ["header"])

        admitted.mutate { $0 = false }

        let error = await #expect(throws: LiveKitError.self) {
            try await writer.write("after-revoke")
        }
        #expect(error?.type == .cancelled)
        #expect(sentKinds == ["header"])
        #expect(await room.outgoingStreamManager.openStreamCount == 0)
    }

    /// A stream that ends normally leaves no context behind.
    @Test func closedStreamReleasesItsContext() async throws {
        let room = try makeRoom()
        let writer = try await room.localParticipant.streamText(for: "complete")
        #expect(await room.outgoingStreamManager.openStreamCount == 1)

        try await writer.write("payload")
        try await writer.close()

        #expect(sentKinds == ["header", "chunk", "trailer"])
        #expect(await room.outgoingStreamManager.openStreamCount == 0)
    }
}

private struct SendGate: Sendable {
    let entered = AsyncCompleter<Void>(label: "Send entered", defaultTimeout: 5)
    let release = AsyncCompleter<Void>(label: "Send released", defaultTimeout: 5)
}

/// Holds the first send until released, then lets the channel decide as usual.
private final class GatedDataChannelPair: MockDataChannelPair, @unchecked Sendable {
    private let gate: SendGate

    init(gate: SendGate, packetHandler: @escaping (Livekit_DataPacket) -> Void) {
        self.gate = gate
        super.init(packetHandler: packetHandler)
    }

    override func send(dataPacket packet: Livekit_DataPacket, expectedSendGeneration: UInt64) async throws {
        gate.entered.resume(returning: ())
        try await gate.release.wait()
        try await super.send(dataPacket: packet, expectedSendGeneration: expectedSendGeneration)
    }
}

/// Fails the trailer of every stream.
private final class FailingTrailerDataChannelPair: MockDataChannelPair, @unchecked Sendable {
    static let message = "trailer failed"

    override func send(dataPacket packet: Livekit_DataPacket, expectedSendGeneration: UInt64) async throws {
        if case .streamTrailer = packet.value {
            throw LiveKitError(.cancelled, message: Self.message)
        }
        try await super.send(dataPacket: packet, expectedSendGeneration: expectedSendGeneration)
    }
}
