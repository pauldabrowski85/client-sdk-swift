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

/// A stream that loses its sender or its transport half way. The reader must find out: a read
/// that waits for a trailer nobody will send holds its handler, and its slot, for good.
@Suite(.serialized, .tags(.dataStream, .e2e))
struct DataStreamInterruptionTests {
    private let topic = "interrupted"

    private struct Received: Sendable {
        var chunks: [String] = []
        var outcomes: [Result<String, Error>] = []
    }

    /// Reads every stream on the topic chunk by chunk, and records how each one ended.
    private func record(on receiver: Room) async throws -> StateSync<Received> {
        let received = StateSync(Received())
        try await receiver.registerTextStreamHandler(for: topic) { reader, _ in
            var text = ""
            do {
                for try await chunk in reader {
                    text += chunk
                    received.mutate { $0.chunks.append(chunk) }
                }
                received.mutate { $0.outcomes.append(.success(text)) }
            } catch {
                received.mutate { $0.outcomes.append(.failure(error)) }
            }
        }
        return received
    }

    private func withPeers(_ body: @escaping @Sendable (_ receiver: Room, _ sender: Room) async throws -> Void) async throws {
        try await TestEnvironment.withRooms([
            RoomTestingOptions(canPublishData: true, canSubscribe: true),
            RoomTestingOptions(canPublishData: true, canSubscribe: true),
        ]) { rooms in
            try await body(rooms[0], rooms[1])
        }
    }

    /// A full reconnect of the receiver ends the streams it was reading: their remaining packets
    /// belong to a session that is gone. The handler stays registered for the streams that follow.
    @Test func receiverFullReconnectEndsInFlightStream() async throws {
        try await withPeers { receiver, sender in
            let received = try await record(on: receiver)
            let writer = try await sender.localParticipant.streamText(for: topic)
            try await writer.write("before")
            try await poll(timeout: 30, for: "the first chunk") { received.copy().chunks == ["before"] }

            try await receiver.debug_simulate(scenario: .fullReconnect)
            #expect(receiver.connectionState == .connected)

            try await poll(timeout: 30, for: "the interrupted read to end") { received.copy().outcomes.count == 1 }
            #expect(throws: StreamError.terminated) { try received.copy().outcomes[0].get() }
            #expect(receiver.dataStreams.admission.openStreamCount == 0)

            // What the sender still writes on the old stream is for nobody.
            try? await writer.write("after")
            try? await writer.close()

            try await sender.localParticipant.sendText("whole", for: topic)
            try await poll(timeout: 30, for: "the stream after the reconnect") { received.copy().outcomes.count == 2 }
            #expect(try received.copy().outcomes[1].get() == "whole")
            #expect(received.copy().chunks.filter { $0 == "after" }.isEmpty)
        }
    }

    /// A sender that leaves in the middle of a stream never sends its trailer. The reader's read
    /// fails instead of waiting for it.
    @Test func readFailsWhenTheSenderLeavesMidStream() async throws {
        try await withPeers { receiver, sender in
            let received = try await record(on: receiver)
            let writer = try await sender.localParticipant.streamText(for: topic)
            try await writer.write("before")
            try await poll(timeout: 30, for: "the first chunk") { received.copy().chunks == ["before"] }

            await sender.disconnect()

            try await poll(timeout: 30, for: "the interrupted read to end") { received.copy().outcomes.count == 1 }
            #expect(throws: StreamError.terminated) { try received.copy().outcomes[0].get() }
            #expect(receiver.dataStreams.admission.openStreamCount == 0)
            try await poll(timeout: 30, for: "the handler slot to be released") {
                receiver.dataStreams.admission.activeHandlerCount == 0
            }
        }
    }

    /// A full reconnect of the sender replaces its participant connection. The stream it had open
    /// cannot continue, so the reader's read fails, and a stream sent afterwards arrives whole.
    @Test func senderFullReconnectEndsTheStreamItWasSending() async throws {
        try await withPeers { receiver, sender in
            let received = try await record(on: receiver)
            let writer = try await sender.localParticipant.streamText(for: topic)
            try await writer.write("before")
            try await poll(timeout: 30, for: "the first chunk") { received.copy().chunks == ["before"] }

            try await sender.debug_simulate(scenario: .fullReconnect)
            #expect(sender.connectionState == .connected)
            await #expect(throws: (any Error).self) { try await writer.write("after") }

            try await poll(timeout: 30, for: "the interrupted read to end") { received.copy().outcomes.count == 1 }
            #expect(throws: StreamError.self) { try received.copy().outcomes[0].get() }

            try await sender.localParticipant.sendText("whole", for: topic)
            try await poll(timeout: 30, for: "the stream after the reconnect") { received.copy().outcomes.count == 2 }
            #expect(try received.copy().outcomes[1].get() == "whole")
            #expect(received.copy().chunks.filter { $0 == "after" }.isEmpty)
        }
    }
}
