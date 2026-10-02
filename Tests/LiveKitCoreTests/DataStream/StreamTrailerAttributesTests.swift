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

/// A sender can attach attributes to a stream's trailer (python agents:
/// `writer.aclose(attributes=...)`). The reader exposes them separately from
/// the header's, once the stream has finished.
@Suite(.tags(.dataStream))
struct StreamTrailerAttributesTests {
    private struct Observed: Equatable {
        let text: String
        let header: [String: String]
        let trailer: [String: String]
    }

    private let topic = "trailer-topic"
    private let sender = Participant.Identity(from: "agent")

    @Test func textTrailerAttributesAreExposedAfterTheStreamEnds() async throws {
        let observed = try await receiveText(
            headerAttributes: ["enact.served": "pick"],
            trailerAttributes: ["enact.served": "backup", "extra": "1"]
        )
        #expect(observed == Observed(
            text: "hello world",
            header: ["enact.served": "pick"],
            trailer: ["enact.served": "backup", "extra": "1"]
        ))
    }

    @Test func textTrailerWithoutAttributesExposesNone() async throws {
        let observed = try await receiveText(
            headerAttributes: ["enact.served": "pick"],
            trailerAttributes: [:]
        )
        #expect(observed.trailer.isEmpty)
        #expect(observed.header == ["enact.served": "pick"])
    }

    @Test func headerAttributesAreNotChangedByTheTrailer() async throws {
        let observed = try await receiveText(
            headerAttributes: ["a": "header", "b": "kept"],
            trailerAttributes: ["a": "trailer"]
        )
        #expect(observed.header == ["a": "header", "b": "kept"])
        #expect(observed.trailer == ["a": "trailer"])
    }

    @Test func trailerAttributesAreEmptyWhileTheStreamIsOpen() async throws {
        let manager = IncomingStreamManager()
        let seenWhileOpen = StateSync<[String: String]?>(nil)
        try await manager.registerTextStreamHandler(for: topic) { reader, _ in
            seenWhileOpen.mutate { $0 = reader.trailerAttributes }
            _ = try await reader.readAll()
        }
        manager.handle(.header(textHeader(streamID: "open", attributes: [:]), sender.stringValue, nil, 0, .none))
        try await poll(timeout: 5, for: "the handler to observe the stream") { seenWhileOpen.copy() != nil }
        #expect(seenWhileOpen.copy() == [:])
        manager.handle(.trailer(trailer(streamID: "open", attributes: ["late": "1"]), sender.stringValue, nil, 0, .none))
        await manager.unregisterTextStreamHandler(for: topic)
    }

    @Test func abnormalTrailerStillCarriesItsAttributes() async throws {
        let manager = IncomingStreamManager()
        let result = StateSync<[String: String]?>(nil)
        try await manager.registerTextStreamHandler(for: topic) { reader, _ in
            do {
                _ = try await reader.readAll()
            } catch {
                result.mutate { $0 = reader.trailerAttributes }
            }
        }
        manager.handle(.header(textHeader(streamID: "abort", attributes: [:]), sender.stringValue, nil, 0, .none))
        let aborted = Livekit_DataStream.Trailer.with {
            $0.streamID = "abort"
            $0.reason = "aborted"
            $0.attributes = ["why": "aborted"]
        }
        manager.handle(.trailer(aborted, sender.stringValue, nil, 0, .none))
        try await poll(timeout: 5, for: "the handler to observe the stream") { result.copy() != nil }
        #expect(result.copy() == ["why": "aborted"])
        await manager.unregisterTextStreamHandler(for: topic)
    }

    @Test func trailerFromAnotherSenderDoesNotInjectAttributes() async throws {
        let manager = IncomingStreamManager()
        let result = StateSync<[String: String]?>(nil)
        try await manager.registerTextStreamHandler(for: topic) { reader, _ in
            do {
                _ = try await reader.readAll()
            } catch {
                result.mutate { $0 = reader.trailerAttributes }
            }
        }
        manager.handle(.header(textHeader(streamID: "forged", attributes: [:]), sender.stringValue, nil, 0, .none))
        manager.handle(.trailer(
            trailer(streamID: "forged", attributes: ["enact.served": "forged"]),
            "someone-else",
            nil,
            0,
            .none
        ))
        try await poll(timeout: 5, for: "the handler to observe the stream") { result.copy() != nil }
        #expect(result.copy() == [:])
        await manager.unregisterTextStreamHandler(for: topic)
    }

    @Test func trailerWithMismatchedEncryptionDoesNotInjectAttributes() async throws {
        let manager = IncomingStreamManager()
        let result = StateSync<[String: String]?>(nil)
        try await manager.registerTextStreamHandler(for: topic) { reader, _ in
            do {
                _ = try await reader.readAll()
            } catch {
                result.mutate { $0 = reader.trailerAttributes }
            }
        }
        manager.handle(.header(textHeader(streamID: "e2ee", attributes: [:]), sender.stringValue, nil, 0, .none))
        manager.handle(.trailer(
            trailer(streamID: "e2ee", attributes: ["enact.served": "injected"]),
            sender.stringValue,
            nil,
            0,
            .gcm
        ))
        try await poll(timeout: 5, for: "the handler to observe the stream") { result.copy() != nil }
        #expect(result.copy() == [:])
        await manager.unregisterTextStreamHandler(for: topic)
    }

    @Test func byteTrailerAttributesAreExposed() async throws {
        let manager = IncomingStreamManager()
        let result = StateSync<[String: String]?>(nil)
        try await manager.registerByteStreamHandler(for: topic) { reader, _ in
            _ = try await reader.readAll()
            result.mutate { $0 = reader.trailerAttributes }
        }
        let header = Livekit_DataStream.Header.with {
            $0.streamID = "bytes"
            $0.topic = topic
            $0.contentHeader = .byteHeader(Livekit_DataStream.ByteHeader())
        }
        manager.handle(.header(header, sender.stringValue, nil, 0, .none))
        manager.handle(.chunk(chunk(streamID: "bytes", index: 0, content: Data([1, 2, 3])), sender.stringValue, nil, 0, .none))
        manager.handle(.trailer(trailer(streamID: "bytes", attributes: ["sha": "abc"]), sender.stringValue, nil, 0, .none))
        try await poll(timeout: 5, for: "the handler to observe the stream") { result.copy() != nil }
        #expect(result.copy() == ["sha": "abc"])
        await manager.unregisterByteStreamHandler(for: topic)
    }

    // MARK: - Helpers

    private func receiveText(
        headerAttributes: [String: String],
        trailerAttributes: [String: String]
    ) async throws -> Observed {
        let manager = IncomingStreamManager()
        let result = StateSync<Observed?>(nil)
        try await manager.registerTextStreamHandler(for: topic) { reader, _ in
            var text = ""
            for try await piece in reader {
                text += piece
            }
            result.mutate {
                $0 = Observed(text: text, header: reader.info.attributes, trailer: reader.trailerAttributes)
            }
        }
        let streamID = UUID().uuidString
        manager.handle(.header(textHeader(streamID: streamID, attributes: headerAttributes), sender.stringValue, nil, 0, .none))
        for (index, piece) in ["hello", " world"].enumerated() {
            manager.handle(.chunk(
                chunk(streamID: streamID, index: index, content: Data(piece.utf8)),
                sender.stringValue,
                nil,
                0,
                .none
            ))
        }
        manager.handle(.trailer(trailer(streamID: streamID, attributes: trailerAttributes), sender.stringValue, nil, 0, .none))

        try await poll(timeout: 5, for: "the handler to observe the stream") { result.copy() != nil }
        await manager.unregisterTextStreamHandler(for: topic)
        return try #require(result.copy())
    }

    private func textHeader(streamID: String, attributes: [String: String]) -> Livekit_DataStream.Header {
        Livekit_DataStream.Header.with {
            $0.streamID = streamID
            $0.topic = topic
            $0.attributes = attributes
            $0.contentHeader = .textHeader(Livekit_DataStream.TextHeader())
        }
    }

    private func chunk(streamID: String, index: Int, content: Data) -> Livekit_DataStream.Chunk {
        Livekit_DataStream.Chunk.with {
            $0.streamID = streamID
            $0.chunkIndex = UInt64(index)
            $0.content = content
        }
    }

    private func trailer(streamID: String, attributes: [String: String]) -> Livekit_DataStream.Trailer {
        Livekit_DataStream.Trailer.with {
            $0.streamID = streamID
            $0.attributes = attributes
        }
    }
}
