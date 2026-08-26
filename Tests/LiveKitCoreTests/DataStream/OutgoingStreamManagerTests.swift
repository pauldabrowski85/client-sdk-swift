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

@Suite(.tags(.dataStream))
struct OutgoingStreamManagerTests {
    @Test func streamBytes() async throws {
        let testChunks = [
            Data(repeating: 0xAB, count: 128),
            Data(repeating: 0xCD, count: 128),
            Data(repeating: 0xEF, count: 256),
            Data(repeating: 0x12, count: 32),
        ]
        let streamID = UUID().uuidString
        let topic = "some-topic"

        let counter = ConcurrentCounter()

        try await confirmation("Produces header packet") { headerConfirm in
            try await confirmation("Produces chunk packets") { chunkConfirm in
                try await confirmation("Produces trailer packet") { trailerConfirm in
                    let manager = OutgoingStreamManager { packet, _, _ in
                        // Simulate data channel send
                        try await Task.sleep(nanoseconds: 10_000_000)

                        switch packet.value {
                        case let .streamHeader(header):
                            #expect(header.streamID == streamID)
                            #expect(header.topic == topic)
                            #expect(header.mimeType == "application/octet-stream")

                            headerConfirm()

                        case let .streamChunk(chunk):
                            let currentChunk = await counter.increment()
                            #expect(chunk.streamID == streamID)
                            #expect(chunk.chunkIndex == UInt64(currentChunk))
                            #expect(chunk.content == testChunks[currentChunk])

                            if await counter.getCount() == testChunks.count {
                                chunkConfirm()
                            }

                        case let .streamTrailer(trailer):
                            #expect(trailer.streamID == streamID)
                            #expect(trailer.reason == "")

                            trailerConfirm()

                        default: Issue.record("Produced unexpected packet type")
                        }
                    } sendGenerationProvider: {
                        0
                    } encryptionProvider: {
                        .none
                    }

                    let writer = try await manager.streamBytes(
                        options: StreamByteOptions(topic: topic, id: streamID),
                    )

                    for chunk in testChunks {
                        try await writer.write(chunk)
                    }
                    try await writer.close()
                }
            }
        }
    }

    @Test func streamText() async throws {
        let testChunks = [
            String(repeating: "A", count: 128),
            String(repeating: "B", count: 128),
            String(repeating: "C", count: 256),
            String(repeating: "D", count: 32),
        ]
        let streamID = UUID().uuidString
        let topic = "some-topic"

        let counter = ConcurrentCounter()

        try await confirmation("Produces header packet") { headerConfirm in
            try await confirmation("Produces chunk packets") { chunkConfirm in
                try await confirmation("Produces trailer packet") { trailerConfirm in
                    let manager = OutgoingStreamManager { packet, _, _ in
                        // Simulate data channel send
                        try await Task.sleep(nanoseconds: 10_000_000)

                        switch packet.value {
                        case let .streamHeader(header):
                            #expect(header.streamID == streamID)
                            #expect(header.topic == topic)
                            #expect(header.mimeType == "text/plain")

                            headerConfirm()

                        case let .streamChunk(chunk):
                            let currentChunk = await counter.increment()
                            #expect(chunk.streamID == streamID)
                            #expect(chunk.chunkIndex == UInt64(currentChunk))
                            #expect(chunk.content == Data(testChunks[currentChunk].utf8))

                            if await counter.getCount() == testChunks.count {
                                chunkConfirm()
                            }

                        case let .streamTrailer(trailer):
                            #expect(trailer.streamID == streamID)
                            #expect(trailer.reason == "")

                            trailerConfirm()

                        default: Issue.record("Produced unexpected packet type")
                        }
                    } sendGenerationProvider: {
                        0
                    } encryptionProvider: {
                        .none
                    }

                    let writer = try await manager.streamText(
                        options: StreamTextOptions(topic: topic, id: streamID),
                    )

                    for chunk in testChunks {
                        try await writer.write(chunk)
                    }
                    try await writer.close()
                }
            }
        }
    }

    @Test func errorPropagation() async throws {
        let testError = LiveKitError(.cancelled, message: "Test error")

        try await confirmation("Error propagates to caller") { confirm in
            let manager = OutgoingStreamManager { packet, _, _ in
                switch packet.value {
                case .streamChunk:
                    // Wait until first chunk to produce error
                    throw testError
                default: break
                }
            } sendGenerationProvider: {
                0
            } encryptionProvider: {
                .none
            }

            let writer = try await manager.streamText(
                options: StreamTextOptions(topic: "some-topic"),
            )
            do {
                try await writer.write("Hello, world!")
            } catch {
                #expect(error as? LiveKitError == testError)
                confirm()
            }
        }
    }

    @Test func streamCannotCrossDataChannelSendGenerationAfterHeader() async throws {
        let currentGeneration = StateSync<UInt64>(0)
        let sentPacketKinds = StateSync<[String]>([])
        let manager = OutgoingStreamManager { packet, expectedGeneration, _ in
            guard expectedGeneration == currentGeneration.copy() else {
                throw LiveKitError(.cancelled, message: "Data channel generation changed")
            }
            sentPacketKinds.mutate { kinds in
                switch packet.value {
                case .streamHeader: kinds.append("header")
                case .streamChunk: kinds.append("chunk")
                case .streamTrailer: kinds.append("trailer")
                default: kinds.append("other")
                }
            }
        } sendGenerationProvider: {
            currentGeneration.copy()
        } encryptionProvider: {
            .none
        }

        let writer = try await manager.streamText(
            options: StreamTextOptions(topic: "generation-bound")
        )
        #expect(sentPacketKinds.copy() == ["header"])
        currentGeneration.mutate { $0 = 1 }

        await #expect(throws: LiveKitError.self) {
            try await writer.write("must-not-cross")
        }

        #expect(sentPacketKinds.copy() == ["header"])
        #expect(await manager.openStreamCount == 0)
        await #expect(throws: StreamError.self) {
            try await writer.close()
        }
    }

    @Test func sendTextPropagatesTrailerFailureAndTerminalizesDescriptor() async throws {
        let trailerError = LiveKitError(.cancelled, message: "trailer failed")
        let sentPacketKinds = StateSync<[String]>([])
        let manager = OutgoingStreamManager { packet, _, _ in
            switch packet.value {
            case .streamHeader:
                sentPacketKinds.mutate { $0.append("header") }
            case .streamChunk:
                sentPacketKinds.mutate { $0.append("chunk") }
            case .streamTrailer:
                sentPacketKinds.mutate { $0.append("trailer") }
                throw trailerError
            default:
                break
            }
        } sendGenerationProvider: {
            0
        } encryptionProvider: {
            .none
        }

        await #expect(throws: LiveKitError.self) {
            _ = try await manager.sendText(
                "complete-payload",
                options: StreamTextOptions(topic: "trailer-failure")
            )
        }

        #expect(sentPacketKinds.copy() == ["header", "chunk", "trailer"])
        #expect(await manager.openStreamCount == 0)
    }

    @Test func resetDuringPendingHeaderCannotInstallStaleDescriptor() async throws {
        let firstHeader = StateSync(true)
        let firstHeaderEntered = OutgoingTestGate()
        let releaseFirstHeader = OutgoingTestGate()
        let manager = OutgoingStreamManager { packet, _, _ in
            guard case .streamHeader = packet.value else { return }
            let shouldSuspend = firstHeader.mutate { first -> Bool in
                defer { first = false }
                return first
            }
            guard shouldSuspend else { return }
            await firstHeaderEntered.open()
            await releaseFirstHeader.wait()
        } sendGenerationProvider: {
            0
        } encryptionProvider: {
            .none
        }
        let streamID = "reused-after-reset"

        let staleOpen = Task {
            try await manager.streamText(
                options: StreamTextOptions(topic: "pending-open", id: streamID)
            )
        }
        await firstHeaderEntered.wait()

        await manager.reset()
        let replacementWriter = try await manager.streamText(
            options: StreamTextOptions(topic: "pending-open", id: streamID)
        )
        #expect(await manager.openStreamCount == 1)

        await releaseFirstHeader.open()
        await #expect(throws: StreamError.self) {
            _ = try await staleOpen.value
        }
        #expect(await replacementWriter.isOpen)
        #expect(await manager.openStreamCount == 1)
        try await replacementWriter.close()
        #expect(await manager.openStreamCount == 0)
    }

    @Test func concurrentWritesUseOnePerDescriptorFifoAndUniqueChunkIndices() async throws {
        let firstChunkEntered = OutgoingTestGate()
        let releaseFirstChunk = OutgoingTestGate()
        let shouldSuspendFirstChunk = StateSync(true)
        let observedChunks = StateSync<[(UInt64, String)]>([])
        let manager = OutgoingStreamManager { packet, _, _ in
            guard case let .streamChunk(chunk) = packet.value else { return }
            observedChunks.mutate {
                $0.append((chunk.chunkIndex, String(decoding: chunk.content, as: UTF8.self)))
            }
            let shouldSuspend = shouldSuspendFirstChunk.mutate { first -> Bool in
                defer { first = false }
                return first
            }
            guard shouldSuspend else { return }
            await firstChunkEntered.open()
            await releaseFirstChunk.wait()
        } sendGenerationProvider: {
            0
        } encryptionProvider: {
            .none
        }
        let writer = try await manager.streamText(
            options: StreamTextOptions(topic: "concurrent-writes")
        )
        let queuedWrites = StateSync(0)
        await manager.setOperationObserver { operation in
            guard operation == .write else { return }
            queuedWrites.mutate { $0 += 1 }
        }

        let firstWrite = Task { try await writer.write("first") }
        await firstChunkEntered.wait()
        let secondWrite = Task { try await writer.write("second") }
        let deadline = Date().addingTimeInterval(5)
        while queuedWrites.copy() < 2, Date() < deadline {
            await Task.yield()
        }
        #expect(queuedWrites.copy() == 2)

        await releaseFirstChunk.open()
        try await firstWrite.value
        try await secondWrite.value

        #expect(observedChunks.copy().map(\.0) == [0, 1])
        #expect(observedChunks.copy().map(\.1) == ["first", "second"])
        try await writer.close()
    }

    @Test func closeQueuedBehindSuspendedWriteCannotSendTrailerBeforeChunk() async throws {
        let firstChunkEntered = OutgoingTestGate()
        let releaseFirstChunk = OutgoingTestGate()
        let packetOrder = StateSync<[String]>([])
        let manager = OutgoingStreamManager { packet, _, _ in
            switch packet.value {
            case .streamHeader:
                packetOrder.mutate { $0.append("header") }
            case .streamChunk:
                packetOrder.mutate { $0.append("chunk") }
                await firstChunkEntered.open()
                await releaseFirstChunk.wait()
            case .streamTrailer:
                packetOrder.mutate { $0.append("trailer") }
            default:
                break
            }
        } sendGenerationProvider: {
            0
        } encryptionProvider: {
            .none
        }
        let writer = try await manager.streamText(
            options: StreamTextOptions(topic: "write-before-close")
        )
        let closeQueued = StateSync(false)
        await manager.setOperationObserver { operation in
            if operation == .close { closeQueued.mutate { $0 = true } }
        }

        let writeTask = Task { try await writer.write("payload") }
        await firstChunkEntered.wait()
        let closeTask = Task { try await writer.close() }
        let deadline = Date().addingTimeInterval(5)
        while !closeQueued.copy(), Date() < deadline {
            await Task.yield()
        }
        #expect(closeQueued.copy())
        #expect(packetOrder.copy() == ["header", "chunk"])

        await releaseFirstChunk.open()
        try await writeTask.value
        try await closeTask.value
        #expect(packetOrder.copy() == ["header", "chunk", "trailer"])
        #expect(await manager.openStreamCount == 0)
    }
}

private actor OutgoingTestGate {
    private var isOpen = false
    private var waiters: [CheckedContinuation<Void, Never>] = []

    func wait() async {
        guard !isOpen else { return }
        await withCheckedContinuation { waiters.append($0) }
    }

    func open() {
        isOpen = true
        let continuations = waiters
        waiters.removeAll()
        continuations.forEach { $0.resume() }
    }
}
