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

// swiftlint:disable file_length

import Foundation
@testable import LiveKit
import Testing
#if canImport(LiveKitTestSupport)
import LiveKitTestSupport
#endif

@Suite(.tags(.dataStream))
struct IncomingStreamManagerTests: @unchecked Sendable {
    private var manager: IncomingStreamManager

    private let topicName = "someTopic"
    private let participant = Participant.Identity(from: "someName")

    init() {
        manager = IncomingStreamManager()
    }

    @Test func registerByteHandler() async throws {
        try await manager.registerByteStreamHandler(for: topicName) { _, _ in }

        await confirmation("Throws on duplicate registration") { confirm in
            do {
                try await manager.registerByteStreamHandler(for: topicName) { _, _ in }
            } catch {
                #expect(error as? StreamError == .handlerAlreadyRegistered)
                confirm()
            }
        }

        await manager.unregisterByteStreamHandler(for: topicName)
    }

    @Test func registerTextHandler() async throws {
        try await manager.registerTextStreamHandler(for: topicName) { _, _ in }

        await confirmation("Throws on duplicate registration") { confirm in
            do {
                try await manager.registerTextStreamHandler(for: topicName) { _, _ in }
            } catch {
                #expect(error as? StreamError == .handlerAlreadyRegistered)
                confirm()
            }
        }

        await manager.unregisterTextStreamHandler(for: topicName)
    }

    @Test func byteStream() async throws {
        try await confirmation("Receives payload") { confirm in
            let testChunks = [
                Data(repeating: 0xAB, count: 128),
                Data(repeating: 0xCD, count: 128),
                Data(repeating: 0xEF, count: 256),
                Data(repeating: 0x12, count: 32),
            ]
            let testPayload = testChunks.reduce(Data()) { $0 + $1 }

            try await manager.registerByteStreamHandler(for: topicName) { reader, participant in
                #expect(participant == self.participant)
                let payload = try await reader.readAll()
                #expect(payload == testPayload)
                confirm()
            }

            await sendByteStream(chunks: testChunks)
        }
    }

    @Test func textStream() async throws {
        try await confirmation("Receives payload") { confirm in
            let testChunks = [
                String(repeating: "A", count: 128),
                String(repeating: "B", count: 128),
                String(repeating: "C", count: 256),
                String(repeating: "D", count: 32),
            ]
            let testPayload = testChunks.reduce("") { $0 + $1 }

            try await manager.registerTextStreamHandler(for: topicName) { reader, participant in
                #expect(participant == self.participant)
                let payload = try await reader.readAll()
                #expect(payload == testPayload)
                confirm()
            }

            await sendTextStream(chunks: testChunks)
        }
    }

    @Test func nonTextData() async throws {
        try await confirmation("Throws error on non-text data") { confirm in
            let testPayload = Data(repeating: 0xAB, count: 128)

            try await manager.registerTextStreamHandler(for: topicName) { reader, _ in
                do {
                    _ = try await reader.readAll()
                } catch {
                    #expect(error as? StreamError == .decodeFailed)
                    confirm()
                }
            }

            await sendTextStream(rawPayload: testPayload, totalLength: UInt64(testPayload.count))
        }
    }

    @Test func abnormalClosure() async throws {
        try await confirmation("Throws error on abnormal closure") { confirm in
            let closureReason = "test"

            try await manager.registerByteStreamHandler(for: topicName) { reader, _ in
                do {
                    _ = try await reader.readAll()
                } catch {
                    #expect(error as? StreamError == .abnormalEnd(reason: closureReason))
                    confirm()
                }
            }

            let streamID = UUID().uuidString

            let header = Livekit_DataStream.Header.with {
                $0.streamID = streamID
                $0.topic = topicName
                $0.contentHeader = .byteHeader(Livekit_DataStream.ByteHeader())
            }
            manager.handle(.header(header, participant.stringValue, nil, 0, .none))

            let trailer = Livekit_DataStream.Trailer.with {
                $0.streamID = streamID
                $0.reason = closureReason
            }
            manager.handle(.trailer(trailer, participant.stringValue, nil, 0, .none))

            // Handler processes asynchronously — give it time to complete
            await withCheckedContinuation { (c: CheckedContinuation<Void, Never>) in
                Task {
                    try? await Task.sleep(nanoseconds: 100_000_000)
                    c.resume()
                }
            }
        }
    }

    @Test func incomplete() async throws {
        try await confirmation("Throws error on incomplete stream") { confirm in
            let testPayload = Data(repeating: 0xAB, count: 128)

            try await manager.registerByteStreamHandler(for: topicName) { reader, _ in
                do {
                    _ = try await reader.readAll()
                } catch {
                    #expect(error as? StreamError == .incomplete)
                    confirm()
                }
            }

            let streamID = UUID().uuidString

            let header = Livekit_DataStream.Header.with {
                $0.streamID = streamID
                $0.topic = topicName
                $0.contentHeader = .byteHeader(Livekit_DataStream.ByteHeader())
                $0.totalLength = UInt64(testPayload.count + 10) // expect more bytes
            }
            manager.handle(.header(header, participant.stringValue, nil, 0, .none))

            let chunk = Livekit_DataStream.Chunk.with {
                $0.streamID = streamID
                $0.chunkIndex = 0
                $0.content = Data(testPayload)
            }
            manager.handle(.chunk(chunk, participant.stringValue, nil, 0, .none))

            let trailer = Livekit_DataStream.Trailer.with {
                $0.streamID = streamID
                $0.reason = ""
            }
            manager.handle(.trailer(trailer, participant.stringValue, nil, 0, .none))

            await withCheckedContinuation { (c: CheckedContinuation<Void, Never>) in
                Task {
                    try? await Task.sleep(nanoseconds: 100_000_000)
                    c.resume()
                }
            }
        }
    }

    @Test func encryptionTypeMismatch() async throws {
        let manager = IncomingStreamManager()
        let topic = "test-encryption-mismatch"

        try await confirmation("Stream should receive error") { confirm in
            try await manager.registerByteStreamHandler(for: topic) { reader, _ in
                do {
                    _ = try await reader.readAll()
                } catch let error as StreamError {
                    if case let .encryptionTypeMismatch(expected, received) = error {
                        #expect(expected == .gcm)
                        #expect(received == .none)
                        confirm()
                    } else {
                        Issue.record("Expected encryptionTypeMismatch error, got \(error)")
                    }
                }
            }

            let header = Livekit_DataStream.Header.with {
                $0.streamID = "test-stream-id"
                $0.topic = topic
                $0.mimeType = "application/octet-stream"
                $0.timestamp = Int64(Date().timeIntervalSince1970 * 1000)
                $0.contentHeader = .byteHeader(.with {
                    $0.name = "test-file.bin"
                })
            }
            manager.handle(.header(header, "test-participant", nil, 0, .gcm))

            let chunk = Livekit_DataStream.Chunk.with {
                $0.streamID = "test-stream-id"
                $0.chunkIndex = 0
                $0.content = Data("test data".utf8)
            }
            manager.handle(.chunk(chunk, "test-participant", nil, 0, .none))

            await withCheckedContinuation { (c: CheckedContinuation<Void, Never>) in
                Task {
                    try? await Task.sleep(nanoseconds: 100_000_000)
                    c.resume()
                }
            }
        }
    }

    @Test func declaredTextLengthIsRejectedBeforeHandlerDispatch() async throws {
        let rejected = StateSync<StreamError?>(nil)
        try await manager.registerTextStreamHandler(
            for: topicName,
            limits: IncomingStreamLimits(
                maxStreamBytes: 4,
                maxConcurrentStreams: 2,
                maxBufferedChunks: 2
            ),
            onStreamRejected: { rejection in rejected.mutate { $0 = rejection.error } }
        ) { _, _ in
            Issue.record("Oversized declared stream reached its handler")
        }

        let header = Livekit_DataStream.Header.with {
            $0.streamID = "declared-oversize"
            $0.topic = topicName
            $0.totalLength = 5
            $0.contentHeader = .textHeader(Livekit_DataStream.TextHeader())
        }
        manager.handle(.header(header, participant.stringValue, nil, 0, .none))
        await waitForRejection(rejected)

        #expect(rejected.copy() == .streamSizeExceeded(maximumBytes: 4))
        #expect(await manager.openStreamCount == 0)
    }

    @Test func unrepresentableDeclaredLengthsRejectTextAndByteBeforeReaderCreation() async throws {
        let rejections = StateSync<[StreamError]>([])
        let byteTopic = "\(topicName)-byte"
        try await manager.registerTextStreamHandler(
            for: topicName,
            onStreamRejected: { rejection in rejections.mutate { $0.append(rejection.error) } }
        ) { _, _ in
            Issue.record("Unrepresentable text stream reached its handler")
        }
        try await manager.registerByteStreamHandler(
            for: byteTopic,
            onStreamRejected: { rejection in rejections.mutate { $0.append(rejection.error) } }
        ) { _, _ in
            Issue.record("Unrepresentable byte stream reached its handler")
        }

        let textHeader = Livekit_DataStream.Header.with {
            $0.streamID = "text-uint64-max"
            $0.topic = topicName
            $0.totalLength = UInt64.max
            $0.contentHeader = .textHeader(Livekit_DataStream.TextHeader())
        }
        manager.handle(.header(textHeader, participant.stringValue, nil, 0, .none))

        let byteHeader = Livekit_DataStream.Header.with {
            $0.streamID = "byte-uint64-max"
            $0.topic = byteTopic
            $0.totalLength = UInt64.max
            $0.contentHeader = .byteHeader(Livekit_DataStream.ByteHeader())
        }
        manager.handle(.header(byteHeader, participant.stringValue, nil, 0, .none))

        let deadline = Date().addingTimeInterval(10)
        while rejections.copy().count < 2, Date() < deadline {
            try? await Task.sleep(nanoseconds: 10_000_000)
        }
        #expect(rejections.copy() == [.invalidDeclaredLength, .invalidDeclaredLength])
        #expect(await manager.openStreamCount == 0)
    }

    @Test func suspendedHandlerFloodFailsAtBoundedChunkBuffer() async throws {
        let handlerStarted = TestGate()
        let releaseHandler = TestGate()
        let rejected = StateSync<StreamError?>(nil)
        let observedReaderError = StateSync<StreamError?>(nil)
        try await manager.registerTextStreamHandler(
            for: topicName,
            limits: IncomingStreamLimits(
                maxStreamBytes: 1_024,
                maxConcurrentStreams: 2,
                maxBufferedChunks: 2
            ),
            onStreamRejected: { rejection in rejected.mutate { $0 = rejection.error } }
        ) { reader, _ in
            await handlerStarted.open()
            await releaseHandler.wait()
            do {
                _ = try await reader.readAll()
            } catch let error as StreamError {
                observedReaderError.mutate { $0 = error }
            }
        }

        await sendTextHeader(streamID: "suspended")
        await handlerStarted.wait()
        for index in 0 ..< 3 {
            await sendTextChunk(streamID: "suspended", content: "\(index)")
        }
        await waitForRejection(rejected)

        #expect(rejected.copy() == .bufferOverflow)
        #expect(await manager.openStreamCount == 0)
        await releaseHandler.open()
        await waitForRejection(observedReaderError)
        #expect(observedReaderError.copy() == .bufferOverflow)
        await waitForNoActiveHandlers()
        #expect(await manager.activeHandlerCount == 0)
    }

    @Test func receivedBytesAreRejectedBeforeYieldingPastLimit() async throws {
        let handlerStarted = TestGate()
        let releaseHandler = TestGate()
        let rejected = StateSync<StreamError?>(nil)
        try await manager.registerTextStreamHandler(
            for: topicName,
            limits: IncomingStreamLimits(
                maxStreamBytes: 4,
                maxConcurrentStreams: 2,
                maxBufferedChunks: 8
            ),
            onStreamRejected: { rejection in rejected.mutate { $0 = rejection.error } }
        ) { _, _ in
            await handlerStarted.open()
            await releaseHandler.wait()
        }

        await sendTextHeader(streamID: "received-oversize")
        await handlerStarted.wait()
        await sendTextChunk(streamID: "received-oversize", content: "abc")
        await sendTextChunk(streamID: "received-oversize", content: "de")
        await waitForRejection(rejected)

        #expect(rejected.copy() == .streamSizeExceeded(maximumBytes: 4))
        #expect(await manager.openStreamCount == 0)
        await releaseHandler.open()
    }

    @Test func differentParticipantCannotInjectChunkIntoAuthenticatedStream() async throws {
        let publisherSid = Participant.Sid(from: "PA_authorized")
        let rejected = StateSync<StreamError?>(nil)
        let readerError = StateSync<StreamError?>(nil)
        try await manager.registerTextStreamHandler(
            for: topicName,
            onStreamRejected: { rejection in rejected.mutate { $0 = rejection.error } }
        ) { reader, _ in
            do {
                _ = try await reader.readAll()
            } catch let error as StreamError {
                readerError.mutate { $0 = error }
            }
        }

        let header = Livekit_DataStream.Header.with {
            $0.streamID = "authenticated-chunk"
            $0.topic = topicName
            $0.contentHeader = .textHeader(Livekit_DataStream.TextHeader())
        }
        manager.handle(.header(header, participant.stringValue, publisherSid, 0, .none))
        await waitForOpenStreams(1)

        let chunk = Livekit_DataStream.Chunk.with {
            $0.streamID = header.streamID
            $0.content = Data("forged".utf8)
        }
        manager.handle(.chunk(
            chunk,
            "other-participant",
            Participant.Sid(from: "PA_other"),
            0,
            .none
        ))

        await waitForRejection(rejected)
        await waitForRejection(readerError)
        #expect(rejected.copy() == .senderMismatch)
        #expect(readerError.copy() == .senderMismatch)
        #expect(await manager.openStreamCount == 0)
    }

    @Test func differentParticipantCannotCloseAuthenticatedStreamWithTrailer() async throws {
        let publisherSid = Participant.Sid(from: "PA_authorized")
        let rejected = StateSync<StreamError?>(nil)
        let readerError = StateSync<StreamError?>(nil)
        try await manager.registerTextStreamHandler(
            for: topicName,
            onStreamRejected: { rejection in rejected.mutate { $0 = rejection.error } }
        ) { reader, _ in
            do {
                _ = try await reader.readAll()
            } catch let error as StreamError {
                readerError.mutate { $0 = error }
            }
        }

        let header = Livekit_DataStream.Header.with {
            $0.streamID = "authenticated-trailer"
            $0.topic = topicName
            $0.contentHeader = .textHeader(Livekit_DataStream.TextHeader())
        }
        manager.handle(.header(header, participant.stringValue, publisherSid, 0, .none))
        await waitForOpenStreams(1)

        let trailer = Livekit_DataStream.Trailer.with {
            $0.streamID = header.streamID
        }
        manager.handle(.trailer(
            trailer,
            "other-participant",
            Participant.Sid(from: "PA_other"),
            0,
            .none
        ))

        await waitForRejection(rejected)
        await waitForRejection(readerError)
        #expect(rejected.copy() == .senderMismatch)
        #expect(readerError.copy() == .senderMismatch)
        #expect(await manager.openStreamCount == 0)
    }

    @Test func concurrentStreamAdmissionIsCappedBeforeCreatingAnotherReader() async throws {
        let releaseHandlers = TestGate()
        let rejected = StateSync<StreamError?>(nil)
        try await manager.registerTextStreamHandler(
            for: topicName,
            limits: IncomingStreamLimits(
                maxStreamBytes: 64,
                maxConcurrentStreams: 2,
                maxBufferedChunks: 2
            ),
            onStreamRejected: { rejection in rejected.mutate { $0 = rejection.error } }
        ) { _, _ in
            await releaseHandlers.wait()
        }

        await sendTextHeader(streamID: "open-1")
        await sendTextHeader(streamID: "open-2")
        await waitForOpenStreams(2)
        await sendTextHeader(streamID: "rejected-3")
        await waitForRejection(rejected)

        #expect(rejected.copy() == .tooManyOpenStreams(maximum: 2))
        #expect(await manager.openStreamCount == 2)
        await releaseHandlers.open()
        await manager.reset()
    }

    @Test func fastCloseFloodCannotOutrunHandlerAdmission() async throws {
        let releaseHandlers = TestGate()
        let handlerStarts = StateSync(0)
        let rejections = StateSync<[IncomingStreamRejection]>([])
        try await manager.registerTextStreamHandler(
            for: topicName,
            limits: IncomingStreamLimits(
                maxStreamBytes: 64,
                maxConcurrentStreams: 2,
                maxBufferedChunks: 2
            ),
            onStreamRejected: { rejection in rejections.mutate { $0.append(rejection) } }
        ) { _, _ in
            handlerStarts.mutate { $0 += 1 }
            await releaseHandlers.wait()
        }

        for index in 0 ..< 2 {
            await sendTextHeader(streamID: "admitted-\(index)")
            await sendTextTrailer(streamID: "admitted-\(index)")
        }
        let handlerDeadline = Date().addingTimeInterval(10)
        while handlerStarts.copy() < 2, Date() < handlerDeadline {
            try? await Task.sleep(nanoseconds: 10_000_000)
        }
        await waitForNoOpenStreams()
        #expect(handlerStarts.copy() == 2)
        #expect(await manager.activeHandlerCount == 2)

        for index in 0 ..< 100 {
            await sendTextHeader(streamID: "rejected-\(index)")
            await sendTextTrailer(streamID: "rejected-\(index)")
        }
        let rejectionDeadline = Date().addingTimeInterval(10)
        while rejections.copy().count < 100, Date() < rejectionDeadline {
            try? await Task.sleep(nanoseconds: 10_000_000)
        }

        #expect(rejections.copy().count == 100)
        #expect(rejections.copy().allSatisfy {
            $0.error == .tooManyOpenStreams(maximum: 2) && !$0.handlerWasDispatched
        })
        #expect(handlerStarts.copy() == 2)
        #expect(await manager.openStreamCount == 0)
        #expect(await manager.activeHandlerCount == 2)

        await releaseHandlers.open()
        let completionDeadline = Date().addingTimeInterval(10)
        while await manager.activeHandlerCount != 0, Date() < completionDeadline {
            try? await Task.sleep(nanoseconds: 10_000_000)
        }
        #expect(await manager.activeHandlerCount == 0)
    }

    @Test func perConnectionStreamAdmissionIncludesHandlersAfterDescriptorClose() async throws {
        let releaseHandlers = TestGate()
        let rejections = StateSync<[IncomingStreamRejection]>([])
        try await manager.registerTextStreamHandler(
            for: RpcStreamTopic.request,
            limits: RpcStreamLimits.incomingRequest,
            onStreamRejected: { rejection in rejections.mutate { $0.append(rejection) } }
        ) { _, _ in
            await releaseHandlers.wait()
        }

        let callerA = Participant.Identity(from: "caller-a")
        let callerASid = Participant.Sid(from: "PA_caller_a")
        let callerB = Participant.Identity(from: "caller-b")
        let callerBSid = Participant.Sid(from: "PA_caller_b")
        let receiveGeneration: UInt64 = 7
        await manager.reset(to: receiveGeneration)

        for index in 0 ..< RpcInvocationLimits.maximumInFlightPerConnection {
            sendTextHeader(
                streamID: "caller-a-\(index)",
                topic: RpcStreamTopic.request,
                participant: callerA,
                publisherParticipantSid: callerASid,
                dataPacketReceiveGeneration: receiveGeneration
            )
        }
        await waitForOpenStreams(RpcInvocationLimits.maximumInFlightPerConnection)
        for index in 0 ..< RpcInvocationLimits.maximumInFlightPerConnection {
            let trailer = Livekit_DataStream.Trailer.with {
                $0.streamID = "caller-a-\(index)"
            }
            manager.handle(.trailer(
                trailer,
                callerA.stringValue,
                callerASid,
                receiveGeneration,
                .none
            ))
        }
        await waitForNoOpenStreams()
        #expect(await manager.activeHandlerCount == RpcInvocationLimits.maximumInFlightPerConnection)

        sendTextHeader(
            streamID: "caller-a-over-cap",
            topic: RpcStreamTopic.request,
            participant: callerA,
            publisherParticipantSid: callerASid,
            dataPacketReceiveGeneration: receiveGeneration,
            attributes: [RpcStreamAttribute.requestId: "request-over-cap"]
        )
        let rejectionDeadline = Date().addingTimeInterval(10)
        while rejections.copy().isEmpty, Date() < rejectionDeadline {
            try? await Task.sleep(nanoseconds: 10_000_000)
        }

        sendTextHeader(
            streamID: "caller-b-admitted",
            topic: RpcStreamTopic.request,
            participant: callerB,
            publisherParticipantSid: callerBSid,
            dataPacketReceiveGeneration: receiveGeneration
        )
        await waitForOpenStreams(1)

        let rejection = try #require(rejections.copy().first)
        #expect(rejection.participantIdentity == callerA)
        #expect(rejection.publisherParticipantSid == callerASid)
        #expect(rejection.dataPacketReceiveGeneration == receiveGeneration)
        #expect(rejection.attributes[RpcStreamAttribute.requestId] == "request-over-cap")
        #expect(!rejection.handlerWasDispatched)
        #expect(
            rejection.error == .tooManyOpenStreams(
                maximum: RpcInvocationLimits.maximumInFlightPerConnection
            )
        )
        #expect(await manager.openStreamCount == 1)
        #expect(await manager.activeHandlerCount == RpcInvocationLimits.maximumInFlightPerConnection + 1)
        await releaseHandlers.open()
        await manager.reset()
    }

    @Test func packetEventFloodTripsBoundedIngress() async throws {
        let manager = IncomingStreamManager(eventBufferCapacity: 1)
        let rejected = StateSync<StreamError?>(nil)
        try await manager.registerTextStreamHandler(
            for: topicName,
            onStreamRejected: { rejection in rejected.mutate { $0 = rejection.error } }
        ) { _, _ in }

        for index in 0 ..< 1_000 {
            let header = Livekit_DataStream.Header.with {
                $0.streamID = "flood-\(index)"
                $0.topic = topicName
                $0.contentHeader = .textHeader(Livekit_DataStream.TextHeader())
            }
            manager.handle(.header(header, participant.stringValue, nil, 0, .none))
        }

        #expect(manager.hasIngressOverflowed)
        await waitForRejection(rejected)
        #expect(rejected.copy() == .ingressBufferOverflow)
        let deadline = Date().addingTimeInterval(10)
        while await manager.openStreamCount != 0, Date() < deadline {
            try? await Task.sleep(nanoseconds: 10_000_000)
        }
        #expect(await manager.openStreamCount == 0)
    }

    @Test func staleIngressOverflowCleanupCannotFinishNewGenerationStream() async throws {
        let manager = IncomingStreamManager(eventBufferCapacity: 1)
        let release = TestGate()
        try await manager.registerTextStreamHandler(for: topicName) { _, _ in
            await release.wait()
        }

        for index in 0 ..< 1_000 {
            let header = Livekit_DataStream.Header.with {
                $0.streamID = "old-overflow-\(index)"
                $0.topic = topicName
                $0.contentHeader = .textHeader(Livekit_DataStream.TextHeader())
            }
            manager.handle(.header(header, participant.stringValue, nil, 0, .none))
        }
        #expect(manager.hasIngressOverflowed)
        let oldToken = manager.ingressOverflowTokenForTests

        await manager.reset(to: 1)
        let newHeader = Livekit_DataStream.Header.with {
            $0.streamID = "new-generation"
            $0.topic = topicName
            $0.contentHeader = .textHeader(Livekit_DataStream.TextHeader())
        }
        manager.handle(.header(newHeader, participant.stringValue, nil, 1, .none))
        let deadline = Date().addingTimeInterval(10)
        while await manager.openStreamCount != 1, Date() < deadline {
            try? await Task.sleep(nanoseconds: 10_000_000)
        }

        await manager.failForIngressOverflow(receiveGeneration: 0, overflowToken: oldToken)
        #expect(await manager.openStreamCount == 1)
        await release.open()
        await manager.reset()
    }

    @Test func resetCannotInterleaveBetweenIngressAdmissionAndBoundedYield() async throws {
        let admitted = StateSync(false)
        let releaseYield = DispatchSemaphore(value: 0)
        let firstAdmission = StateSync(true)
        let resetStarted = StateSync(false)
        let resetCompleted = StateSync(false)
        let receivedCurrent = StateSync(false)
        let manager = IncomingStreamManager(
            eventBufferCapacity: 1,
            onEventAdmittedBeforeYield: {
                let shouldPause = firstAdmission.mutate { first -> Bool in
                    defer { first = false }
                    return first
                }
                guard shouldPause else { return }
                admitted.mutate { $0 = true }
                _ = releaseYield.wait(timeout: .now() + 5)
            }
        )
        try await manager.registerTextStreamHandler(for: topicName) { reader, _ in
            if reader.info.dataPacketReceiveGeneration == 1 {
                receivedCurrent.mutate { $0 = true }
            }
        }

        let staleHeader = Livekit_DataStream.Header.with {
            $0.streamID = "admitted-before-reset"
            $0.topic = topicName
            $0.contentHeader = .textHeader(Livekit_DataStream.TextHeader())
        }
        let staleHandle = Task.detached {
            manager.handle(.header(staleHeader, self.participant.stringValue, nil, 0, .none))
        }
        let admissionDeadline = Date().addingTimeInterval(5)
        while !admitted.copy(), Date() < admissionDeadline {
            await Task.yield()
        }
        #expect(admitted.copy())

        let resetTask = Task {
            resetStarted.mutate { $0 = true }
            await manager.reset(to: 1)
            resetCompleted.mutate { $0 = true }
        }
        let resetDeadline = Date().addingTimeInterval(5)
        while !resetStarted.copy(), Date() < resetDeadline {
            await Task.yield()
        }
        #expect(resetStarted.copy())
        #expect(!resetCompleted.copy())

        releaseYield.signal()
        await staleHandle.value
        await resetTask.value
        #expect(resetCompleted.copy())

        let currentHeader = Livekit_DataStream.Header.with {
            $0.streamID = "current-after-reset"
            $0.topic = topicName
            $0.contentHeader = .textHeader(Livekit_DataStream.TextHeader())
        }
        manager.handle(.header(currentHeader, participant.stringValue, nil, 1, .none))

        let deadline = Date().addingTimeInterval(10)
        while !receivedCurrent.copy(), Date() < deadline {
            try? await Task.sleep(nanoseconds: 10_000_000)
        }
        #expect(receivedCurrent.copy())
        #expect(!manager.hasIngressOverflowed)
        await manager.reset()
    }

    @Test func unknownTopicIngressOverflowSurfacesManagerLevelRecoverySignal() async {
        let observedGeneration = StateSync<UInt64?>(nil)
        let manager = IncomingStreamManager(
            eventBufferCapacity: 1,
            onIngressOverflow: { generation in
                observedGeneration.mutate { $0 = generation }
            }
        )

        for index in 0 ..< 1_000 {
            let header = Livekit_DataStream.Header.with {
                $0.streamID = "unknown-overflow-\(index)"
                $0.topic = "unregistered-\(index)"
                $0.contentHeader = .textHeader(Livekit_DataStream.TextHeader())
            }
            manager.handle(.header(header, participant.stringValue, nil, 0, .none))
        }

        #expect(manager.hasIngressOverflowed)
        #expect(observedGeneration.copy() == 0)
        #expect(await manager.openStreamCount == 0)
    }

    @Test func unknownTopicDiagnosticRetentionIsBoundedAndClearedOnGenerationReset() async {
        let manager = IncomingStreamManager(eventBufferCapacity: 256)
        for index in 0 ..< 128 {
            let header = Livekit_DataStream.Header.with {
                $0.streamID = "unknown-stream-\(index)"
                $0.topic = "attacker-topic-\(index)"
                $0.contentHeader = .textHeader(Livekit_DataStream.TextHeader())
            }
            manager.handle(.header(header, participant.stringValue, nil, 0, .none))
        }

        let deadline = Date().addingTimeInterval(10)
        while await manager.failedTopicDiagnosticCount < 64, Date() < deadline {
            try? await Task.sleep(nanoseconds: 10_000_000)
        }
        #expect(await manager.failedTopicDiagnosticCount == 64)

        await manager.reset(to: 1)
        #expect(await manager.failedTopicDiagnosticCount == 0)
    }

    // MARK: - Helpers

    private func sendByteStream(chunks: [Data]) async {
        let streamID = UUID().uuidString

        let header = Livekit_DataStream.Header.with {
            $0.streamID = streamID
            $0.topic = topicName
            $0.contentHeader = .byteHeader(Livekit_DataStream.ByteHeader())
        }
        manager.handle(.header(header, participant.stringValue, nil, 0, .none))

        for (index, chunkData) in chunks.enumerated() {
            let chunk = Livekit_DataStream.Chunk.with {
                $0.streamID = streamID
                $0.chunkIndex = UInt64(index)
                $0.content = chunkData
            }
            manager.handle(.chunk(chunk, participant.stringValue, nil, 0, .none))
        }

        let trailer = Livekit_DataStream.Trailer.with {
            $0.streamID = streamID
            $0.reason = ""
        }
        manager.handle(.trailer(trailer, participant.stringValue, nil, 0, .none))

        // Handler processes asynchronously — give it time to complete
        await withCheckedContinuation { (c: CheckedContinuation<Void, Never>) in
            Task {
                try? await Task.sleep(nanoseconds: 100_000_000)
                c.resume()
            }
        }
    }

    private func sendTextStream(
        chunks: [String]? = nil,
        rawPayload: Data? = nil,
        totalLength: UInt64? = nil,
        streamID: String = UUID().uuidString,
        dataPacketReceiveGeneration: UInt64 = 0,
        settle: Bool = true
    ) async {
        let header = Livekit_DataStream.Header.with {
            $0.streamID = streamID
            $0.topic = topicName
            $0.contentHeader = .textHeader(Livekit_DataStream.TextHeader())
            if let totalLength { $0.totalLength = totalLength }
        }
        manager.handle(.header(
            header,
            participant.stringValue,
            nil,
            dataPacketReceiveGeneration,
            .none
        ))

        if let chunks {
            for (index, chunkData) in chunks.enumerated() {
                let chunk = Livekit_DataStream.Chunk.with {
                    $0.streamID = streamID
                    $0.chunkIndex = UInt64(index)
                    $0.content = Data(chunkData.utf8)
                }
                manager.handle(.chunk(
                    chunk,
                    participant.stringValue,
                    nil,
                    dataPacketReceiveGeneration,
                    .none
                ))
            }
        } else if let rawPayload {
            let chunk = Livekit_DataStream.Chunk.with {
                $0.streamID = streamID
                $0.chunkIndex = 0
                $0.content = rawPayload
            }
            manager.handle(.chunk(
                chunk,
                participant.stringValue,
                nil,
                dataPacketReceiveGeneration,
                .none
            ))
        }

        let trailer = Livekit_DataStream.Trailer.with {
            $0.streamID = streamID
            $0.reason = ""
        }
        manager.handle(.trailer(
            trailer,
            participant.stringValue,
            nil,
            dataPacketReceiveGeneration,
            .none
        ))

        guard settle else { return }
        // Handler processes asynchronously — give it time to complete
        await withCheckedContinuation { (c: CheckedContinuation<Void, Never>) in
            Task {
                try? await Task.sleep(nanoseconds: 100_000_000)
                c.resume()
            }
        }
    }
}

/// One-shot latch: `wait()` suspends until `open()`; waiters after `open()` pass through.
private actor TestGate {
    private var isOpen = false
    private var waiters: [CheckedContinuation<Void, Never>] = []

    func wait() async {
        guard !isOpen else { return }
        await withCheckedContinuation { waiters.append($0) }
    }

    func open() {
        isOpen = true
        let continuations = waiters
        waiters = []
        for continuation in continuations {
            continuation.resume()
        }
    }
}

private struct ObservedStreamProvenance: Equatable {
    let payload: String
    let publisherParticipantSid: String?
    let dataPacketReceiveGeneration: UInt64?
}

extension IncomingStreamManagerTests {
    /// `handle(_:)` only enqueues onto the manager's event loop, so tests that
    /// call cleanup APIs directly must first wait for the events to be processed.
    private func waitForOpenStreams(_ count: Int) async {
        let deadline = Date().addingTimeInterval(10)
        while await manager.openStreamCount < count, Date() < deadline {
            try? await Task.sleep(nanoseconds: 10_000_000)
        }
    }

    private func waitForNoOpenStreams() async {
        let deadline = Date().addingTimeInterval(10)
        while await manager.openStreamCount != 0, Date() < deadline {
            try? await Task.sleep(nanoseconds: 10_000_000)
        }
    }

    private func waitForNoActiveHandlers() async {
        let deadline = Date().addingTimeInterval(10)
        while await manager.activeHandlerCount != 0, Date() < deadline {
            try? await Task.sleep(nanoseconds: 10_000_000)
        }
    }

    private func waitForRejection(_ error: StateSync<StreamError?>) async {
        let deadline = Date().addingTimeInterval(10)
        while error.copy() == nil, Date() < deadline {
            try? await Task.sleep(nanoseconds: 10_000_000)
        }
    }

    private func sendTextHeader(
        streamID: String,
        publisherParticipantSid: Participant.Sid? = nil,
        dataPacketReceiveGeneration: UInt64 = 0
    ) async {
        let header = Livekit_DataStream.Header.with {
            $0.streamID = streamID
            $0.topic = topicName
            $0.contentHeader = .textHeader(Livekit_DataStream.TextHeader())
        }
        manager.handle(.header(
            header,
            participant.stringValue,
            publisherParticipantSid,
            dataPacketReceiveGeneration,
            .none
        ))
    }

    private func sendTextHeader(
        streamID: String,
        topic: String,
        participant: Participant.Identity,
        publisherParticipantSid: Participant.Sid?,
        dataPacketReceiveGeneration: UInt64,
        attributes: [String: String] = [:]
    ) {
        let header = Livekit_DataStream.Header.with {
            $0.streamID = streamID
            $0.topic = topic
            $0.attributes = attributes
            $0.contentHeader = .textHeader(Livekit_DataStream.TextHeader())
        }
        manager.handle(.header(
            header,
            participant.stringValue,
            publisherParticipantSid,
            dataPacketReceiveGeneration,
            .none
        ))
    }

    private func sendTextChunk(
        streamID: String,
        content: String,
        publisherParticipantSid: Participant.Sid? = nil,
        dataPacketReceiveGeneration: UInt64 = 0
    ) async {
        let chunk = Livekit_DataStream.Chunk.with {
            $0.streamID = streamID
            $0.content = Data(content.utf8)
        }
        manager.handle(.chunk(
            chunk,
            participant.stringValue,
            publisherParticipantSid,
            dataPacketReceiveGeneration,
            .none
        ))
    }

    private func sendTextTrailer(
        streamID: String,
        publisherParticipantSid: Participant.Sid? = nil,
        dataPacketReceiveGeneration: UInt64 = 0
    ) async {
        let trailer = Livekit_DataStream.Trailer.with {
            $0.streamID = streamID
        }
        manager.handle(.trailer(
            trailer,
            participant.stringValue,
            publisherParticipantSid,
            dataPacketReceiveGeneration,
            .none
        ))
    }

    /// Senders may reuse one stream ID for consecutive streams (each `sendText`
    /// in a transcription segment does). Descriptor cleanup used to run in the
    /// reader's `onTermination` task, which raced the reopening header and made
    /// `openStream` silently drop the new stream.
    @Test func reusedStreamIDDeliversEveryStream() async throws {
        let payloads = ["one", "two", "three"]
        let received = StateSync<[String]>([])

        try await manager.registerTextStreamHandler(for: topicName) { reader, _ in
            let payload = try await reader.readAll()
            received.mutate { $0.append(payload) }
        }

        // Back-to-back, no settling between streams: the reopening header must
        // hit the event loop while the previous stream's cleanup could still be
        // pending.
        let streamID = UUID().uuidString
        for payload in payloads {
            await sendTextStream(chunks: [payload], streamID: streamID, settle: false)
        }

        let deadline = Date().addingTimeInterval(10)
        while received.copy().count < payloads.count, Date() < deadline {
            try? await Task.sleep(nanoseconds: 10_000_000)
        }

        #expect(received.copy().sorted() == payloads.sorted())
        await manager.unregisterTextStreamHandler(for: topicName)
    }

    /// A stream failing mid-flight (here: exceeding its declared length) must not
    /// block a new stream that immediately reuses the same stream ID.
    @Test func reusedStreamIDAfterChunkErrorDeliversNextStream() async throws {
        let received = StateSync<[String]>([])

        try await manager.registerTextStreamHandler(for: topicName) { reader, _ in
            let payload = try await reader.readAll()
            received.mutate { $0.append(payload) }
        }

        let streamID = UUID().uuidString
        // 8-byte chunk against a declared total of 4 → lengthExceeded.
        await sendTextStream(rawPayload: Data("ABCDEFGH".utf8), totalLength: 4, streamID: streamID, settle: false)
        await sendTextStream(chunks: ["ok"], streamID: streamID, settle: false)

        let deadline = Date().addingTimeInterval(10)
        while received.copy().isEmpty, Date() < deadline {
            try? await Task.sleep(nanoseconds: 10_000_000)
        }

        #expect(received.copy() == ["ok"])
        await manager.unregisterTextStreamHandler(for: topicName)
    }

    /// Ordering must compose transitively: C waits on B even while B is itself
    /// still waiting on A. If the chain breaks, B and C complete while A's
    /// handler is gated and the order comes out wrong.
    @Test func orderedTopicChainsAcrossFinishingHandlers() async throws {
        let received = StateSync<[String]>([])
        let gate = TestGate()

        try await manager.registerTextStreamHandler(for: topicName, ordered: true) { reader, _ in
            let payload = try await reader.readAll()
            // First handler stalls after its stream closed, becoming a
            // still-finishing predecessor for the streams sent after it.
            if payload == "a" { await gate.wait() }
            received.mutate { $0.append(payload) }
        }

        for payload in ["a", "b", "c"] {
            await sendTextStream(chunks: [payload], settle: false)
        }
        await gate.open()

        let deadline = Date().addingTimeInterval(10)
        while received.copy().count < 3, Date() < deadline {
            try? await Task.sleep(nanoseconds: 10_000_000)
        }

        #expect(received.copy() == ["a", "b", "c"])
        await manager.unregisterTextStreamHandler(for: topicName)
    }

    /// A still-open stream must not delay streams that overlap with it on the
    /// wire (e.g. a user's live transcript arriving while an agent's message
    /// stream is still open). Ordering applies only to non-overlapping streams.
    @Test func orderedTopicDoesNotDelayOverlappingStreams() async throws {
        let received = StateSync<[String]>([])

        try await manager.registerTextStreamHandler(for: topicName, ordered: true) { reader, _ in
            let payload = try await reader.readAll()
            received.mutate { $0.append(payload) }
        }

        // Stream A opens and stays open; stream B opens, delivers, and closes
        // while A is still open — B's handler must complete without waiting.
        await sendTextHeader(streamID: "open-a")
        await sendTextChunk(streamID: "open-a", content: "a")
        await sendTextStream(chunks: ["b"], streamID: "b", settle: false)

        var deadline = Date().addingTimeInterval(10)
        while received.copy().isEmpty, Date() < deadline {
            try? await Task.sleep(nanoseconds: 10_000_000)
        }
        #expect(received.copy() == ["b"])

        // A still completes normally once its trailer arrives.
        await sendTextTrailer(streamID: "open-a")
        deadline = Date().addingTimeInterval(10)
        while received.copy().count < 2, Date() < deadline {
            try? await Task.sleep(nanoseconds: 10_000_000)
        }
        #expect(received.copy() == ["b", "a"])
        await manager.unregisterTextStreamHandler(for: topicName)
    }

    /// A sender disconnecting before its trailer leaves the stream open forever;
    /// `closeStreams(from:)` must fail it so an ordered topic's queue drains.
    @Test func closeStreamsUnblocksOrderedTopic() async throws {
        let received = StateSync<[String]>([])
        let errors = StateSync<[StreamError]>([])
        let participantSid = Participant.Sid(from: "PA_orphan")

        try await manager.registerTextStreamHandler(for: topicName, ordered: true) { reader, _ in
            do {
                let payload = try await reader.readAll()
                received.mutate { $0.append(payload) }
            } catch let error as StreamError {
                errors.mutate { $0.append(error) }
                throw error
            }
        }

        // Header only — no trailer ever arrives, so the handler blocks in readAll
        // and, at the head of the ordered queue, would block every later stream.
        await sendTextHeader(streamID: "orphan", publisherParticipantSid: participantSid)
        await waitForOpenStreams(1)

        await manager.closeStreams(
            from: participant,
            participantSid: participantSid,
            dataPacketReceiveGeneration: 0
        )
        await sendTextStream(chunks: ["after"], settle: false)

        let deadline = Date().addingTimeInterval(10)
        while received.copy().isEmpty, Date() < deadline {
            try? await Task.sleep(nanoseconds: 10_000_000)
        }

        #expect(received.copy() == ["after"])
        #expect(errors.copy() == [.terminated])
        await waitForNoActiveHandlers()
        #expect(await manager.openStreamCount == 0)
        #expect(await manager.activeHandlerCount == 0)
        await manager.unregisterTextStreamHandler(for: topicName)
    }

    /// Same shape as above via the room-lifecycle path: `reset()` fails all open
    /// streams but keeps handlers registered for after a reconnect.
    @Test func resetUnblocksOrderedTopicAndKeepsHandler() async throws {
        let received = StateSync<[String]>([])

        try await manager.registerTextStreamHandler(for: topicName, ordered: true) { reader, _ in
            let payload = try await reader.readAll()
            received.mutate { $0.append(payload) }
        }

        await sendTextHeader(streamID: "orphan")
        await waitForOpenStreams(1)

        await manager.reset()
        await sendTextStream(chunks: ["after-reset"], settle: false)

        let deadline = Date().addingTimeInterval(10)
        while received.copy().isEmpty, Date() < deadline {
            try? await Task.sleep(nanoseconds: 10_000_000)
        }

        #expect(received.copy() == ["after-reset"])
        await waitForNoActiveHandlers()
        #expect(await manager.openStreamCount == 0)
        #expect(await manager.activeHandlerCount == 0)
        await manager.unregisterTextStreamHandler(for: topicName)
    }

    /// Handlers for an `ordered` topic must observe streams in wire order, not
    /// the scheduling order of independently spawned handler tasks.
    @Test func orderedTopicDeliversStreamsInOrder() async throws {
        let count = 16
        let received = StateSync<[String]>([])

        try await manager.registerTextStreamHandler(for: topicName, ordered: true) { reader, _ in
            let payload = try await reader.readAll()
            received.mutate { $0.append(payload) }
        }

        for index in 0 ..< count {
            await sendTextStream(chunks: ["payload-\(index)"], settle: false)
        }

        let deadline = Date().addingTimeInterval(10)
        while received.copy().count < count, Date() < deadline {
            try? await Task.sleep(nanoseconds: 10_000_000)
        }

        #expect(received.copy() == (0 ..< count).map { "payload-\($0)" })
        await manager.unregisterTextStreamHandler(for: topicName)
    }

    /// A closed stream whose handler is queued behind a predecessor retains the
    /// generation stamped at header admission. A new stream can reuse the same
    /// participant SID after reset without making the old handler look current.
    @Test func delayedHandlerRetainsHeaderGenerationAcrossSameSidReconnect() async throws {
        let firstHandlerStarted = TestGate()
        let releaseFirstHandler = TestGate()
        let replacementHandlerCompleted = TestGate()
        let observations = StateSync<[ObservedStreamProvenance]>([])
        let publisherSid = Participant.Sid(from: "PA_reused")

        try await manager.registerTextStreamHandler(for: topicName, ordered: true) { reader, _ in
            let payload = try await reader.readAll()
            if payload == "first" {
                await firstHandlerStarted.open()
                await releaseFirstHandler.wait()
                return
            }
            observations.mutate {
                $0.append(ObservedStreamProvenance(
                    payload: payload,
                    publisherParticipantSid: reader.info.publisherParticipantSid?.stringValue,
                    dataPacketReceiveGeneration: reader.info.dataPacketReceiveGeneration
                ))
            }
            if payload == "replacement" { await replacementHandlerCompleted.open() }
        }

        await sendTextStream(chunks: ["first"], streamID: "first", settle: false)
        await firstHandlerStarted.wait()

        await sendTextHeader(
            streamID: "old-queued",
            publisherParticipantSid: publisherSid,
            dataPacketReceiveGeneration: 0
        )
        await waitForOpenStreams(1)
        await sendTextChunk(
            streamID: "old-queued",
            content: "old",
            publisherParticipantSid: publisherSid,
            dataPacketReceiveGeneration: 0
        )
        await sendTextTrailer(
            streamID: "old-queued",
            publisherParticipantSid: publisherSid,
            dataPacketReceiveGeneration: 0
        )
        await waitForNoOpenStreams()

        await manager.reset(to: 1)
        await sendTextHeader(
            streamID: "replacement",
            publisherParticipantSid: publisherSid,
            dataPacketReceiveGeneration: 1
        )
        await waitForOpenStreams(1)
        await sendTextChunk(
            streamID: "replacement",
            content: "replacement",
            publisherParticipantSid: publisherSid,
            dataPacketReceiveGeneration: 1
        )
        await sendTextTrailer(
            streamID: "replacement",
            publisherParticipantSid: publisherSid,
            dataPacketReceiveGeneration: 1
        )

        #expect(observations.copy().isEmpty)
        await releaseFirstHandler.open()
        await replacementHandlerCompleted.wait()

        #expect(observations.copy() == [
            ObservedStreamProvenance(
                payload: "old",
                publisherParticipantSid: publisherSid.stringValue,
                dataPacketReceiveGeneration: 0
            ),
            ObservedStreamProvenance(
                payload: "replacement",
                publisherParticipantSid: publisherSid.stringValue,
                dataPacketReceiveGeneration: 1
            ),
        ])
        await manager.unregisterTextStreamHandler(for: topicName)
    }

    @Test func resetRejectsStaleHeaderChunkAndTrailerGenerations() async throws {
        let received = StateSync<[String]>([])
        await manager.reset(to: 1)

        try await manager.registerTextStreamHandler(for: topicName) { reader, _ in
            let payload = try await reader.readAll()
            received.mutate { $0.append(payload) }
        }

        await sendTextStream(
            chunks: ["stale-header"],
            streamID: "stale-header",
            dataPacketReceiveGeneration: 0,
            settle: false
        )
        await sendTextHeader(
            streamID: "current",
            dataPacketReceiveGeneration: 1
        )
        await waitForOpenStreams(1)
        await sendTextChunk(
            streamID: "current",
            content: "stale-chunk",
            dataPacketReceiveGeneration: 0
        )
        await sendTextTrailer(
            streamID: "current",
            dataPacketReceiveGeneration: 0
        )
        await sendTextChunk(
            streamID: "current",
            content: "current",
            dataPacketReceiveGeneration: 1
        )
        await sendTextTrailer(
            streamID: "current",
            dataPacketReceiveGeneration: 1
        )

        let deadline = Date().addingTimeInterval(10)
        while received.copy().isEmpty, Date() < deadline {
            try? await Task.sleep(nanoseconds: 10_000_000)
        }

        #expect(received.copy() == ["current"])
        await manager.unregisterTextStreamHandler(for: topicName)
    }
}
