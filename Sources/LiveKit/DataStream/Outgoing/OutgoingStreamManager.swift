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

// Extending the generic builder needs the runtime module by name; the facades
// themselves are re-exported through LiveKit.

/// Manages state of outgoing data streams.
actor OutgoingStreamManager: Loggable {
    enum OperationKind: Equatable, Sendable {
        case write
        case close
    }

    typealias PacketHandler = @Sendable (
        Livekit_DataPacket,
        UInt64,
        (@Sendable () -> Bool)?
    ) async throws -> Void
    typealias SendGenerationProvider = @Sendable () -> UInt64
    typealias EncryptionProvider = @Sendable () -> EncryptionType

    private nonisolated let packetHandler: PacketHandler
    private nonisolated let sendGenerationProvider: SendGenerationProvider
    private nonisolated let encryptionProvider: EncryptionProvider
    private var operationObserver: (@Sendable (OperationKind) -> Void)?

    init(
        packetHandler: @escaping PacketHandler,
        sendGenerationProvider: @escaping SendGenerationProvider,
        encryptionProvider: @escaping EncryptionProvider
    ) {
        self.packetHandler = packetHandler
        self.sendGenerationProvider = sendGenerationProvider
        self.encryptionProvider = encryptionProvider
    }

    // MARK: - Opening streams

    func sendText(_ text: String, options: StreamTextOptions) async throws -> TextStreamInfo {
        let info = TextStreamInfo(
            id: options.id ?? Self.uniqueID(),
            topic: options.topic,
            timestamp: Date(),
            totalLength: text.utf8.count, // Number of bytes in UTF-8 representation
            attributes: options.attributes,
            encryptionType: encryptionProvider(),
            operationType: .create,
            version: options.version,
            replyToStreamID: options.replyToStreamID,
            attachedStreamIDs: options.attachedStreamIDs,
            generated: false,
        )
        let writer = try await openTextStream(
            with: info,
            sendingTo: options.destinationIdentities,
        )
        try await writer.write(text)
        try await writer.close()

        return writer.info
    }

    func sendFile(_ fileURL: URL, options: StreamByteOptions) async throws -> ByteStreamInfo {
        guard let fileInfo = FileInfo(for: fileURL) else {
            throw StreamError.fileInfoUnavailable
        }
        let info = ByteStreamInfo(
            id: options.id ?? Self.uniqueID(),
            topic: options.topic,
            timestamp: Date(),
            totalLength: fileInfo.size, // Not overridable
            attributes: options.attributes,
            encryptionType: encryptionProvider(),
            mimeType: options.mimeType ?? fileInfo.mimeType ?? Self.byteMimeType,
            name: options.name ?? fileInfo.name,
        )
        let writer = try await openByteStream(
            with: info,
            sendingTo: options.destinationIdentities,
        )
        try await writer.write(contentsOf: fileURL)
        try await writer.close()

        return writer.info
    }

    func streamText(options: StreamTextOptions) async throws -> TextStreamWriter {
        try await streamText(options: options, admission: nil)
    }

    func streamText(
        options: StreamTextOptions,
        admission: (@Sendable () -> Bool)?
    ) async throws -> TextStreamWriter {
        let info = TextStreamInfo(
            id: options.id ?? Self.uniqueID(),
            topic: options.topic,
            timestamp: Date(),
            totalLength: nil,
            attributes: options.attributes,
            encryptionType: encryptionProvider(),
            operationType: .create,
            version: options.version,
            replyToStreamID: options.replyToStreamID,
            attachedStreamIDs: options.attachedStreamIDs,
            generated: false,
        )
        return try await openTextStream(
            with: info,
            sendingTo: options.destinationIdentities,
            admission: admission
        )
    }

    func streamBytes(options: StreamByteOptions) async throws -> ByteStreamWriter {
        let info = ByteStreamInfo(
            id: options.id ?? Self.uniqueID(),
            topic: options.topic,
            timestamp: Date(),
            totalLength: options.totalSize,
            attributes: options.attributes,
            encryptionType: encryptionProvider(),
            mimeType: options.mimeType ?? Self.byteMimeType,
            name: options.name,
        )
        return try await openByteStream(
            with: info,
            sendingTo: options.destinationIdentities,
        )
    }

    private func openTextStream(
        with info: TextStreamInfo,
        sendingTo recipients: [Participant.Identity],
        admission: (@Sendable () -> Bool)? = nil
    ) async throws -> TextStreamWriter {
        let descriptor = try await openStream(
            with: info,
            sendingTo: recipients,
            admission: admission
        )
        return TextStreamWriter(
            info: info,
            destination: Destination(
                streamID: info.id,
                descriptorGeneration: descriptor.generation,
                manager: self
            ),
        )
    }

    private func openByteStream(
        with info: ByteStreamInfo,
        sendingTo recipients: [Participant.Identity],
    ) async throws -> ByteStreamWriter {
        let descriptor = try await openStream(with: info, sendingTo: recipients)
        return ByteStreamWriter(
            info: info,
            destination: Destination(
                streamID: info.id,
                descriptorGeneration: descriptor.generation,
                manager: self
            ),
        )
    }

    // MARK: - State

    /// Information about an open data stream.
    private final class Descriptor: @unchecked Sendable {
        let info: StreamInfo
        let generation = UUID()
        let dataChannelSendGeneration: UInt64
        let admission: (@Sendable () -> Bool)?
        let operationLane = SerialRunnerActor<Void>()
        var writtenLength: Int = 0
        var chunkIndex: UInt64 = 0

        init(
            info: StreamInfo,
            dataChannelSendGeneration: UInt64,
            admission: (@Sendable () -> Bool)?
        ) {
            self.info = info
            self.dataChannelSendGeneration = dataChannelSendGeneration
            self.admission = admission
        }
    }

    /// Mapping between stream ID and descriptor for open streams.
    private var openStreams: [String: Descriptor] = [:]
    var openStreamCount: Int { openStreams.count }

    func setOperationObserver(_ observer: (@Sendable (OperationKind) -> Void)?) {
        operationObserver = observer
    }

    private func hasOpenStream(for streamID: String, generation: UUID) -> Bool {
        openStreams[streamID]?.generation == generation
    }

    // MARK: - Packet sending

    private func openStream(
        with info: StreamInfo,
        sendingTo recipients: [Participant.Identity],
        admission: (@Sendable () -> Bool)? = nil
    ) async throws -> Descriptor {
        guard openStreams[info.id] == nil else {
            throw StreamError.alreadyOpened
        }

        // Reserve the exact descriptor before the first suspension. Reset and
        // same-ID opens can now terminalize or reject this pending header rather
        // than letting it install stale state after teardown.
        let descriptor = Descriptor(
            info: info,
            dataChannelSendGeneration: sendGenerationProvider(),
            admission: admission
        )
        openStreams[info.id] = descriptor

        let header = Livekit_DataStream.Header(info)
        let packet = Livekit_DataPacket.with {
            $0.value = .streamHeader(header)
            $0.destinationIdentities = recipients.map(\.stringValue)
        }

        do {
            try await descriptor.operationLane.run { [packetHandler] in
                try await packetHandler(
                    packet,
                    descriptor.dataChannelSendGeneration,
                    descriptor.admission
                )
            }
        } catch {
            removeStream(id: info.id, descriptor: descriptor)
            throw error
        }

        guard openStreams[info.id] === descriptor else {
            throw StreamError.terminated
        }
        return descriptor
    }

    private func send(
        _ data: some StreamData,
        to id: String,
        descriptorGeneration: UUID
    ) async throws {
        let descriptor = try descriptor(for: id, generation: descriptorGeneration)
        operationObserver?(.write)
        try await descriptor.operationLane.run { [weak self] in
            guard let self else { throw StreamError.terminated }
            for chunk in data.chunks(of: Self.chunkSize) {
                try await self.sendChunk(chunk, to: id, descriptor: descriptor)
            }
        }
    }

    private func sendChunk(
        _ data: Data,
        to id: String,
        descriptor: Descriptor
    ) async throws {
        guard openStreams[id] === descriptor else { throw StreamError.terminated }
        let chunk = Livekit_DataStream.Chunk.with {
            $0.streamID = id
            $0.chunkIndex = descriptor.chunkIndex
            $0.content = data
        }
        let packet = Livekit_DataPacket.with {
            $0.value = .streamChunk(chunk)
        }
        do {
            try await packetHandler(
                packet,
                descriptor.dataChannelSendGeneration,
                descriptor.admission
            )
        } catch {
            removeStream(id: id, descriptor: descriptor)
            throw error
        }

        guard openStreams[id] === descriptor else {
            throw StreamError.terminated
        }
        descriptor.writtenLength += data.count
        descriptor.chunkIndex += 1
    }

    private func closeStream(
        with id: String,
        descriptorGeneration: UUID,
        reason: String?
    ) async throws {
        let descriptor = try descriptor(for: id, generation: descriptorGeneration)
        operationObserver?(.close)
        try await descriptor.operationLane.run { [weak self] in
            guard let self else { throw StreamError.terminated }
            guard await self.owns(descriptor, for: id) else {
                throw StreamError.terminated
            }

            let trailer = Livekit_DataStream.Trailer.with {
                $0.streamID = id
                $0.reason = reason ?? ""
            }
            let packet = Livekit_DataPacket.with {
                $0.value = .streamTrailer(trailer)
            }

            do {
                try await self.packetHandler(
                    packet,
                    descriptor.dataChannelSendGeneration,
                    descriptor.admission
                )
            } catch {
                await self.removeStream(id: id, descriptor: descriptor)
                throw error
            }
            await self.removeStream(id: id, descriptor: descriptor)
        }
    }

    func reset() {
        openStreams.removeAll()
    }

    private func descriptor(for id: String, generation: UUID) throws -> Descriptor {
        guard let descriptor = openStreams[id] else { throw StreamError.unknownStream }
        guard descriptor.generation == generation else { throw StreamError.terminated }
        return descriptor
    }

    private func owns(_ descriptor: Descriptor, for id: String) -> Bool {
        openStreams[id] === descriptor
    }

    private func removeStream(id: String, descriptor: Descriptor) {
        guard openStreams[id] === descriptor else { return }
        openStreams[id] = nil
    }

    // MARK: - Destination

    fileprivate struct Destination: StreamWriterDestination {
        let streamID: String
        let descriptorGeneration: UUID
        weak var manager: OutgoingStreamManager?

        var isOpen: Bool {
            get async {
                guard let manager else { return false }
                return await manager.hasOpenStream(
                    for: streamID,
                    generation: descriptorGeneration
                )
            }
        }

        func write(_ data: some StreamData) async throws {
            guard let manager else { throw StreamError.terminated }
            try await manager.send(
                data,
                to: streamID,
                descriptorGeneration: descriptorGeneration
            )
        }

        func close(reason: String?) async throws {
            guard let manager else { throw StreamError.terminated }
            try await manager.closeStream(
                with: streamID,
                descriptorGeneration: descriptorGeneration,
                reason: reason
            )
        }
    }

    // MARK: - Constants & helpers

    /// Generates a unqiue ID for a new stream.
    private static func uniqueID() -> String {
        UUID().uuidString
    }

    /// Maximum number of bytes to send in a single chunk.
    private static let chunkSize = 15 * 1024

    /// Default MIME type to use for text streams.
    fileprivate static let textMimeType = "text/plain"

    /// Default MIME type to use for byte streams.
    private static let byteMimeType = "application/octet-stream"
}

// MARK: - To protocol types

extension Livekit_DataStream.Header {
    init(_ streamInfo: StreamInfo) {
        self = Livekit_DataStream.Header.with {
            $0.streamID = streamInfo.id
            $0.mimeType = (streamInfo as? ByteStreamInfo)?.mimeType ?? OutgoingStreamManager.textMimeType
            $0.topic = streamInfo.topic
            $0.timestampDate = streamInfo.timestamp
            if let totalLength = streamInfo.totalLength {
                $0.totalLength = UInt64(totalLength)
            }
            $0.attributes = streamInfo.attributes
            $0.encryptionType = streamInfo.encryptionType.toPBType()
            $0.contentHeader = Livekit_DataStream_Header_OneOf_ContentHeader(streamInfo)
        }
    }

    // Stream timestamps are in ms (13 digits)
    var timestampDate: Date {
        Date(timeIntervalSince1970: TimeInterval(timestamp) / TimeInterval(1000))
    }
}

extension Livekit_DataStream_Header.Builder {
    // Mirrors `Livekit_DataStream.Header.timestampDate`; setters live on the builder.
    var timestampDate: Date {
        get { Date(timeIntervalSince1970: TimeInterval(timestamp) / TimeInterval(1000)) }
        nonmutating set { timestamp = Int64(newValue.timeIntervalSince1970 * TimeInterval(1000)) }
    }
}

extension Livekit_DataStream_Header_OneOf_ContentHeader {
    init?(_ streamInfo: StreamInfo) {
        if let textStreamInfo = streamInfo as? TextStreamInfo {
            self = .textHeader(Livekit_DataStream.TextHeader.with {
                $0.operationType = Livekit_DataStream.OperationType(textStreamInfo.operationType)
                $0.version = Int32(textStreamInfo.version)
                $0.replyToStreamID = textStreamInfo.replyToStreamID ?? ""
                $0.attachedStreamIds = textStreamInfo.attachedStreamIDs
                $0.generated = textStreamInfo.generated
            })
            return
        } else if let byteStreamInfo = streamInfo as? ByteStreamInfo {
            self = .byteHeader(Livekit_DataStream.ByteHeader.with {
                if let name = byteStreamInfo.name { $0.name = name }
            })
            return
        }
        return nil
    }
}

extension Livekit_DataStream.OperationType {
    init(_ operationType: TextStreamInfo.OperationType) {
        self = Livekit_DataStream.OperationType(rawValue: operationType.rawValue) ?? .create
    }
}
