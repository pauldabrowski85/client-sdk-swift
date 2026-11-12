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

/// Information about a data stream.
public protocol StreamInfo: Sendable {
    /// Unique identifier of the stream.
    var id: String { get }

    /// Topic name used to route the stream to the appropriate handler.
    var topic: String { get }

    /// When the stream was created.
    var timestamp: Date { get }

    /// Total expected size in bytes (UTF-8 for text), if known. `nil` also when the sender declared
    /// a size that `Int` cannot hold.
    var totalLength: Int? { get }

    /// Additional attributes as needed for your application.
    var attributes: [String: String] { get }

    /// The encryption type used for this stream.
    var encryptionType: EncryptionType { get }
}

/// Information about a text data stream.
@objcMembers
public final class TextStreamInfo: NSObject, StreamInfo {
    public let id: String
    public let topic: String
    public let timestamp: Date
    public let totalLength: Int?
    public let attributes: [String: String]
    public let encryptionType: EncryptionType

    @objc(TextStreamInfoOperationType)
    public enum OperationType: Int, Sendable {
        case create
        case update
        case delete
        case reaction
    }

    public let operationType: OperationType
    public let version: Int
    public let replyToStreamID: String?
    public let attachedStreamIDs: [String]
    public let generated: Bool
    /// Server-generated SID of the participant that published the data packet.
    /// Unlike `Participant.Identity`, this value changes when a same-identity
    /// participant is replaced and can therefore bind a stream to its sender
    /// epoch without consulting mutable room state.
    public let publisherParticipantSid: Participant.Sid?
    /// Client-local receive generation captured with the stream header before
    /// handler dispatch. Combined with the publisher SID, this distinguishes
    /// delayed work from an earlier transport even when the server reuses a SID.
    @nonobjc public let dataPacketReceiveGeneration: UInt64?

    /// Continuous monotonic time captured when the header entered the receive
    /// pipeline, before buffering or handler scheduling. It advances through sleep.
    @nonobjc let receivedAtContinuousTimeNanoseconds: UInt64

    /// The MIME type the sender declared for the text. `text/plain` unless the sender set another.
    public let mimeType: String

    init(
        id: String,
        topic: String,
        timestamp: Date,
        totalLength: Int?,
        attributes: [String: String],
        encryptionType: EncryptionType,
        mimeType: String,
        operationType: OperationType,
        version: Int,
        replyToStreamID: String?,
        attachedStreamIDs: [String],
        generated: Bool,
        publisherParticipantSid: Participant.Sid? = nil,
        dataPacketReceiveGeneration: UInt64? = nil,
        receivedAtContinuousTimeNanoseconds: UInt64 = RpcContinuousClock.nowNanoseconds(),
    ) {
        self.id = id
        self.topic = topic
        self.timestamp = timestamp
        self.totalLength = totalLength
        self.attributes = attributes
        self.encryptionType = encryptionType
        self.mimeType = mimeType
        self.operationType = operationType
        self.version = version
        self.replyToStreamID = replyToStreamID
        self.attachedStreamIDs = attachedStreamIDs
        self.generated = generated
        self.publisherParticipantSid = publisherParticipantSid
        self.dataPacketReceiveGeneration = dataPacketReceiveGeneration
        self.receivedAtContinuousTimeNanoseconds = receivedAtContinuousTimeNanoseconds
    }

    convenience init(_ ffi: LiveKitFFI.TextStreamInfo, encryptionType: EncryptionType) {
        self.init(
            id: ffi.id,
            topic: ffi.topic,
            timestamp: Date(timeIntervalSince1970: TimeInterval(ffi.timestampMs) / 1000),
            totalLength: ffi.totalLength.flatMap { Int(exactly: $0) },
            attributes: ffi.attributes,
            encryptionType: encryptionType,
            mimeType: ffi.mimeType,
            operationType: OperationType(ffi.operationType),
            version: Int(ffi.version),
            replyToStreamID: ffi.replyToStreamId,
            attachedStreamIDs: ffi.attachedStreamIds,
            generated: ffi.generated,
        )
    }

    /// The info of an incoming stream. The core describes the content; `source` holds what only
    /// the data packet said: who sent it, on which transport, and when.
    convenience init(_ ffi: LiveKitFFI.TextStreamInfo, source: IncomingStreamSource) {
        self.init(
            id: ffi.id,
            topic: ffi.topic,
            timestamp: Date(timeIntervalSince1970: TimeInterval(ffi.timestampMs) / 1000),
            totalLength: ffi.totalLength.flatMap { Int(exactly: $0) },
            attributes: source.attributes,
            encryptionType: source.encryptionType,
            mimeType: ffi.mimeType,
            operationType: OperationType(ffi.operationType),
            version: Int(ffi.version),
            replyToStreamID: ffi.replyToStreamId,
            attachedStreamIDs: ffi.attachedStreamIds,
            generated: ffi.generated,
            publisherParticipantSid: source.publisherParticipantSid,
            dataPacketReceiveGeneration: source.dataPacketReceiveGeneration,
            receivedAtContinuousTimeNanoseconds: source.receivedAtContinuousTimeNanoseconds,
        )
    }
}

extension TextStreamInfo.OperationType {
    init(_ ffi: LiveKitFFI.OperationType) {
        switch ffi {
        case .create: self = .create
        case .update: self = .update
        case .delete: self = .delete
        case .reaction: self = .reaction
        // The FFI enum is resilient: an operation this SDK predates reads as a new message, the
        // only reading that can't corrupt an existing one.
        @unknown default: self = .create
        }
    }

    var ffiValue: LiveKitFFI.OperationType {
        switch self {
        case .create: .create
        case .update: .update
        case .delete: .delete
        case .reaction: .reaction
        }
    }
}

/// Information about a byte data stream.
@objcMembers
public final class ByteStreamInfo: NSObject, StreamInfo {
    public let id: String
    public let topic: String
    public let timestamp: Date
    public let totalLength: Int?
    public let attributes: [String: String]
    public let encryptionType: EncryptionType

    /// The MIME type of the stream data.
    public let mimeType: String

    /// The name of the file being sent.
    public let name: String?

    init(
        id: String,
        topic: String,
        timestamp: Date,
        totalLength: Int?,
        attributes: [String: String],
        encryptionType: EncryptionType,
        mimeType: String,
        name: String?,
    ) {
        self.id = id
        self.mimeType = mimeType
        self.topic = topic
        self.timestamp = timestamp
        self.totalLength = totalLength
        self.attributes = attributes
        self.encryptionType = encryptionType
        self.name = name
    }

    convenience init(_ ffi: LiveKitFFI.ByteStreamInfo, encryptionType: EncryptionType) {
        self.init(
            id: ffi.id,
            topic: ffi.topic,
            timestamp: Date(timeIntervalSince1970: TimeInterval(ffi.timestampMs) / 1000),
            totalLength: ffi.totalLength.flatMap { Int(exactly: $0) },
            attributes: ffi.attributes,
            encryptionType: encryptionType,
            mimeType: ffi.mimeType,
            name: ffi.name.isEmpty ? nil : ffi.name,
        )
    }

    /// The info of an incoming stream, without the attribute admission stamped on its header.
    convenience init(_ ffi: LiveKitFFI.ByteStreamInfo, source: IncomingStreamSource) {
        self.init(
            id: ffi.id,
            topic: ffi.topic,
            timestamp: Date(timeIntervalSince1970: TimeInterval(ffi.timestampMs) / 1000),
            totalLength: ffi.totalLength.flatMap { Int(exactly: $0) },
            attributes: source.attributes,
            encryptionType: source.encryptionType,
            mimeType: ffi.mimeType,
            name: ffi.name.isEmpty ? nil : ffi.name,
        )
    }
}
