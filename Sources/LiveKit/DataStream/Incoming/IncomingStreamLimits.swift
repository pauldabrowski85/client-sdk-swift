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

/// Resource limits applied before an incoming stream reaches its handler.
public struct IncomingStreamLimits: Equatable, Sendable {
    /// Compatibility defaults keep total stream length unrestricted while
    /// bounding retained chunk and descriptor counts.
    public static let `default` = IncomingStreamLimits()

    public let maxStreamBytes: Int?
    public let maxConcurrentStreams: Int
    public let maxBufferedChunks: Int

    public init(
        maxStreamBytes: Int? = nil,
        maxConcurrentStreams: Int = 64,
        maxBufferedChunks: Int = 256
    ) {
        precondition(maxStreamBytes == nil || maxStreamBytes! > 0)
        precondition(maxConcurrentStreams > 0)
        precondition(maxBufferedChunks > 0)
        self.maxStreamBytes = maxStreamBytes
        self.maxConcurrentStreams = maxConcurrentStreams
        self.maxBufferedChunks = maxBufferedChunks
    }
}

/// A stream rejected by SDK admission before its handler could consume it.
public struct IncomingStreamRejection: Equatable, Sendable {
    public let streamID: String
    public let topic: String
    public let participantIdentity: Participant.Identity
    public let publisherParticipantSid: Participant.Sid?
    public let dataPacketReceiveGeneration: UInt64
    public let attributes: [String: String]
    public let handlerWasDispatched: Bool
    public let error: StreamError

    public init(
        streamID: String,
        topic: String,
        participantIdentity: Participant.Identity,
        publisherParticipantSid: Participant.Sid?,
        dataPacketReceiveGeneration: UInt64,
        attributes: [String: String],
        handlerWasDispatched: Bool,
        error: StreamError
    ) {
        self.streamID = streamID
        self.topic = topic
        self.participantIdentity = participantIdentity
        self.publisherParticipantSid = publisherParticipantSid
        self.dataPacketReceiveGeneration = dataPacketReceiveGeneration
        self.attributes = attributes
        self.handlerWasDispatched = handlerWasDispatched
        self.error = error
    }
}

/// Called synchronously when SDK admission rejects an incoming stream before
/// its handler can consume it.
public typealias IncomingStreamRejectionHandler = @Sendable (IncomingStreamRejection) -> Void
