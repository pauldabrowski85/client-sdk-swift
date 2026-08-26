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

/// Specialized error handling for RPC methods.
///
/// Instances of this type, when thrown in a RPC method handler, will have their `message`
/// serialized and sent across the wire. The sender will receive an equivalent error on the other side.
///
/// Built-in types are included but developers may use any message string, with a max length of 256 bytes.
public struct RpcError: Error, Sendable {
    /// The error code of the RPC call. Error codes 1001-1999 are reserved for built-in errors.
    ///
    /// See `RpcError.BuiltInError` for built-in error information.
    public let code: Int

    /// A message to include. Strings over 256 bytes will be truncated.
    public let message: String

    /// An optional data payload. Must be smaller than 15KB in size, or else will be truncated.
    public let data: String

    public enum BuiltInError {
        case applicationError
        case connectionTimeout
        case responseTimeout
        case recipientDisconnected
        case responsePayloadTooLarge
        case sendFailed
        case unsupportedMethod
        case recipientNotFound
        case requestPayloadTooLarge
        case unsupportedServer
        case unsupportedVersion

        public var code: Int {
            switch self {
            case .applicationError: 1500
            case .connectionTimeout: 1501
            case .responseTimeout: 1502
            case .recipientDisconnected: 1503
            case .responsePayloadTooLarge: 1504
            case .sendFailed: 1505
            case .unsupportedMethod: 1400
            case .recipientNotFound: 1401
            case .requestPayloadTooLarge: 1402
            case .unsupportedServer: 1403
            case .unsupportedVersion: 1404
            }
        }

        public var message: String {
            switch self {
            case .applicationError: "Application error in method handler"
            case .connectionTimeout: "Connection timeout"
            case .responseTimeout: "Response timeout"
            case .recipientDisconnected: "Recipient disconnected"
            case .responsePayloadTooLarge: "Response payload too large"
            case .sendFailed: "Failed to send"
            case .unsupportedMethod: "Method not supported at destination"
            case .recipientNotFound: "Recipient not found"
            case .requestPayloadTooLarge: "Request payload too large"
            case .unsupportedServer: "RPC not supported by server"
            case .unsupportedVersion: "Unsupported RPC version"
            }
        }

        func create(data: String = "") -> RpcError {
            RpcError(code: code, message: message, data: data)
        }
    }

    static func builtIn(_ key: BuiltInError, data: String = "") -> RpcError {
        RpcError(code: key.code, message: key.message, data: data)
    }

    static let receiverOverloaded = RpcError(
        code: BuiltInError.applicationError.code,
        message: "RPC receiver overloaded",
        data: "resource_exhausted"
    )

    static let MAX_MESSAGE_BYTES = 256
    static let MAX_DATA_BYTES = 15360 // 15 KB

    static func fromProto(_ proto: Livekit_RpcError) -> RpcError {
        RpcError(
            code: Int(proto.code),
            message: (proto.message).truncate(maxBytes: MAX_MESSAGE_BYTES),
            data: proto.data.truncate(maxBytes: MAX_DATA_BYTES),
        )
    }

    func toProto() -> Livekit_RpcError {
        Livekit_RpcError.with {
            $0.code = UInt32(code)
            $0.message = message
            $0.data = data
        }
    }
}

/// Maximum payload size for RPC v1 requests and responses.
let MAX_RPC_PAYLOAD_BYTES = 15360 // 15 KB

// MARK: - RPC v2 stream constants

enum RpcStreamTopic {
    static let request = "lk.rpc_request"
    static let response = "lk.rpc_response"
}

enum RpcStreamAttribute {
    static let requestId = "lk.rpc_request_id"
    static let method = "lk.rpc_request_method"
    static let timeoutMs = "lk.rpc_request_response_timeout_ms"
    static let version = "lk.rpc_request_version"
}

enum RpcInvocationLimits {
    static let maximumInFlight = 32
    static let maximumInFlightPerConnection = 8
}

enum RpcStreamLimits {
    static let maximumPayloadBytes = 1_048_576
    static let incomingRequest = IncomingStreamLimits(
        maxStreamBytes: maximumPayloadBytes,
        maxConcurrentStreams: RpcInvocationLimits.maximumInFlight,
        maxConcurrentStreamsPerParticipantConnection: RpcInvocationLimits.maximumInFlightPerConnection,
        maxBufferedChunks: 128
    )
    static let incomingResponse = IncomingStreamLimits(
        maxStreamBytes: maximumPayloadBytes,
        maxConcurrentStreams: RpcInvocationLimits.maximumInFlight,
        maxConcurrentStreamsPerParticipantConnection: RpcInvocationLimits.maximumInFlightPerConnection,
        maxBufferedChunks: 128
    )
}

enum RpcContinuousClock {
    static func nowNanoseconds() -> UInt64 {
        clock_gettime_nsec_np(CLOCK_MONOTONIC_RAW)
    }
}

let RPC_STREAM_VERSION = "2"

/// A handler that processes an RPC request and returns a string
/// that will be sent back to the requester.
///
/// Throwing an `RpcError` will send the error back to the requester.
///
/// - SeeAlso: `LocalParticipant.registerRpcMethod`
public typealias RpcHandler = @Sendable (RpcInvocationData) async throws -> String

public struct RpcInvocationData: Sendable {
    /// A unique identifier for this RPC request
    public let requestId: String

    /// The identity of the RemoteParticipant who initiated the RPC call
    public let callerIdentity: Participant.Identity

    /// Server-authenticated SID of the participant connection that published
    /// the request. This can be `nil` when the transport does not provide
    /// publisher provenance.
    public let callerParticipantSid: Participant.Sid?

    /// Client-local receive generation captured with the request packet before
    /// handler dispatch. This can be `nil` for manually constructed invocation
    /// data that predates receive-generation provenance.
    public let callerDataPacketReceiveGeneration: UInt64?

    /// The data sent by the caller (as a string)
    public let payload: String

    /// The maximum time available to return a response
    public let responseTimeout: TimeInterval

    /// Absolute deadline on the receiver's continuous monotonic clock, in
    /// nanoseconds. This clock advances while the system is asleep.
    /// SDK-delivered invocations always provide this value. It can be `nil`
    /// only for invocation values manually constructed by older integrations.
    public let responseDeadlineContinuousTimeNanoseconds: UInt64?
}

struct PendingRpcResponse {
    let participantConnection: RpcParticipantConnection
    let completer: AsyncCompleter<String>
    let responseDeadlineContinuousTimeNanoseconds: UInt64
    let publicationGate: RpcPublicationGate
    let publicationController: RpcPublicationController
    var responseStreamReader: TextStreamReader?
}

private struct RpcPublicationTerminated: Error {}

final class RpcPublicationController: @unchecked Sendable {
    private struct State {
        var isRevoked = false
        var phaseIsResolved = false
        var task: Task<Void, Never>?
        var streamCancellation: OutgoingStreamCancellation?
    }

    private let state = StateSync(State())
    private let phaseCompleter: AsyncCompleter<Void>

    init(timeout: TimeInterval) {
        phaseCompleter = AsyncCompleter(
            label: "rpc-request-publication",
            defaultTimeout: timeout
        )
    }

    func install(task: Task<Void, Never>) {
        let shouldCancel = state.mutate { state in
            guard !state.isRevoked, !state.phaseIsResolved else { return true }
            state.task = task
            return false
        }
        if shouldCancel { task.cancel() }
    }

    func register(streamCancellation: OutgoingStreamCancellation) -> Bool {
        state.mutate { state in
            guard !state.isRevoked, !state.phaseIsResolved else { return false }
            state.streamCancellation = streamCancellation
            return true
        }
    }

    func publicationSucceeded() {
        resolvePublication(with: .success(()))
    }

    func publicationFailed(_ error: Error) {
        resolvePublication(with: .failure(error))
    }

    func wait(timeout: TimeInterval) async throws {
        try await phaseCompleter.wait(timeout: timeout)
    }

    func revoke() {
        let task = state.mutate { state -> Task<Void, Never>? in
            state.isRevoked = true
            defer { state.task = nil }
            return state.task
        }
        task?.cancel()
    }

    func cancelAndWaitForStreamCleanup() async {
        let cancellationState = state.mutate { state -> (
            task: Task<Void, Never>?,
            streamCancellation: OutgoingStreamCancellation?
        ) in
            state.isRevoked = true
            defer { state.task = nil }
            return (state.task, state.streamCancellation)
        }
        cancellationState.task?.cancel()
        if let cancellation = cancellationState.streamCancellation {
            await cancellation.cancel()
        }

        let shouldResume = state.mutate { state in
            guard !state.phaseIsResolved else { return false }
            state.phaseIsResolved = true
            state.streamCancellation = nil
            return true
        }
        if shouldResume {
            phaseCompleter.resume(throwing: RpcPublicationTerminated())
        }
    }

    private func resolvePublication(with result: Result<Void, Error>) {
        let shouldResume = state.mutate { state in
            guard !state.isRevoked, !state.phaseIsResolved else { return false }
            state.phaseIsResolved = true
            state.task = nil
            state.streamCancellation = nil
            return true
        }
        if shouldResume {
            phaseCompleter.resume(with: result)
        }
    }
}

final class RpcPublicationGate: @unchecked Sendable {
    private let revoked = StateSync(false)
    private let responseDeadlineContinuousTimeNanoseconds: UInt64
    private let nowNanoseconds: @Sendable () -> UInt64
    private let additionalAdmission: DataChannelSendAdmission

    init(
        responseDeadlineContinuousTimeNanoseconds: UInt64,
        nowNanoseconds: @escaping @Sendable () -> UInt64,
        additionalAdmission: DataChannelSendAdmission
    ) {
        self.responseDeadlineContinuousTimeNanoseconds = responseDeadlineContinuousTimeNanoseconds
        self.nowNanoseconds = nowNanoseconds
        self.additionalAdmission = additionalAdmission
    }

    var sendAdmission: DataChannelSendAdmission {
        DataChannelSendAdmission(
            preflight: { [weak self] in
                guard let self else {
                    return LiveKitError(.cancelled, message: "RPC publication gate was released")
                }
                return self.rejectionError()
            },
            attempt: { [weak self] send in
                guard let self else {
                    return .rejected(LiveKitError(.cancelled, message: "RPC publication gate was released"))
                }
                return revoked.mutate { revoked in
                    guard !revoked,
                          nowNanoseconds() < responseDeadlineContinuousTimeNanoseconds
                    else {
                        return .rejected(LiveKitError(
                            .cancelled,
                            message: "RPC publication deadline or destination admission was revoked"
                        ))
                    }
                    return additionalAdmission.attempt(send)
                }
            }
        )
    }

    func revoke() {
        revoked.mutate { $0 = true }
    }

    private func rejectionError() -> Error? {
        revoked.read { revoked in
            guard !revoked,
                  nowNanoseconds() < responseDeadlineContinuousTimeNanoseconds
            else {
                return LiveKitError(
                    .cancelled,
                    message: "RPC publication deadline or destination admission was revoked"
                )
            }
            return additionalAdmission.preflight()
        }
    }
}

struct RpcParticipantConnection: @unchecked Sendable {
    let identity: Participant.Identity
    let sid: Participant.Sid
    let dataPacketReceiveGeneration: UInt64
    let participant: RemoteParticipant

    static func resolve(
        in room: Room,
        identity: Participant.Identity,
        sid: Participant.Sid?,
        dataPacketReceiveGeneration: UInt64?
    ) -> RpcParticipantConnection? {
        guard let sid,
              let dataPacketReceiveGeneration,
              dataPacketReceiveGeneration == room.dataPacketReceiveGeneration
        else { return nil }
        return room._state.read { state in
            guard let participant = state.remoteParticipants[identity],
                  participant.sid == sid,
                  participant.dataPacketReceiveGeneration == dataPacketReceiveGeneration
            else { return nil }
            return RpcParticipantConnection(
                identity: identity,
                sid: sid,
                dataPacketReceiveGeneration: dataPacketReceiveGeneration,
                participant: participant
            )
        }
    }

    static func resolveCurrent(
        in room: Room,
        identity: Participant.Identity
    ) -> RpcParticipantConnection? {
        let receiveGeneration = room.dataPacketReceiveGeneration
        return room._state.read { state in
            guard let participant = state.remoteParticipants[identity],
                  let sid = participant.sid,
                  participant.dataPacketReceiveGeneration == receiveGeneration
            else { return nil }
            return RpcParticipantConnection(
                identity: identity,
                sid: sid,
                dataPacketReceiveGeneration: receiveGeneration,
                participant: participant
            )
        }
    }

    func isCurrent(in room: Room) -> Bool {
        guard room.dataPacketReceiveGeneration == dataPacketReceiveGeneration else { return false }
        return room._state.read { state in
            guard let current = state.remoteParticipants[identity] else { return false }
            return current === participant &&
                current.sid == sid &&
                current.dataPacketReceiveGeneration == dataPacketReceiveGeneration
        }
    }

    func sendAdmission(in room: Room) -> DataChannelSendAdmission {
        DataChannelSendAdmission(
            preflight: { [weak room] in
                guard let room, self.isCurrent(in: room) else {
                    return LiveKitError(
                        .cancelled,
                        message: "RPC participant connection was replaced"
                    )
                }
                return nil
            },
            attempt: { [weak room] send in
                guard let room else {
                    return .rejected(LiveKitError(
                        .cancelled,
                        message: "RPC participant room was released"
                    ))
                }
                return room.attemptRpcPublication(for: self, send)
            }
        )
    }
}
