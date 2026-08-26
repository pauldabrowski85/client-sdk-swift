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

/// Handler-side RPC.
///
/// Owns the registered method-handler table and the wire-level handling of incoming
/// RPC requests (both v1 packets and v2 streams). `Room.registerRpcMethod` and
/// `Room.unregisterRpcMethod` are one-line proxies that forward into this actor.
actor RpcServerManager: Loggable {
    private weak var room: Room?

    /// Method-name → handler map. Persists across `Room.cleanUp` and reconnects so callers
    /// don't have to re-register on every transient disconnect; cleared only when this
    /// actor deallocates with its owning `Room`.
    private var handlers: [String: RpcHandler] = [:]

    func attach(to room: Room) {
        self.room = room
    }

    // MARK: - Public handler registration

    func registerHandler(_ method: String, handler: @escaping RpcHandler) throws {
        guard !isRpcMethodRegistered(method) else {
            throw LiveKitError(.invalidState, message: "RPC method '\(method)' already registered")
        }
        handlers[method] = handler
    }

    func unregisterHandler(_ method: String) {
        if handlers.removeValue(forKey: method) == nil {
            log("No handler registered for RPC method '\(method)'", .warning)
        }
    }

    func isRpcMethodRegistered(_ method: String) -> Bool {
        handlers[method] != nil
    }

    // MARK: - Incoming dispatch

    // swiftlint:disable function_parameter_count
    /// Handle an RPC request that arrived as a v1 `RpcRequest` packet. Successful
    /// responses follow the caller's advertised `clientProtocol`: v2-capable callers
    /// get a v2 data-stream response (uncapped), legacy callers get a v1 packet.
    /// Errors always use a v1 packet per spec.
    func handleIncomingRequest(callerIdentity: Participant.Identity,
                               callerParticipantSid: Participant.Sid? = nil,
                               callerDataPacketReceiveGeneration: UInt64? = nil,
                               requestId: String,
                               method: String,
                               payload: String,
                               responseTimeout: TimeInterval,
                               version: Int) async
    {
        guard let room = try? requireRoom() else { return }
        guard let callerConnection = RpcParticipantConnection.resolve(
            in: room,
            identity: callerIdentity,
            sid: callerParticipantSid,
            dataPacketReceiveGeneration: callerDataPacketReceiveGeneration
        ) else {
            log("[Rpc] Ignoring request \(requestId) with stale or missing caller provenance", .error)
            return
        }

        do {
            try await publishAck(in: room, callerConnection: callerConnection, requestId: requestId)
        } catch {
            log("[Rpc] Failed to publish RPC ack for \(requestId)", .error)
        }

        guard version == 1 else {
            do {
                try await publishResponse(in: room,
                                          callerConnection: callerConnection,
                                          requestId: requestId,
                                          payload: nil,
                                          error: RpcError.builtIn(.unsupportedVersion))
            } catch {
                log("[Rpc] Failed to publish RPC error response for \(requestId)", .error)
            }
            return
        }

        let result = await dispatchToHandler(callerConnection: callerConnection,
                                             requestId: requestId,
                                             method: method,
                                             payload: payload,
                                             responseTimeout: responseTimeout)
        do {
            try await publishResult(result,
                                    in: room,
                                    callerConnection: callerConnection,
                                    requestId: requestId)
        } catch {
            log("[Rpc] Failed to publish RPC response for \(requestId)", .error)
        }
    }

    // swiftlint:enable function_parameter_count

    // swiftlint:disable function_body_length
    /// Handle an RPC request that arrived as a v2 data stream on the `lk.rpc_request` topic.
    /// Successful responses are sent back as a data stream on `lk.rpc_response`; errors are
    /// sent as v1 `RpcResponse` packets per the spec.
    func handleIncomingRequestStream(reader: TextStreamReader,
                                     callerIdentity: Participant.Identity) async
    {
        guard let room = try? requireRoom() else { return }
        guard let callerConnection = RpcParticipantConnection.resolve(
            in: room,
            identity: callerIdentity,
            sid: reader.info.publisherParticipantSid,
            dataPacketReceiveGeneration: reader.info.dataPacketReceiveGeneration
        ) else {
            log("[Rpc] Ignoring request stream with stale or missing caller provenance", .error)
            return
        }

        let attrs = reader.info.attributes
        // requestId is the correlation key; without it we can't send a typed error back,
        // so log and bail (the caller will hit its own response timeout).
        guard let requestId = attrs[RpcStreamAttribute.requestId] else {
            log("[Rpc] Incoming v2 RPC request stream is missing request id; cannot correlate", .error)
            return
        }
        guard let method = attrs[RpcStreamAttribute.method],
              let timeoutMsString = attrs[RpcStreamAttribute.timeoutMs],
              let timeoutMs = UInt32(timeoutMsString),
              let version = attrs[RpcStreamAttribute.version]
        else {
            log("[Rpc] Incoming v2 RPC request stream for \(requestId) is missing required attributes", .error)
            do {
                try await publishResponse(in: room,
                                          callerConnection: callerConnection,
                                          requestId: requestId,
                                          payload: nil,
                                          error: RpcError(code: RpcError.BuiltInError.applicationError.code,
                                                          message: "RPC data stream malformed",
                                                          data: ""))
            } catch {
                log("[Rpc] Failed to publish malformed-attrs error response for \(requestId)", .error)
            }
            return
        }
        let responseTimeout = TimeInterval(timeoutMs) / 1000

        do {
            try await publishAck(in: room, callerConnection: callerConnection, requestId: requestId)
        } catch {
            log("[Rpc] Failed to publish RPC ack for \(requestId)", .error)
        }

        guard version == RPC_STREAM_VERSION else {
            do {
                try await publishResponse(in: room,
                                          callerConnection: callerConnection,
                                          requestId: requestId,
                                          payload: nil,
                                          error: RpcError.builtIn(.unsupportedVersion))
            } catch {
                log("[Rpc] Failed to publish RPC error response for \(requestId)", .error)
            }
            return
        }

        let payload: String
        do {
            payload = try await reader.readAll()
        } catch {
            log("[Rpc] Failed to read v2 RPC request payload for \(requestId): \(error)", .error)
            let rpcError: RpcError = if case StreamError.streamSizeExceeded = error {
                .builtIn(.requestPayloadTooLarge)
            } else {
                RpcError(code: RpcError.BuiltInError.applicationError.code,
                         message: "Error reading RPC request payload",
                         data: "")
            }
            do {
                try await publishResponse(in: room,
                                          callerConnection: callerConnection,
                                          requestId: requestId,
                                          payload: nil,
                                          error: rpcError)
            } catch {
                log("[Rpc] Failed to publish read-failure error response for \(requestId)", .error)
            }
            return
        }

        guard callerConnection.isCurrent(in: room) else { return }
        let result = await dispatchToHandler(callerConnection: callerConnection,
                                             requestId: requestId,
                                             method: method,
                                             payload: payload,
                                             responseTimeout: responseTimeout)
        do {
            try await publishResult(result,
                                    in: room,
                                    callerConnection: callerConnection,
                                    requestId: requestId)
        } catch {
            log("[Rpc] Failed to publish RPC response for \(requestId)", .error)
        }
    }

    func handleIncomingRequestStreamRejection(_ rejection: IncomingStreamRejection) async {
        guard let requestId = rejection.attributes[RpcStreamAttribute.requestId],
              let room = try? requireRoom()
        else {
            log("[Rpc] Rejected v2 request stream is missing request id", .error)
            return
        }
        guard let callerConnection = RpcParticipantConnection.resolve(
            in: room,
            identity: rejection.participantIdentity,
            sid: rejection.publisherParticipantSid,
            dataPacketReceiveGeneration: rejection.dataPacketReceiveGeneration
        ) else { return }
        let error: RpcError = switch rejection.error {
        case .streamSizeExceeded, .invalidDeclaredLength:
            .builtIn(.requestPayloadTooLarge)
        default:
            .builtIn(.applicationError)
        }
        do {
            try await publishResponse(
                in: room,
                callerConnection: callerConnection,
                requestId: requestId,
                payload: nil,
                error: error
            )
        } catch {
            log("[Rpc] Failed to publish rejection response for \(requestId)", .error)
        }
    }

    // swiftlint:enable function_body_length

    // MARK: - Handler dispatch

    private enum DispatchResult {
        case success(String)
        case failure(RpcError)
    }

    /// Look up the handler for `method`, invoke it, and produce a payload-or-error result.
    /// Size-checking the response is the responsibility of the publisher: the v1 wire has
    /// a 15 KB cap (enforced in `publishResponse`), the v2 stream wire is unbounded.
    private func dispatchToHandler(callerConnection: RpcParticipantConnection,
                                   requestId: String,
                                   method: String,
                                   payload: String,
                                   responseTimeout: TimeInterval) async -> DispatchResult
    {
        guard let room, callerConnection.isCurrent(in: room) else {
            return .failure(RpcError.builtIn(.recipientDisconnected))
        }
        guard let handler = handlers[method] else {
            return .failure(RpcError.builtIn(.unsupportedMethod))
        }

        do {
            let response = try await handler(RpcInvocationData(requestId: requestId,
                                                               callerIdentity: callerConnection.identity,
                                                               callerParticipantSid: callerConnection.sid,
                                                               callerDataPacketReceiveGeneration: callerConnection.dataPacketReceiveGeneration,
                                                               payload: payload,
                                                               responseTimeout: responseTimeout))
            return .success(response)
        } catch let error as RpcError {
            return .failure(error)
        } catch {
            log("[Rpc] Uncaught error returned by RPC handler for \(method): \(error). Returning APPLICATION_ERROR instead.", .warning)
            return .failure(RpcError.builtIn(.applicationError))
        }
    }

    /// Publish a handler dispatch outcome. Successful responses follow the caller's
    /// advertised `clientProtocol`: v2-capable peers receive a v2 stream (uncapped),
    /// legacy peers receive a v1 packet (capped at `MAX_RPC_PAYLOAD_BYTES`). Error
    /// responses always use a v1 packet per spec, regardless of caller transport.
    private func publishResult(_ result: DispatchResult,
                               in room: Room,
                               callerConnection: RpcParticipantConnection,
                               requestId: String) async throws
    {
        try requireCurrent(callerConnection, in: room)
        switch result {
        case let .success(payload):
            let callerProtocol = callerConnection.participant.clientProtocol
            if callerProtocol >= .v1 {
                if payload.byteLength > RpcStreamLimits.maximumPayloadBytes {
                    try await publishResponse(
                        in: room,
                        callerConnection: callerConnection,
                        requestId: requestId,
                        payload: nil,
                        error: .builtIn(.responsePayloadTooLarge)
                    )
                } else {
                    try await publishResponseStream(in: room,
                                                    callerConnection: callerConnection,
                                                    requestId: requestId,
                                                    payload: payload)
                }
            } else {
                try await publishResponse(in: room,
                                          callerConnection: callerConnection,
                                          requestId: requestId,
                                          payload: payload,
                                          error: nil)
            }
        case let .failure(error):
            try await publishResponse(in: room,
                                      callerConnection: callerConnection,
                                      requestId: requestId,
                                      payload: nil,
                                      error: error)
        }
    }

    // MARK: - Outgoing wire

    /// Publish a v1 `RpcResponse` packet. The 15 KB cap is a v1 wire-format constraint and
    /// is enforced here: if `payload` exceeds it, the packet is sent as a
    /// `responsePayloadTooLarge` error instead. v2 stream responses go through
    /// `publishResponseStream` and have no size limit.
    private func publishResponse(in room: Room,
                                 callerConnection: RpcParticipantConnection,
                                 requestId: String,
                                 payload: String?,
                                 error: RpcError?) async throws
    {
        try requireCurrent(callerConnection, in: room)
        var outgoingPayload = payload
        var outgoingError = error
        if let p = payload, p.byteLength > MAX_RPC_PAYLOAD_BYTES {
            log("[Rpc] Response payload too large for v1 packet (requestId=\(requestId))", .warning)
            outgoingPayload = nil
            outgoingError = RpcError.builtIn(.responsePayloadTooLarge)
        }

        let dataPacket = Livekit_DataPacket.with {
            $0.destinationIdentities = [callerConnection.identity.stringValue]
            $0.kind = .reliable
            $0.rpcResponse = Livekit_RpcResponse.with {
                $0.requestID = requestId
                if let outgoingError {
                    $0.error = outgoingError.toProto()
                } else {
                    $0.payload = outgoingPayload ?? ""
                }
            }
        }

        try requireCurrent(callerConnection, in: room)
        let sendGeneration = room.publisherDataChannel.sendGeneration
        try await room.send(
            dataPacket: dataPacket,
            expectedDataChannelSendGeneration: sendGeneration,
            admission: { callerConnection.isCurrent(in: room) }
        )
    }

    private func publishResponseStream(in room: Room,
                                       callerConnection: RpcParticipantConnection,
                                       requestId: String,
                                       payload: String) async throws
    {
        try requireCurrent(callerConnection, in: room)
        let options = StreamTextOptions(
            topic: RpcStreamTopic.response,
            attributes: [RpcStreamAttribute.requestId: requestId],
            destinationIdentities: [callerConnection.identity],
        )
        let writer = try await room.outgoingStreamManager.streamText(
            options: options,
            admission: { callerConnection.isCurrent(in: room) }
        )
        try requireCurrent(callerConnection, in: room)
        try await writer.write(payload)
        try requireCurrent(callerConnection, in: room)
        try await writer.close()
    }

    private func publishAck(in room: Room,
                            callerConnection: RpcParticipantConnection,
                            requestId: String) async throws
    {
        try requireCurrent(callerConnection, in: room)
        let dataPacket = Livekit_DataPacket.with {
            $0.destinationIdentities = [callerConnection.identity.stringValue]
            $0.kind = .reliable
            $0.rpcAck = Livekit_RpcAck.with {
                $0.requestID = requestId
            }
        }

        let sendGeneration = room.publisherDataChannel.sendGeneration
        try await room.send(
            dataPacket: dataPacket,
            expectedDataChannelSendGeneration: sendGeneration,
            admission: { callerConnection.isCurrent(in: room) }
        )
    }

    // MARK: - Helpers

    private func requireRoom() throws -> Room {
        guard let room else { throw LiveKitError(.invalidState, message: "Room is nil") }
        return room
    }

    private func requireCurrent(
        _ connection: RpcParticipantConnection,
        in room: Room
    ) throws {
        guard connection.isCurrent(in: room) else {
            throw RpcError.builtIn(.recipientDisconnected)
        }
    }
}
