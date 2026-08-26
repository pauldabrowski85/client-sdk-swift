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

/// Caller-side RPC.
///
/// Owns the in-flight bookkeeping (pending acks and pending responses) and the wire-level
/// publishing of v1 RPC packets / v2 RPC request streams. `LocalParticipant.performRpc`
/// is a one-line proxy that forwards into this actor.
actor RpcClientManager: Loggable {
    /// Default upper bound on round-trip latency to the destination, in seconds. Used by the
    /// ack watchdog and to clamp the effective response timeout. Exposed so the public
    /// `LocalParticipant.performRpc` overloads can share the same default.
    static let defaultMaxRoundTripLatency: TimeInterval = 7

    /// Default timeout for receiving a response after the initial connection, in seconds.
    static let defaultResponseTimeout: TimeInterval = 15

    private weak var room: Room?

    private var pendingAcks: Set<String> = Set()
    private var pendingResponses: [String: PendingRpcResponse] = [:]
    private var ackWatchdogs: [String: AnyTaskCancellable] = [:]

    /// Hook fired once after the RPC request has been published but before the caller
    /// starts waiting on the completer. Receives the freshly-generated `requestId` so
    /// a caller can simulate a fast remote ack/response that races the wait. Used by
    /// tests (reachable via `@testable import LiveKit`).
    var afterPublish: (@Sendable (String) async -> Void)?

    func attach(to room: Room) {
        self.room = room
    }

    func setAfterPublish(_ hook: (@Sendable (String) async -> Void)?) {
        afterPublish = hook
    }

    // MARK: - Public entry point

    /// Initiate an RPC call to a remote participant. Transport selection is automatic and
    /// matches the SDK's documented behavior: peer's `clientProtocol >= .v1` → v2 data stream,
    /// otherwise v1 packet.
    func performRpc(destinationIdentity: Participant.Identity,
                    method: String,
                    payload: String,
                    responseTimeout: TimeInterval = RpcClientManager.defaultResponseTimeout,
                    maxRoundTripLatency: TimeInterval = RpcClientManager.defaultMaxRoundTripLatency) async throws -> String
    {
        guard responseTimeout.isFinite,
              responseTimeout > 0,
              maxRoundTripLatency.isFinite,
              maxRoundTripLatency >= 0
        else {
            throw LiveKitError(.invalidParameter, message: "RPC responseTimeout must be positive and finite, and maxRoundTripLatency must be finite and nonnegative")
        }
        let minEffectiveTimeout = maxRoundTripLatency + 1
        let requestedEffectiveTimeout = max(responseTimeout, minEffectiveTimeout)
        let responseTimeoutMillisecondsDouble = (requestedEffectiveTimeout * 1000).rounded(.up)
        guard requestedEffectiveTimeout.isFinite,
              responseTimeoutMillisecondsDouble <= TimeInterval(UInt32.max)
        else {
            throw LiveKitError(.invalidParameter, message: "RPC response timeout exceeds the wire protocol limit")
        }
        let responseTimeoutMilliseconds = UInt32(responseTimeoutMillisecondsDouble)

        let room = try requireRoom()
        guard let destinationConnection = RpcParticipantConnection.resolveCurrent(
            in: room,
            identity: destinationIdentity
        ) else {
            throw RpcError.builtIn(.recipientDisconnected)
        }

        let remoteClientProtocol = destinationConnection.participant.clientProtocol
        let useStreamTransport = remoteClientProtocol >= .v1 && Self.serverSupportsRpcV2(room.serverVersion)

        let requestId = UUID().uuidString
        // Pre-register pending state synchronously on the actor *before* publishing the
        // request. Prevents a race where a fast remote can ack/respond before registration
        // completes — the response would otherwise log "received for unexpected request" and
        // the call would hang to the outer responseTimeout.
        let completer = AsyncCompleter<String>(label: "rpc-\(requestId)", defaultTimeout: responseTimeout)
        pendingAcks.insert(requestId)
        pendingResponses[requestId] = PendingRpcResponse(
            participantConnection: destinationConnection,
            completer: completer,
        )

        do {
            try requireCurrent(destinationConnection, in: room)
            if useStreamTransport {
                try await publishRequestStream(in: room,
                                               destinationConnection: destinationConnection,
                                               requestId: requestId,
                                               method: method,
                                               payload: payload,
                                               responseTimeoutMilliseconds: responseTimeoutMilliseconds)
            } else {
                try await publishRequest(in: room,
                                         destinationConnection: destinationConnection,
                                         requestId: requestId,
                                         method: method,
                                         payload: payload,
                                         responseTimeoutMilliseconds: responseTimeoutMilliseconds)
            }
        } catch {
            // Publish failed — clean up the registered state before re-throwing.
            removeAllPending(requestId)
            throw error
        }

        if let hook = afterPublish {
            await hook(requestId)
        }

        // Ack watchdog: fail fast if no ack arrives within maxRoundTripLatency.
        // Stored as `AnyTaskCancellable` so the deferred cleanup cancels the sleep
        // when the call resolves early.
        ackWatchdogs[requestId] = Task { [weak self] in
            try? await Task.sleep(nanoseconds: UInt64(maxRoundTripLatency * 1_000_000_000))
            guard let self else { return }
            await fireAckTimeoutIfPending(requestId: requestId)
        }.cancellable()

        defer { removeAllPending(requestId) }
        do {
            return try await completer.wait()
        } catch {
            // AsyncCompleter signals its own `defaultTimeout` expiry with `LiveKitError(.timedOut)`.
            // That path means "we got the ack (or the ack-watchdog hasn't fired yet) but the
            // user-supplied `responseTimeout` elapsed without a response" → `responseTimeout`
            // (1502). The ack-watchdog path resolves the completer directly with
            // `connectionTimeout` (1501) and falls through the `throw error` branch.
            if let error = error as? LiveKitError, error.type == .timedOut {
                throw RpcError.builtIn(.responseTimeout)
            }
            throw error
        }
    }

    // MARK: - Server-version compatibility

    /// v2 data-stream RPC requires server ≥ 1.8.0. Returns `true` when the server version
    /// is unknown (e.g. before signaling completes) so we don't downgrade unnecessarily.
    /// Older servers silently fall back to the v1 packet path; a >15 KB payload then
    /// surfaces as `requestPayloadTooLarge` (1402) on `publishRequest`.
    static func serverSupportsRpcV2(_ serverVersion: String?) -> Bool {
        guard let serverVersion else { return true }
        return serverVersion.compare("1.8.0", options: .numeric) != .orderedAscending
    }

    /// Watchdog terminal action: if `requestId` is still awaiting an ack, clear pending
    /// state and resolve its completer with `connectionTimeout`. Actor isolation and removing
    /// `pendingResponses[requestId]` before resolution give exactly one terminal path ownership.
    func fireAckTimeoutIfPending(requestId: String) {
        guard pendingAcks.contains(requestId) else { return }
        pendingAcks.remove(requestId)
        let pending = pendingResponses.removeValue(forKey: requestId)
        pending?.completer.resume(throwing: RpcError.builtIn(.connectionTimeout))
    }

    // MARK: - Incoming dispatch

    /// Resolve a pending RPC call from a v1 `RpcResponse` packet. Note that `pendingAcks`
    /// is intentionally not cleared here — the watchdog's gate stays armed until either
    /// `handleIncomingAck` or `fireAckTimeoutIfPending` clears it. A response removes the
    /// pending entry before resolution, so a later watchdog cannot resolve it again.
    func handleIncomingResponse(requestId: String,
                                payload: String?,
                                error: RpcError?,
                                senderIdentity: Participant.Identity,
                                senderParticipantSid: Participant.Sid?,
                                dataPacketReceiveGeneration: UInt64)
    {
        guard let pending = validatedPending(
            requestId: requestId,
            senderIdentity: senderIdentity,
            senderParticipantSid: senderParticipantSid,
            dataPacketReceiveGeneration: dataPacketReceiveGeneration
        ) else {
            log("[Rpc] Response received for unexpected RPC request, id = \(requestId)", .error)
            return
        }
        pendingResponses.removeValue(forKey: requestId)
        if let error {
            pending.completer.resume(throwing: error)
        } else {
            pending.completer.resume(returning: payload ?? "")
        }
    }

    /// Resolve a pending RPC call from a v2 response stream on `lk.rpc_response`. Reads the
    /// `lk.rpc_request_id` attribute to match against pending requests, then resolves
    /// with the streamed payload — but only if `senderIdentity` matches the original
    /// destination of the call. A response from any other peer is ignored (and the
    /// pending entry is left in place so the legitimate sender can still resolve).
    func handleIncomingResponseStream(reader: TextStreamReader, senderIdentity: Participant.Identity) async {
        guard let requestId = reader.info.attributes[RpcStreamAttribute.requestId] else {
            log("[Rpc] Incoming v2 RPC response stream is missing request id attribute", .error)
            return
        }

        // Validate the sender BEFORE reading the stream — matches JS and avoids
        // burning cycles (or worse, hitting `readAll` side effects) on a spoofed
        // payload. After this check passes, both the success and read-failure
        // paths trust the sender and resume the pending call directly.
        guard validatedPending(
            requestId: requestId,
            senderIdentity: senderIdentity,
            senderParticipantSid: reader.info.publisherParticipantSid,
            dataPacketReceiveGeneration: reader.info.dataPacketReceiveGeneration
        ) != nil else {
            log("[Rpc] Response stream for \(requestId) has stale or mismatched sender provenance; ignoring", .error)
            return
        }

        let payload: String
        do {
            payload = try await reader.readAll()
        } catch {
            log("[Rpc] Failed to read v2 RPC response payload for \(requestId): \(error)", .error)
            // Fail the pending call fast instead of letting it hang to responseTimeout.
            if let pending = validatedPending(
                requestId: requestId,
                senderIdentity: senderIdentity,
                senderParticipantSid: reader.info.publisherParticipantSid,
                dataPacketReceiveGeneration: reader.info.dataPacketReceiveGeneration
            ) {
                pendingResponses.removeValue(forKey: requestId)
                let rpcError: RpcError = if case StreamError.streamSizeExceeded = error {
                    .builtIn(.responsePayloadTooLarge)
                } else {
                    RpcError(code: RpcError.BuiltInError.applicationError.code,
                             message: "Error reading RPC response payload",
                             data: "")
                }
                pending.completer.resume(throwing: rpcError)
            }
            return
        }

        guard let pending = validatedPending(
            requestId: requestId,
            senderIdentity: senderIdentity,
            senderParticipantSid: reader.info.publisherParticipantSid,
            dataPacketReceiveGeneration: reader.info.dataPacketReceiveGeneration
        ) else {
            log("[Rpc] Response stream received for unexpected RPC request, id = \(requestId)", .error)
            return
        }
        pendingResponses.removeValue(forKey: requestId)
        pending.completer.resume(returning: payload)
    }

    func handleIncomingResponseStreamRejection(_ rejection: IncomingStreamRejection) {
        guard let requestId = rejection.attributes[RpcStreamAttribute.requestId] else {
            log("[Rpc] Rejected v2 response stream is missing request id", .error)
            return
        }
        guard let pending = validatedPending(
            requestId: requestId,
            senderIdentity: rejection.participantIdentity,
            senderParticipantSid: rejection.publisherParticipantSid,
            dataPacketReceiveGeneration: rejection.dataPacketReceiveGeneration
        )
        else { return }
        pendingResponses.removeValue(forKey: requestId)
        let error: RpcError = switch rejection.error {
        case .streamSizeExceeded, .invalidDeclaredLength:
            .builtIn(.responsePayloadTooLarge)
        default:
            .builtIn(.applicationError)
        }
        pending.completer.resume(throwing: error)
    }

    /// Clear the pending-ack flag for a request when an `RpcAck` arrives.
    func handleIncomingAck(requestId: String,
                           senderIdentity: Participant.Identity,
                           senderParticipantSid: Participant.Sid?,
                           dataPacketReceiveGeneration: UInt64)
    {
        guard validatedPending(
            requestId: requestId,
            senderIdentity: senderIdentity,
            senderParticipantSid: senderParticipantSid,
            dataPacketReceiveGeneration: dataPacketReceiveGeneration
        ) != nil else { return }
        pendingAcks.remove(requestId)
    }

    /// Reject every in-flight RPC targeting `identity` with `recipientDisconnected`
    /// (1503). Called from `Room._onParticipantDidDisconnect(identity:)` so the caller
    /// learns immediately instead of waiting for the user-supplied `responseTimeout`.
    /// Actor isolation and removing each pending entry before resolution make this safe
    /// when a real response races the disconnect.
    func handleParticipantDisconnected(
        identity: Participant.Identity,
        participantSid: Participant.Sid,
        dataPacketReceiveGeneration: UInt64,
        participant: RemoteParticipant
    ) {
        let toReap = pendingResponses.filter {
            let connection = $0.value.participantConnection
            return connection.identity == identity &&
                connection.sid == participantSid &&
                connection.dataPacketReceiveGeneration == dataPacketReceiveGeneration &&
                connection.participant === participant
        }
        for (requestId, pending) in toReap {
            pendingResponses.removeValue(forKey: requestId)
            pendingAcks.remove(requestId)
            ackWatchdogs.removeValue(forKey: requestId)
            pending.completer.resume(throwing: RpcError.builtIn(.recipientDisconnected))
        }
    }

    /// Reject every in-flight RPC with `recipientDisconnected`. Called from
    /// `Room.cleanUp(...)` during teardown / full reconnect — at that point no
    /// participant survives, so identity-filtering is unnecessary.
    func handleAllPendingDisconnected() {
        for (_, pending) in pendingResponses {
            pending.completer.resume(throwing: RpcError.builtIn(.recipientDisconnected))
        }
        pendingResponses.removeAll()
        pendingAcks.removeAll()
        ackWatchdogs.removeAll()
    }

    // MARK: - State ops

    /// Number of in-flight RPCs awaiting a response. Exposed for test-time leak checks.
    var pendingCount: Int {
        pendingResponses.count
    }

    func removeAllPending(_ requestId: String) {
        pendingAcks.remove(requestId)
        pendingResponses.removeValue(forKey: requestId)
        ackWatchdogs.removeValue(forKey: requestId)
    }

    // MARK: - Outgoing wire

    private func validatedPending(
        requestId: String,
        senderIdentity: Participant.Identity,
        senderParticipantSid: Participant.Sid?,
        dataPacketReceiveGeneration: UInt64?
    ) -> PendingRpcResponse? {
        guard let room,
              let pending = pendingResponses[requestId],
              let senderParticipantSid,
              let dataPacketReceiveGeneration
        else { return nil }
        let connection = pending.participantConnection
        guard connection.identity == senderIdentity,
              connection.sid == senderParticipantSid,
              connection.dataPacketReceiveGeneration == dataPacketReceiveGeneration,
              connection.isCurrent(in: room)
        else { return nil }
        return pending
    }

    private func requireCurrent(
        _ connection: RpcParticipantConnection,
        in room: Room
    ) throws {
        guard connection.isCurrent(in: room) else {
            throw RpcError.builtIn(.recipientDisconnected)
        }
    }

    // swiftlint:disable:next function_parameter_count
    private func publishRequest(in room: Room,
                                destinationConnection: RpcParticipantConnection,
                                requestId: String,
                                method: String,
                                payload: String,
                                responseTimeoutMilliseconds: UInt32) async throws
    {
        try requireCurrent(destinationConnection, in: room)
        guard payload.byteLength <= MAX_RPC_PAYLOAD_BYTES else {
            throw RpcError.builtIn(.requestPayloadTooLarge)
        }

        let dataPacket = Livekit_DataPacket.with {
            $0.destinationIdentities = [destinationConnection.identity.stringValue]
            $0.kind = .reliable
            $0.rpcRequest = Livekit_RpcRequest.with {
                $0.id = requestId
                $0.method = method
                $0.payload = payload
                $0.responseTimeoutMs = responseTimeoutMilliseconds
                $0.version = 1
            }
        }

        try requireCurrent(destinationConnection, in: room)
        let sendGeneration = room.publisherDataChannel.sendGeneration
        try await room.send(
            dataPacket: dataPacket,
            expectedDataChannelSendGeneration: sendGeneration,
            admission: { destinationConnection.isCurrent(in: room) }
        )
    }

    // swiftlint:disable:next function_parameter_count
    private func publishRequestStream(in room: Room,
                                      destinationConnection: RpcParticipantConnection,
                                      requestId: String,
                                      method: String,
                                      payload: String,
                                      responseTimeoutMilliseconds: UInt32) async throws
    {
        try requireCurrent(destinationConnection, in: room)
        guard payload.byteLength <= RpcStreamLimits.maximumPayloadBytes else {
            throw RpcError.builtIn(.requestPayloadTooLarge)
        }
        let options = StreamTextOptions(
            topic: RpcStreamTopic.request,
            attributes: [
                RpcStreamAttribute.requestId: requestId,
                RpcStreamAttribute.method: method,
                RpcStreamAttribute.timeoutMs: String(responseTimeoutMilliseconds),
                RpcStreamAttribute.version: RPC_STREAM_VERSION,
            ],
            destinationIdentities: [destinationConnection.identity],
        )
        try requireCurrent(destinationConnection, in: room)
        let writer = try await room.outgoingStreamManager.streamText(
            options: options,
            admission: { destinationConnection.isCurrent(in: room) }
        )
        try requireCurrent(destinationConnection, in: room)
        try await writer.write(payload)
        try requireCurrent(destinationConnection, in: room)
        try await writer.close()
    }

    // MARK: - Helpers

    private func requireRoom() throws -> Room {
        guard let room else { throw LiveKitError(.invalidState, message: "Room is nil") }
        return room
    }
}
