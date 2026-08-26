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

private struct RpcInvocationDeadlineExceeded: Error {}

private final class RpcWeakParticipant: @unchecked Sendable {
    weak var value: RemoteParticipant?

    init(_ value: RemoteParticipant) {
        self.value = value
    }
}

private struct RpcCallerConnectionToken: Sendable {
    let identity: Participant.Identity
    let sid: Participant.Sid
    let dataPacketReceiveGeneration: UInt64
    let participant: RpcWeakParticipant

    init(_ connection: RpcParticipantConnection) {
        identity = connection.identity
        sid = connection.sid
        dataPacketReceiveGeneration = connection.dataPacketReceiveGeneration
        participant = RpcWeakParticipant(connection.participant)
    }

    func isCurrent(in room: Room) -> Bool {
        guard let expectedParticipant = participant.value,
              room.dataPacketReceiveGeneration == dataPacketReceiveGeneration
        else { return false }
        return room._state.read { state in
            guard let current = state.remoteParticipants[identity] else { return false }
            return current === expectedParticipant &&
                current.sid == sid &&
                current.dataPacketReceiveGeneration == dataPacketReceiveGeneration
        }
    }
}

private struct RpcWorkOwner: Hashable, Sendable {
    let identity: Participant.Identity
    let sid: Participant.Sid?
    let dataPacketReceiveGeneration: UInt64

    init(_ connection: RpcParticipantConnection) {
        identity = connection.identity
        sid = connection.sid
        dataPacketReceiveGeneration = connection.dataPacketReceiveGeneration
    }

    init(
        identity: Participant.Identity,
        sid: Participant.Sid?,
        dataPacketReceiveGeneration: UInt64
    ) {
        self.identity = identity
        self.sid = sid
        self.dataPacketReceiveGeneration = dataPacketReceiveGeneration
    }
}

private struct RpcInvocationClock: Sendable {
    let nowNanoseconds: @Sendable () -> UInt64
    let sleepUntil: @Sendable (UInt64) async throws -> Void

    static let live = RpcInvocationClock(
        nowNanoseconds: RpcContinuousClock.nowNanoseconds,
        sleepUntil: { deadline in
            while true {
                let now = RpcContinuousClock.nowNanoseconds()
                guard deadline > now else { return }
                // The legacy Darwin API is continuous across system sleep and
                // back-deploys to every platform version supported by this SDK.
                try await Task.sleep(nanoseconds: deadline - now)
            }
        }
    )
}

final class RpcInvocationLease: @unchecked Sendable {
    private struct State {
        var handlerFinished = false
        var publicationFinished = false
        var released = false
    }

    private let state = StateSync(State())
    private let onRelease: @Sendable () -> Void

    init(onRelease: @escaping @Sendable () -> Void) {
        self.onRelease = onRelease
    }

    func release() {
        let shouldRelease = state.mutate { state in
            guard !state.released else { return false }
            state.released = true
            return true
        }
        if shouldRelease { onRelease() }
    }

    func handlerDidFinish() {
        let shouldRelease = state.mutate { state in
            guard !state.released else { return false }
            state.handlerFinished = true
            guard state.publicationFinished else { return false }
            state.released = true
            return true
        }
        if shouldRelease { onRelease() }
    }

    func publicationDidFinish() {
        let shouldRelease = state.mutate { state in
            guard !state.released else { return false }
            state.publicationFinished = true
            guard state.handlerFinished else { return false }
            state.released = true
            return true
        }
        if shouldRelease { onRelease() }
    }

    deinit {
        release()
    }
}

private final class RpcInvocationAdmission: @unchecked Sendable {
    private let owners = StateSync<[UUID: RpcWorkOwner]>([:])

    var count: Int { owners.read(\.count) }

    func admit(_ owner: RpcWorkOwner) -> RpcInvocationLease? {
        let id = UUID()
        let admitted = owners.mutate { owners in
            guard owners.count < RpcInvocationLimits.maximumInFlight else { return false }
            let ownerCount = owners.values.lazy.filter { $0 == owner }.count
            let identityCount = owners.values.lazy.filter { $0.identity == owner.identity }.count
            guard ownerCount < RpcInvocationLimits.maximumInFlightPerConnection,
                  identityCount < RpcInvocationLimits.maximumInFlightPerConnection
            else { return false }
            owners[id] = owner
            return true
        }
        guard admitted else { return nil }
        return RpcInvocationLease { [weak self] in
            self?.owners.mutate { $0.removeValue(forKey: id) }
        }
    }
}

private final class RpcControlPublicationGate: @unchecked Sendable {
    private let revoked = StateSync(false)

    var isAdmitted: Bool { !revoked.copy() && !Task.isCancelled }

    func sendAdmission(
        additionalAdmission: DataChannelSendAdmission
    ) -> DataChannelSendAdmission {
        DataChannelSendAdmission(
            preflight: { [weak self] in
                guard let self, !revoked.copy() else {
                    return LiveKitError(.cancelled, message: "RPC control publication was revoked")
                }
                return additionalAdmission.preflight()
            },
            attempt: { [weak self] send in
                guard let self else {
                    return .rejected(LiveKitError(.cancelled, message: "RPC control publication was released"))
                }
                return revoked.mutate { revoked in
                    guard !revoked else {
                        return .rejected(LiveKitError(
                            .cancelled,
                            message: "RPC control publication was revoked"
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
}

private final class RpcControlReplyQueue: @unchecked Sendable {
    private struct Entry {
        let owner: RpcWorkOwner
        let gate: RpcControlPublicationGate
        var task: Task<Void, Never>?
    }

    private struct State {
        var entries: [UUID: Entry] = [:]
        var droppedCount = 0
    }

    private let state = StateSync(State())

    var count: Int { state.read { $0.entries.count } }
    var droppedCount: Int { state.droppedCount }

    @discardableResult
    func submit(
        owner: RpcWorkOwner,
        operation: @escaping @Sendable (RpcControlPublicationGate) async -> Void
    ) -> Bool {
        let id = UUID()
        let gate = RpcControlPublicationGate()
        let admitted = state.mutate { state in
            guard state.entries.count < RpcInvocationLimits.maximumInFlight else {
                state.droppedCount += 1
                return false
            }
            let ownerCount = state.entries.values.lazy.filter { $0.owner == owner }.count
            let identityCount = state.entries.values.lazy.filter { $0.owner.identity == owner.identity }.count
            guard ownerCount < RpcInvocationLimits.maximumInFlightPerConnection,
                  identityCount < RpcInvocationLimits.maximumInFlightPerConnection
            else {
                state.droppedCount += 1
                return false
            }
            state.entries[id] = Entry(owner: owner, gate: gate, task: nil)
            return true
        }
        guard admitted else { return false }

        let task = Task { [weak self] in
            await operation(gate)
            self?.state.mutate { $0.entries.removeValue(forKey: id) }
        }
        let entryStillExists = state.mutate { state in
            guard var entry = state.entries[id] else { return false }
            entry.task = task
            state.entries[id] = entry
            return true
        }
        if !entryStillExists {
            gate.revoke()
            task.cancel()
        }
        return true
    }

    func drain(staleTo currentGeneration: UInt64) {
        let stale = state.read { state in
            state.entries.values.filter { $0.owner.dataPacketReceiveGeneration != currentGeneration }
        }
        for entry in stale {
            entry.gate.revoke()
            entry.task?.cancel()
        }
    }

    deinit {
        for entry in state.entries.values {
            entry.gate.revoke()
            entry.task?.cancel()
        }
    }
}

/// Handler-side RPC.
///
/// Owns the registered method-handler table and the wire-level handling of incoming
/// RPC requests (both v1 packets and v2 streams). `Room.registerRpcMethod` and
/// `Room.unregisterRpcMethod` are one-line proxies that forward into this actor.
actor RpcServerManager: Loggable {
    private weak var room: Room?
    private nonisolated let invocationAdmission = RpcInvocationAdmission()
    private nonisolated let controlReplyQueue = RpcControlReplyQueue()
    private var afterRequestStreamRead: (@Sendable () async -> Void)?
    private var beforeHandlerPreflight: (@Sendable () async -> Void)?
    private var invocationClock = RpcInvocationClock.live

    /// Method-name → handler map. Persists across `Room.cleanUp` and reconnects so callers
    /// don't have to re-register on every transient disconnect; cleared only when this
    /// actor deallocates with its owning `Room`.
    private var handlers: [String: RpcHandler] = [:]

    func attach(to room: Room) {
        self.room = room
    }

    func setAfterRequestStreamRead(_ hook: (@Sendable () async -> Void)?) {
        afterRequestStreamRead = hook
    }

    func setBeforeHandlerPreflight(_ hook: (@Sendable () async -> Void)?) {
        beforeHandlerPreflight = hook
    }

    func setContinuousTimeNanosecondsProvider(_ provider: @escaping @Sendable () -> UInt64) {
        invocationClock = RpcInvocationClock(
            nowNanoseconds: provider,
            sleepUntil: invocationClock.sleepUntil
        )
    }

    func setInvocationClock(
        nowNanoseconds: @escaping @Sendable () -> UInt64,
        sleepUntil: @escaping @Sendable (UInt64) async throws -> Void
    ) {
        invocationClock = RpcInvocationClock(
            nowNanoseconds: nowNanoseconds,
            sleepUntil: sleepUntil
        )
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
    nonisolated func enqueueIncomingRequest(
        callerIdentity: Participant.Identity,
        callerParticipantSid: Participant.Sid?,
        callerDataPacketReceiveGeneration: UInt64,
        requestId: String,
        method: String,
        payload: String,
        responseTimeout: TimeInterval,
        receivedAtContinuousTimeNanoseconds: UInt64,
        version: Int
    ) {
        let owner = RpcWorkOwner(
            identity: callerIdentity,
            sid: callerParticipantSid,
            dataPacketReceiveGeneration: callerDataPacketReceiveGeneration
        )
        guard let invocationLease = invocationAdmission.admit(owner) else {
            enqueueControlResponse(
                owner: owner,
                requestId: requestId,
                error: .receiverOverloaded,
                publishesAck: true
            )
            return
        }
        Task.discarding { [weak self] in
            guard let self else {
                invocationLease.release()
                return
            }
            await self.handleIncomingRequest(
                callerIdentity: callerIdentity,
                callerParticipantSid: callerParticipantSid,
                callerDataPacketReceiveGeneration: callerDataPacketReceiveGeneration,
                requestId: requestId,
                method: method,
                payload: payload,
                responseTimeout: responseTimeout,
                receivedAtContinuousTimeNanoseconds: receivedAtContinuousTimeNanoseconds,
                version: version,
                preadmittedInvocationLease: invocationLease
            )
        }
    }
    // swiftlint:enable function_parameter_count

    // swiftlint:disable function_parameter_count
    /// Handle an RPC request that arrived as a v1 `RpcRequest` packet. Successful
    /// responses follow the caller's advertised `clientProtocol`: v2-capable callers
    /// get a v2 data-stream response (capped at 1 MiB), legacy callers get a v1 packet.
    /// Errors always use a v1 packet per spec.
    func handleIncomingRequest(callerIdentity: Participant.Identity,
                               callerParticipantSid: Participant.Sid? = nil,
                               callerDataPacketReceiveGeneration: UInt64? = nil,
                               requestId: String,
                               method: String,
                               payload: String,
                               responseTimeout: TimeInterval,
                               receivedAtContinuousTimeNanoseconds: UInt64 = RpcContinuousClock.nowNanoseconds(),
                               version: Int,
                               preadmittedInvocationLease: RpcInvocationLease? = nil) async
    {
        guard let room = try? requireRoom() else {
            preadmittedInvocationLease?.release()
            return
        }
        guard let callerConnection = RpcParticipantConnection.resolve(
            in: room,
            identity: callerIdentity,
            sid: callerParticipantSid,
            dataPacketReceiveGeneration: callerDataPacketReceiveGeneration
        ) else {
            preadmittedInvocationLease?.release()
            log("[Rpc] Ignoring request \(requestId) with stale or missing caller provenance", .error)
            return
        }

        guard let invocationLease = preadmittedInvocationLease ?? admitInvocation(for: callerConnection) else {
            enqueueControlResponse(
                for: callerConnection,
                requestId: requestId,
                error: .receiverOverloaded,
                publishesAck: true
            )
            return
        }

        do {
            try await publishAck(in: room, callerConnection: callerConnection, requestId: requestId)
        } catch {
            log("[Rpc] Failed to publish RPC ack for \(requestId)", .error)
        }

        guard version == 1 else {
            defer { invocationLease.release() }
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
        guard payload.byteLength <= MAX_RPC_PAYLOAD_BYTES else {
            defer { invocationLease.release() }
            do {
                try await publishResponse(
                    in: room,
                    callerConnection: callerConnection,
                    requestId: requestId,
                    payload: nil,
                    error: RpcError.builtIn(.requestPayloadTooLarge)
                )
            } catch {
                log("[Rpc] Failed to publish oversized-request response for \(requestId)", .error)
            }
            return
        }

        guard let responseDeadlineContinuousTimeNanoseconds = Self.responseDeadlineContinuousTimeNanoseconds(
            receivedAtContinuousTimeNanoseconds: receivedAtContinuousTimeNanoseconds,
            responseTimeout: responseTimeout
        ) else {
            defer { invocationLease.release() }
            await publishInvalidRequest(
                in: room,
                callerConnection: callerConnection,
                requestId: requestId
            )
            return
        }

        defer { invocationLease.publicationDidFinish() }
        let result = await dispatchToHandler(callerConnection: callerConnection,
                                             requestId: requestId,
                                             method: method,
                                             payload: payload,
                                             responseDeadlineContinuousTimeNanoseconds: responseDeadlineContinuousTimeNanoseconds,
                                             invocationLease: invocationLease)
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
        guard let room = try? requireRoom() else {
            await reader.cancel()
            return
        }
        guard let callerConnection = RpcParticipantConnection.resolve(
            in: room,
            identity: callerIdentity,
            sid: reader.info.publisherParticipantSid,
            dataPacketReceiveGeneration: reader.info.dataPacketReceiveGeneration
        ) else {
            await reader.cancel()
            log("[Rpc] Ignoring request stream with stale or missing caller provenance", .error)
            return
        }

        let attrs = reader.info.attributes
        // requestId is the correlation key; without it we can't send a typed error back,
        // so log and bail (the caller will hit its own response timeout).
        guard let requestId = attrs[RpcStreamAttribute.requestId] else {
            await reader.cancel()
            log("[Rpc] Incoming v2 RPC request stream is missing request id; cannot correlate", .error)
            return
        }
        guard let method = attrs[RpcStreamAttribute.method],
              let timeoutMsString = attrs[RpcStreamAttribute.timeoutMs],
              let timeoutMs = UInt32(timeoutMsString),
              let version = attrs[RpcStreamAttribute.version]
        else {
            await reader.cancel()
            log("[Rpc] Incoming v2 RPC request stream for \(requestId) is missing required attributes", .error)
            enqueueControlResponse(
                for: callerConnection,
                requestId: requestId,
                error: RpcError(code: RpcError.BuiltInError.applicationError.code,
                                message: "RPC data stream malformed",
                                data: ""),
                publishesAck: false
            )
            return
        }
        let responseTimeout = TimeInterval(timeoutMs) / 1000

        guard version == RPC_STREAM_VERSION else {
            await reader.cancel()
            enqueueControlResponse(
                for: callerConnection,
                requestId: requestId,
                error: .builtIn(.unsupportedVersion),
                publishesAck: true
            )
            return
        }

        guard let responseDeadlineContinuousTimeNanoseconds = Self.responseDeadlineContinuousTimeNanoseconds(
            receivedAtContinuousTimeNanoseconds: reader.info.receivedAtContinuousTimeNanoseconds,
            responseTimeout: responseTimeout
        ) else {
            await reader.cancel()
            enqueueControlResponse(
                for: callerConnection,
                requestId: requestId,
                error: .builtIn(.applicationError),
                publishesAck: true
            )
            return
        }
        guard let invocationLease = admitInvocation(for: callerConnection) else {
            await reader.cancel()
            enqueueControlResponse(
                for: callerConnection,
                requestId: requestId,
                error: .receiverOverloaded,
                publishesAck: true
            )
            return
        }

        do {
            try await publishAck(in: room, callerConnection: callerConnection, requestId: requestId)
        } catch {
            log("[Rpc] Failed to publish RPC ack for \(requestId)", .error)
        }

        let payload: String
        do {
            payload = try await readPayload(
                from: reader,
                responseDeadlineContinuousTimeNanoseconds: responseDeadlineContinuousTimeNanoseconds
            )
        } catch {
            defer { invocationLease.release() }
            log("[Rpc] Failed to read v2 RPC request payload for \(requestId): \(error)", .error)
            let rpcError: RpcError = if error is RpcInvocationDeadlineExceeded {
                .builtIn(.responseTimeout)
            } else if case StreamError.streamSizeExceeded = error {
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

        if let afterRequestStreamRead { await afterRequestStreamRead() }
        guard callerConnection.isCurrent(in: room) else {
            invocationLease.release()
            return
        }
        guard invocationClock.nowNanoseconds() < responseDeadlineContinuousTimeNanoseconds else {
            defer { invocationLease.release() }
            do {
                try await publishResponse(
                    in: room,
                    callerConnection: callerConnection,
                    requestId: requestId,
                    payload: nil,
                    error: RpcError.builtIn(.responseTimeout)
                )
            } catch {
                log("[Rpc] Failed to publish expired-request response for \(requestId)", .error)
            }
            return
        }
        defer { invocationLease.publicationDidFinish() }
        let result = await dispatchToHandler(callerConnection: callerConnection,
                                             requestId: requestId,
                                             method: method,
                                             payload: payload,
                                             responseDeadlineContinuousTimeNanoseconds: responseDeadlineContinuousTimeNanoseconds,
                                             invocationLease: invocationLease)
        do {
            try await publishResult(result,
                                    in: room,
                                    callerConnection: callerConnection,
                                    requestId: requestId)
        } catch {
            log("[Rpc] Failed to publish RPC response for \(requestId)", .error)
        }
    }

    nonisolated func enqueueIncomingRequestStreamRejection(_ rejection: IncomingStreamRejection) {
        guard let requestId = rejection.attributes[RpcStreamAttribute.requestId] else {
            Self.log("[Rpc] Rejected v2 request stream is missing request id", .error)
            return
        }
        let error: RpcError = switch rejection.error {
        case .streamSizeExceeded, .invalidDeclaredLength:
            .builtIn(.requestPayloadTooLarge)
        case .tooManyOpenStreams:
            .receiverOverloaded
        default:
            .builtIn(.applicationError)
        }
        enqueueControlResponse(
            owner: RpcWorkOwner(
                identity: rejection.participantIdentity,
                sid: rejection.publisherParticipantSid,
                dataPacketReceiveGeneration: rejection.dataPacketReceiveGeneration
            ),
            requestId: requestId,
            error: error,
            publishesAck: false
        )
    }

    // swiftlint:enable function_body_length

    // MARK: - Admission and deadlines

    nonisolated var activeInvocationCount: Int { invocationAdmission.count }
    nonisolated var activeControlReplyCount: Int { controlReplyQueue.count }
    nonisolated var droppedControlReplyCount: Int { controlReplyQueue.droppedCount }

    nonisolated func drainControlReplies(to currentGeneration: UInt64) {
        controlReplyQueue.drain(staleTo: currentGeneration)
    }

    private static func responseDeadlineContinuousTimeNanoseconds(
        receivedAtContinuousTimeNanoseconds: UInt64,
        responseTimeout: TimeInterval
    ) -> UInt64? {
        let timeoutNanosecondsDouble = (responseTimeout * 1_000_000_000).rounded(.up)
        guard responseTimeout.isFinite,
              responseTimeout > 0,
              timeoutNanosecondsDouble <= Double(UInt64.max)
        else { return nil }
        let timeoutNanoseconds = UInt64(timeoutNanosecondsDouble)
        let (deadline, overflow) = receivedAtContinuousTimeNanoseconds.addingReportingOverflow(timeoutNanoseconds)
        return overflow ? nil : deadline
    }

    private func admitInvocation(for callerConnection: RpcParticipantConnection) -> RpcInvocationLease? {
        invocationAdmission.admit(RpcWorkOwner(callerConnection))
    }

    private nonisolated func enqueueControlResponse(
        for callerConnection: RpcParticipantConnection,
        requestId: String,
        error: RpcError,
        publishesAck: Bool
    ) {
        enqueueControlResponse(
            owner: RpcWorkOwner(callerConnection),
            requestId: requestId,
            error: error,
            publishesAck: publishesAck
        )
    }

    private nonisolated func enqueueControlResponse(
        owner: RpcWorkOwner,
        requestId: String,
        error: RpcError,
        publishesAck: Bool
    ) {
        let submitted = controlReplyQueue.submit(owner: owner) { [weak self] gate in
            await self?.publishControlResponse(
                owner: owner,
                requestId: requestId,
                error: error,
                publishesAck: publishesAck,
                gate: gate
            )
        }
        guard !submitted else { return }
        let droppedCount = controlReplyQueue.droppedCount
        if droppedCount == 1 || droppedCount.nonzeroBitCount == 1 {
            Self.log(
                "[Rpc] Dropped bounded control response for \(requestId); total drops = \(droppedCount)",
                .warning
            )
        }
    }

    private func publishControlResponse(
        owner: RpcWorkOwner,
        requestId: String,
        error: RpcError,
        publishesAck: Bool,
        gate: RpcControlPublicationGate
    ) async {
        guard gate.isAdmitted,
              let room = try? requireRoom(),
              let callerConnection = RpcParticipantConnection.resolve(
                  in: room,
                  identity: owner.identity,
                  sid: owner.sid,
                  dataPacketReceiveGeneration: owner.dataPacketReceiveGeneration
              )
        else { return }
        do {
            if publishesAck {
                try await publishAck(
                    in: room,
                    callerConnection: callerConnection,
                    requestId: requestId,
                    controlGate: gate
                )
            }
            try await publishResponse(
                in: room,
                callerConnection: callerConnection,
                requestId: requestId,
                payload: nil,
                error: error,
                controlGate: gate
            )
        } catch {
            log("[Rpc] Failed to publish bounded control response for \(requestId)", .error)
        }
    }

    private func publishInvalidRequest(
        in room: Room,
        callerConnection: RpcParticipantConnection,
        requestId: String
    ) async {
        do {
            try await publishResponse(
                in: room,
                callerConnection: callerConnection,
                requestId: requestId,
                payload: nil,
                error: RpcError.builtIn(.applicationError)
            )
        } catch {
            log("[Rpc] Failed to publish rejected-request response for \(requestId)", .error)
        }
    }

    private func readPayload(
        from reader: TextStreamReader,
        responseDeadlineContinuousTimeNanoseconds: UInt64
    ) async throws -> String {
        let now = invocationClock.nowNanoseconds()
        guard responseDeadlineContinuousTimeNanoseconds > now else {
            await reader.cancel()
            throw RpcInvocationDeadlineExceeded()
        }
        let invocationClock = invocationClock
        let payload = try await withThrowingTaskGroup(of: String.self) { group in
            group.addTask { try await reader.readAll() }
            group.addTask {
                try await invocationClock.sleepUntil(responseDeadlineContinuousTimeNanoseconds)
                throw RpcInvocationDeadlineExceeded()
            }
            do {
                guard let first = try await group.next() else {
                    throw RpcInvocationDeadlineExceeded()
                }
                group.cancelAll()
                return first
            } catch {
                group.cancelAll()
                await reader.cancel()
                throw error
            }
        }
        guard invocationClock.nowNanoseconds() < responseDeadlineContinuousTimeNanoseconds else {
            await reader.cancel()
            throw RpcInvocationDeadlineExceeded()
        }
        return payload
    }

    // MARK: - Handler dispatch

    private enum DispatchResult: Sendable {
        case success(String)
        case failure(RpcError)
    }

    /// Look up the handler for `method`, invoke it, and produce a payload-or-error result.
    /// Size-checking the response is the responsibility of the publisher: the v1 wire has
    /// a 15 KB cap (enforced in `publishResponse`), the v2 stream wire has a 1 MiB cap.
    private func dispatchToHandler(callerConnection: RpcParticipantConnection,
                                   requestId: String,
                                   method: String,
                                   payload: String,
                                   responseDeadlineContinuousTimeNanoseconds: UInt64,
                                   invocationLease: RpcInvocationLease) async -> DispatchResult
    {
        guard let room, callerConnection.isCurrent(in: room) else {
            invocationLease.handlerDidFinish()
            return .failure(RpcError.builtIn(.recipientDisconnected))
        }
        guard let handler = handlers[method] else {
            invocationLease.handlerDidFinish()
            return .failure(RpcError.builtIn(.unsupportedMethod))
        }
        let now = invocationClock.nowNanoseconds()
        guard responseDeadlineContinuousTimeNanoseconds > now else {
            invocationLease.handlerDidFinish()
            return .failure(RpcError.builtIn(.responseTimeout))
        }
        let remaining = TimeInterval(responseDeadlineContinuousTimeNanoseconds - now) / 1_000_000_000

        let invocation = RpcInvocationData(
            requestId: requestId,
            callerIdentity: callerConnection.identity,
            callerParticipantSid: callerConnection.sid,
            callerDataPacketReceiveGeneration: callerConnection.dataPacketReceiveGeneration,
            payload: payload,
            responseTimeout: remaining,
            responseDeadlineContinuousTimeNanoseconds: responseDeadlineContinuousTimeNanoseconds
        )
        let callerToken = RpcCallerConnectionToken(callerConnection)
        let callerIsCurrent: @Sendable () -> Bool = { [weak room] in
            guard let room else { return false }
            return callerToken.isCurrent(in: room)
        }
        let result = AsyncCompleter<DispatchResult>(
            label: "rpc-handler-\(requestId)",
            defaultTimeout: remaining
        )
        let invocationClock = invocationClock
        let beforeHandlerPreflight = beforeHandlerPreflight
        let handlerTask = Task.detached { [handler, invocationLease] in
            defer { invocationLease.handlerDidFinish() }
            if let beforeHandlerPreflight { await beforeHandlerPreflight() }
            guard !Task.isCancelled,
                  callerIsCurrent(),
                  invocationClock.nowNanoseconds() < responseDeadlineContinuousTimeNanoseconds
            else {
                result.resume(returning: .failure(RpcError.builtIn(.responseTimeout)))
                return
            }

            let dispatchResult: DispatchResult
            do {
                dispatchResult = .success(try await handler(invocation))
            } catch let error as RpcError {
                dispatchResult = .failure(error)
            } catch {
                RpcServerManager.log(
                    "[Rpc] Uncaught error returned by RPC handler for \(method): \(error). Returning APPLICATION_ERROR instead.",
                    .warning
                )
                dispatchResult = .failure(RpcError.builtIn(.applicationError))
            }
            result.resume(returning: dispatchResult)
        }
        let deadlineTask = Task.detached {
            do {
                try await invocationClock.sleepUntil(responseDeadlineContinuousTimeNanoseconds)
            } catch {
                return
            }
            result.resume(returning: .failure(RpcError.builtIn(.responseTimeout)))
        }
        defer { deadlineTask.cancel() }

        do {
            let dispatchResult = try await result.wait(timeout: remaining + 1)
            guard invocationClock.nowNanoseconds() < responseDeadlineContinuousTimeNanoseconds else {
                handlerTask.cancel()
                return .failure(RpcError.builtIn(.responseTimeout))
            }
            return dispatchResult
        } catch let error as LiveKitError where error.type == .timedOut {
            handlerTask.cancel()
            return .failure(RpcError.builtIn(.responseTimeout))
        } catch {
            handlerTask.cancel()
            return .failure(RpcError.builtIn(.applicationError))
        }
    }

    /// Publish a handler dispatch outcome. Successful responses follow the caller's
    /// advertised `clientProtocol`: v2-capable peers receive a v2 stream (capped at 1 MiB),
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
    /// `publishResponseStream` and are capped at `RpcStreamLimits.maximumPayloadBytes`.
    private func publishResponse(in room: Room,
                                 callerConnection: RpcParticipantConnection,
                                 requestId: String,
                                 payload: String?,
                                 error: RpcError?,
                                 controlGate: RpcControlPublicationGate? = nil) async throws
    {
        guard controlGate?.isAdmitted != false else { throw LiveKitError(.cancelled) }
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
        let connectionAdmission = callerConnection.sendAdmission(in: room)
        let sendAdmission = controlGate?.sendAdmission(additionalAdmission: connectionAdmission) ??
            connectionAdmission
        try await room.send(
            dataPacket: dataPacket,
            expectedDataChannelSendGeneration: sendGeneration,
            admission: sendAdmission
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
            admission: callerConnection.sendAdmission(in: room)
        )
        try await writer.write(payload)
        try await writer.close()
    }

    private func publishAck(in room: Room,
                            callerConnection: RpcParticipantConnection,
                            requestId: String,
                            controlGate: RpcControlPublicationGate? = nil) async throws
    {
        guard controlGate?.isAdmitted != false else { throw LiveKitError(.cancelled) }
        try requireCurrent(callerConnection, in: room)
        let dataPacket = Livekit_DataPacket.with {
            $0.destinationIdentities = [callerConnection.identity.stringValue]
            $0.kind = .reliable
            $0.rpcAck = Livekit_RpcAck.with {
                $0.requestID = requestId
            }
        }

        let sendGeneration = room.publisherDataChannel.sendGeneration
        let connectionAdmission = callerConnection.sendAdmission(in: room)
        let sendAdmission = controlGate?.sendAdmission(additionalAdmission: connectionAdmission) ??
            connectionAdmission
        try await room.send(
            dataPacket: dataPacket,
            expectedDataChannelSendGeneration: sendGeneration,
            admission: sendAdmission
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
