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

internal import LiveKitWebRTC

// MARK: - Internal delegate

protocol DataChannelDelegate: AnyObject, Sendable {
    /// The encryption type is captured before decryption rewrites the packet payload.
    func dataChannel(
        _ dataChannelPair: DataChannelPair,
        didReceiveDataPacket dataPacket: Livekit_DataPacket,
        encryptionType: EncryptionType,
        receiveGeneration: UInt64,
        receivedAtContinuousTimeNanoseconds: UInt64
    )

    func dataChannel(
        _ dataChannelPair: DataChannelPair,
        didFailToDecryptDataPacket dataPacket: Livekit_DataPacket,
        error: LiveKitError,
        receiveGeneration: UInt64
    )
}

/// The lossy and reliable data channels for one peer connection, plus the packet semantics they
/// share: authenticated receive provenance, encryption, deduplication, and generation-bound sends.
///
/// Each channel owns its queue and flow control in a DataChannelDrain. This type binds those drains
/// to the Room generation that installed them. A replaced channel can finish an already-running
/// callback, so every receive is revalidated against the exact channel identity and generation
/// before it can mutate deduplication state or reach a delegate.
class DataChannelPair: NSObject, @unchecked Sendable, Loggable {
    // MARK: - Public

    let delegates = MulticastDelegate<DataChannelDelegate>(label: "DataChannelDelegate")
    let openCompleter = AsyncCompleter<Void>(
        label: "Data channel open",
        defaultTimeout: .defaultPublisherDataChannelOpen
    )

    var isOpen: Bool { lossy.isOpen && reliable.isOpen }
    var sendGeneration: UInt64 { _state.sendGeneration }

    // MARK: - Provenance

    enum ChannelKind: Equatable, Sendable {
        case lossy
        case reliable
    }

    struct ChannelOwnership: Equatable, Sendable {
        let channelIdentifier: ObjectIdentifier
        let receiveGeneration: UInt64
    }

    enum ReceiveAdmission: Equatable, Sendable {
        case accepted
        case duplicate
        case stale
    }

    struct PreparedSend: Sendable {
        let packet: Livekit_DataPacket
        let sendGeneration: UInt64
        let admission: (@Sendable () -> Bool)?
    }

    // MARK: - Private

    private struct State {
        var lossyChannelIdentifier: ObjectIdentifier?
        var reliableChannelIdentifier: ObjectIdentifier?
        var lossyReceiveGeneration: UInt64 = 0
        var reliableReceiveGeneration: UInt64 = 0
        var sendGeneration: UInt64 = 0
        var isResetting = false
        var lastResetError: Error?
        var reliableReceivedState = TTLDictionary<String, UInt32>(
            ttl: DataChannelPair.reliableReceivedStateTTL
        )
        var e2eeManager: E2EEManager?
    }

    private let _state = StateSync(State())

    // Assigned after super.init and never replaced. Each drain owns the delegate slot of its
    // currently attached WebRTC channel and rejects callbacks from superseded channels.
    private var lossy: DataChannelDrain<LossyStage>!
    private var reliable: DataChannelDrain<ReliableStage>!

    // MARK: - Init

    init(
        delegate: DataChannelDelegate? = nil,
        lossyChannel: LKRTCDataChannel? = nil,
        reliableChannel: LKRTCDataChannel? = nil,
        receiveGeneration: UInt64 = 0,
        onBufferStatusChange: (@Sendable (Bool, DataChannelKind) -> Void)? = nil
    ) {
        super.init()

        if let delegate {
            delegates.add(delegate: delegate)
        }

        lossy = DataChannelDrain(
            label: LKRTCDataChannel.Labels.lossy,
            lowWaterMark: Self.lossyLowThreshold,
            overflow: .dropOldest,
            stage: LossyStage(),
            maxMessageSize: Self.defaultMaxMessageSize,
            onMessage: { [weak self] data, channel in
                self?.handle(received: data, from: channel, kind: .lossy)
            },
            onStateChange: { [weak self] _ in self?.handleStateChange() },
            onBufferStatusChange: { isLow in onBufferStatusChange?(isLow, .lossy) }
        )
        reliable = DataChannelDrain(
            label: LKRTCDataChannel.Labels.reliable,
            lowWaterMark: Self.reliableLowThreshold,
            overflow: .park,
            stage: ReliableStage(retryFloor: Self.reliableRetryAmount),
            maxMessageSize: Self.defaultMaxMessageSize,
            onMessage: { [weak self] data, channel in
                self?.handle(received: data, from: channel, kind: .reliable)
            },
            onStateChange: { [weak self] _ in self?.handleStateChange() },
            onBufferStatusChange: { isLow in onBufferStatusChange?(isLow, .reliable) }
        )

        if let lossyChannel {
            set(lossy: lossyChannel, receiveGeneration: receiveGeneration)
        }
        if let reliableChannel {
            set(reliable: reliableChannel, receiveGeneration: receiveGeneration)
        }
    }

    // MARK: - Channels

    func set(reliable channel: LKRTCDataChannel?, receiveGeneration: UInt64) {
        setChannel(channel, kind: .reliable, receiveGeneration: receiveGeneration)
    }

    func set(lossy channel: LKRTCDataChannel?, receiveGeneration: UInt64) {
        setChannel(channel, kind: .lossy, receiveGeneration: receiveGeneration)
    }

    private func setChannel(
        _ channel: LKRTCDataChannel?,
        kind: ChannelKind,
        receiveGeneration: UInt64
    ) {
        _state.mutate { state in
            switch kind {
            case .lossy:
                state.lossyChannelIdentifier = channel.map(ObjectIdentifier.init)
                state.lossyReceiveGeneration = receiveGeneration
            case .reliable:
                state.reliableChannelIdentifier = channel.map(ObjectIdentifier.init)
                state.reliableReceiveGeneration = receiveGeneration
            }
        }

        switch kind {
        case .lossy:
            lossy.setChannel(channel)
        case .reliable:
            reliable.setChannel(channel)
        }
        handleStateChange()
    }

    private func handleStateChange() {
        if isOpen {
            openCompleter.resume(returning: ())
        }
    }

    func set(maxMessageSize: UInt64) {
        lossy.set(maxMessageSize: maxMessageSize)
        reliable.set(maxMessageSize: maxMessageSize)
    }

    func set(e2eeManager: E2EEManager?) {
        _state.mutate { $0.e2eeManager = e2eeManager }
    }

    /// Advances provenance without replacing the underlying channels, such as during a room move.
    func advanceReceiveGeneration(to generation: UInt64) {
        _state.mutate { state in
            if state.lossyChannelIdentifier != nil, generation >= state.lossyReceiveGeneration {
                state.lossyReceiveGeneration = generation
            }
            if state.reliableChannelIdentifier != nil, generation >= state.reliableReceiveGeneration {
                state.reliableReceiveGeneration = generation
            }
            state.reliableReceivedState.removeAll()
        }
    }

    func reset(throwing error: Error? = nil) {
        let nextGeneration = _state.mutate { state -> UInt64 in
            state.lossyChannelIdentifier = nil
            state.reliableChannelIdentifier = nil
            state.sendGeneration &+= 1
            state.isResetting = true
            state.lastResetError = error
            state.reliableReceivedState.removeAll()
            return state.sendGeneration
        }

        // Both fail events are enqueued before a send can observe isResetting == false. A send
        // prepared under the replacement generation therefore cannot be drained by the teardown
        // event for its predecessor.
        lossy.reset(throwing: error)
        reliable.reset(throwing: error)
        set(maxMessageSize: Self.defaultMaxMessageSize)
        openCompleter.reset(throwing: error)

        _state.mutate { state in
            if state.sendGeneration == nextGeneration {
                state.isResetting = false
            }
        }
    }

    // MARK: - Receive

    func currentOwnership(for dataChannel: LKRTCDataChannel) -> ChannelOwnership? {
        let kind = dataChannel.dataChannelPairKind
        return _state.read { state in
            let identifier = ObjectIdentifier(dataChannel)
            let currentIdentifier: ObjectIdentifier?
            let generation: UInt64
            switch kind {
            case .lossy:
                currentIdentifier = state.lossyChannelIdentifier
                generation = state.lossyReceiveGeneration
            case .reliable:
                currentIdentifier = state.reliableChannelIdentifier
                generation = state.reliableReceiveGeneration
            }
            guard currentIdentifier == identifier else { return nil }
            return ChannelOwnership(
                channelIdentifier: identifier,
                receiveGeneration: generation
            )
        }
    }

    func admitReceivedPacket(
        _ dataPacket: Livekit_DataPacket,
        from kind: ChannelKind,
        ownership: ChannelOwnership
    ) -> ReceiveAdmission {
        _state.mutate { state in
            guard Self.owns(ownership, kind: kind, state: state) else {
                return .stale
            }
            guard kind == .reliable,
                  dataPacket.sequence > 0,
                  !dataPacket.participantSid.isEmpty
            else {
                return .accepted
            }
            if let lastSequence = state.reliableReceivedState[dataPacket.participantSid],
               dataPacket.sequence <= lastSequence
            {
                return .duplicate
            }
            state.reliableReceivedState[dataPacket.participantSid] = dataPacket.sequence
            return .accepted
        }
    }

    private static func owns(
        _ ownership: ChannelOwnership,
        kind: ChannelKind,
        state: State
    ) -> Bool {
        switch kind {
        case .lossy:
            state.lossyChannelIdentifier == ownership.channelIdentifier &&
                state.lossyReceiveGeneration == ownership.receiveGeneration
        case .reliable:
            state.reliableChannelIdentifier == ownership.channelIdentifier &&
                state.reliableReceiveGeneration == ownership.receiveGeneration
        }
    }

    private func handle(
        received data: Data,
        from dataChannel: LKRTCDataChannel,
        kind: ChannelKind
    ) {
        let receivedAtContinuousTimeNanoseconds = RpcContinuousClock.nowNanoseconds()
        guard let ownership = currentOwnership(for: dataChannel) else {
            log("Ignoring data message from a superseded data channel", .warning)
            return
        }
        guard let dataPacket = try? Livekit_DataPacket(serializedBytes: data) else {
            log("Could not decode data message", .error)
            return
        }

        switch admitReceivedPacket(dataPacket, from: kind, ownership: ownership) {
        case .accepted:
            break
        case .duplicate:
            log("Ignoring duplicate/out-of-order reliable data message", .warning)
            return
        case .stale:
            log("Ignoring data message from a superseded data channel", .warning)
            return
        }

        guard let encryptedPacket = dataPacket.encryptedPacketOrNil,
              let e2eeManager = _state.e2eeManager
        else {
            delegates.notify {
                $0.dataChannel(
                    self,
                    didReceiveDataPacket: dataPacket,
                    encryptionType: .none,
                    receiveGeneration: ownership.receiveGeneration,
                    receivedAtContinuousTimeNanoseconds: receivedAtContinuousTimeNanoseconds
                )
            }
            return
        }

        let encryptionType = encryptedPacket.encryptionType.toLKType()
        do {
            let decryptedData = try e2eeManager.handle(
                encryptedData: encryptedPacket.toRTCEncryptedPacket(),
                participantIdentity: dataPacket.participantIdentity
            )
            let decryptedPayload = try Livekit_EncryptedPacketPayload(serializedBytes: decryptedData)
            let decrypted = dataPacket.modifying { decryptedPayload.applyTo(&$0) }

            delegates.notify { [decrypted] in
                $0.dataChannel(
                    self,
                    didReceiveDataPacket: decrypted,
                    encryptionType: encryptionType,
                    receiveGeneration: ownership.receiveGeneration,
                    receivedAtContinuousTimeNanoseconds: receivedAtContinuousTimeNanoseconds
                )
            }
        } catch {
            log("Failed to decrypt data packet: \(error)", .error)
            delegates.notify {
                $0.dataChannel(
                    self,
                    didFailToDecryptDataPacket: dataPacket,
                    error: LiveKitError(.decryptionFailed, internalError: error),
                    receiveGeneration: ownership.receiveGeneration
                )
            }
        }
    }

    // MARK: - Send

    func send(userPacket: Livekit_UserPacket, kind: Livekit_DataPacket_Kind) async throws {
        try await send(dataPacket: .with {
            $0.kind = kind
            $0.user = userPacket
        })
    }

    func send(dataPacket packet: consuming Livekit_DataPacket) async throws {
        try await send(prepared: prepareSend(dataPacket: packet))
    }

    func send(
        dataPacket packet: Livekit_DataPacket,
        expectedSendGeneration: UInt64
    ) async throws {
        try await send(prepared: prepareSend(
            dataPacket: packet,
            expectedSendGeneration: expectedSendGeneration
        ))
    }

    func send(
        dataPacket packet: Livekit_DataPacket,
        expectedSendGeneration: UInt64,
        admission: @escaping @Sendable () -> Bool
    ) async throws {
        try await send(prepared: prepareSend(
            dataPacket: packet,
            expectedSendGeneration: expectedSendGeneration,
            admission: admission
        ))
    }

    func prepareSend(dataPacket packet: Livekit_DataPacket) throws -> PreparedSend {
        let generation = try currentSendGeneration()
        return PreparedSend(
            packet: try withEncryption(packet),
            sendGeneration: generation,
            admission: nil
        )
    }

    private func prepareSend(
        dataPacket packet: Livekit_DataPacket,
        expectedSendGeneration: UInt64,
        admission: (@Sendable () -> Bool)? = nil
    ) throws -> PreparedSend {
        let generation = try currentSendGeneration()
        guard generation == expectedSendGeneration else {
            throw staleSendError()
        }
        return PreparedSend(
            packet: try withEncryption(packet),
            sendGeneration: generation,
            admission: admission
        )
    }

    func send(prepared: PreparedSend) async throws {
        let kind: ChannelKind = prepared.packet.kind == .lossy ? .lossy : .reliable
        let gate = makeSendAdmission(
            kind: kind,
            generation: prepared.sendGeneration,
            additionalAdmission: prepared.admission
        )
        switch kind {
        case .lossy:
            try await lossy.send(prepared.packet, admission: gate)
        case .reliable:
            try await reliable.send(prepared.packet, admission: gate)
        }
    }

    private func currentSendGeneration() throws -> UInt64 {
        let snapshot = _state.read {
            ($0.sendGeneration, $0.isResetting, $0.lastResetError)
        }
        guard !snapshot.1 else {
            throw snapshot.2 ?? LiveKitError(
                .cancelled,
                message: "Data channel generation is resetting"
            )
        }
        return snapshot.0
    }

    private func makeSendAdmission(
        kind: ChannelKind,
        generation: UInt64,
        additionalAdmission: (@Sendable () -> Bool)?
    ) -> DrainSendAdmission {
        DrainSendAdmission(
            preflight: { [weak self] in
                guard let self else {
                    return LiveKitError(
                        .cancelled,
                        message: "Data channel owner was released"
                    )
                }
                return self._state.read { state in
                    guard !state.isResetting, state.sendGeneration == generation else {
                        return Self.staleSendError(from: state)
                    }
                    guard additionalAdmission?() != false else {
                        return LiveKitError(
                            .cancelled,
                            message: "Data channel send admission was revoked"
                        )
                    }
                    return nil
                }
            },
            attempt: { [weak self] dataChannel, send in
                guard let self else {
                    return .rejected(LiveKitError(
                        .cancelled,
                        message: "Data channel owner was released"
                    ))
                }
                return self._state.read { state in
                    guard !state.isResetting, state.sendGeneration == generation else {
                        return .rejected(Self.staleSendError(from: state))
                    }
                    guard additionalAdmission?() != false else {
                        return .rejected(LiveKitError(
                            .cancelled,
                            message: "Data channel send admission was revoked"
                        ))
                    }
                    let currentIdentifier = switch kind {
                    case .lossy: state.lossyChannelIdentifier
                    case .reliable: state.reliableChannelIdentifier
                    }
                    guard currentIdentifier == ObjectIdentifier(dataChannel) else {
                        return .unavailable
                    }
                    return send() ? .sent : .failed
                }
            }
        )
    }

    private func withEncryption(_ packet: Livekit_DataPacket) throws -> Livekit_DataPacket {
        guard let e2eeManager = _state.e2eeManager,
              e2eeManager.isDataChannelEncryptionEnabled,
              let payload = Livekit_EncryptedPacketPayload(dataPacket: packet)
        else {
            return packet
        }
        do {
            let payloadData = try payload.serializedData()
            let encrypted = try Livekit_EncryptedPacket(
                rtcPacket: e2eeManager.encrypt(data: payloadData)
            )
            return packet.modifying { $0.encryptedPacket = encrypted }
        } catch {
            throw LiveKitError(.encryptionFailed, internalError: error)
        }
    }

    func retryReliable(lastSequence: UInt32) {
        let generation = _state.sendGeneration
        reliable.submit(
            command: .replay(after: lastSequence),
            admission: { [weak self] in
                self?._state.read {
                    !$0.isResetting && $0.sendGeneration == generation
                } ?? false
            }
        )
    }

    private func staleSendError() -> Error {
        _state.read(Self.staleSendError(from:))
    }

    private static func staleSendError(from state: State) -> Error {
        state.lastResetError ?? LiveKitError(
            .cancelled,
            message: "Data channel generation changed"
        )
    }

    // MARK: - Sync state

    func infos() -> [Livekit_DataChannelInfo] {
        [lossy.info(), reliable.info()].compactMap(\.self)
    }

    func receiveStates() -> [Livekit_DataChannelReceiveState] {
        _state.read { state in
            state.reliableReceivedState.map { sid, sequence in
                Livekit_DataChannelReceiveState.with {
                    $0.publisherSid = sid
                    $0.lastSeq = sequence
                }
            }
        }
    }

    // MARK: - Constants

    private static let reliableLowThreshold: UInt64 = 2 * 1024 * 1024
    private static let lossyLowThreshold: UInt64 = reliableLowThreshold
    private static let reliableRetryAmount: UInt64 = .init(
        Double(reliableLowThreshold) * 1.25
    )
    private static let reliableReceivedStateTTL: TimeInterval = 30

    /// Default before SDP negotiation and the clamp for malformed peer advertisements.
    static let defaultMaxMessageSize: UInt64 = 64000
}

private extension LKRTCDataChannel {
    var dataChannelPairKind: DataChannelPair.ChannelKind {
        label == Labels.lossy ? .lossy : .reliable
    }
}

// MARK: - SDP parsing

/// Parses the RFC 8841 max-message-size attribute. A value of zero means no limit.
func parseSDPMaxMessageSize(_ sdp: String) -> UInt64? {
    for line in sdp.components(separatedBy: .newlines) {
        let trimmed = line.trimmingCharacters(in: .whitespaces)
        let prefix = "a=max-message-size:"
        guard trimmed.hasPrefix(prefix) else { continue }
        let value = trimmed.dropFirst(prefix.count).trimmingCharacters(in: .whitespaces)
        return UInt64(value)
    }
    return nil
}
