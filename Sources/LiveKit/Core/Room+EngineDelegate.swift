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

extension Room {
    // swiftlint:disable:next cyclomatic_complexity function_body_length
    func engine(_: Room, didMutateState state: Room.State, oldState: Room.State) {
        if state.connectionState != oldState.connectionState {
            // connectionState did update

            // only if quick-reconnect
            if case .connected = state.connectionState, case .quick = state.isReconnectingWithMode {
                resetTrackSettings()
            }

            // Re-send track permissions
            if case .connected = state.connectionState {
                Task {
                    do {
                        try await localParticipant.sendTrackSubscriptionPermissions()
                    } catch {
                        log("Failed to send track subscription permissions, error: \(error)", .error)
                    }
                }
            }

            delegates.notify(label: { "room.didUpdate connectionState: \(state.connectionState) oldValue: \(oldState.connectionState)" }) {
                $0.room?(self, didUpdateConnectionState: state.connectionState, from: oldState.connectionState)
            }

            // Individual connectionState delegates
            if case .connected = state.connectionState {
                // Connected
                if case .reconnecting = oldState.connectionState {
                    delegates.notify { $0.roomDidReconnect?(self) }
                } else {
                    delegates.notify { $0.roomDidConnect?(self) }
                }
            } else if case .reconnecting = state.connectionState {
                // Re-connecting
                delegates.notify { $0.roomIsReconnecting?(self) }
            } else if case .disconnected = state.connectionState {
                // Disconnected
                if case .connecting = oldState.connectionState {
                    delegates.notify { $0.room?(self, didFailToConnectWithError: oldState.disconnectError) }
                } else {
                    delegates.notify { $0.room?(self, didDisconnectWithError: state.disconnectError) }
                }
            }
        }

        if state.connectionState == .connected,
           oldState.connectionState == .reconnecting,
           oldState.isReconnectingWithMode == .full,
           state.roomOptions.autoRepublishLocalTracksOnFullReconnect
        {
            // Did complete a full reconnect
            log("Re-publishing local tracks...")
            Task.detached { [weak self] in
                guard let self else { return }
                do {
                    try await localParticipant.republishAllTracks()
                } catch {
                    log("Failed to re-publish local tracks, error: \(error)", .error)
                }
            }
        }

        // Notify when reconnection starts
        if oldState.isReconnectingWithMode == nil, state.isReconnectingWithMode != nil {
            if let startMode = state.isReconnectingWithMode {
                delegates.notify(label: { "room.didStartReconnectWithMode: \(startMode)" }) {
                    $0.room?(self, didStartReconnectWithMode: startMode)
                }
            }
        }

        // Notify when reconnection completes
        if oldState.isReconnectingWithMode != nil, state.isReconnectingWithMode == nil {
            if let completedMode = oldState.isReconnectingWithMode {
                delegates.notify(label: { "room.didCompleteReconnectWithMode: \(completedMode)" }) {
                    $0.room?(self, didCompleteReconnectWithMode: completedMode)
                }
            }
        }

        // Notify when reconnection mode changes
        if state.isReconnectingWithMode != oldState.isReconnectingWithMode,
           let mode = state.isReconnectingWithMode
        {
            delegates.notify(label: { "room.didUpdate reconnectionMode: \(String(describing: state.isReconnectingWithMode)) oldValue: \(String(describing: oldState.isReconnectingWithMode))" }) {
                $0.room?(self, didUpdateReconnectMode: mode)
            }
        }

        // Notify change when engine's state mutates
        Task { @MainActor in
            self.objectWillChange.send()
        }
    }

    func engine(_ engine: Room, didUpdateSpeakers speakers: [Livekit_SpeakerInfo]) {
        let activeSpeakers = _state.mutate { state -> [Participant] in
            var activeSpeakers: [Participant] = []
            var seenParticipantSids = [Participant.Sid: Bool]()
            for speaker in speakers {
                let participantSid = Participant.Sid(from: speaker.sid)
                seenParticipantSids[participantSid] = true
                if participantSid == localParticipant.sid {
                    localParticipant._state.mutate {
                        $0.audioLevel = speaker.level
                        if !$0.isSpeaking {
                            $0.lastSpokeAt = Date()
                        }
                        $0.isSpeaking = true
                    }
                    activeSpeakers.append(localParticipant)
                } else {
                    if let participant = state.remoteParticipant(forSid: participantSid) {
                        participant._state.mutate {
                            $0.audioLevel = speaker.level
                            if !$0.isSpeaking {
                                $0.lastSpokeAt = Date()
                            }
                            $0.isSpeaking = true
                        }
                        activeSpeakers.append(participant)
                    }
                }
            }

            if let localParticipantSid = localParticipant.sid, seenParticipantSids[localParticipantSid] == nil {
                localParticipant._state.mutate {
                    $0.audioLevel = 0.0
                    $0.isSpeaking = false
                }
            }

            for participant in state.remoteParticipants.values {
                if let participantSid = participant.sid, seenParticipantSids[participantSid] == nil {
                    participant._state.mutate {
                        $0.audioLevel = 0.0
                        $0.isSpeaking = false
                    }
                }
            }

            return activeSpeakers
        }

        if case .connected = engine._state.connectionState {
            delegates.notify(label: { "room.didUpdate speakers: \(activeSpeakers)" }) {
                $0.room?(self, didUpdateSpeakingParticipants: activeSpeakers)
            }
        }
    }

    func engine(
        _: Room,
        didAddTrack track: LKRTCMediaStreamTrack,
        rtpReceiver: LKRTCRtpReceiver,
        stream: LKRTCMediaStream,
        sourceTransport: Transport,
        receiveGeneration: UInt64
    ) async {
        let parseResult = parse(streamId: stream.streamId)
        let trackId = parseResult.trackId ?? Track.Sid(from: track.trackId)

        let participant = currentRemoteParticipant(
            forSid: parseResult.participantSid,
            sourceTransport: sourceTransport,
            receiveGeneration: receiveGeneration
        )

        guard let participant else {
            log("RemoteParticipant not found for sid: \(parseResult.participantSid), remoteParticipants: \(remoteParticipants)", .warning)
            return
        }

        guard let publication = participant.trackPublications[trackId] as? RemoteTrackPublication,
              let subscriptionAdmission = publication.currentSubscriptionAdmissionSnapshot()
        else {
            track.isEnabled = false
            return
        }

        let addTrack: @Sendable () async throws -> Void = {
            guard self.isCurrentMediaSource(
                sourceTransport,
                participant: participant,
                receiveGeneration: receiveGeneration
            ) else { return }

            // TODO: Only retry for TrackError.state = error
            try await participant.addSubscribedMediaTrack(
                rtcTrack: track,
                rtpReceiver: rtpReceiver,
                trackSid: trackId,
                sourceTransport: sourceTransport,
                receiveGeneration: receiveGeneration,
                subscriptionAdmission: subscriptionAdmission
            )
        }

        do {
            if subscriptionAdmission.isLegacy {
                let task = Task.retrying(retryDelay: 0.2) { _, _ in
                    try await addTrack()
                }
                try await task.value
            } else {
                try await addTrack()
            }
        } catch {
            log("addSubscribedMediaTrack failed, error: \(error)", .error)
            if !subscriptionAdmission.isLegacy {
                await disconnect()
            }
        }
    }

    func engine(
        _: Room,
        didRemoveTrack track: LKRTCMediaStreamTrack,
        sourceTransport: Transport,
        receiveGeneration: UInt64
    ) async throws {
        let trackSid = Track.Sid(from: track.trackId)
        guard let publication = currentRemotePublication(
            forSid: trackSid,
            sourceTransport: sourceTransport,
            receiveGeneration: receiveGeneration
        ) else { return }

        guard isCurrentMediaSource(
            sourceTransport,
            publication: publication,
            receiveGeneration: receiveGeneration
        ) else { return }

        guard let subscribedTrack = publication.track,
              subscribedTrack.mediaTrack === track
        else { return }

        guard await publication.replaceSubscribedTrack(expected: subscribedTrack, with: nil) else {
            return
        }

        do {
            try await publication.retireRetainedRemoteTrack(subscribedTrack)
        } catch {
            publication.invalidateSubscriptionAdmissionForOwnershipLoss()
            throw error
        }
    }

    private func currentRemoteParticipant(
        forSid participantSid: Participant.Sid,
        sourceTransport: Transport,
        receiveGeneration: UInt64
    ) -> RemoteParticipant? {
        guard receiveGeneration == dataPacketReceiveGeneration else { return nil }

        return _state.read { state in
            guard state.transport?.subscriber === sourceTransport,
                  sourceTransport.dataPacketReceiveGeneration == receiveGeneration
            else { return nil }
            return state.remoteParticipant(forSid: participantSid)
        }
    }

    private func currentRemotePublication(
        forSid trackSid: Track.Sid,
        sourceTransport: Transport,
        receiveGeneration: UInt64
    ) -> RemoteTrackPublication? {
        guard receiveGeneration == dataPacketReceiveGeneration else { return nil }

        return _state.read { state in
            guard state.transport?.subscriber === sourceTransport,
                  sourceTransport.dataPacketReceiveGeneration == receiveGeneration
            else { return nil }
            return state.remoteParticipants.values
                .lazy
                .compactMap { $0._state.trackPublications[trackSid] as? RemoteTrackPublication }
                .first
        }
    }

    func isCurrentMediaSource(
        _ sourceTransport: Transport,
        participant: RemoteParticipant,
        publication: RemoteTrackPublication? = nil,
        track: Track? = nil,
        rtcTrack: LKRTCMediaStreamTrack? = nil,
        receiveGeneration: UInt64
    ) -> Bool {
        guard receiveGeneration == dataPacketReceiveGeneration else { return false }

        return _state.read { state in
            guard state.transport?.subscriber === sourceTransport,
                  sourceTransport.dataPacketReceiveGeneration == receiveGeneration,
                  let participantSid = participant.sid
            else { return false }

            guard state.remoteParticipant(forSid: participantSid) === participant else { return false }
            guard let publication else { return true }
            guard (participant._state.trackPublications[publication.sid] as? RemoteTrackPublication) === publication else {
                return false
            }
            if let track, publication.track !== track { return false }
            if let rtcTrack, track?.mediaTrack !== rtcTrack { return false }
            return true
        }
    }

    private func isCurrentMediaSource(
        _ sourceTransport: Transport,
        publication: RemoteTrackPublication,
        receiveGeneration: UInt64
    ) -> Bool {
        guard receiveGeneration == dataPacketReceiveGeneration else { return false }

        return _state.read { state in
            guard state.transport?.subscriber === sourceTransport,
                  sourceTransport.dataPacketReceiveGeneration == receiveGeneration
            else { return false }

            return state.remoteParticipants.values.contains { participant in
                (participant._state.trackPublications[publication.sid] as? RemoteTrackPublication) === publication
            }
        }
    }

    func engine(
        _ engine: Room,
        didReceiveUserPacket packet: Livekit_UserPacket,
        from participantIdentity: String,
        participantSid: Participant.Sid?,
        dataPacketReceiveGeneration: UInt64,
        encryptionType: EncryptionType
    ) {
        guard let publisher = UserDataPublisher.resolve(
            in: engine,
            identity: participantIdentity,
            sid: participantSid,
            dataPacketReceiveGeneration: dataPacketReceiveGeneration
        ) else {
            log("Ignoring user data packet with incomplete or stale publisher provenance", .warning)
            return
        }

        delegates.notify(label: { "room.didReceive data: \(packet.payload)" }) {
            guard publisher.isCurrent(in: engine) else { return }
            $0.room?(
                self,
                participant: publisher.participant,
                didReceiveData: packet.payload,
                forTopic: packet.topic,
                encryptionType: encryptionType
            )
        }

        guard let participant = publisher.participant else { return }
        participant.delegates.notify(label: { "participant.didReceive data: \(packet.payload)" }) { [weak participant] delegate in
            guard let participant,
                  publisher.isCurrent(in: engine)
            else { return }
            delegate.participant?(
                participant,
                didReceiveData: packet.payload,
                forTopic: packet.topic,
                encryptionType: encryptionType
            )
        }
    }

    func room(didReceiveTranscriptionPacket packet: Livekit_Transcription) {
        // Try to find matching Participant.
        guard let participant = allParticipants[Participant.Identity(from: packet.transcribedParticipantIdentity)] else {
            log("[Transcription] Could not find participant: \(packet.transcribedParticipantIdentity)", .warning)
            return
        }

        guard let publication = participant._state.read({ $0.trackPublications[Track.Sid(from: packet.trackID)] }) else {
            log("[Transcription] Could not find publication: \(packet.trackID)", .warning)
            return
        }

        guard !packet.segments.isEmpty else {
            log("[Transcription] Received segments are empty", .warning)
            return
        }

        let segments = packet.segments.map { segment in
            TranscriptionSegment(id: segment.id,
                                 text: segment.text,
                                 language: segment.language,
                                 firstReceivedTime: _state.transcriptionReceivedTimes[segment.id] ?? Date(),
                                 lastReceivedTime: Date(),
                                 isFinal: segment.final)
        }

        _state.mutate { state in
            for segment in segments {
                if segment.isFinal {
                    state.transcriptionReceivedTimes.removeValue(forKey: segment.id)
                } else {
                    state.transcriptionReceivedTimes[segment.id] = segment.firstReceivedTime
                }
            }
        }

        delegates.notify {
            $0.room?(self, participant: participant, trackPublication: publication, didReceiveTranscriptionSegments: segments)
        }

        participant.delegates.notify {
            $0.participant?(participant, trackPublication: publication, didReceiveTranscriptionSegments: segments)
        }
    }

    func room(
        didReceiveRpcResponse response: Livekit_RpcResponse,
        from participantIdentity: String,
        participantSid: Participant.Sid?,
        dataPacketReceiveGeneration: UInt64
    ) {
        let (payload, error): (String?, RpcError?) = switch response.value {
        case let .payload(v): (v, nil)
        case let .error(e): (nil, RpcError.fromProto(e))
        default: (nil, nil)
        }

        Task.discarding { [rpcClient] in
            await rpcClient.handleIncomingResponse(requestId: response.requestID,
                                                   payload: payload,
                                                   error: error,
                                                   senderIdentity: Participant.Identity(from: participantIdentity),
                                                   senderParticipantSid: participantSid,
                                                   dataPacketReceiveGeneration: dataPacketReceiveGeneration)
        }
    }

    func room(
        didReceiveRpcAck ack: Livekit_RpcAck,
        from participantIdentity: String,
        participantSid: Participant.Sid?,
        dataPacketReceiveGeneration: UInt64
    ) {
        Task.discarding { [rpcClient] in
            await rpcClient.handleIncomingAck(
                requestId: ack.requestID,
                senderIdentity: Participant.Identity(from: participantIdentity),
                senderParticipantSid: participantSid,
                dataPacketReceiveGeneration: dataPacketReceiveGeneration
            )
        }
    }

    func room(
        didReceiveRpcRequest request: Livekit_RpcRequest,
        from participantIdentity: String,
        participantSid: Participant.Sid?,
        dataPacketReceiveGeneration: UInt64,
        receivedAtContinuousTimeNanoseconds: UInt64
    ) {
        let callerIdentity = Participant.Identity(from: participantIdentity)
        let requestId = request.id
        let method = request.method
        let payload = request.payload
        let responseTimeout = TimeInterval(request.responseTimeoutMs) / TimeInterval(msecPerSec)
        let version = Int(request.version)

        Task.discarding { [rpcServer] in
            await rpcServer.handleIncomingRequest(callerIdentity: callerIdentity,
                                                  callerParticipantSid: participantSid,
                                                  callerDataPacketReceiveGeneration: dataPacketReceiveGeneration,
                                                  requestId: requestId,
                                                  method: method,
                                                  payload: payload,
                                                  responseTimeout: responseTimeout,
                                                  receivedAtContinuousTimeNanoseconds: receivedAtContinuousTimeNanoseconds,
                                                  version: version)
        }
    }
}

private enum UserDataPublisher: Sendable {
    case server(dataPacketReceiveGeneration: UInt64)
    case participant(
        identity: Participant.Identity,
        sid: Participant.Sid,
        dataPacketReceiveGeneration: UInt64,
        participant: RemoteParticipant
    )

    var participant: RemoteParticipant? {
        switch self {
        case .server: nil
        case let .participant(_, _, _, participant): participant
        }
    }

    static func resolve(
        in room: Room,
        identity identityString: String,
        sid: Participant.Sid?,
        dataPacketReceiveGeneration: UInt64
    ) -> UserDataPublisher? {
        guard dataPacketReceiveGeneration == room.dataPacketReceiveGeneration else { return nil }
        if identityString.isEmpty, sid == nil {
            return room._state.read { state in
                guard state.connectionState == .connected else { return nil }
                return .server(dataPacketReceiveGeneration: dataPacketReceiveGeneration)
            }
        }
        guard !identityString.isEmpty, let sid else { return nil }

        let identity = Participant.Identity(from: identityString)
        return room._state.read { state in
            guard case .connected = state.connectionState,
                  let participant = state.remoteParticipants[identity],
                  participant.sid == sid,
                  participant.dataPacketReceiveGeneration == dataPacketReceiveGeneration
            else { return nil }
            return .participant(
                identity: identity,
                sid: sid,
                dataPacketReceiveGeneration: dataPacketReceiveGeneration,
                participant: participant
            )
        }
    }

    func isCurrent(in room: Room) -> Bool {
        switch self {
        case let .server(dataPacketReceiveGeneration):
            guard dataPacketReceiveGeneration == room.dataPacketReceiveGeneration else { return false }
            return room._state.read { $0.connectionState == .connected }

        case let .participant(identity, sid, dataPacketReceiveGeneration, participant):
            guard dataPacketReceiveGeneration == room.dataPacketReceiveGeneration else { return false }
            return room._state.read { state in
                guard state.connectionState == .connected,
                      let current = state.remoteParticipants[identity]
                else { return false }
                return current === participant &&
                    current.sid == sid &&
                    current.dataPacketReceiveGeneration == dataPacketReceiveGeneration
            }
        }
    }
}
