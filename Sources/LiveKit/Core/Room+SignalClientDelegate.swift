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

#if !COCOAPODS && !LK_XCFRAMEWORK
import LiveKitNanopb
#endif
import Foundation

internal import LiveKitWebRTC

extension Room: SignalClientDelegate {
    func signalClient(_: SignalClient, didUpdateConnectionState connectionState: ConnectionState,
                      oldState: ConnectionState,
                      disconnectError: LiveKitError?) async
    {
        // connectionState did update
        if connectionState != oldState,
           // did disconnect
           case .disconnected = connectionState,
           // Only attempt re-connect if not cancelled
           let errorType = disconnectError?.type, errorType != .cancelled,
           // engine is currently connected state
           case .connected = _state.connectionState
        {
            Task {
                do {
                    try await startReconnect(reason: .websocket)
                } catch {
                    log("Failed calling startReconnect, error: \(error)", .error)
                }
            }
        }
    }

    func signalClient(_: SignalClient, didReceiveLeave action: Livekit_LeaveRequest_Action, reason: Livekit_DisconnectReason, regions: Livekit_RegionSettings?) async {
        log("action: \(action), reason: \(reason)")

        if let regions, let providedUrl = _state.providedUrl, let regionManager = await regionManager(for: providedUrl) {
            await regionManager.updateFromServerReportedRegions(regions)
        }

        let error = LiveKitError.from(reason: reason)
        switch action {
        case .reconnect:
            // Force .full for next reconnect
            _state.mutate { $0.nextReconnectMode = .full }
            fallthrough
        case .resume:
            // Abort current attempt
            await signalClient.cleanUp(withError: error)
        case .disconnect:
            await cleanUp(withError: error)
        default:
            log("Unknown leave action: \(action), ignoring", .warning)
        }
    }

    func signalClient(_: SignalClient, didUpdateSubscribedCodecs codecs: [Livekit_SubscribedCodec],
                      qualities: [Livekit_SubscribedQuality],
                      forTrackSid trackSid: String) async
    {
        // Check if dynacast is enabled
        guard _state.roomOptions.dynacast else { return }

        log("[Publish/Backup] Qualities: \(qualities.map { String(describing: $0) }.joined(separator: ", ")), Codecs: \(codecs.map { String(describing: $0) }.joined(separator: ", "))")

        let trackSid = Track.Sid(from: trackSid)
        guard let publication = localParticipant.trackPublications[trackSid] as? LocalTrackPublication else {
            log("Received subscribed quality update for an unknown track", .warning)
            return
        }

        if !codecs.isEmpty {
            guard let videoTrack = publication.track as? LocalVideoTrack else { return }
            let missingSubscribedCodecs = await (try? videoTrack._set(subscribedCodecs: codecs)) ?? []

            if !missingSubscribedCodecs.isEmpty {
                log("Missing codecs: \(missingSubscribedCodecs)")
                for missingSubscribedCodec in missingSubscribedCodecs {
                    do {
                        log("Publishing additional codec: \(missingSubscribedCodec)")
                        try await localParticipant.publish(additionalVideoCodec: missingSubscribedCodec, for: publication)
                    } catch {
                        log("Failed publishing additional codec: \(missingSubscribedCodec), error: \(error)", .error)
                    }
                }
            }

        } else {
            await localParticipant._set(subscribedQualities: qualities, forTrackSid: trackSid)
        }
    }

    func signalClient(_: SignalClient, didReceiveConnectResponse connectResponse: SignalClient.ConnectResponse) async {
        if case let .join(joinResponse) = connectResponse {
            log("\(joinResponse.serverInfo)", .info)

            if e2eeManager != nil, !joinResponse.sifTrailer.isEmpty {
                e2eeManager?.keyProvider.setSifTrailer(trailer: joinResponse.sifTrailer)
            }

            _state.mutate {
                $0.apply(roomInfo: joinResponse.room)
                $0.serverInfo = joinResponse.serverInfo.owned()

                localParticipant.set(info: joinResponse.participant, connectionState: $0.connectionState)
                localParticipant.set(enabledPublishCodecs: joinResponse.enabledPublishCodecs)

                if !joinResponse.otherParticipants.isEmpty {
                    for otherParticipant in joinResponse.otherParticipants {
                        $0.updateRemoteParticipant(info: otherParticipant, room: self)
                    }
                }
            }
        }
    }

    func signalClient(_: SignalClient, didUpdateRoom room: Livekit_Room) async {
        _state.mutate { $0.apply(roomInfo: room) }
    }

    func signalClient(_: SignalClient, didReceiveRoomMoved response: Livekit_RoomMovedResponse) async {
        let receiveGeneration = incrementDataPacketReceiveGeneration()
        await incomingStreamManager.reset(to: receiveGeneration)
        let moveError = LiveKitError(.cancelled, message: "Room moved; replacing transports")
        let wasConnected = _state.connectionState == .connected
        publisherDataChannel.reset(throwing: moveError)
        subscriberDataChannel.reset(throwing: moveError)

        log("didReceiveRoomMoved to room: \(response.hasRoom ? response.room.name : "unknown")")

        _updateState(for: response)

        if response.hasRoom {
            _notifyRoomMoved(name: response.room.name)
        }

        guard wasConnected else { return }

        do {
            try await startReconnect(reason: .transport, nextReconnectMode: .full)
        } catch {
            log("Unable to replace transports after room move: \(error)", .error)
            await disconnect()
        }
    }

    func signalClient(_: SignalClient, didUpdateSpeakers speakers: [Livekit_SpeakerInfo]) async {
        let activeSpeakers = _state.mutate { state -> [Participant] in
            var lastSpeakers = state.activeSpeakers.reduce(into: [Sid: Participant]()) { $0[$1.sid] = $1 }
            for speaker in speakers {
                let participantSid = Participant.Sid(from: speaker.sid)
                guard let participant = participantSid == localParticipant.sid ? localParticipant : state.remoteParticipant(forSid: participantSid) else {
                    continue
                }

                participant._state.mutate {
                    $0.audioLevel = speaker.level
                    if !$0.isSpeaking, speaker.active {
                        $0.lastSpokeAt = Date()
                    }
                    $0.isSpeaking = speaker.active
                }

                if speaker.active {
                    lastSpeakers[participantSid] = participant
                } else {
                    lastSpeakers.removeValue(forKey: participantSid)
                }
            }

            state.activeSpeakers = lastSpeakers.values.sorted(by: { $1.audioLevel > $0.audioLevel })

            return state.activeSpeakers
        }

        if case .connected = _state.connectionState {
            delegates.notify(label: { "room.didUpdate speakers: \(speakers)" }) {
                $0.room?(self, didUpdateSpeakingParticipants: activeSpeakers)
            }
        }
    }

    func signalClient(_: SignalClient, didUpdateConnectionQuality connectionQuality: [Livekit_ConnectionQualityInfo]) async {
        for entry in connectionQuality {
            let participantSid = Participant.Sid(from: entry.participantSid)
            if participantSid == localParticipant.sid {
                // update for LocalParticipant
                localParticipant._state.mutate { $0.connectionQuality = entry.quality.toLKType() }
            } else if let participant = _state.read({ $0.remoteParticipant(forSid: participantSid) }) {
                // udpate for RemoteParticipant
                participant._state.mutate { $0.connectionQuality = entry.quality.toLKType() }
            }
        }
    }

    func signalClient(_: SignalClient, didUpdateRemoteMute trackSid: Track.Sid, muted: Bool) async {
        log("trackSid: \(trackSid) isMuted: \(muted)")

        guard let publication = localParticipant._state.trackPublications[trackSid] as? LocalTrackPublication else {
            // publication was not found but the delegate was handled
            return
        }

        do {
            if muted {
                try await publication.mute()
            } else {
                try await publication.unmute()
            }
        } catch {
            log("Failed to update mute for publication, error: \(error)", .error)
        }
    }

    func signalClient(_: SignalClient, didUpdateSubscriptionPermission subscriptionPermission: Livekit_SubscriptionPermissionUpdate) async {
        log("did update subscriptionPermission: \(subscriptionPermission)")

        let participantSid = Participant.Sid(from: subscriptionPermission.participantSid)
        let trackSid = Track.Sid(from: subscriptionPermission.trackSid)

        guard let participant = _state.read({ $0.remoteParticipant(forSid: participantSid) }),
              let publication = participant.trackPublications[trackSid] as? RemoteTrackPublication
        else {
            return
        }

        do {
            let mustRetireRoom = try await publication.applySubscriptionPermission(
                subscriptionPermission.allowed
            )
            if mustRetireRoom,
               _state.read({ state in
                   state.remoteParticipant(forSid: participantSid) === participant &&
                       participant.trackPublications[trackSid] === publication
               })
            {
                await disconnect()
            }
        } catch {
            log("Failed to retire denied protected subscription: \(error)", .error)
            await disconnect()
        }
    }

    func signalClient(_: SignalClient, didUpdateTrackStreamStates trackStates: [Livekit_StreamStateInfo]) async {
        log("did update trackStates: \(trackStates.map { "(\($0.trackSid): \(String(describing: $0.state)))" }.joined(separator: ", "))")

        for update in trackStates {
            let participantSid = Participant.Sid(from: update.participantSid)
            let trackSid = Track.Sid(from: update.trackSid)

            // Try to find RemoteParticipant
            guard let participant = _state.read({ $0.remoteParticipant(forSid: participantSid) }) else { continue }
            // Try to find RemoteTrackPublication
            guard let trackPublication = participant._state.trackPublications[trackSid] as? RemoteTrackPublication else { continue }
            // Update streamState (and notify)
            trackPublication._state.mutate { $0.streamState = update.state.toLKType() }
        }
    }

    func signalClient(_: SignalClient, didUpdateParticipants participants: [Livekit_ParticipantInfo]) async {
        log("participants: \(participants)")

        var disconnectedParticipants = [(
            RemoteParticipant,
            Participant.Identity,
            Participant.Sid,
            UInt64
        )]()
        var newParticipants = [RemoteParticipant]()

        _state.mutate {
            for info in participants where info.state == .disconnected {
                let identity = Participant.Identity(from: info.identity)
                let sid = Participant.Sid(from: info.sid)
                if let participant = $0.remoteParticipants[identity], participant.sid == sid {
                    disconnectedParticipants.append((
                        participant,
                        identity,
                        sid,
                        participant.dataPacketReceiveGeneration
                    ))
                    $0.remoteParticipants[identity] = nil
                }
            }

            for info in participants {
                let infoIdentity = Participant.Identity(from: info.identity)

                if infoIdentity == localParticipant.identity {
                    localParticipant.set(info: info, connectionState: $0.connectionState)
                    continue
                }

                if info.state == .disconnected {
                    continue
                } else {
                    let infoSid = Participant.Sid(from: info.sid)
                    let current = $0.remoteParticipants[infoIdentity]
                    let isNewParticipant = current?.sid != infoSid
                    // A new SID under a known identity is a new connection replacing the old one
                    // (the publisher's full reconnect, or the server evicting a duplicate
                    // identity). The old SID's disconnect may never arrive, or arrive after this
                    // update when it no longer matches, so retire the replaced connection here
                    // exactly as if its disconnect had come first: its tracks are unpublished
                    // and it disconnects before the replacement connects.
                    if let current, isNewParticipant, let currentSid = current.sid {
                        disconnectedParticipants.append((
                            current,
                            infoIdentity,
                            currentSid,
                            current.dataPacketReceiveGeneration
                        ))
                    }
                    let participant = $0.updateRemoteParticipant(info: info, room: self)

                    if isNewParticipant {
                        newParticipants.append(participant)
                    } else {
                        participant.set(info: info, connectionState: $0.connectionState)
                    }
                }
            }
        }

        for (participant, _, _, _) in disconnectedParticipants {
            participant.invalidateAllSubscriptionAdmissionsForOwnershipLoss()
        }
        if !disconnectedParticipants.isEmpty {
            await runAfterParticipantRetirementForTests()
        }

        await withTaskGroup { group in
            for (participant, identity, sid, receiveGeneration) in disconnectedParticipants {
                group.addTask {
                    do {
                        try await self._onParticipantDidDisconnect(
                            participant: participant,
                            identity: identity,
                            sid: sid,
                            receiveGeneration: receiveGeneration
                        )
                    } catch {
                        self.log("Failed to process participant disconnection, error: \(error)", .error)
                    }
                }
            }

            await group.waitForAll()
        }

        if case .connected = _state.connectionState {
            for participant in newParticipants {
                delegates.notify(label: { "room.remoteParticipantDidConnect: \(participant)" }) {
                    $0.room?(self, participantDidConnect: participant)
                }
            }
        }
    }

    func signalClient(_: SignalClient, didUnpublishLocalTrack localTrack: Livekit_TrackUnpublishedResponse) async {
        log()

        let trackSid = Track.Sid(from: localTrack.trackSid)

        guard let publication = localParticipant._state.trackPublications[trackSid] as? LocalTrackPublication else {
            log("track publication not found", .warning)
            return
        }

        do {
            try await localParticipant.unpublish(publication: publication)
            log("Unpublished track(\(localTrack.trackSid)")
        } catch {
            log("Failed to unpublish track(\(localTrack.trackSid), error: \(error)", .warning)
        }
    }

    func signalClient(_: SignalClient, didReceiveIceCandidate iceCandidate: IceCandidate, target: Livekit_SignalTarget) async {
        guard let mode = _state.transport else {
            log("Failed to add ice candidate, transport is nil for target: \(target)", .error)
            return
        }

        do {
            try await mode.transport(for: target).add(iceCandidate: iceCandidate)
        } catch {
            log("Failed to add ice candidate for target: \(target), error: \(error)", .error)
        }
    }

    func signalClient(_: SignalClient, didReceiveAnswer answer: LKRTCSessionDescription, offerId: UInt32) async {
        log("Received answer for offerId: \(offerId)")

        // Clamp to the SDK default — libwebrtc advertises larger (~256 KiB)
        // than LiveKit/pion can deliver end-to-end (~64 KiB), so we trust
        // the answer no more than our compiled-in ceiling.
        let parsed = parseSDPMaxMessageSize(answer.sdp) ?? DataChannelPair.defaultMaxMessageSize
        let maxMessageSize = min(parsed, DataChannelPair.defaultMaxMessageSize)
        publisherDataChannel.set(maxMessageSize: maxMessageSize)
        log("Negotiated data channel max-message-size: \(maxMessageSize) bytes", .debug)

        do {
            let publisher = try requirePublisher()
            try await publisher.set(remoteDescription: answer, offerId: offerId)
        } catch {
            log("Failed to set remote description with offerId: \(offerId), error: \(error)", .error)
        }
    }

    func signalClient(_ signalClient: SignalClient, didReceiveOffer offer: LKRTCSessionDescription, offerId: UInt32) async {
        guard let subscriber = _state.transport?.dedicatedSubscriber else {
            log("Received offer but not in dual PC mode, ignoring")
            return
        }

        log("Received offer with offerId: \(offerId), creating & sending answer...")

        do {
            try await subscriber.set(remoteDescription: offer)
            var answer = try await subscriber.createAnswer()
            answer = try await subscriber.set(localDescription: answer, munging: [
                { Transport.mungeOpusStereo($0, matchingOffer: offer.sdp) },
                { Transport.mungeOpusNack($0, matchingOffer: offer.sdp) },
            ])
            try await signalClient.send(answer: answer, offerId: offerId)
            connectSpan?.record("answer_sent")
        } catch {
            log("Failed to send answer for offerId: \(offerId), error: \(error)", .error)
        }
    }

    func signalClient(_: SignalClient, didUpdateToken token: String) async {
        // update token
        _state.mutate { $0.token = token }
    }

    func signalClient(_: SignalClient, didSubscribeTrack trackSid: Track.Sid) async {
        // Find the local track publication.
        guard let track = localParticipant.trackPublications[trackSid] as? LocalTrackPublication else {
            log("Could not find local track publication for subscribed event")
            return
        }

        // Notify Room.
        delegates.notify {
            $0.room?(self, participant: self.localParticipant, remoteDidSubscribeTrack: track)
        }

        // Notify LocalParticipant.
        localParticipant.delegates.notify {
            $0.participant?(self.localParticipant, remoteDidSubscribeTrack: track)
        }
    }

    /// Feeds the data track managers, which parse these themselves. Ordered after the decoded
    /// form, so the participants each message announces are already registered and
    /// `onTrackPublished` can resolve its publisher.
    func signalClient(_: SignalClient, didReceiveEncodedResponse response: SignalClient.EncodedResponse) async {
        switch response {
        case let .join(encoded):
            dataTracks?.handleJoinResponse(encoded)
        case let .participantUpdate(encoded):
            guard let identity = localParticipant.identity?.stringValue else { return }
            dataTracks?.handleParticipantUpdate(encoded, localIdentity: identity)
        }
    }

    func signalClient(_: SignalClient, didReceiveDataTrackResponse data: Data) async {
        dataTracks?.handleSignalResponse(data)
    }

    func signalClient(_: SignalClient, didReceiveMediaSectionsRequirement requirement: Livekit_MediaSectionsRequirement) async {
        guard case let .publisherOnly(publisher) = _state.transport else { return }

        let transceiverInit = LKRTCRtpTransceiverInit()
        transceiverInit.direction = .recvOnly

        do {
            try await RTC.run {
                for _ in 0 ..< requirement.numAudios {
                    _ = try publisher.addTransceiver(ofType: .audio, transceiverInit: transceiverInit)
                }
                for _ in 0 ..< requirement.numVideos {
                    _ = try publisher.addTransceiver(ofType: .video, transceiverInit: transceiverInit)
                }
            }
            try await publisherShouldNegotiate()
        } catch {
            log("Failed to add transceivers for media sections requirement: \(error)", .error)
        }
    }
}

private extension Room {
    func _updateState(for response: Livekit_RoomMovedResponse) {
        // Update token
        if !response.token.isEmpty {
            _state.mutate { $0.token = response.token }
        }

        // Update room info if available
        guard response.hasRoom else { return }

        _state.mutate { $0.apply(roomInfo: response.room) }
    }

    func _notifyRoomMoved(name: String) {
        // Emit room moved event with new room name (on both Room and LocalParticipant delegates)
        delegates.notify(label: { "room.didMoveToRoomNamed \(name)" }) {
            $0.room?(self, didMoveToRoomNamed: name)
        }
        localParticipant.delegates.notify(label: { "participant.didMoveToRoomNamed \(name)" }) {
            $0.participant?(self.localParticipant, didMoveToRoomNamed: name)
        }
    }

}
