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

/// Opaque, publication-scoped authorization for one protected remote-track
/// subscription generation.
public struct RemoteTrackSubscriptionAdmission: Sendable {
    fileprivate let publicationNonce: UUID
    fileprivate let generation: UInt64
    fileprivate let tokenNonce: UUID
}

struct RemoteTrackSubscriptionAdmissionSnapshot: Sendable, Equatable {
    let publicationNonce: UUID
    let generation: UInt64
    let tokenNonce: UUID?
    let isLegacy: Bool
}

@objc
public enum SubscriptionState: Int, Codable {
    case subscribed
    case notAllowed
    case unsubscribed
}

@objcMembers
public class RemoteTrackPublication: TrackPublication, @unchecked Sendable {
    typealias SubscriptionRequestSender = @Sendable (
        _ room: Room,
        _ participantSid: Participant.Sid,
        _ trackSid: Track.Sid,
        _ isSubscribed: Bool,
        _ admission: @escaping @Sendable () -> Bool
    ) async throws -> Void

    private let _subscriptionSerialRunner = SerialRunnerActor<Void>()
    var subscriptionRequestSender: SubscriptionRequestSender?
    var remoteAudioPlayoutCoordinator = RemoteAudioPlayoutCoordinator.shared

    // MARK: - Public

    public var isSubscriptionAllowed: Bool { _state.isSubscriptionAllowed }

    public var isEnabled: Bool { _state.trackSettings.isEnabled }

    override public var isMuted: Bool { track?.isMuted ?? _state.isMetadataMuted }

    // MARK: - Private

    // adaptiveStream
    // this must be on .main queue
    private var _asTimer = AsyncTimer(interval: 0.3)

    override func updateFromInfo(info: Livekit_TrackInfo) {
        super.updateFromInfo(info: info)
        track?.set(muted: info.muted)
        set(metadataMuted: info.muted)
    }

    override public var isSubscribed: Bool {
        if !isSubscriptionAllowed { return false }
        return _state.isSubscribePreferred != false && super.isSubscribed
    }

    var isDesired: Bool {
        _state.isSubscribePreferred != false
    }

    public var subscriptionState: SubscriptionState {
        if !isSubscriptionAllowed { return .notAllowed }
        return isSubscribed ? .subscribed : .unsubscribed
    }

    /// Subscribe or unsubscribe from this track.
    public func set(subscribed newValue: Bool) async throws {
        if !newValue {
            try await revokeSubscription(requiresExplicitAdmission: false)
            return
        }

        guard _state.isSubscribePreferred != true else { return }
        let snapshot = try beginLegacySubscription()
        try await setSubscribed(snapshot: snapshot)
    }

    /// Creates an opaque authorization for a protected subscription attempt.
    /// The token is valid only for this publication and is invalidated by any
    /// revocation or participant/room ownership boundary.
    @nonobjc
    public func admitSubscription() throws -> RemoteTrackSubscriptionAdmission {
        try _state.mutate { state in
            let admission = state.subscriptionAdmission
            let admissionIsInactive: Bool
            switch admission.mode {
            case .legacy, .revoked:
                admissionIsInactive = true
            case .admitted:
                admissionIsInactive = false
            }
            guard state.isSubscriptionAllowed,
                  admissionIsInactive,
                  state.isSubscribePreferred != true,
                  state.track == nil,
                  admission.revocationInFlightCount == 0,
                  !admission.revocationNeedsRetry,
                  state.remoteAudioPlayoutOwner == nil,
                  !state.remoteAudioPlayoutOwnerNeedsRelease,
                  state.remoteAudioLegacyDemandOwner == nil,
                  !state.remoteAudioLegacyDemandOwnerNeedsRelease
            else {
                throw LiveKitError(
                    .invalidState,
                    message: "Subscription revocation has not reached a confirmed signaling boundary"
                )
            }

            let tokenNonce = UUID()
            state.subscriptionAdmission.generation &+= 1
            state.subscriptionAdmission.mode = .admitted(tokenNonce: tokenNonce)
            state.subscriptionAdmission.requiresExplicitAdmission = true
            return RemoteTrackSubscriptionAdmission(
                publicationNonce: state.subscriptionAdmission.publicationNonce,
                generation: state.subscriptionAdmission.generation,
                tokenNonce: tokenNonce
            )
        }
    }

    /// Applies a protected subscription intent using an exact admission token.
    @nonobjc
    public func set(
        subscribed newValue: Bool,
        admission: RemoteTrackSubscriptionAdmission
    ) async throws {
        guard newValue else {
            try await revokeSubscription(
                requiresExplicitAdmission: true,
                matching: admission
            )
            return
        }
        let snapshot = try snapshot(for: admission)
        try await setSubscribed(snapshot: snapshot)
    }

    /// Invalidates every outstanding protected subscription token, removes and
    /// silences the exact attached track locally, then confirms the unsubscribe
    /// request. Local revocation remains in force if signaling throws, and the
    /// method can be retried safely.
    @nonobjc
    public func revokeSubscription() async throws {
        try await revokeSubscription(requiresExplicitAdmission: true)
    }

    /// Enable or disable server from sending down data for this track.
    ///
    /// This is useful when the participant is off screen, you may disable streaming down their video to reduce bandwidth requirements.
    public func set(enabled newValue: Bool) async throws {
        // No-op if already the desired value
        let trackSettings = _state.trackSettings
        guard trackSettings.isEnabled != newValue else { return }

        try await checkUserCanModifyTrackSettings()

        let settings = trackSettings.copyWith(isEnabled: .value(newValue))
        // Attempt to set the new settings
        try await send(trackSettings: settings)
    }

    /// Set preferred video FPS for this track.
    public func set(preferredFPS newValue: UInt) async throws {
        // No-op if already the desired value
        let trackSettings = _state.trackSettings
        guard trackSettings.preferredFPS != newValue else { return }

        try await checkUserCanModifyTrackSettings()

        let settings = trackSettings.copyWith(preferredFPS: .value(newValue))
        // Attempt to set the new settings
        try await send(trackSettings: settings)
    }

    /// Set preferred video dimensions for this track.
    ///
    /// Based on this value, server will decide which layer to send.
    /// Use ``RemoteTrackPublication/set(videoQuality:)`` to explicitly set layer instead.
    public func set(preferredDimensions newValue: Dimensions) async throws {
        // No-op if already the desired value
        let trackSettings = _state.trackSettings
        guard trackSettings.dimensions != newValue else { return }

        try await checkUserCanModifyTrackSettings()

        let settings = trackSettings.copyWith(dimensions: .value(newValue))
        // Attempt to set the new settings
        try await send(trackSettings: settings)
    }

    /// For tracks that support simulcasting, adjust subscribed quality.
    ///
    /// This indicates the highest quality the client can accept. if network
    /// bandwidth does not allow, server will automatically reduce quality to
    /// optimize for uninterrupted video.
    public func set(videoQuality newValue: VideoQuality) async throws {
        // No-op if already the desired value
        let trackSettings = _state.trackSettings
        guard trackSettings.videoQuality != newValue else { return }

        try await checkUserCanModifyTrackSettings()

        let settings = trackSettings.copyWith(videoQuality: .value(newValue))
        // Attempt to set the new settings
        try await send(trackSettings: settings)
    }

    @discardableResult
    override func set(track newValue: Track?) async -> Track? {
        log("RemoteTrackPublication set track: \(String(describing: newValue))")

        let oldValue = await super.set(track: newValue)
        if newValue != oldValue {
            // always suspend adaptiveStream timer first
            _asTimer.cancel()

            if let newValue {
                // Copy meta-data to track
                newValue._state.mutate {
                    $0.sid = sid
                    $0.dimensions = $0.dimensions == nil ? dimensions : $0.dimensions
                }

                // reset track settings, track is initially disabled only if adaptive stream and is a video track
                resetTrackSettings()

                log("[adaptiveStream] did reset trackSettings: \(_state.trackSettings), kind: \(newValue.kind)")

                // start adaptiveStream timer only if it's a video track
                if isAdaptiveStreamEnabled {
                    _asTimer.setTimerBlock { [weak self] in
                        await self?.onAdaptiveStreamTimer()
                    }
                    _asTimer.restart()
                }

                // if new Track has been set to this RemoteTrackPublication,
                // update the Track's muted state from the latest info.
                newValue.set(muted: _state.isMetadataMuted,
                             notify: false)
            }

            if oldValue != nil, newValue == nil,
               let participant = participant as? RemoteParticipant,
               let room = participant._room
            {
                participant.delegates.notify(label: { "participant.didUnsubscribe \(self)" }) {
                    $0.participant?(participant, didUnsubscribeTrack: self)
                }
                room.delegates.notify(label: { "room.didUnsubscribe \(self)" }) {
                    $0.room?(room, participant: participant, didUnsubscribeTrack: self)
                }
            }
        }

        return oldValue
    }

    func replaceSubscribedTrack(expected: Track?, with newValue: Track?) async -> Bool {
        await replaceSubscribedTrack(expected: expected, with: newValue, admission: nil)
    }

    func replaceSubscribedTrack(
        expected: Track?,
        with newValue: Track?,
        admission: RemoteTrackSubscriptionAdmissionSnapshot?
    ) async -> Bool {
        newValue?.add(delegate: self)
        let didReplace = admitFailedRemoteTrackRetirement { state -> (Bool, Bool) in
            if let admission, !Self.matches(admission, state: state) {
                return (false, false)
            }
            let ownsExpectedTrack = switch (state.track, expected) {
            case (nil, nil): true
            case let (current?, expected?): current === expected
            default: false
            }
            guard ownsExpectedTrack else { return (false, false) }
            var admittedRetirement = false
            if let expected, expected !== newValue {
                Self.silenceAndDetach(expected)
                state.failedRemoteRevocationTracks[ObjectIdentifier(expected)] = expected
                state.subscriptionAdmission.revocationNeedsRetry = true
                admittedRetirement = true
            }
            state.track = newValue
            return (true, admittedRetirement)
        }
        guard didReplace else {
            newValue?.remove(delegate: self)
            return false
        }

        expected?.remove(delegate: self)
        _asTimer.cancel()

        if let newValue {
            newValue._state.mutate {
                $0.sid = sid
                $0.dimensions = $0.dimensions == nil ? dimensions : $0.dimensions
            }
            resetTrackSettings()
            if isAdaptiveStreamEnabled {
                _asTimer.setTimerBlock { [weak self] in
                    await self?.onAdaptiveStreamTimer()
                }
                _asTimer.restart()
            }
            newValue.set(muted: _state.isMetadataMuted, notify: false)
        }

        return true
    }

    /// Removes a track created by a superseded transport without emitting
    /// unsubscribe events for a participant that no longer owns the room slot.
    func removeStaleTrack(_ staleTrack: Track) async throws {
        retainRemoteTrackForRetirement(staleTrack, detachIfCurrent: true)
        try await retireRetainedRemoteTrack(staleTrack)
    }

    /// Stops and detaches an exact track already retained by a publication
    /// mutation. A failure leaves the disabled object retained for a later
    /// `revokeSubscription()` retry and blocks any new protected admission.
    func retireRetainedRemoteTrack(_ track: Track) async throws {
        do {
            try await track.stop()
            await track.set(transport: nil, rtpReceiver: nil)
        } catch {
            await track.set(transport: nil, rtpReceiver: nil)
            (participant as? RemoteParticipant)?._room?.retainFailedRemoteTrackRetirement(self)
            throw error
        }

        let completedGeneration = _state.mutate { state -> UInt64? in
            state.failedRemoteRevocationTracks.removeValue(forKey: ObjectIdentifier(track))
            if state.failedRemoteRevocationTracks.isEmpty,
               !state.remoteAudioPlayoutOwnerNeedsRelease,
               !state.remoteAudioLegacyDemandOwnerNeedsRelease,
               state.subscriptionAdmission.revocationInFlightCount == 0
            {
                state.subscriptionAdmission.revocationNeedsRetry = false
            }
            guard state.failedRemoteRevocationTracks.isEmpty,
                  !state.remoteAudioPlayoutOwnerNeedsRelease,
                  !state.remoteAudioLegacyDemandOwnerNeedsRelease
            else { return nil }
            return state.failedRemoteTrackRetirementGeneration
        }
        if let completedGeneration {
            (participant as? RemoteParticipant)?._room?.releaseFailedRemoteTrackRetirement(
                self,
                generation: completedGeneration
            )
        }
    }

    /// Drains every exact displaced/removed track retained by this publication.
    /// Safe to retry: successfully stopped tracks are removed atomically, while
    /// failures remain disabled and strongly referenced.
    func stopRetainedRemoteTracks() async throws {
        let tracks = _state.read { Array($0.failedRemoteRevocationTracks.values) }
        var firstError: Error?
        for track in tracks {
            do {
                try await retireRetainedRemoteTrack(track)
            } catch {
                if firstError == nil { firstError = error }
            }
        }
        let ownerToRelease = _state.read { state in
            state.remoteAudioPlayoutOwnerNeedsRelease ? state.remoteAudioPlayoutOwner : nil
        }
        if let ownerToRelease {
            do {
                try await remoteAudioPlayoutCoordinator.release(owner: ownerToRelease)
                _state.mutate { state in
                    guard state.remoteAudioPlayoutOwner == ownerToRelease else { return }
                    state.remoteAudioPlayoutOwner = nil
                    state.remoteAudioPlayoutOwnerNeedsRelease = false
                }
            } catch {
                (participant as? RemoteParticipant)?._room?.retainFailedRemoteTrackRetirement(self)
                if firstError == nil { firstError = error }
            }
        } else {
            _state.mutate { state in
                guard state.remoteAudioPlayoutOwnerNeedsRelease,
                      state.remoteAudioPlayoutOwner == nil
                else { return }
                state.remoteAudioPlayoutOwnerNeedsRelease = false
            }
        }
        let legacyOwnerToRelease = _state.read { state in
            state.remoteAudioLegacyDemandOwnerNeedsRelease ? state.remoteAudioLegacyDemandOwner : nil
        }
        if let legacyOwnerToRelease {
            do {
                try await remoteAudioPlayoutCoordinator.releaseLegacyDemand(owner: legacyOwnerToRelease)
                _state.mutate { state in
                    guard state.remoteAudioLegacyDemandOwner == legacyOwnerToRelease else { return }
                    state.remoteAudioLegacyDemandOwner = nil
                    state.remoteAudioLegacyDemandOwnerNeedsRelease = false
                }
            } catch {
                (participant as? RemoteParticipant)?._room?.retainFailedRemoteTrackRetirement(self)
                if firstError == nil { firstError = error }
            }
        } else {
            _state.mutate { state in
                guard state.remoteAudioLegacyDemandOwnerNeedsRelease,
                      state.remoteAudioLegacyDemandOwner == nil
                else { return }
                state.remoteAudioLegacyDemandOwnerNeedsRelease = false
            }
        }
        let completedGeneration = _state.read { state -> UInt64? in
            guard state.failedRemoteRevocationTracks.isEmpty,
                  !state.remoteAudioPlayoutOwnerNeedsRelease,
                  !state.remoteAudioLegacyDemandOwnerNeedsRelease
            else { return nil }
            return state.failedRemoteTrackRetirementGeneration
        }
        if let completedGeneration {
            _state.mutate { state in
                guard state.subscriptionAdmission.revocationInFlightCount == 0 else { return }
                state.subscriptionAdmission.revocationNeedsRetry = false
            }
            (participant as? RemoteParticipant)?._room?.releaseFailedRemoteTrackRetirement(
                self,
                generation: completedGeneration
            )
        }
        if let firstError { throw firstError }
    }

    func currentSubscriptionAdmissionSnapshot() -> RemoteTrackSubscriptionAdmissionSnapshot? {
        _state.read { state in
            switch state.subscriptionAdmission.mode {
            case .revoked:
                return nil
            case .legacy:
                guard !state.subscriptionAdmission.requiresExplicitAdmission else { return nil }
                return RemoteTrackSubscriptionAdmissionSnapshot(
                    publicationNonce: state.subscriptionAdmission.publicationNonce,
                    generation: state.subscriptionAdmission.generation,
                    tokenNonce: nil,
                    isLegacy: true
                )
            case let .admitted(tokenNonce):
                return RemoteTrackSubscriptionAdmissionSnapshot(
                    publicationNonce: state.subscriptionAdmission.publicationNonce,
                    generation: state.subscriptionAdmission.generation,
                    tokenNonce: tokenNonce,
                    isLegacy: false
                )
            }
        }
    }

    func isSubscriptionAdmissionCurrent(
        _ snapshot: RemoteTrackSubscriptionAdmissionSnapshot,
        track: Track? = nil
    ) -> Bool {
        _state.read { state in
            guard Self.matches(snapshot, state: state) else { return false }
            guard let track else { return true }
            return state.track === track
        }
    }

    func activateSubscribedTrack(
        _ track: Track,
        admission: RemoteTrackSubscriptionAdmissionSnapshot
    ) async throws -> Bool {
        if track is RemoteAudioTrack, admission.isLegacy {
            let owner = try prepareRemoteAudioLegacyDemandOwner(for: admission)
            if let owner {
                do {
                    try await remoteAudioPlayoutCoordinator.reserveLegacyDemand(
                        owner: owner,
                        admissionIsCurrent: { [weak self, weak track] in
                            guard let self, let track else { return false }
                            return isSubscriptionAdmissionCurrent(admission, track: track) &&
                                _state.read { state in
                                    state.remoteAudioLegacyDemandOwner == owner &&
                                        !state.remoteAudioLegacyDemandOwnerNeedsRelease
                                }
                        }
                    )
                    try await remoteAudioPlayoutCoordinator.validateLegacyDemand(
                        owner: owner,
                        admissionIsCurrent: { [weak self, weak track] in
                            guard let self, let track else { return false }
                            return isSubscriptionAdmissionCurrent(admission, track: track)
                        }
                    )
                } catch {
                    markRemoteAudioLegacyDemandOwnerForRelease(owner)
                    try await stopRetainedRemoteTracks()
                    throw error
                }
            }
        }

        guard let audioTrack = track as? RemoteAudioTrack,
              !admission.isLegacy
        else {
            return activateSubscribedTrackAfterPlayout(
                track,
                admission: admission,
                audioTrack: nil
            )
        }

        guard let owner = _state.read({ state -> RemoteAudioPlayoutOwner? in
            guard Self.matches(admission, state: state),
                  !state.remoteAudioPlayoutOwnerNeedsRelease
            else { return nil }
            return state.remoteAudioPlayoutOwner
        }) else {
            return false
        }

        try await remoteAudioPlayoutCoordinator.validate(owner: owner) { [weak self, weak track] in
            guard let self, let track else { return false }
            return isSubscriptionAdmissionCurrent(admission, track: track) &&
                _state.read { state in
                    state.remoteAudioPlayoutOwner == owner &&
                        !state.remoteAudioPlayoutOwnerNeedsRelease
                }
        }

        let activated = activateSubscribedTrackAfterPlayout(
            track,
            admission: admission,
            audioTrack: audioTrack
        )
        return activated
    }

    private func activateSubscribedTrackAfterPlayout(
        _ track: Track,
        admission: RemoteTrackSubscriptionAdmissionSnapshot,
        audioTrack: RemoteAudioTrack?
    ) -> Bool {
        _state.mutate { state in
            guard Self.matches(admission, state: state),
                  !state.subscriptionAdmission.revocationNeedsRetry,
                  state.track === track
            else { return false }
            track._state.mutate { $0.trackState = .started }
            if let audioTrack {
                audioTrack.volume = 1
            }
            // Enabled inside the admission lock so a revoked admission cannot race it. Like
            // `RemoteAudioTrack.volume`, this is a blocking hop onto the RTC executor.
            track.mediaTrack.blocking { $0.isEnabled = true }
            return true
        }
    }

    func invalidateSubscriptionAdmissionForOwnershipLoss() {
        let participant = participant as? RemoteParticipant
        let room = participant?._room
        let result = admitFailedRemoteTrackRetirement(room: room) { state in
            let result = Self.revokeForOwnershipLoss(state: &state)
            return (result, result.requiresRetirement)
        }
        result.track?.remove(delegate: self)
        _asTimer.cancel()
    }
}

private extension RemoteTrackPublication {
    struct RevocationContext {
        let tracks: [Track]
        let detachedCurrentTrack: Track?
        let participant: RemoteParticipant?
        let participantSid: Participant.Sid?
        let room: Room?
    }

    func admitFailedRemoteTrackRetirement<Result>(
        room: Room? = nil,
        _ mutation: (inout TrackPublication.State) throws -> (Result, Bool)
    ) rethrows -> Result {
        if let room = room ?? (participant as? RemoteParticipant)?._room {
            return try room.admitFailedRemoteTrackRetirement(self, mutation)
        }
        return try _state.mutate { state in
            let (result, admittedWork) = try mutation(&state)
            if admittedWork {
                state.failedRemoteTrackRetirementGeneration &+= 1
            }
            return result
        }
    }

    func beginLegacySubscription() throws -> RemoteTrackSubscriptionAdmissionSnapshot {
        try _state.mutate { state in
            guard !state.subscriptionAdmission.requiresExplicitAdmission else {
                throw LiveKitError(
                    .invalidState,
                    message: "This publication requires an explicit subscription admission"
                )
            }
            guard state.subscriptionAdmission.revocationInFlightCount == 0,
                  !state.subscriptionAdmission.revocationNeedsRetry,
                  state.remoteAudioPlayoutOwner == nil,
                  !state.remoteAudioPlayoutOwnerNeedsRelease,
                  state.remoteAudioLegacyDemandOwner == nil,
                  !state.remoteAudioLegacyDemandOwnerNeedsRelease
            else {
                throw LiveKitError(
                    .invalidState,
                    message: "Subscription revocation has not reached a confirmed signaling boundary"
                )
            }
            state.subscriptionAdmission.generation &+= 1
            state.subscriptionAdmission.mode = .legacy
            state.isSubscribePreferred = true
            return RemoteTrackSubscriptionAdmissionSnapshot(
                publicationNonce: state.subscriptionAdmission.publicationNonce,
                generation: state.subscriptionAdmission.generation,
                tokenNonce: nil,
                isLegacy: true
            )
        }
    }

    func snapshot(
        for admission: RemoteTrackSubscriptionAdmission
    ) throws -> RemoteTrackSubscriptionAdmissionSnapshot {
        try _state.read { state in
            guard state.subscriptionAdmission.publicationNonce == admission.publicationNonce,
                  state.subscriptionAdmission.generation == admission.generation,
                  state.subscriptionAdmission.mode == .admitted(tokenNonce: admission.tokenNonce)
            else {
                throw LiveKitError(.invalidState, message: "Subscription admission is stale or belongs to another publication")
            }
            return RemoteTrackSubscriptionAdmissionSnapshot(
                publicationNonce: admission.publicationNonce,
                generation: admission.generation,
                tokenNonce: admission.tokenNonce,
                isLegacy: false
            )
        }
    }

    func prepareRemoteAudioPlayoutOwner(
        for snapshot: RemoteTrackSubscriptionAdmissionSnapshot
    ) throws -> RemoteAudioPlayoutOwner? {
        guard kind == .audio, !snapshot.isLegacy,
              let admissionTokenNonce = snapshot.tokenNonce
        else { return nil }

        return try _state.mutate { state in
            guard Self.matches(snapshot, state: state),
                  !state.remoteAudioPlayoutOwnerNeedsRelease
            else {
                throw LiveKitError(.invalidState, message: "Subscription admission was revoked")
            }
            if let owner = state.remoteAudioPlayoutOwner {
                guard owner.publicationNonce == snapshot.publicationNonce,
                      owner.admissionGeneration == snapshot.generation,
                      owner.admissionTokenNonce == admissionTokenNonce
                else {
                    throw LiveKitError(
                        .invalidState,
                        message: "A different protected audio admission still owns playout"
                    )
                }
                return owner
            }
            let owner = RemoteAudioPlayoutOwner(
                publicationNonce: snapshot.publicationNonce,
                admissionGeneration: snapshot.generation,
                admissionTokenNonce: admissionTokenNonce
            )
            state.remoteAudioPlayoutOwner = owner
            return owner
        }
    }

    func prepareRemoteAudioLegacyDemandOwner(
        for snapshot: RemoteTrackSubscriptionAdmissionSnapshot
    ) throws -> RemoteAudioLegacyDemandOwner? {
        guard kind == .audio, snapshot.isLegacy else { return nil }
        return try _state.mutate { state in
            guard Self.matches(snapshot, state: state),
                  !state.remoteAudioLegacyDemandOwnerNeedsRelease
            else {
                throw LiveKitError(.invalidState, message: "Legacy subscription admission was revoked")
            }
            if let owner = state.remoteAudioLegacyDemandOwner { return owner }
            let owner = RemoteAudioLegacyDemandOwner(
                publicationNonce: snapshot.publicationNonce,
                admissionGeneration: snapshot.generation
            )
            state.remoteAudioLegacyDemandOwner = owner
            return owner
        }
    }

    func isCurrentPlayoutOwner(
        _ owner: RemoteAudioPlayoutOwner,
        snapshot: RemoteTrackSubscriptionAdmissionSnapshot,
        participant: RemoteParticipant,
        in room: Room
    ) -> Bool {
        isCurrentSubscription(snapshot, participant: participant, in: room) &&
            _state.read { state in
                state.remoteAudioPlayoutOwner == owner &&
                    !state.remoteAudioPlayoutOwnerNeedsRelease
            }
    }

    func markRemoteAudioPlayoutOwnerForRelease(_ owner: RemoteAudioPlayoutOwner) {
        _ = admitFailedRemoteTrackRetirement { state -> (Bool, Bool) in
            guard state.remoteAudioPlayoutOwner == owner else { return (false, false) }
            state.remoteAudioPlayoutOwnerNeedsRelease = true
            state.subscriptionAdmission.revocationNeedsRetry = true
            return (true, true)
        }
    }

    func markRemoteAudioLegacyDemandOwnerForRelease(_ owner: RemoteAudioLegacyDemandOwner) {
        _ = admitFailedRemoteTrackRetirement { state -> (Bool, Bool) in
            guard state.remoteAudioLegacyDemandOwner == owner else { return (false, false) }
            state.remoteAudioLegacyDemandOwnerNeedsRelease = true
            state.subscriptionAdmission.revocationNeedsRetry = true
            return (true, true)
        }
    }

    func setSubscribed(snapshot: RemoteTrackSubscriptionAdmissionSnapshot) async throws {
        try await _subscriptionSerialRunner.run { [weak self] in
            guard let self else { return }
            let participant = try await requireParticipant()
            guard let participant = participant as? RemoteParticipant else {
                throw LiveKitError(.invalidState, message: "Remote participant is unavailable")
            }
            let room = try participant.requireRoom()
            guard let participantSid = participant.sid else {
                throw LiveKitError(.invalidState, message: "Remote participant SID is unavailable")
            }

            try requireCurrentSubscription(snapshot, participant: participant, in: room)
            let legacyDemandOwner = try prepareRemoteAudioLegacyDemandOwner(for: snapshot)
            if let legacyDemandOwner {
                do {
                    try await remoteAudioPlayoutCoordinator.reserveLegacyDemand(
                        owner: legacyDemandOwner,
                        admissionIsCurrent: { [weak self, weak participant, weak room] in
                            guard let self, let participant, let room else { return false }
                            return self.isCurrentSubscription(
                                snapshot,
                                participant: participant,
                                in: room
                            ) && self._state.read {
                                $0.remoteAudioLegacyDemandOwner == legacyDemandOwner &&
                                    !$0.remoteAudioLegacyDemandOwnerNeedsRelease
                            }
                        }
                    )
                } catch {
                    markRemoteAudioLegacyDemandOwnerForRelease(legacyDemandOwner)
                    try await stopRetainedRemoteTracks()
                    _state.mutate { state in
                        guard Self.matches(snapshot, state: state) else { return }
                        state.isSubscribePreferred = false
                    }
                    throw error
                }
            }
            let playoutOwner = try prepareRemoteAudioPlayoutOwner(for: snapshot)
            if let playoutOwner {
                do {
                    try await remoteAudioPlayoutCoordinator.acquire(
                        owner: playoutOwner,
                        admissionIsCurrent: { [weak self, weak participant, weak room] in
                            guard let self, let participant, let room else { return false }
                            return self.isCurrentPlayoutOwner(
                                playoutOwner,
                                snapshot: snapshot,
                                participant: participant,
                                in: room
                            )
                        },
                        onGlobalQuarantine: { [weak self, weak participant, weak room] in
                            guard let self, let participant, let room,
                                  self.isCurrentPlayoutOwner(
                                      playoutOwner,
                                      snapshot: snapshot,
                                      participant: participant,
                                      in: room
                                  )
                            else { return }
                            self.invalidateSubscriptionAdmissionForOwnershipLoss()
                            Task.detached { [weak room] in
                                await room?.disconnect()
                            }
                        }
                    )
                } catch {
                    markRemoteAudioPlayoutOwnerForRelease(playoutOwner)
                    do {
                        try await stopRetainedRemoteTracks()
                    } catch let releaseError {
                        throw releaseError
                    }
                    throw error
                }
            }

            try _state.mutate { state in
                guard Self.matches(snapshot, state: state),
                      playoutOwner == nil || (
                          state.remoteAudioPlayoutOwner == playoutOwner &&
                              !state.remoteAudioPlayoutOwnerNeedsRelease
                      ),
                      legacyDemandOwner == nil || (
                          state.remoteAudioLegacyDemandOwner == legacyDemandOwner &&
                              !state.remoteAudioLegacyDemandOwnerNeedsRelease
                      )
                else {
                    throw LiveKitError(.invalidState, message: "Subscription admission was revoked")
                }
                state.isSubscribePreferred = true
            }

            do {
                try await sendSubscriptionRequest(
                    room: room,
                    participant: participant,
                    participantSid: participantSid,
                    isSubscribed: true,
                    admission: {
                        self.isCurrentSubscription(snapshot, participant: participant, in: room)
                    }
                )
            } catch {
                if let legacyDemandOwner {
                    markRemoteAudioLegacyDemandOwnerForRelease(legacyDemandOwner)
                    try await stopRetainedRemoteTracks()
                    _state.mutate { state in
                        guard Self.matches(snapshot, state: state) else { return }
                        state.isSubscribePreferred = false
                    }
                    throw error
                }
                invalidateSubscriptionAdmissionForOwnershipLoss()
                do {
                    try await stopRetainedRemoteTracks()
                    try await sendCompensatingUnsubscribe(
                        room: room,
                        participant: participant,
                        participantSid: participantSid
                    )
                } catch let rollbackError {
                    throw rollbackError
                }
                throw error
            }

            guard isCurrentSubscription(snapshot, participant: participant, in: room) else {
                if let legacyDemandOwner {
                    markRemoteAudioLegacyDemandOwnerForRelease(legacyDemandOwner)
                    try await stopRetainedRemoteTracks()
                }
                if let playoutOwner {
                    markRemoteAudioPlayoutOwnerForRelease(playoutOwner)
                    try await stopRetainedRemoteTracks()
                }
                try await sendCompensatingUnsubscribe(
                    room: room,
                    participant: participant,
                    participantSid: participantSid
                )
                throw LiveKitError(.invalidState, message: "Subscription admission was revoked during signaling")
            }
        }
    }

    func revokeSubscription(
        requiresExplicitAdmission: Bool,
        matching admission: RemoteTrackSubscriptionAdmission? = nil
    ) async throws {
        let context = try beginSubscriptionRevocation(
            requiresExplicitAdmission: requiresExplicitAdmission,
            matching: admission
        )
        var confirmed = false
        do {
            try await _subscriptionSerialRunner.run { [weak self] in
                guard let self else { return }
                var firstRetirementError: Error?
                do {
                    try await stopRetainedRemoteTracks()
                } catch {
                    firstRetirementError = error
                }

                if let participant = context.participant,
                   let participantSid = context.participantSid,
                   let room = context.room,
                   owns(participant: participant, in: room)
                {
                    try await sendSubscriptionRequest(
                        room: room,
                        participant: participant,
                        participantSid: participantSid,
                        isSubscribed: false,
                        admission: { self.owns(participant: participant, in: room) }
                    )
                }

                if let firstRetirementError { throw firstRetirementError }
            }
            confirmed = true
            finishSubscriptionRevocation(confirmed: true)
        } catch {
            finishSubscriptionRevocation(confirmed: confirmed)
            throw error
        }

        if context.detachedCurrentTrack != nil,
           let participant = context.participant,
           let room = context.room
        {
            participant.delegates.notify(label: { "participant.didUnsubscribe \(self)" }) {
                $0.participant?(participant, didUnsubscribeTrack: self)
            }
            room.delegates.notify(label: { "room.didUnsubscribe \(self)" }) {
                $0.room?(room, participant: participant, didUnsubscribeTrack: self)
            }
        }
    }

    func beginSubscriptionRevocation(
        requiresExplicitAdmission: Bool,
        matching admission: RemoteTrackSubscriptionAdmission?
    ) throws -> RevocationContext {
        let participant = participant as? RemoteParticipant
        let room = participant?._room
        let participantSid = participant?.sid
        let result = try admitFailedRemoteTrackRetirement(room: room) { state -> (
            (tracks: [Track], detached: Track?, requiresRetirement: Bool),
            Bool
        ) in
            if let admission {
                guard state.subscriptionAdmission.publicationNonce == admission.publicationNonce,
                      state.subscriptionAdmission.generation == admission.generation,
                      state.subscriptionAdmission.mode == .admitted(tokenNonce: admission.tokenNonce)
                else {
                    throw LiveKitError(
                        .invalidState,
                        message: "Subscription admission is stale or belongs to another publication"
                    )
                }
            }
            state.subscriptionAdmission.generation &+= 1
            state.subscriptionAdmission.mode = .revoked
            state.subscriptionAdmission.requiresExplicitAdmission =
                state.subscriptionAdmission.requiresExplicitAdmission || requiresExplicitAdmission
            state.subscriptionAdmission.revocationInFlightCount += 1
            state.subscriptionAdmission.revocationNeedsRetry = true
            state.isSubscribePreferred = false
            state.remoteAudioPlayoutOwnerNeedsRelease =
                state.remoteAudioPlayoutOwner != nil
            state.remoteAudioLegacyDemandOwnerNeedsRelease =
                state.remoteAudioLegacyDemandOwner != nil

            let detached = state.track
            if let detached {
                Self.silenceAndDetach(detached)
                state.track = nil
                state.failedRemoteRevocationTracks[ObjectIdentifier(detached)] = detached
            }
            for track in state.failedRemoteRevocationTracks.values {
                Self.silenceAndDetach(track)
            }
            let result = (
                Array(state.failedRemoteRevocationTracks.values),
                detached,
                !state.failedRemoteRevocationTracks.isEmpty ||
                    state.remoteAudioPlayoutOwnerNeedsRelease ||
                    state.remoteAudioLegacyDemandOwnerNeedsRelease
            )
            return (result, result.2)
        }

        result.detached?.remove(delegate: self)
        _asTimer.cancel()
        return RevocationContext(
            tracks: result.tracks,
            detachedCurrentTrack: result.detached,
            participant: participant,
            participantSid: participantSid,
            room: room
        )
    }

    func finishSubscriptionRevocation(confirmed: Bool) {
        _state.mutate { state in
            state.subscriptionAdmission.revocationInFlightCount = max(
                0,
                state.subscriptionAdmission.revocationInFlightCount - 1
            )
            if confirmed {
                state.subscriptionAdmission.revocationNeedsRetry =
                    !state.failedRemoteRevocationTracks.isEmpty ||
                    state.remoteAudioPlayoutOwnerNeedsRelease ||
                    state.remoteAudioLegacyDemandOwnerNeedsRelease
            }
        }
    }

    func retainRemoteTrackForRetirement(
        _ track: Track,
        detachIfCurrent: Bool
    ) {
        let detachedCurrent = admitFailedRemoteTrackRetirement { state -> (Bool, Bool) in
            Self.silenceAndDetach(track)
            state.failedRemoteRevocationTracks[ObjectIdentifier(track)] = track
            state.subscriptionAdmission.revocationNeedsRetry = true
            guard detachIfCurrent, state.track === track else { return (false, true) }
            state.track = nil
            return (true, true)
        }
        if detachedCurrent {
            track.remove(delegate: self)
            _asTimer.cancel()
        }
    }

    static func silenceAndDetach(_ track: Track) {
        if let audioTrack = track as? RemoteAudioTrack {
            audioTrack.volume = 0
        }
        track.mediaTrack.blocking { $0.isEnabled = false }
        track.detachRemoteTransportSynchronously()
    }

    static func revokeForOwnershipLoss(
        state: inout TrackPublication.State
    ) -> (track: Track?, requiresRetirement: Bool) {
        state.subscriptionAdmission.generation &+= 1
        state.subscriptionAdmission.mode = .revoked
        state.subscriptionAdmission.requiresExplicitAdmission = true
        state.subscriptionAdmission.revocationNeedsRetry = true
        state.isSubscribePreferred = false
        state.remoteAudioPlayoutOwnerNeedsRelease =
            state.remoteAudioPlayoutOwner != nil
        state.remoteAudioLegacyDemandOwnerNeedsRelease =
            state.remoteAudioLegacyDemandOwner != nil
        let detached = state.track
        if let detached {
            silenceAndDetach(detached)
            state.failedRemoteRevocationTracks[ObjectIdentifier(detached)] = detached
            state.track = nil
        }
        for track in state.failedRemoteRevocationTracks.values {
            silenceAndDetach(track)
        }
        return (
            detached,
            !state.failedRemoteRevocationTracks.isEmpty ||
                state.remoteAudioPlayoutOwnerNeedsRelease ||
                state.remoteAudioLegacyDemandOwnerNeedsRelease
        )
    }

    func sendCompensatingUnsubscribe(
        room: Room,
        participant: RemoteParticipant,
        participantSid: Participant.Sid
    ) async throws {
        guard owns(participant: participant, in: room) else { return }
        try await sendSubscriptionRequest(
            room: room,
            participant: participant,
            participantSid: participantSid,
            isSubscribed: false,
            admission: { self.owns(participant: participant, in: room) }
        )
    }

    func sendSubscriptionRequest(
        room: Room,
        participant _: RemoteParticipant,
        participantSid: Participant.Sid,
        isSubscribed: Bool,
        admission: @escaping @Sendable () -> Bool
    ) async throws {
        if let subscriptionRequestSender {
            try await subscriptionRequestSender(
                room,
                participantSid,
                sid,
                isSubscribed,
                admission
            )
        } else {
            try await room.signalClient.sendProtectedUpdateSubscription(
                participantSid: participantSid,
                trackSid: sid,
                isSubscribed: isSubscribed,
                admission: admission
            )
        }
    }

    func requireCurrentSubscription(
        _ snapshot: RemoteTrackSubscriptionAdmissionSnapshot,
        participant: RemoteParticipant,
        in room: Room
    ) throws {
        guard isCurrentSubscription(snapshot, participant: participant, in: room) else {
            throw LiveKitError(.invalidState, message: "Subscription admission is no longer current")
        }
    }

    func isCurrentSubscription(
        _ snapshot: RemoteTrackSubscriptionAdmissionSnapshot,
        participant: RemoteParticipant,
        in room: Room
    ) -> Bool {
        owns(participant: participant, in: room) &&
            _state.read { Self.matches(snapshot, state: $0) }
    }

    func owns(participant: RemoteParticipant, in room: Room) -> Bool {
        guard participant._room === room,
              let identity = participant.identity,
              room._state.read({ $0.remoteParticipants[identity] === participant }),
              participant.trackPublications[sid] === self
        else { return false }
        return true
    }

    static func matches(
        _ snapshot: RemoteTrackSubscriptionAdmissionSnapshot,
        state: TrackPublication.State
    ) -> Bool {
        guard state.subscriptionAdmission.publicationNonce == snapshot.publicationNonce,
              state.subscriptionAdmission.generation == snapshot.generation
        else { return false }
        if snapshot.isLegacy {
            return state.subscriptionAdmission.mode == .legacy &&
                !state.subscriptionAdmission.requiresExplicitAdmission
        }
        guard let tokenNonce = snapshot.tokenNonce else { return false }
        return state.subscriptionAdmission.mode == .admitted(tokenNonce: tokenNonce)
    }
}

// MARK: - Private

private extension RemoteTrackPublication {
    var isAdaptiveStreamEnabled: Bool { (participant?._room?._state.roomOptions ?? RoomOptions()).adaptiveStream && kind == .video }

    var engineConnectionState: ConnectionState {
        guard let participant, let room = participant._room else {
            log("Participant is nil", .warning)
            return .disconnected
        }

        return room._state.connectionState
    }

    func checkUserCanModifyTrackSettings() async throws {
        // adaptiveStream must be disabled and must be subscribed
        if isAdaptiveStreamEnabled || !isSubscribed {
            throw LiveKitError(.invalidState, message: "adaptiveStream must be disabled and track must be subscribed")
        }
    }
}

// MARK: - Internal

extension RemoteTrackPublication {
    func set(metadataMuted newValue: Bool) {
        guard _state.isMetadataMuted != newValue else { return }

        guard let participant, let room = participant._room else {
            log("Participant is nil", .warning)
            return
        }

        _state.mutate { $0.isMetadataMuted = newValue }

        // if track exists, track will emit the following events
        if track == nil {
            participant.delegates.notify(label: { "participant.didUpdatePublication isMuted: \(newValue)" }) {
                $0.participant?(participant, trackPublication: self, didUpdateIsMuted: newValue)
            }
            room.delegates.notify(label: { "room.didUpdatePublication isMuted: \(newValue)" }) {
                $0.room?(room, participant: participant, trackPublication: self, didUpdateIsMuted: newValue)
            }
        }
    }

    func set(subscriptionAllowed newValue: Bool) {
        guard _state.isSubscriptionAllowed != newValue else { return }
        _state.mutate { $0.isSubscriptionAllowed = newValue }

        guard let participant = participant as? RemoteParticipant, let room = participant._room else { return }
        participant.delegates.notify(label: { "participant.didUpdate permission: \(newValue)" }) {
            $0.participant?(participant, trackPublication: self, didUpdateIsSubscriptionAllowed: newValue)
        }
        room.delegates.notify(label: { "room.didUpdate permission: \(newValue)" }) {
            $0.room?(room, participant: participant, trackPublication: self, didUpdateIsSubscriptionAllowed: newValue)
        }
    }

    @discardableResult
    func applySubscriptionPermission(_ isAllowed: Bool) async throws -> Bool {
        guard !isAllowed else {
            set(subscriptionAllowed: true)
            return false
        }

        let participant = participant as? RemoteParticipant
        let room = participant?._room
        let result = admitFailedRemoteTrackRetirement(room: room) { state -> (
            (didChange: Bool, revokedTrack: Track?, requiresRetirement: Bool),
            Bool
        ) in
            let didChange = state.isSubscriptionAllowed
            state.isSubscriptionAllowed = false
            guard state.subscriptionAdmission.requiresExplicitAdmission else {
                return ((didChange, nil, false), false)
            }
            let revocation = Self.revokeForOwnershipLoss(state: &state)
            return (
                (didChange, revocation.track, revocation.requiresRetirement),
                revocation.requiresRetirement
            )
        }

        result.revokedTrack?.remove(delegate: self)
        if result.revokedTrack != nil { _asTimer.cancel() }
        if result.didChange, let participant, let room {
            participant.delegates.notify(label: { "participant.didUpdate permission: false" }) {
                $0.participant?(participant, trackPublication: self, didUpdateIsSubscriptionAllowed: false)
            }
            room.delegates.notify(label: { "room.didUpdate permission: false" }) {
                $0.room?(room, participant: participant, trackPublication: self, didUpdateIsSubscriptionAllowed: false)
            }
        }

        if result.requiresRetirement {
            try await stopRetainedRemoteTracks()
        }
        return _state.subscriptionAdmission.requiresExplicitAdmission
    }
}

// MARK: - TrackSettings

extension RemoteTrackPublication {
    // reset track settings
    func resetTrackSettings() {
        // track is initially disabled when adaptive stream is enabled
        let initiallyEnabled = !isAdaptiveStreamEnabled
        _state.mutate { $0.trackSettings = TrackSettings(enabled: initiallyEnabled) }
    }

    // attempt to send track settings
    func send(trackSettings newValue: TrackSettings) async throws {
        let participant = try await requireParticipant()
        let room = try participant.requireRoom()

        log("[adaptiveStream] sending \(newValue), sid: \(sid)")

        let state = _state.copy()

        if state.isSendingTrackSettings {
            log("send(trackSettings:) called while previous send not completed", .error)
            // Previous send hasn't completed yet...
            throw LiveKitError(.invalidState, message: "Already busy sending new track settings")
        }

        // update state
        _state.mutate {
            $0.trackSettings = newValue
            $0.isSendingTrackSettings = true
        }

        // Attempt to set the new settings
        do {
            try await room.signalClient.sendUpdateTrackSettings(trackSid: sid, settings: newValue)
            _state.mutate { $0.isSendingTrackSettings = false }
        } catch {
            // Revert track settings on failure
            _state.mutate {
                $0.trackSettings = state.trackSettings
                $0.isSendingTrackSettings = false
            }

            log("Failed to send track settings: \(newValue), sid: \(sid), error: \(error)")
        }
    }
}

// MARK: - Adaptive Stream

@MainActor
extension Collection<VideoRenderer> {
    func containsOneOrMoreAdaptiveStreamEnabledRenderers() -> Bool {
        // not visible if no entry
        if isEmpty { return false }
        // at least 1 entry should be visible
        return contains { $0.isAdaptiveStreamEnabled }
    }

    func largestSize() -> CGSize? {
        func maxCGSize(_ s1: CGSize, _ s2: CGSize) -> CGSize {
            CGSize(width: Swift.max(s1.width, s2.width),
                   height: Swift.max(s1.height, s2.height))
        }

        // use post-layout nativeRenderer's view size otherwise return nil
        // which results lower layer to be requested (enabled: true, dimensions: 0x0)
        return filter(\.isAdaptiveStreamEnabled)
            .compactMap { $0.adaptiveStreamSize != .zero ? $0.adaptiveStreamSize : nil }
            .reduce(into: nil as CGSize?) { previous, current in
                guard let unwrappedPrevious = previous else {
                    previous = current
                    return
                }
                previous = maxCGSize(unwrappedPrevious, current)
            }
    }
}

extension RemoteTrackPublication {
    // executed on .main
    @MainActor
    private func onAdaptiveStreamTimer() async {
        // don't continue if the engine is disconnected
        guard engineConnectionState != .disconnected else {
            log("engine is disconnected")
            return
        }

        let videoRenderers = track?._state.videoRendererAdapters.objectEnumerator()?.allObjects.compactMap { ($0 as? VideoRendererAdapter)?.renderer } ?? []
        let isEnabled = videoRenderers.containsOneOrMoreAdaptiveStreamEnabledRenderers()
        var dimensions: Dimensions = .zero

        // compute the largest video view size
        if isEnabled, let maxSize = videoRenderers.largestSize() {
            dimensions = Dimensions(width: Int32(ceil(maxSize.width)),
                                    height: Int32(ceil(maxSize.height)))
        }

        let newSettings = _state.trackSettings.copyWith(
            isEnabled: .value(isEnabled),
            dimensions: .value(dimensions),
        )

        guard _state.trackSettings != newSettings else {
            // no settings updated
            return
        }

        // keep old settings
        let oldSettings = _state.trackSettings
        // update state
        _state.mutate { $0.trackSettings = newSettings }

        // log when flipping from enabled -> disabled
        if oldSettings.isEnabled, !newSettings.isEnabled {
            let viewsString = videoRenderers.enumerated().map { i, v in "videoRenderer\(i)(adaptiveStreamIsEnabled: \(v.isAdaptiveStreamEnabled), adaptiveStreamSize: \(v.adaptiveStreamSize))" }.joined(separator: ", ")
            log("[adaptiveStream] disabling sid: \(sid), videoRenderersCount: \(videoRenderers.count), \(viewsString)")
        }

        if let mediaTrack = track?.mediaTrack, mediaTrack.kind == kLKRTCMediaStreamTrackKindVideo {
            log("VideoTrack.shouldReceive: \(isEnabled)")
            await RTC.run { (mediaTrack.raw as? LKRTCVideoTrack)?.shouldReceive = isEnabled }
        }

        do {
            try await send(trackSettings: newSettings)
        } catch {
            // Revert to old settings on failure
            _state.mutate { $0.trackSettings = oldSettings }
            log("[adaptiveStream] failed to send trackSettings, sid: \(sid) error: \(error)", .error)
        }
    }
}
