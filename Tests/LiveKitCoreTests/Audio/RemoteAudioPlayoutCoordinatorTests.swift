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

@testable import LiveKit
import LiveKitWebRTC
import Testing

@Suite(.serialized, .tags(.audio))
struct RemoteAudioPlayoutCoordinatorTests {
    @Test func initializationFailureNeverStartsOrRetainsAnOwner() async throws {
        let probe = PlayoutDriverProbe(failInitialization: true)
        let coordinator = RemoteAudioPlayoutCoordinator(driver: probe.driver)

        await #expect(throws: (any Error).self) {
            try await coordinator.acquire(
                owner: makeOwner(),
                admissionIsCurrent: { true },
                onGlobalQuarantine: {}
            )
        }

        #expect(coordinator.activeOwnerCount == 0)
        #expect(probe.snapshot.initializeCount == 1)
        #expect(probe.snapshot.startCount == 0)
        #expect(probe.snapshot.sessionAcquireCount == 1)
        #expect(probe.snapshot.sessionReleaseCount == 1)
    }

    @Test func startFailureRollsBackOnlyTheAttemptItOwns() async throws {
        let probe = PlayoutDriverProbe(failStart: true)
        let coordinator = RemoteAudioPlayoutCoordinator(driver: probe.driver)

        await #expect(throws: (any Error).self) {
            try await coordinator.acquire(
                owner: makeOwner(),
                admissionIsCurrent: { true },
                onGlobalQuarantine: {}
            )
        }

        #expect(coordinator.activeOwnerCount == 0)
        #expect(probe.snapshot.startCount == 1)
        #expect(probe.snapshot.stopCount == 1)
        #expect(probe.snapshot.sessionReleaseCount == 1)
    }

    @Test func revocationDuringStartCannotProduceAnActiveOwner() async throws {
        let startEntered = PlayoutTestGate()
        let releaseStart = PlayoutTestGate()
        let admissionIsCurrent = StateSync(true)
        let probe = PlayoutDriverProbe(
            startEntered: startEntered,
            releaseStart: releaseStart
        )
        let coordinator = RemoteAudioPlayoutCoordinator(driver: probe.driver)
        let owner = makeOwner()
        let acquireTask = Task {
            try await coordinator.acquire(
                owner: owner,
                admissionIsCurrent: { admissionIsCurrent.copy() },
                onGlobalQuarantine: {}
            )
        }

        await startEntered.wait()
        admissionIsCurrent.mutate { $0 = false }
        await releaseStart.open()

        await #expect(throws: (any Error).self) {
            try await acquireTask.value
        }
        #expect(coordinator.activeOwnerCount == 0)
        #expect(probe.snapshot.stopCount == 1)
        #expect(!probe.snapshot.isPlaying)
        #expect(!probe.snapshot.isEngineRunning)
    }

    @Test func exactOwnersReferenceCountOneGlobalPlayoutLifecycle() async throws {
        let probe = PlayoutDriverProbe()
        let coordinator = RemoteAudioPlayoutCoordinator(driver: probe.driver)
        let first = makeOwner()
        let second = makeOwner()

        try await coordinator.acquire(
            owner: first,
            admissionIsCurrent: { true },
            onGlobalQuarantine: {}
        )
        try await coordinator.acquire(
            owner: second,
            admissionIsCurrent: { true },
            onGlobalQuarantine: {}
        )

        #expect(coordinator.activeOwnerCount == 2)
        #expect(probe.snapshot.startCount == 1)
        #expect(probe.snapshot.sessionAcquireCount == 1)
        #expect(probe.snapshot.sessionReleaseCount == 1)

        try await coordinator.release(owner: first)
        #expect(probe.snapshot.stopCount == 0)
        #expect(coordinator.activeOwnerCount == 1)

        try await coordinator.release(owner: second)
        #expect(probe.snapshot.stopCount == 1)
        #expect(coordinator.activeOwnerCount == 0)
    }

    @Test func recordingOnlyBaselineAddsAndRemovesOnlyPlayout() async throws {
        let probe = PlayoutDriverProbe(
            initialized: true,
            playing: false,
            recording: true,
            engineRunning: true
        )
        let coordinator = RemoteAudioPlayoutCoordinator(driver: probe.driver)
        let owner = makeOwner()

        try await coordinator.acquire(
            owner: owner,
            admissionIsCurrent: { true },
            onGlobalQuarantine: {}
        )
        #expect(probe.snapshot.startCount == 1)
        #expect(probe.snapshot.isEngineRunning)

        try await coordinator.release(owner: owner)
        #expect(probe.snapshot.stopCount == 1)
        #expect(!probe.snapshot.isPlaying)
        #expect(probe.snapshot.isRecording)
        #expect(probe.snapshot.isEngineRunning)
    }

    @Test func recordingStartedDuringExactPlayoutIsPreservedOnFinalRelease() async throws {
        let probe = PlayoutDriverProbe()
        let coordinator = RemoteAudioPlayoutCoordinator(driver: probe.driver)
        let owner = makeOwner()

        try await coordinator.acquire(
            owner: owner,
            admissionIsCurrent: { true },
            onGlobalQuarantine: {}
        )
        probe.forceLifecycle(playing: true, recording: true, engineRunning: true)

        try await coordinator.release(owner: owner)

        #expect(!probe.snapshot.isPlaying)
        #expect(probe.snapshot.isRecording)
        #expect(probe.snapshot.isEngineRunning)
        #expect(coordinator.failedOwnerCount == 0)
    }

    @Test func recordingStoppedDuringExactPlayoutAllowsIdleFinalRelease() async throws {
        let probe = PlayoutDriverProbe(
            initialized: true,
            recording: true,
            engineRunning: true
        )
        let coordinator = RemoteAudioPlayoutCoordinator(driver: probe.driver)
        let owner = makeOwner()

        try await coordinator.acquire(
            owner: owner,
            admissionIsCurrent: { true },
            onGlobalQuarantine: {}
        )
        probe.forceLifecycle(playing: true, recording: false, engineRunning: true)

        try await coordinator.release(owner: owner)

        #expect(!probe.snapshot.isPlaying)
        #expect(!probe.snapshot.isRecording)
        #expect(!probe.snapshot.isEngineRunning)
        #expect(coordinator.failedOwnerCount == 0)
    }

    @Test func recordingOnlyBaselineLossDuringStopIsQuarantined() async throws {
        let probe = PlayoutDriverProbe(
            initialized: true,
            recording: true,
            engineRunning: true,
            stopClearsRecording: true
        )
        let coordinator = RemoteAudioPlayoutCoordinator(driver: probe.driver)
        let owner = makeOwner()
        try await coordinator.acquire(
            owner: owner,
            admissionIsCurrent: { true },
            onGlobalQuarantine: {}
        )

        await #expect(throws: (any Error).self) {
            try await coordinator.release(owner: owner)
        }

        #expect(coordinator.failedOwnerCount == 1)
        #expect(!probe.snapshot.isRecording)
        #expect(!probe.snapshot.isEngineRunning)
    }

    @Test func recordingStopRacingOwnedPlayoutStopIsQuarantined() async throws {
        let stopEntered = PlayoutTestGate()
        let releaseStop = PlayoutTestGate()
        let probe = PlayoutDriverProbe(
            stopEntered: stopEntered,
            releaseStop: releaseStop
        )
        let coordinator = RemoteAudioPlayoutCoordinator(driver: probe.driver)
        let owner = makeOwner()
        try await coordinator.acquire(
            owner: owner,
            admissionIsCurrent: { true },
            onGlobalQuarantine: {}
        )
        probe.forceLifecycle(playing: true, recording: true, engineRunning: true)

        let release = Task { try await coordinator.release(owner: owner) }
        await stopEntered.wait()
        probe.forceLifecycle(playing: true, recording: false, engineRunning: true)
        await releaseStop.open()

        await #expect(throws: (any Error).self) {
            try await release.value
        }
        #expect(coordinator.failedOwnerCount == 1)
        #expect(!probe.snapshot.isPlaying)
        #expect(!probe.snapshot.isRecording)
        #expect(!probe.snapshot.isEngineRunning)
    }

    @Test func partialLegacyPlayoutStateFailsClosedWithoutChangingGlobalDemand() async throws {
        let probe = PlayoutDriverProbe(
            initialized: true,
            playing: true,
            engineRunning: false
        )
        let coordinator = RemoteAudioPlayoutCoordinator(driver: probe.driver)

        await #expect(throws: (any Error).self) {
            try await coordinator.acquire(
                owner: makeOwner(),
                admissionIsCurrent: { true },
                onGlobalQuarantine: {}
            )
        }

        #expect(probe.snapshot.startCount == 0)
        #expect(probe.snapshot.stopCount == 0)
        #expect(coordinator.activeOwnerCount == 0)
    }

    @Test func unacknowledgedEngineStartFailsClosed() async throws {
        let probe = PlayoutDriverProbe(acknowledgeEngineStart: false)
        let coordinator = RemoteAudioPlayoutCoordinator(driver: probe.driver)

        await #expect(throws: (any Error).self) {
            try await coordinator.acquire(
                owner: makeOwner(),
                admissionIsCurrent: { true },
                onGlobalQuarantine: {}
            )
        }

        #expect(coordinator.activeOwnerCount == 0)
        #expect(probe.snapshot.stopCount == 1)
        #expect(probe.snapshot.sessionReleaseCount == 1)
    }

    @Test func failedFinalStopRetainsExactOwnerUntilRetrySucceeds() async throws {
        let probe = PlayoutDriverProbe(stopFailures: 1)
        let coordinator = RemoteAudioPlayoutCoordinator(driver: probe.driver)
        let owner = makeOwner()
        try await coordinator.acquire(
            owner: owner,
            admissionIsCurrent: { true },
            onGlobalQuarantine: {}
        )

        await #expect(throws: (any Error).self) {
            try await coordinator.release(owner: owner)
        }
        #expect(coordinator.failedOwnerCount == 1)

        try await coordinator.release(owner: owner)
        #expect(coordinator.failedOwnerCount == 0)
        #expect(probe.snapshot.stopCount == 2)
        #expect(!probe.snapshot.isPlaying)
    }

    @Test func failedPreflightHandleReleaseIsRetriedBeforeRollbackCompletes() async throws {
        let probe = PlayoutDriverProbe(sessionReleaseFailures: 1)
        let coordinator = RemoteAudioPlayoutCoordinator(driver: probe.driver)

        await #expect(throws: (any Error).self) {
            try await coordinator.acquire(
                owner: makeOwner(),
                admissionIsCurrent: { true },
                onGlobalQuarantine: {}
            )
        }

        #expect(coordinator.activeOwnerCount == 0)
        #expect(coordinator.failedOwnerCount == 0)
        #expect(probe.snapshot.sessionReleaseCount == 2)
        #expect(probe.snapshot.stopCount == 1)
        #expect(!probe.snapshot.isPlaying)
    }

    @Test func rawAudioEnablesOnlyAfterExactPlayoutAcknowledgement() async throws {
        let startEntered = PlayoutTestGate()
        let releaseStart = PlayoutTestGate()
        let probe = PlayoutDriverProbe(
            startEntered: startEntered,
            releaseStart: releaseStart
        )
        let coordinator = RemoteAudioPlayoutCoordinator(driver: probe.driver)
        let fixture = try makePublicationFixture(coordinator: coordinator)
        let subscriptionSendCount = StateSync(0)
        fixture.publication.subscriptionRequestSender = { _, _, _, _, admission in
            guard admission() else {
                throw LiveKitError(.invalidState, message: "Injected stale subscription admission")
            }
            subscriptionSendCount.mutate { $0 += 1 }
        }
        let subscription = Task {
            try await fixture.publication.set(
                subscribed: true,
                admission: fixture.admissionToken
            )
        }

        await startEntered.wait()
        #expect(!fixture.rtcTrack.isEnabled)
        #expect(subscriptionSendCount.copy() == 0)

        await releaseStart.open()
        try await subscription.value
        #expect(subscriptionSendCount.copy() == 1)
        let activation = Task {
            try await fixture.publication.activateSubscribedTrack(
                fixture.track,
                admission: fixture.admission
            )
        }
        #expect(try await activation.value)
        #expect(fixture.rtcTrack.isEnabled)
        #expect(fixture.track.volume == 1)
        #expect(probe.snapshot.isPlaying)
        #expect(probe.snapshot.isEngineRunning)

        fixture.publication.invalidateSubscriptionAdmissionForOwnershipLoss()
        try await fixture.publication.stopRetainedRemoteTracks()
        #expect(probe.snapshot.stopCount == 1)
    }

    @Test func exactRevocationDuringPlayoutStartNeverEnablesRawAudio() async throws {
        let startEntered = PlayoutTestGate()
        let releaseStart = PlayoutTestGate()
        let probe = PlayoutDriverProbe(
            startEntered: startEntered,
            releaseStart: releaseStart
        )
        let coordinator = RemoteAudioPlayoutCoordinator(driver: probe.driver)
        let fixture = try makePublicationFixture(coordinator: coordinator)
        let subscription = Task {
            try await fixture.publication.set(
                subscribed: true,
                admission: fixture.admissionToken
            )
        }

        await startEntered.wait()
        fixture.publication.invalidateSubscriptionAdmissionForOwnershipLoss()
        #expect(!fixture.rtcTrack.isEnabled)
        await releaseStart.open()

        await #expect(throws: (any Error).self) {
            try await subscription.value
        }
        #expect(!fixture.rtcTrack.isEnabled)
        try await fixture.publication.stopRetainedRemoteTracks()
        #expect(coordinator.activeOwnerCount == 0)
    }

    @Test func deniedProtectedSubscriptionWithoutTrackReleasesExactPlayoutOwner() async throws {
        let probe = PlayoutDriverProbe()
        let coordinator = RemoteAudioPlayoutCoordinator(driver: probe.driver)
        let fixture = try makePublicationFixture(coordinator: coordinator)
        fixture.publication._state.mutate { $0.track = nil }

        try await fixture.publication.set(
            subscribed: true,
            admission: fixture.admissionToken
        )
        #expect(coordinator.activeOwnerCount == 1)

        await fixture.room.signalClient(
            fixture.room.signalClient,
            didUpdateSubscriptionPermission: .with {
                $0.participantSid = "PA_playout"
                $0.trackSid = "TR_playout"
                $0.allowed = false
            }
        )

        #expect(!fixture.publication.isSubscriptionAllowed)
        #expect(fixture.publication.currentSubscriptionAdmissionSnapshot() == nil)
        #expect(coordinator.activeOwnerCount == 0)
        #expect(probe.snapshot.stopCount == 1)
        #expect(fixture.room.connectionState == .disconnected)
        #expect(throws: (any Error).self) {
            _ = try fixture.publication.admitSubscription()
        }
    }

    @Test func protectedAndLegacyAudioDemandCannotOverlap() async throws {
        let probe = PlayoutDriverProbe()
        let coordinator = RemoteAudioPlayoutCoordinator(driver: probe.driver)
        let protected = try makePublicationFixture(coordinator: coordinator)
        protected.publication._state.mutate { $0.track = nil }
        let legacy = try makeLegacyPublication(coordinator: coordinator, suffix: "legacy")

        try await protected.publication.set(
            subscribed: true,
            admission: protected.admissionToken
        )
        await #expect(throws: (any Error).self) {
            try await legacy.publication.set(subscribed: true)
        }
        #expect(legacy.subscribeSendCount.copy() == 0)
        #expect(coordinator.legacyOwnerCount == 0)

        try await protected.publication.revokeSubscription()
        try await legacy.publication.set(subscribed: true)
        #expect(legacy.subscribeSendCount.copy() == 1)
        #expect(coordinator.legacyOwnerCount == 1)

        let secondProtected = try makePublicationFixture(coordinator: coordinator)
        secondProtected.publication._state.mutate { $0.track = nil }
        await #expect(throws: (any Error).self) {
            try await secondProtected.publication.set(
                subscribed: true,
                admission: secondProtected.admissionToken
            )
        }
        #expect(coordinator.activeOwnerCount == 0)

        try await legacy.publication.set(subscribed: false)
        #expect(coordinator.legacyOwnerCount == 0)
        try await secondProtected.publication.set(
            subscribed: true,
            admission: secondProtected.admissionToken
        )
        #expect(coordinator.activeOwnerCount == 1)
        try await secondProtected.publication.revokeSubscription()
    }

    @Test func cancelledQueuedLegacyReleaseRetainsExactOwnerUntilRetry() async throws {
        let probe = PlayoutDriverProbe()
        let coordinator = RemoteAudioPlayoutCoordinator(driver: probe.driver)
        let legacy = try makeLegacyPublication(coordinator: coordinator, suffix: "cancelled-release")
        try await legacy.publication.set(subscribed: true)
        #expect(coordinator.legacyOwnerCount == 1)

        let blockerEntered = StateSync(false)
        let releaseBlocker = DispatchSemaphore(value: 0)
        let blockerOwner = RemoteAudioLegacyDemandOwner(
            publicationNonce: UUID(),
            admissionGeneration: 1
        )
        let blockerTask = Task {
            try await coordinator.reserveLegacyDemand(
                owner: blockerOwner,
                admissionIsCurrent: {
                    blockerEntered.mutate { $0 = true }
                    releaseBlocker.wait()
                    return true
                }
            )
        }
        await waitUntil { blockerEntered.copy() }

        let revokeTask = Task {
            try await legacy.publication.set(subscribed: false)
        }
        await waitUntil {
            legacy.publication._state.remoteAudioLegacyDemandOwnerNeedsRelease
        }
        revokeTask.cancel()
        releaseBlocker.signal()
        try await blockerTask.value

        await #expect(throws: CancellationError.self) {
            try await revokeTask.value
        }
        #expect(legacy.publication._state.remoteAudioLegacyDemandOwner != nil)
        #expect(legacy.publication._state.remoteAudioLegacyDemandOwnerNeedsRelease)
        #expect(legacy.room.failedRemoteTrackRetirementCount == 1)
        #expect(coordinator.legacyOwnerCount == 2)

        try await coordinator.releaseLegacyDemand(owner: blockerOwner)
        try await legacy.publication.set(subscribed: false)
        #expect(coordinator.legacyOwnerCount == 0)
        #expect(legacy.publication._state.remoteAudioLegacyDemandOwner == nil)
        #expect(legacy.room.failedRemoteTrackRetirementCount == 0)

        let protected = try makePublicationFixture(coordinator: coordinator, suffix: "after-legacy-retry")
        protected.publication._state.mutate { $0.track = nil }
        try await protected.publication.set(
            subscribed: true,
            admission: protected.admissionToken
        )
        #expect(coordinator.activeOwnerCount == 1)
        try await protected.publication.revokeSubscription()
    }

    @Test func protectedSubscriptionSignalFailureReleasesPreSignalPlayoutOwner() async throws {
        let probe = PlayoutDriverProbe()
        let coordinator = RemoteAudioPlayoutCoordinator(driver: probe.driver)
        let fixture = try makePublicationFixture(coordinator: coordinator)
        fixture.publication._state.mutate { $0.track = nil }
        fixture.publication.subscriptionRequestSender = { _, _, _, isSubscribed, _ in
            if isSubscribed {
                throw LiveKitError(.network, message: "Injected subscribe failure")
            }
        }

        await #expect(throws: (any Error).self) {
            try await fixture.publication.set(
                subscribed: true,
                admission: fixture.admissionToken
            )
        }

        #expect(coordinator.activeOwnerCount == 0)
        #expect(coordinator.failedOwnerCount == 0)
        #expect(probe.snapshot.stopCount == 1)
        #expect(fixture.publication.currentSubscriptionAdmissionSnapshot() == nil)
    }

    @Test func staleExactUnsubscribeCannotRevokeReplacementAdmission() async throws {
        let probe = PlayoutDriverProbe()
        let coordinator = RemoteAudioPlayoutCoordinator(driver: probe.driver)
        let fixture = try makePublicationFixture(coordinator: coordinator)
        fixture.publication._state.mutate { $0.track = nil }
        let admissionA = fixture.admissionToken

        try await fixture.publication.revokeSubscription()
        let admissionB = try fixture.publication.admitSubscription()
        try await fixture.publication.set(subscribed: true, admission: admissionB)
        let snapshotB = try #require(fixture.publication.currentSubscriptionAdmissionSnapshot())
        #expect(coordinator.activeOwnerCount == 1)
        #expect(fixture.publication.isDesired)

        await #expect(throws: (any Error).self) {
            try await fixture.publication.set(subscribed: false, admission: admissionA)
        }

        #expect(fixture.publication.currentSubscriptionAdmissionSnapshot() == snapshotB)
        #expect(coordinator.activeOwnerCount == 1)
        #expect(fixture.publication.isDesired)

        try await fixture.publication.set(subscribed: false, admission: admissionB)
        #expect(fixture.publication.currentSubscriptionAdmissionSnapshot() == nil)
        #expect(coordinator.activeOwnerCount == 0)
        #expect(!fixture.publication.isDesired)
    }

    @Test func revocationDuringSubscribeSignalCompensatesAndReleasesPlayout() async throws {
        let subscribeEntered = PlayoutTestGate()
        let releaseSubscribe = PlayoutTestGate()
        let unsubscribeCount = StateSync(0)
        let probe = PlayoutDriverProbe()
        let coordinator = RemoteAudioPlayoutCoordinator(driver: probe.driver)
        let fixture = try makePublicationFixture(coordinator: coordinator)
        fixture.publication._state.mutate { $0.track = nil }
        fixture.publication.subscriptionRequestSender = { _, _, _, isSubscribed, admission in
            if isSubscribed {
                await subscribeEntered.open()
                await releaseSubscribe.wait()
                guard admission() else {
                    throw LiveKitError(.invalidState, message: "Injected revoked signal window")
                }
            } else {
                unsubscribeCount.mutate { $0 += 1 }
            }
        }

        let subscribe = Task {
            try await fixture.publication.set(
                subscribed: true,
                admission: fixture.admissionToken
            )
        }
        await subscribeEntered.wait()
        let revoke = Task { try await fixture.publication.revokeSubscription() }
        await waitUntil {
            fixture.publication.currentSubscriptionAdmissionSnapshot() == nil
        }
        await releaseSubscribe.open()

        await #expect(throws: (any Error).self) {
            try await subscribe.value
        }
        try await revoke.value
        #expect(unsubscribeCount.copy() >= 1)
        #expect(coordinator.activeOwnerCount == 0)
        #expect(probe.snapshot.stopCount == 1)
    }

    @Test func globalPlayoutLossSilencesAndRetiresEveryOwningRoom() async throws {
        let probe = PlayoutDriverProbe()
        let coordinator = RemoteAudioPlayoutCoordinator(driver: probe.driver)
        let first = try makePublicationFixture(coordinator: coordinator, suffix: "first")
        let second = try makePublicationFixture(coordinator: coordinator, suffix: "second")

        try await first.publication.set(subscribed: true, admission: first.admissionToken)
        try await second.publication.set(subscribed: true, admission: second.admissionToken)
        #expect(try await first.publication.activateSubscribedTrack(first.track, admission: first.admission))
        #expect(try await second.publication.activateSubscribedTrack(second.track, admission: second.admission))
        #expect(first.rtcTrack.isEnabled)
        #expect(second.rtcTrack.isEnabled)

        let firstOwner = try #require(first.publication._state.remoteAudioPlayoutOwner)
        probe.forceLifecycle(playing: false, recording: false, engineRunning: false)
        await #expect(throws: (any Error).self) {
            try await coordinator.validate(owner: firstOwner) { true }
        }

        #expect(!first.rtcTrack.isEnabled)
        #expect(!second.rtcTrack.isEnabled)
        await waitUntil {
            first.room.connectionState == .disconnected &&
                second.room.connectionState == .disconnected
        }
        #expect(first.room.connectionState == .disconnected)
        #expect(second.room.connectionState == .disconnected)
    }
}

private struct PlayoutDriverSnapshot {
    var isInitialized: Bool
    var isPlaying: Bool
    var isRecording: Bool
    var isEngineRunning: Bool
    var initializeCount: Int
    var startCount: Int
    var stopCount: Int
    var sessionAcquireCount: Int
    var sessionReleaseCount: Int
}

private final class PlayoutDriverProbe: @unchecked Sendable {
    private struct State {
        var isInitialized: Bool
        var isPlaying: Bool
        var isRecording: Bool
        var isEngineRunning: Bool
        var initializeCount = 0
        var startCount = 0
        var stopCount = 0
        var sessionAcquireCount = 0
        var sessionReleaseCount = 0
        var stopFailures: Int
        var sessionReleaseFailures: Int
    }

    private let state: StateSync<State>
    private let failInitialization: Bool
    private let failStart: Bool
    private let acknowledgeEngineStart: Bool
    private let startEntered: PlayoutTestGate?
    private let releaseStart: PlayoutTestGate?
    private let stopEntered: PlayoutTestGate?
    private let releaseStop: PlayoutTestGate?
    private let stopClearsRecording: Bool

    init(
        initialized: Bool = false,
        playing: Bool = false,
        recording: Bool = false,
        engineRunning: Bool = false,
        failInitialization: Bool = false,
        failStart: Bool = false,
        acknowledgeEngineStart: Bool = true,
        stopFailures: Int = 0,
        sessionReleaseFailures: Int = 0,
        stopClearsRecording: Bool = false,
        startEntered: PlayoutTestGate? = nil,
        releaseStart: PlayoutTestGate? = nil,
        stopEntered: PlayoutTestGate? = nil,
        releaseStop: PlayoutTestGate? = nil
    ) {
        state = StateSync(State(
            isInitialized: initialized,
            isPlaying: playing,
            isRecording: recording,
            isEngineRunning: engineRunning,
            stopFailures: stopFailures,
            sessionReleaseFailures: sessionReleaseFailures
        ))
        self.failInitialization = failInitialization
        self.failStart = failStart
        self.acknowledgeEngineStart = acknowledgeEngineStart
        self.startEntered = startEntered
        self.releaseStart = releaseStart
        self.stopEntered = stopEntered
        self.releaseStop = releaseStop
        self.stopClearsRecording = stopClearsRecording
    }

    var driver: RemoteAudioPlayoutDriver {
        RemoteAudioPlayoutDriver(
            acquirePlaybackSession: { [self] in
                state.mutate { $0.sessionAcquireCount += 1 }
                return SessionRequirementHandle { [self] in
                    let shouldFail = state.mutate { state -> Bool in
                        state.sessionReleaseCount += 1
                        guard state.sessionReleaseFailures > 0 else { return false }
                        state.sessionReleaseFailures -= 1
                        return true
                    }
                    if shouldFail {
                        throw LiveKitError(.audioEngine, message: "Injected session release failure")
                    }
                }
            },
            isPlayoutInitialized: { [self] in state.isInitialized },
            initializePlayout: { [self] in
                state.mutate { $0.initializeCount += 1 }
                if failInitialization {
                    throw LiveKitError(.audioEngine, message: "Injected initialization failure")
                }
                state.mutate { $0.isInitialized = true }
            },
            isPlaying: { [self] in state.isPlaying },
            isRecording: { [self] in state.isRecording },
            isEngineRunning: { [self] in state.isEngineRunning },
            startPlayout: { [self] in
                state.mutate { $0.startCount += 1 }
                await startEntered?.open()
                await releaseStart?.wait()
                if failStart {
                    throw LiveKitError(.audioEngine, message: "Injected start failure")
                }
                state.mutate {
                    $0.isPlaying = true
                    $0.isEngineRunning = acknowledgeEngineStart
                }
            },
            stopPlayout: { [self] in
                await stopEntered?.open()
                await releaseStop?.wait()
                let shouldFail = state.mutate { state -> Bool in
                    state.stopCount += 1
                    guard state.stopFailures > 0 else { return false }
                    state.stopFailures -= 1
                    return true
                }
                if shouldFail {
                    throw LiveKitError(.audioEngine, message: "Injected stop failure")
                }
                state.mutate {
                    $0.isPlaying = false
                    if stopClearsRecording {
                        $0.isRecording = false
                    }
                    $0.isEngineRunning = $0.isRecording
                }
            }
        )
    }

    var snapshot: PlayoutDriverSnapshot {
        state.read {
            PlayoutDriverSnapshot(
                isInitialized: $0.isInitialized,
                isPlaying: $0.isPlaying,
                isRecording: $0.isRecording,
                isEngineRunning: $0.isEngineRunning,
                initializeCount: $0.initializeCount,
                startCount: $0.startCount,
                stopCount: $0.stopCount,
                sessionAcquireCount: $0.sessionAcquireCount,
                sessionReleaseCount: $0.sessionReleaseCount
            )
        }
    }

    func forceLifecycle(playing: Bool, recording: Bool, engineRunning: Bool) {
        state.mutate {
            $0.isPlaying = playing
            $0.isRecording = recording
            $0.isEngineRunning = engineRunning
        }
    }
}

private struct PlayoutPublicationFixture {
    let room: Room
    let participant: RemoteParticipant
    let publication: RemoteTrackPublication
    let admissionToken: RemoteTrackSubscriptionAdmission
    let admission: RemoteTrackSubscriptionAdmissionSnapshot
    let track: RemoteAudioTrack
    let rtcTrack: LKRTCAudioTrack
}

private struct LegacyPlayoutPublicationFixture {
    let room: Room
    let participant: RemoteParticipant
    let publication: RemoteTrackPublication
    let subscribeSendCount: StateSync<Int>
}

private func makePublicationFixture(
    coordinator: RemoteAudioPlayoutCoordinator,
    suffix: String = "playout"
) throws -> PlayoutPublicationFixture {
    let room = Room()
    let participantSid = "PA_\(suffix)"
    let identity = "playout-agent-\(suffix)"
    let trackSid = "TR_\(suffix)"
    let participant = RemoteParticipant(
        info: .with {
            $0.sid = participantSid
            $0.identity = identity
            $0.tracks = [.with {
                $0.sid = trackSid
                $0.name = "playout-audio"
                $0.type = .audio
                $0.source = .microphone
            }]
        },
        room: room,
        connectionState: .connected
    )
    room._state.mutate {
        $0.connectionState = .connected
        $0.remoteParticipants[Participant.Identity(from: identity)] = participant
    }
    let publication = try #require(
        participant.trackPublications[Track.Sid(from: trackSid)] as? RemoteTrackPublication
    )
    publication.remoteAudioPlayoutCoordinator = coordinator
    publication.subscriptionRequestSender = { _, _, _, _, admission in
        guard admission() else {
            throw LiveKitError(.invalidState, message: "Injected stale subscription admission")
        }
    }
    let admissionToken = try publication.admitSubscription()
    let admission = try #require(publication.currentSubscriptionAdmissionSnapshot())
    let rtcTrack = RTC.peerConnectionFactory.audioTrack(
        with: RTC.createAudioSource(nil),
        trackId: trackSid
    )
    rtcTrack.isEnabled = false
    let track = RemoteAudioTrack(
        name: "playout-audio",
        source: .microphone,
        track: rtcTrack,
        reportStatistics: false
    )
    track.volume = 0
    publication._state.mutate { $0.track = track }
    return PlayoutPublicationFixture(
        room: room,
        participant: participant,
        publication: publication,
        admissionToken: admissionToken,
        admission: admission,
        track: track,
        rtcTrack: rtcTrack
    )
}

private func makeLegacyPublication(
    coordinator: RemoteAudioPlayoutCoordinator,
    suffix: String
) throws -> LegacyPlayoutPublicationFixture {
    let room = Room()
    let identity = "playout-agent-\(suffix)"
    let participantSid = "PA_\(suffix)"
    let trackSid = "TR_\(suffix)"
    let participant = RemoteParticipant(
        info: .with {
            $0.sid = participantSid
            $0.identity = identity
            $0.tracks = [.with {
                $0.sid = trackSid
                $0.name = "playout-audio-\(suffix)"
                $0.type = .audio
                $0.source = .microphone
            }]
        },
        room: room,
        connectionState: .connected
    )
    room._state.mutate {
        $0.connectionState = .connected
        $0.remoteParticipants[Participant.Identity(from: identity)] = participant
    }
    let publication = try #require(
        participant.trackPublications[Track.Sid(from: trackSid)] as? RemoteTrackPublication
    )
    let subscribeSendCount = StateSync(0)
    publication.remoteAudioPlayoutCoordinator = coordinator
    publication.subscriptionRequestSender = { _, _, _, isSubscribed, admission in
        guard admission() else {
            throw LiveKitError(.invalidState, message: "Injected stale legacy subscription")
        }
        if isSubscribed {
            subscribeSendCount.mutate { $0 += 1 }
        }
    }
    return LegacyPlayoutPublicationFixture(
        room: room,
        participant: participant,
        publication: publication,
        subscribeSendCount: subscribeSendCount
    )
}

private func makeOwner() -> RemoteAudioPlayoutOwner {
    RemoteAudioPlayoutOwner(
        publicationNonce: UUID(),
        admissionGeneration: 1,
        admissionTokenNonce: UUID()
    )
}

private actor PlayoutTestGate {
    private var isOpen = false
    private var waiters: [CheckedContinuation<Void, Never>] = []

    func wait() async {
        guard !isOpen else { return }
        await withCheckedContinuation { waiters.append($0) }
    }

    func open() {
        isOpen = true
        let continuations = waiters
        waiters.removeAll()
        continuations.forEach { $0.resume() }
    }
}

private func waitUntil(
    attempts: Int = 1_000,
    _ condition: @escaping @Sendable () -> Bool
) async {
    for _ in 0 ..< attempts {
        if condition() { return }
        await Task.yield()
    }
}
