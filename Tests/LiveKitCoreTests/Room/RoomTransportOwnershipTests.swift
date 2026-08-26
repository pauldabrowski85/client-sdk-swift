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
@testable import LiveKit
import LiveKitWebRTC
import Testing

@Suite(.tags(.media))
struct RoomTransportOwnershipTests {
    @Test func staleTransportStateCallbacksCannotResolveOrResetCurrentCompleter() async throws {
        try await withTransportFixture { fixture in
            let waiter = Task {
                try await fixture.room.primaryTransportConnectedCompleter.wait(timeout: 1)
            }
            let deadline = Date().addingTimeInterval(1)
            while fixture.room.primaryTransportConnectedCompleter.waiterCount == 0,
                  Date() < deadline
            {
                await Task.yield()
            }
            #expect(fixture.room.primaryTransportConnectedCompleter.waiterCount == 1)

            fixture.room.transport(fixture.staleSubscriber, didUpdateState: .connected)
            await Task.yield()
            #expect(fixture.room.primaryTransportConnectedCompleter.waiterCount == 1)

            fixture.room.transport(fixture.currentSubscriber, didUpdateState: .connected)
            try await waiter.value

            fixture.room.transport(fixture.staleSubscriber, didUpdateState: .failed)
            try await fixture.room.primaryTransportConnectedCompleter.wait(timeout: 0.1)
            #expect(fixture.room._state.connectionState == .connected)
            #expect(fixture.room._state.isReconnectingWithMode == nil)
        }
    }

    @Test func staleSubscriberCannotAttachTrackToSameSidReplacement() async throws {
        try await withTransportFixture { fixture in
            let publication = try #require(
                fixture.replacement.trackPublications[fixture.trackSid] as? RemoteTrackPublication
            )
            #expect(publication.track == nil)

            fixture.room.transport(
                fixture.staleSubscriber,
                didAddTrack: fixture.rtcTrack,
                rtpReceiver: fixture.rtpReceiver,
                streams: [fixture.stream]
            )
            try await Task.sleep(nanoseconds: 100_000_000)

            #expect(publication.track == nil)
        }
    }

    @Test func rawRemoteTrackIsSilencedSynchronouslyBeforeAdmissionWorkIsQueued() async throws {
        try await withTransportFixture { fixture in
            fixture.installOriginalAsCurrent()
            let publication = try #require(
                fixture.original.trackPublications[fixture.trackSid] as? RemoteTrackPublication
            )
            _ = try publication.admitSubscription()
            fixture.rtcTrack.isEnabled = true

            fixture.room.transport(
                fixture.staleSubscriber,
                didAddTrack: fixture.rtcTrack,
                rtpReceiver: fixture.rtpReceiver,
                streams: [fixture.stream]
            )

            #expect(!fixture.rtcTrack.isEnabled)
            fixture.installReplacementAsCurrent()
            await fixture.room.flushExecutionQueue()
        }
    }

    @Test func staleSubscriberCannotRemoveTrackFromSameSidReplacement() async throws {
        try await withTransportFixture { fixture in
            let publication = try #require(
                fixture.replacement.trackPublications[fixture.trackSid] as? RemoteTrackPublication
            )
            let currentTrack = RemoteAudioTrack(
                name: "replacement-audio",
                source: .microphone,
                track: fixture.rtcTrack,
                reportStatistics: false
            )
            await publication.set(track: currentTrack)
            #expect(publication.track === currentTrack)

            fixture.room.transport(
                fixture.staleSubscriber,
                didRemoveTrack: fixture.rtcTrack
            )
            try await Task.sleep(nanoseconds: 100_000_000)

            #expect(publication.track === currentTrack)
        }
    }

    @Test func queuedAddFromFormerCurrentSubscriberCannotAttachToSameSidReplacement() async throws {
        try await withTransportFixture { fixture in
            let publication = try #require(
                fixture.replacement.trackPublications[fixture.trackSid] as? RemoteTrackPublication
            )
            #expect(publication.track == nil)

            fixture.installOriginalAsCurrent()
            fixture.room.transport(
                fixture.staleSubscriber,
                didAddTrack: fixture.rtcTrack,
                rtpReceiver: fixture.rtpReceiver,
                streams: [fixture.stream]
            )
            await fixture.room.flushExecutionQueue()

            fixture.installReplacementAsCurrent()
            await fixture.room.flushExecutionQueue()
            try await Task.sleep(nanoseconds: 100_000_000)

            #expect(publication.track == nil)
        }
    }

    @Test func queuedRemoveFromFormerCurrentSubscriberCannotDetachSameSidReplacement() async throws {
        try await withTransportFixture { fixture in
            let publication = try #require(
                fixture.replacement.trackPublications[fixture.trackSid] as? RemoteTrackPublication
            )
            let currentTrack = RemoteAudioTrack(
                name: "replacement-audio",
                source: .microphone,
                track: fixture.rtcTrack,
                reportStatistics: false
            )
            await publication.set(track: currentTrack)
            #expect(publication.track === currentTrack)

            fixture.installOriginalAsCurrent()
            fixture.room.transport(
                fixture.staleSubscriber,
                didRemoveTrack: fixture.rtcTrack
            )
            await fixture.room.flushExecutionQueue()

            fixture.installReplacementAsCurrent()
            await fixture.room.flushExecutionQueue()
            try await Task.sleep(nanoseconds: 100_000_000)

            #expect(publication.track === currentTrack)
        }
    }

    @Test func replacementDuringPublicationMutationCannotAttachOrStartStaleTrack() async throws {
        try await withTransportFixture { fixture in
            let didSetTrack = TestMediaGate()
            let releaseSet = TestMediaGate()
            let gatedPublication = GatedRemoteTrackPublication(
                info: .with {
                    $0.sid = fixture.trackSid.stringValue
                    $0.name = "original-audio"
                    $0.type = .audio
                    $0.source = .microphone
                },
                participant: fixture.original,
                didSetTrack: didSetTrack,
                releaseSet: releaseSet
            )
            fixture.original._state.mutate {
                $0.trackPublications[fixture.trackSid] = gatedPublication
            }
            fixture.installOriginalAsCurrent()

            let task = Task {
                await fixture.room.engine(
                    fixture.room,
                    didAddTrack: fixture.rtcTrack,
                    rtpReceiver: fixture.rtpReceiver,
                    stream: fixture.stream,
                    sourceTransport: fixture.staleSubscriber,
                    receiveGeneration: fixture.staleSubscriber.dataPacketReceiveGeneration
                )
            }
            await didSetTrack.wait()

            fixture.installReplacementAsCurrent()
            await releaseSet.open()
            await task.value

            let staleTrack = try #require(gatedPublication.capturedTrack.copy())
            #expect(gatedPublication.track == nil)
            #expect(staleTrack.trackState == .stopped)
            #expect(staleTrack._state.transport == nil)
            let replacementPublication = try #require(
                fixture.replacement.trackPublications[fixture.trackSid] as? RemoteTrackPublication
            )
            #expect(replacementPublication.track == nil)
        }
    }

    @Test func concurrentAddsForSamePublicationLeaveOnlyExactWinningTrackStarted() async throws {
        try await withTransportFixture { fixture in
            let firstInstalled = TestMediaGate()
            let releaseFirst = TestMediaGate()
            let publication = FirstInstallGatedRemoteTrackPublication(
                info: .with {
                    $0.sid = fixture.trackSid.stringValue
                    $0.name = "shared-audio"
                    $0.type = .audio
                    $0.source = .microphone
                },
                participant: fixture.original,
                firstInstalled: firstInstalled,
                releaseFirst: releaseFirst
            )
            fixture.original._state.mutate {
                $0.trackPublications[fixture.trackSid] = publication
            }
            fixture.installOriginalAsCurrent()
            let admission = try #require(publication.currentSubscriptionAdmissionSnapshot())

            let firstTask = Task {
                try await fixture.original.addSubscribedMediaTrack(
                    rtcTrack: fixture.rtcTrack,
                    rtpReceiver: fixture.rtpReceiver,
                    trackSid: fixture.trackSid,
                    sourceTransport: fixture.staleSubscriber,
                    receiveGeneration: fixture.staleSubscriber.dataPacketReceiveGeneration,
                    subscriptionAdmission: admission
                )
            }
            await firstInstalled.wait()

            let secondSource = RTC.createAudioSource(nil)
            let secondRTCTrack = RTC.peerConnectionFactory.audioTrack(
                with: secondSource,
                trackId: fixture.trackSid.stringValue
            )
            let secondTransceiver = try await fixture.staleSubscriber.addTransceiver(
                ofType: .audio,
                transceiverInit: LKRTCRtpTransceiverInit()
            )
            try await fixture.original.addSubscribedMediaTrack(
                rtcTrack: secondRTCTrack,
                rtpReceiver: secondTransceiver.receiver,
                trackSid: fixture.trackSid,
                sourceTransport: fixture.staleSubscriber,
                receiveGeneration: fixture.staleSubscriber.dataPacketReceiveGeneration,
                subscriptionAdmission: admission
            )

            await releaseFirst.open()
            try await firstTask.value

            let winningTrack = try #require(publication.track)
            let losingTrack = try #require(publication.firstTrack.copy())
            #expect(winningTrack.mediaTrack === secondRTCTrack)
            #expect(winningTrack.trackState == .started)
            #expect(losingTrack.trackState == .stopped)
            #expect(losingTrack._state.transport == nil)
        }
    }

    @Test func delayedRemoveForOldRtcTrackCannotClearExactReplacementTrack() async throws {
        try await withTransportFixture { fixture in
            fixture.installOriginalAsCurrent()
            let publication = try #require(
                fixture.original.trackPublications[fixture.trackSid] as? RemoteTrackPublication
            )
            let replacementSource = RTC.createAudioSource(nil)
            let replacementRTCTrack = RTC.peerConnectionFactory.audioTrack(
                with: replacementSource,
                trackId: fixture.trackSid.stringValue
            )
            let replacementTrack = RemoteAudioTrack(
                name: "replacement-audio",
                source: .microphone,
                track: replacementRTCTrack,
                reportStatistics: false
            )
            await publication.set(track: replacementTrack)

            try await fixture.room.engine(
                fixture.room,
                didRemoveTrack: fixture.rtcTrack,
                sourceTransport: fixture.staleSubscriber,
                receiveGeneration: fixture.staleSubscriber.dataPacketReceiveGeneration
            )

            #expect(publication.track === replacementTrack)
        }
    }

    @Test func revocationDuringSubscribeSignalCompensatesAndRejectsOldAdmission() async throws {
        try await withTransportFixture { fixture in
            fixture.installOriginalAsCurrent()
            let publication = try #require(
                fixture.original.trackPublications[fixture.trackSid] as? RemoteTrackPublication
            )
            let admission = try publication.admitSubscription()
            let enteredSubscribeSignal = TestMediaGate()
            let releaseSubscribeSignal = TestMediaGate()
            let signals = StateSync<[Bool]>([])
            publication.subscriptionRequestSender = { _, _, _, isSubscribed, isAdmitted in
                signals.mutate { $0.append(isSubscribed) }
                if isSubscribed {
                    #expect(isAdmitted())
                    await enteredSubscribeSignal.open()
                    await releaseSubscribeSignal.wait()
                    #expect(!isAdmitted())
                } else {
                    #expect(isAdmitted())
                }
            }

            let subscribeTask = Task {
                try await publication.set(subscribed: true, admission: admission)
            }
            await enteredSubscribeSignal.wait()

            let revokeTask = Task {
                try await publication.revokeSubscription()
            }
            while publication.currentSubscriptionAdmissionSnapshot() != nil {
                await Task.yield()
            }

            await releaseSubscribeSignal.open()
            await #expect(throws: LiveKitError.self) {
                try await subscribeTask.value
            }
            try await revokeTask.value

            #expect(signals.copy() == [true, false, false])
            #expect(publication.track == nil)
            await #expect(throws: LiveKitError.self) {
                try await publication.set(subscribed: true, admission: admission)
            }
            #expect(signals.copy() == [true, false, false])
        }
    }

    @Test func revocationDuringDidAddCasNeverStartsOrAttachesTrack() async throws {
        try await withTransportFixture { fixture in
            let didSetTrack = TestMediaGate()
            let releaseSet = TestMediaGate()
            let publication = GatedRemoteTrackPublication(
                info: .with {
                    $0.sid = fixture.trackSid.stringValue
                    $0.name = "protected-audio"
                    $0.type = .audio
                    $0.source = .microphone
                },
                participant: fixture.original,
                didSetTrack: didSetTrack,
                releaseSet: releaseSet
            )
            fixture.original._state.mutate {
                $0.trackPublications[fixture.trackSid] = publication
            }
            fixture.installOriginalAsCurrent()
            _ = try publication.admitSubscription()
            publication.subscriptionRequestSender = { _, _, _, isSubscribed, isAdmitted in
                #expect(!isSubscribed)
                #expect(isAdmitted())
            }

            let addTask = Task {
                await fixture.room.engine(
                    fixture.room,
                    didAddTrack: fixture.rtcTrack,
                    rtpReceiver: fixture.rtpReceiver,
                    stream: fixture.stream,
                    sourceTransport: fixture.staleSubscriber,
                    receiveGeneration: fixture.staleSubscriber.dataPacketReceiveGeneration
                )
            }
            await didSetTrack.wait()
            let capturedTrack = try #require(publication.capturedTrack.copy() as? RemoteAudioTrack)
            #expect(capturedTrack.trackState == .stopped)
            #expect(!fixture.rtcTrack.isEnabled)

            let revokeTask = Task {
                try await publication.revokeSubscription()
            }
            while publication.currentSubscriptionAdmissionSnapshot() != nil {
                await Task.yield()
            }
            #expect(publication.track == nil)
            #expect(!fixture.rtcTrack.isEnabled)

            await releaseSet.open()
            await addTask.value
            try await revokeTask.value

            #expect(publication.track == nil)
            #expect(capturedTrack.trackState == .stopped)
            #expect(capturedTrack._state.transport == nil)
            #expect(!fixture.rtcTrack.isEnabled)
        }
    }

    @Test func repeatedRevocationClearsLateTrackWhenPreferenceAlreadyFalse() async throws {
        try await withTransportFixture { fixture in
            fixture.installOriginalAsCurrent()
            let publication = try #require(
                fixture.original.trackPublications[fixture.trackSid] as? RemoteTrackPublication
            )
            _ = try publication.admitSubscription()
            publication.subscriptionRequestSender = { _, _, _, isSubscribed, isAdmitted in
                #expect(!isSubscribed)
                #expect(isAdmitted())
            }
            try await publication.revokeSubscription()

            let lateTrack = RemoteAudioTrack(
                name: "late-audio",
                source: .microphone,
                track: fixture.rtcTrack,
                reportStatistics: false
            )
            lateTrack.volume = 1
            await publication.set(track: lateTrack)
            #expect(publication.track === lateTrack)

            try await publication.revokeSubscription()

            #expect(publication.track == nil)
            #expect(!fixture.rtcTrack.isEnabled)
        }
    }

    @Test func displacedTrackIsQuarantinedAcrossSuspendedStopAndConcurrentRevocation() async throws {
        try await withTransportFixture { fixture in
            fixture.installOriginalAsCurrent()
            let publication = try #require(
                fixture.original.trackPublications[fixture.trackSid] as? RemoteTrackPublication
            )
            _ = try publication.admitSubscription()
            publication.subscriptionRequestSender = { _, _, _, isSubscribed, isAdmitted in
                #expect(!isSubscribed)
                #expect(isAdmitted())
            }

            let stopEntered = TestMediaGate()
            let releaseStop = TestMediaGate()
            let displaced = GatedStopRemoteAudioTrack(
                name: "displaced-audio",
                source: .microphone,
                track: fixture.rtcTrack,
                reportStatistics: false,
                stopEntered: stopEntered,
                releaseStop: releaseStop
            )
            displaced._state.mutate { $0.trackState = .started }
            displaced.mediaTrack.isEnabled = true
            await displaced.set(transport: fixture.staleSubscriber, rtpReceiver: fixture.rtpReceiver)
            await publication.set(track: displaced)

            let replacementSource = RTC.createAudioSource(nil)
            let replacementRTCTrack = RTC.peerConnectionFactory.audioTrack(
                with: replacementSource,
                trackId: fixture.trackSid.stringValue
            )
            let replacement = RemoteAudioTrack(
                name: "replacement-audio",
                source: .microphone,
                track: replacementRTCTrack,
                reportStatistics: false
            )
            replacement.mediaTrack.isEnabled = false
            let snapshot = try #require(publication.currentSubscriptionAdmissionSnapshot())
            #expect(await publication.replaceSubscribedTrack(
                expected: displaced,
                with: replacement,
                admission: snapshot
            ))
            #expect(!displaced.mediaTrack.isEnabled)
            #expect(displaced._state.transport == nil)

            let retirementTask = Task {
                try await publication.retireRetainedRemoteTrack(displaced)
            }
            await stopEntered.wait()

            let revocationTask = Task {
                try await publication.revokeSubscription()
            }
            while publication.currentSubscriptionAdmissionSnapshot() != nil {
                await Task.yield()
            }

            #expect(publication.track == nil)
            #expect(!displaced.mediaTrack.isEnabled)
            #expect(!replacement.mediaTrack.isEnabled)
            #expect(displaced._state.transport == nil)
            #expect(replacement._state.transport == nil)

            await releaseStop.open()
            try await retirementTask.value
            try await revocationTask.value

            #expect(displaced.trackState == .stopped)
            #expect(replacement.trackState == .stopped)
            _ = try publication.admitSubscription()
        }
    }

    @Test func removedTrackStopFailureRemainsRetainedUntilRevocationRetrySucceeds() async throws {
        try await withTransportFixture { fixture in
            fixture.installOriginalAsCurrent()
            let publication = try #require(
                fixture.original.trackPublications[fixture.trackSid] as? RemoteTrackPublication
            )
            _ = try publication.admitSubscription()
            publication.subscriptionRequestSender = { _, _, _, isSubscribed, isAdmitted in
                #expect(!isSubscribed)
                #expect(isAdmitted())
            }

            let exactTrack = FailOnceRemoteAudioTrack(
                name: "removed-audio",
                source: .microphone,
                track: fixture.rtcTrack,
                reportStatistics: false
            )
            exactTrack._state.mutate { $0.trackState = .started }
            exactTrack.mediaTrack.isEnabled = true
            await exactTrack.set(transport: fixture.staleSubscriber, rtpReceiver: fixture.rtpReceiver)
            await publication.set(track: exactTrack)

            await #expect(throws: LiveKitError.self) {
                try await fixture.room.engine(
                    fixture.room,
                    didRemoveTrack: fixture.rtcTrack,
                    sourceTransport: fixture.staleSubscriber,
                    receiveGeneration: fixture.staleSubscriber.dataPacketReceiveGeneration
                )
            }

            #expect(publication.track == nil)
            #expect(!exactTrack.mediaTrack.isEnabled)
            #expect(exactTrack._state.transport == nil)
            #expect(throws: LiveKitError.self) {
                _ = try publication.admitSubscription()
            }

            try await publication.revokeSubscription()

            #expect(exactTrack.trackState == .stopped)
            #expect(!exactTrack.mediaTrack.isEnabled)
            _ = try publication.admitSubscription()
        }
    }

    @Test func staleRetirementCompletionCannotReleaseNewerRetainedTrack() async throws {
        try await withTransportFixture { fixture in
            fixture.installOriginalAsCurrent()
            let publication = try #require(
                fixture.original.trackPublications[fixture.trackSid] as? RemoteTrackPublication
            )

            let retiredA = RemoteAudioTrack(
                name: "retired-a",
                source: .microphone,
                track: fixture.rtcTrack,
                reportStatistics: false
            )
            publication._state.mutate { state in
                state.failedRemoteRevocationTracks[ObjectIdentifier(retiredA)] = retiredA
                state.subscriptionAdmission.revocationNeedsRetry = true
            }
            let generationA = fixture.room.retainFailedRemoteTrackRetirement(publication)
            #expect(fixture.room.failedRemoteTrackRetirementCount == 1)

            publication._state.mutate { state in
                state.failedRemoteRevocationTracks[ObjectIdentifier(retiredA)] = nil
            }
            let releaseProofPassed = TestMediaGate()
            let allowReleaseRemoval = DispatchSemaphore(value: 0)
            let releaseTask = Task.detached {
                fixture.room.releaseFailedRemoteTrackRetirement(
                    publication,
                    generation: generationA,
                    beforeRemoval: {
                        Task { await releaseProofPassed.open() }
                        allowReleaseRemoval.wait()
                    }
                )
            }
            await releaseProofPassed.wait()

            let replacementRTCTrack = RTC.peerConnectionFactory.audioTrack(
                with: RTC.createAudioSource(nil),
                trackId: "TR_retirement_b"
            )
            let stopEntered = TestMediaGate()
            let allowFailedStop = TestMediaGate()
            let retiredB = GatedFailOnceRemoteAudioTrack(
                name: "retired-b",
                source: .microphone,
                track: replacementRTCTrack,
                reportStatistics: false,
                stopEntered: stopEntered,
                allowFailedStop: allowFailedStop
            )
            retiredB._state.mutate { $0.trackState = .started }
            let insertionAttempted = TestMediaGate()
            let insertionTask = Task.detached {
                await insertionAttempted.open()
                try await publication.removeStaleTrack(retiredB)
            }
            await insertionAttempted.wait()
            #expect(publication._state.failedRemoteRevocationTracks[ObjectIdentifier(retiredB)] == nil)

            allowReleaseRemoval.signal()
            await releaseTask.value
            await stopEntered.wait()

            #expect(fixture.room.failedRemoteTrackRetirementCount == 1)
            #expect(retiredB.trackState == .started)
            await allowFailedStop.open()
            await #expect(throws: LiveKitError.self) {
                try await insertionTask.value
            }

            try await fixture.room.stopFailedRemoteTrackRetirements()

            #expect(retiredB.trackState == .stopped)
            #expect(fixture.room.failedRemoteTrackRetirementCount == 0)
        }
    }
}

private final class GatedStopRemoteAudioTrack: RemoteAudioTrack, @unchecked Sendable {
    private let stopEntered: TestMediaGate
    private let releaseStop: TestMediaGate

    init(
        name: String,
        source: Track.Source,
        track: LKRTCMediaStreamTrack,
        reportStatistics: Bool,
        stopEntered: TestMediaGate,
        releaseStop: TestMediaGate
    ) {
        self.stopEntered = stopEntered
        self.releaseStop = releaseStop
        super.init(
            name: name,
            source: source,
            track: track,
            reportStatistics: reportStatistics
        )
    }

    override func stopCapture() async throws {
        await stopEntered.open()
        await releaseStop.wait()
    }
}

private final class FailOnceRemoteAudioTrack: RemoteAudioTrack, @unchecked Sendable {
    private let stopAttempts = StateSync(0)

    override func stopCapture() async throws {
        let attempt = stopAttempts.mutate { attempts -> Int in
            attempts += 1
            return attempts
        }
        if attempt == 1 {
            throw LiveKitError(.invalidState, message: "injected remote stop failure")
        }
    }
}

private final class GatedFailOnceRemoteAudioTrack: RemoteAudioTrack, @unchecked Sendable {
    private let stopAttempts = StateSync(0)
    private let stopEntered: TestMediaGate
    private let allowFailedStop: TestMediaGate

    init(
        name: String,
        source: Track.Source,
        track: LKRTCMediaStreamTrack,
        reportStatistics: Bool,
        stopEntered: TestMediaGate,
        allowFailedStop: TestMediaGate
    ) {
        self.stopEntered = stopEntered
        self.allowFailedStop = allowFailedStop
        super.init(
            name: name,
            source: source,
            track: track,
            reportStatistics: reportStatistics
        )
    }

    override func stopCapture() async throws {
        let attempt = stopAttempts.mutate { attempts -> Int in
            attempts += 1
            return attempts
        }
        guard attempt == 1 else { return }
        await stopEntered.open()
        await allowFailedStop.wait()
        throw LiveKitError(.invalidState, message: "injected gated retirement failure")
    }
}

private final class GatedRemoteTrackPublication: RemoteTrackPublication, @unchecked Sendable {
    let capturedTrack = StateSync<Track?>(nil)
    private let didSetTrack: TestMediaGate
    private let releaseSet: TestMediaGate

    init(
        info: Livekit_TrackInfo,
        participant: RemoteParticipant,
        didSetTrack: TestMediaGate,
        releaseSet: TestMediaGate
    ) {
        self.didSetTrack = didSetTrack
        self.releaseSet = releaseSet
        super.init(info: info, participant: participant)
    }

    override func replaceSubscribedTrack(
        expected: Track?,
        with newValue: Track?,
        admission: RemoteTrackSubscriptionAdmissionSnapshot?
    ) async -> Bool {
        let didReplace = await super.replaceSubscribedTrack(
            expected: expected,
            with: newValue,
            admission: admission
        )
        if didReplace, let newValue {
            capturedTrack.mutate { $0 = newValue }
            await didSetTrack.open()
            await releaseSet.wait()
        }
        return didReplace
    }
}

private final class FirstInstallGatedRemoteTrackPublication: RemoteTrackPublication, @unchecked Sendable {
    let firstTrack = StateSync<Track?>(nil)
    private let installCount = StateSync(0)
    private let firstInstalled: TestMediaGate
    private let releaseFirst: TestMediaGate

    init(
        info: Livekit_TrackInfo,
        participant: RemoteParticipant,
        firstInstalled: TestMediaGate,
        releaseFirst: TestMediaGate
    ) {
        self.firstInstalled = firstInstalled
        self.releaseFirst = releaseFirst
        super.init(info: info, participant: participant)
    }

    override func replaceSubscribedTrack(
        expected: Track?,
        with newValue: Track?,
        admission: RemoteTrackSubscriptionAdmissionSnapshot?
    ) async -> Bool {
        let didReplace = await super.replaceSubscribedTrack(
            expected: expected,
            with: newValue,
            admission: admission
        )
        guard didReplace, let newValue else { return didReplace }
        let isFirst = installCount.mutate { count -> Bool in
            count += 1
            return count == 1
        }
        if isFirst {
            firstTrack.mutate { $0 = newValue }
            await firstInstalled.open()
            await releaseFirst.wait()
        }
        return didReplace
    }
}

private actor TestMediaGate {
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

private struct TransportOwnershipFixture {
    let room: Room
    let staleJoin: JoinDependencies
    let currentJoin: JoinDependencies
    let staleSubscriber: Transport
    let currentPublisher: Transport
    let currentSubscriber: Transport
    let participantIdentity: Participant.Identity
    let original: RemoteParticipant
    let replacement: RemoteParticipant
    let trackSid: Track.Sid
    let rtcTrack: LKRTCAudioTrack
    let rtpReceiver: LKRTCRtpReceiver
    let stream: LKRTCMediaStream

    func installOriginalAsCurrent() {
        room._state.mutate {
            $0.connectionState = .reconnecting
            $0.isReconnectingWithMode = .full
            $0.remoteParticipants[participantIdentity] = original
            $0.stage = .connected(staleJoin)
        }
    }

    func installReplacementAsCurrent() {
        room._state.mutate {
            $0.connectionState = .connected
            $0.isReconnectingWithMode = nil
            $0.remoteParticipants[participantIdentity] = replacement
            $0.stage = .connected(currentJoin)
        }
    }
}

private extension Room {
    func flushExecutionQueue() async {
        await withCheckedContinuation { continuation in
            _blockProcessQueue.async {
                continuation.resume()
            }
        }
    }
}

private func withTransportFixture(
    _ body: (TransportOwnershipFixture) async throws -> Void
) async throws {
    let room = Room()
    let connection = ConnectionDependencies(
        room: room,
        roomOptions: room._state.roomOptions
    )
    let joinResponse = Livekit_JoinResponse.with { $0.subscriberPrimary = true }
    let staleJoin = try await JoinDependencies.make(
        room: room,
        connection: connection,
        joinResponse: joinResponse,
        rtcConfiguration: .liveKitDefault(),
        singlePeerConnection: false
    )
    let currentJoin = try await JoinDependencies.make(
        room: room,
        connection: connection,
        joinResponse: joinResponse,
        rtcConfiguration: .liveKitDefault(),
        singlePeerConnection: false
    )
    let staleSubscriber = try #require(staleJoin.transport.subscriber)
    let currentPublisher = try #require(currentJoin.transport.publisher)
    let currentSubscriber = try #require(currentJoin.transport.subscriber)

    do {
        let participantSid = "PA_reused"
        let participantIdentity = "same-agent"
        let trackSid = Track.Sid(from: "TR_reused")
        let original = RemoteParticipant(
            info: .with {
                $0.sid = participantSid
                $0.identity = participantIdentity
                $0.tracks = [.with {
                    $0.sid = trackSid.stringValue
                    $0.name = "original-audio"
                    $0.type = .audio
                    $0.source = .microphone
                }]
            },
            room: room,
            connectionState: .connected
        )
        let replacement = RemoteParticipant(
            info: .with {
                $0.sid = participantSid
                $0.identity = participantIdentity
                $0.tracks = [.with {
                    $0.sid = trackSid.stringValue
                    $0.name = "replacement-audio"
                    $0.type = .audio
                    $0.source = .microphone
                }]
            },
            room: room,
            connectionState: .connected
        )
        room._state.mutate {
            $0.connectionState = .connected
            $0.remoteParticipants[Participant.Identity(from: participantIdentity)] = replacement
            $0.stage = .connected(currentJoin)
        }

        let audioSource = RTC.createAudioSource(nil)
        let rtcTrack = RTC.peerConnectionFactory.audioTrack(
            with: audioSource,
            trackId: trackSid.stringValue
        )
        let transceiver = try await staleSubscriber.addTransceiver(
            ofType: .audio,
            transceiverInit: LKRTCRtpTransceiverInit()
        )
        let stream = RTC.peerConnectionFactory.mediaStream(
            withStreamId: "\(participantSid)|\(trackSid.stringValue)"
        )

        #expect(ObjectIdentifier(original) != ObjectIdentifier(replacement))
        try await body(TransportOwnershipFixture(
            room: room,
            staleJoin: staleJoin,
            currentJoin: currentJoin,
            staleSubscriber: staleSubscriber,
            currentPublisher: currentPublisher,
            currentSubscriber: currentSubscriber,
            participantIdentity: Participant.Identity(from: participantIdentity),
            original: original,
            replacement: replacement,
            trackSid: trackSid,
            rtcTrack: rtcTrack,
            rtpReceiver: transceiver.receiver,
            stream: stream
        ))
    } catch {
        await staleJoin.transport.close()
        await currentJoin.transport.close()
        throw error
    }

    await staleJoin.transport.close()
    await currentJoin.transport.close()
}
