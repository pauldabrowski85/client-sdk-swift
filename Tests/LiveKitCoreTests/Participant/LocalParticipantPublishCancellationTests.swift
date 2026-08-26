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

@Suite(.serialized)
struct LocalParticipantPublishCancellationTests {
    @Test func cancellationBeforePublicationCommitStopsExactTrackAndDoesNotInsert() async throws {
        let room = Room()
        let publisher = try Transport(
            config: .liveKitDefault(),
            target: .publisher,
            primary: true,
            delegate: room
        )
        room._state.mutate {
            $0.connectionState = .connected
            $0.transport = .publisherOnly(publisher: publisher)
        }
        room.localParticipant.set(
            info: .with {
                $0.sid = "PA_local"
                $0.identity = "local"
                $0.permission = .with { $0.canPublish = true }
            },
            connectionState: .connected
        )

        let captureStarted = PublishCancellationGate()
        let releaseStart = PublishCancellationGate()
        let track = CancellationProbeAudioTrack(
            captureStarted: captureStarted,
            releaseStart: releaseStart,
            stopFailures: 1
        )
        let task = Task {
            try await room.localParticipant._publish(track: track)
        }

        await captureStarted.wait()
        task.cancel()
        #expect(task.isCancelled)
        releaseStart.open()

        await #expect(throws: CancellationError.self) {
            try await task.value
        }
        #expect(track.trackState == .started)
        #expect(track.stopAttemptCount.copy() == 1)
        #expect(room.localParticipant.failedPublishTrackCount == 1)
        #expect(room.localParticipant.trackPublications.isEmpty)

        try await room.localParticipant.stopFailedPublishTracks()
        #expect(track.trackState == .stopped)
        #expect(track.stopAttemptCount.copy() == 2)
        #expect(room.localParticipant.failedPublishTrackCount == 0)
        await publisher.close()
    }

    @Test func failedUnpublishRetainsExactTrackUntilCaptureStopIsConfirmed() async throws {
        let room = Room()
        let captureStarted = PublishCancellationGate()
        let releaseStart = PublishCancellationGate()
        releaseStart.open()
        let track = CancellationProbeAudioTrack(
            captureStarted: captureStarted,
            releaseStart: releaseStart,
            stopFailures: 2
        )
        try await track.start()

        let publication = LocalTrackPublication(
            info: .with {
                $0.sid = "TR_failed-unpublish"
                $0.name = track.name
                $0.type = .audio
                $0.source = .microphone
            },
            participant: room.localParticipant
        )
        await publication.set(track: track)
        room.localParticipant.add(publication: publication)

        await #expect(throws: CancellationProbeAudioTrack.ProbeError.stopFailed) {
            try await room.localParticipant.unpublish(publication: publication)
        }

        #expect(room.localParticipant.trackPublications[publication.sid] == nil)
        #expect(track.trackState == .started)
        #expect(track.stopAttemptCount.copy() == 2)
        #expect(room.localParticipant.failedPublishTrackCount == 1)

        try await room.localParticipant.stopFailedPublishTracks()

        #expect(track.trackState == .stopped)
        #expect(track.stopAttemptCount.copy() == 3)
        #expect(room.localParticipant.failedPublishTrackCount == 0)
    }
}

private final class CancellationProbeAudioTrack: LocalAudioTrack, @unchecked Sendable {
    enum ProbeError: Error { case stopFailed }

    let stopAttemptCount = StateSync(0)
    private let captureStarted: PublishCancellationGate
    private let releaseStart: PublishCancellationGate
    private let stopFailures: Int

    init(
        captureStarted: PublishCancellationGate,
        releaseStart: PublishCancellationGate,
        stopFailures: Int
    ) {
        self.captureStarted = captureStarted
        self.releaseStart = releaseStart
        self.stopFailures = stopFailures
        let source = RTC.createAudioSource(nil)
        let mediaTrack = RTC.peerConnectionFactory.audioTrack(
            with: source,
            trackId: "cancelled-publication-track"
        )
        super.init(
            name: "cancelled-publication-track",
            source: .microphone,
            track: mediaTrack,
            reportStatistics: false,
            captureOptions: AudioCaptureOptions()
        )
    }

    override func startCapture() async throws {
        captureStarted.open()
        await releaseStart.wait()
    }

    override func stopCapture() async throws {
        let attempt = stopAttemptCount.mutate { count -> Int in
            count += 1
            return count
        }
        if attempt <= stopFailures { throw ProbeError.stopFailed }
    }
}

private final class PublishCancellationGate: @unchecked Sendable {
    private let lock = NSLock()
    private var isOpen = false
    private var waiters: [CheckedContinuation<Void, Never>] = []

    func wait() async {
        await withCheckedContinuation { continuation in
            lock.lock()
            if isOpen {
                lock.unlock()
                continuation.resume()
            } else {
                waiters.append(continuation)
                lock.unlock()
            }
        }
    }

    func open() {
        lock.lock()
        guard !isOpen else {
            lock.unlock()
            return
        }
        isOpen = true
        let pending = waiters
        waiters.removeAll()
        lock.unlock()
        pending.forEach { $0.resume() }
    }
}
