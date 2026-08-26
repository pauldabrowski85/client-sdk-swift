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

struct RemoteAudioPlayoutOwner: Hashable, Sendable {
    let id = UUID()
    let publicationNonce: UUID
    let admissionGeneration: UInt64
    let admissionTokenNonce: UUID
}

struct RemoteAudioLegacyDemandOwner: Hashable, Sendable {
    let id = UUID()
    let publicationNonce: UUID
    let admissionGeneration: UInt64
}

struct RemoteAudioPlayoutDriver: Sendable {
    let acquirePlaybackSession: @Sendable () async throws -> SessionRequirementHandle
    let isPlayoutInitialized: @Sendable () async -> Bool
    let initializePlayout: @Sendable () async throws -> Void
    let isPlaying: @Sendable () async -> Bool
    let isRecording: @Sendable () async -> Bool
    let isEngineRunning: @Sendable () async -> Bool
    let startPlayout: @Sendable () async throws -> Void
    let stopPlayout: @Sendable () async throws -> Void

    static let live = RemoteAudioPlayoutDriver(
        acquirePlaybackSession: {
            try AudioManager.shared.acquireSessionRequirement(.playbackOnly)
        },
        isPlayoutInitialized: {
            RTC.audioDeviceModule.isPlayoutInitialized
        },
        initializePlayout: {
            try AudioManager.shared.checkAdmResult(code: RTC.audioDeviceModule.initPlayout())
        },
        isPlaying: {
            RTC.audioDeviceModule.isPlaying
        },
        isRecording: {
            RTC.audioDeviceModule.isRecording
        },
        isEngineRunning: {
            RTC.audioDeviceModule.isEngineRunning
        },
        startPlayout: {
            try AudioManager.shared.checkAdmResult(code: RTC.audioDeviceModule.startPlayout())
        },
        stopPlayout: {
            try AudioManager.shared.checkAdmResult(code: RTC.audioDeviceModule.stopPlayout())
        }
    )
}

/// Owns the process-global ADM playout lifecycle for protected remote audio.
/// Every exact track admission has a nonce-backed owner. The first owner starts
/// playout and the last exact owner stops it, so one Room cannot tear down ADM
/// while another Room still has admitted audio.
final class RemoteAudioPlayoutCoordinator: @unchecked Sendable {
    static let shared = RemoteAudioPlayoutCoordinator(driver: .live)

    private struct State {
        var activeOwners = Set<RemoteAudioPlayoutOwner>()
        var failedOwners = Set<RemoteAudioPlayoutOwner>()
        var legacyOwners = Set<RemoteAudioLegacyDemandOwner>()
        var quarantineHandlers = [RemoteAudioPlayoutOwner: @Sendable () -> Void]()
        var sessionHandle: SessionRequirementHandle?
        var startedByCoordinator = false
        var mustPreserveRecordingAcrossStop = false
        var quarantineError: LiveKitError?
    }

    private let driver: RemoteAudioPlayoutDriver
    private let operations = SerialRunnerActor<Void>()
    private let _state = StateSync(State())

    init(driver: RemoteAudioPlayoutDriver) {
        self.driver = driver
    }

    func acquire(
        owner: RemoteAudioPlayoutOwner,
        admissionIsCurrent: @escaping @Sendable () -> Bool,
        onGlobalQuarantine: @escaping @Sendable () -> Void
    ) async throws {
        try await operations.run { [weak self] in
            guard let self else { return }
            try await acquireSerialized(
                owner: owner,
                admissionIsCurrent: admissionIsCurrent,
                onGlobalQuarantine: onGlobalQuarantine
            )
        }
    }

    func validate(
        owner: RemoteAudioPlayoutOwner,
        admissionIsCurrent: @escaping @Sendable () -> Bool
    ) async throws {
        try await operations.run { [weak self] in
            guard let self else { return }
            guard admissionIsCurrent(), _state.activeOwners.contains(owner) else {
                throw LiveKitError(.invalidState, message: "Remote-audio playout admission was revoked")
            }
            if let quarantineError = _state.quarantineError { throw quarantineError }
            guard await driver.isPlaying(), await driver.isEngineRunning() else {
                let error = LiveKitError(
                    .audioEngine,
                    message: "Remote-audio playout ownership became ambiguous"
                )
                quarantineAll(with: error)
                throw error
            }
            guard admissionIsCurrent() else {
                throw LiveKitError(.invalidState, message: "Remote-audio playout admission was revoked")
            }
        }
    }

    func release(owner: RemoteAudioPlayoutOwner) async throws {
        try await operations.run { [weak self] in
            guard let self else { return }
            try await releaseSerialized(owner: owner)
        }
    }

    func reserveLegacyDemand(
        owner: RemoteAudioLegacyDemandOwner,
        admissionIsCurrent: @escaping @Sendable () -> Bool
    ) async throws {
        try await operations.run { [weak self] in
            guard let self else { return }
            guard admissionIsCurrent() else {
                throw LiveKitError(.invalidState, message: "Legacy remote-audio subscription was revoked")
            }
            guard _state.activeOwners.isEmpty,
                  _state.failedOwners.isEmpty,
                  _state.quarantineError == nil
            else {
                throw LiveKitError(
                    .invalidState,
                    message: "Legacy remote-audio demand cannot overlap protected playout"
                )
            }
            _state.mutate { $0.legacyOwners.insert(owner) }
        }
    }

    func releaseLegacyDemand(owner: RemoteAudioLegacyDemandOwner) async throws {
        try await operations.run { [weak self] in
            self?._state.mutate { $0.legacyOwners.remove(owner) }
        }
    }

    func validateLegacyDemand(
        owner: RemoteAudioLegacyDemandOwner,
        admissionIsCurrent: @escaping @Sendable () -> Bool
    ) async throws {
        try await operations.run { [weak self] in
            guard let self else { return }
            guard admissionIsCurrent(), _state.legacyOwners.contains(owner) else {
                throw LiveKitError(.invalidState, message: "Legacy remote-audio demand was revoked")
            }
            guard _state.activeOwners.isEmpty, _state.failedOwners.isEmpty else {
                throw LiveKitError(
                    .invalidState,
                    message: "Legacy remote-audio demand cannot overlap protected playout"
                )
            }
        }
    }

    var activeOwnerCount: Int { _state.activeOwners.count }
    var failedOwnerCount: Int { _state.failedOwners.count }
    var legacyOwnerCount: Int { _state.legacyOwners.count }

    private func acquireSerialized(
        owner: RemoteAudioPlayoutOwner,
        admissionIsCurrent: @escaping @Sendable () -> Bool,
        onGlobalQuarantine: @escaping @Sendable () -> Void
    ) async throws {
        guard admissionIsCurrent() else {
            throw LiveKitError(.invalidState, message: "Remote-audio playout admission was revoked")
        }
        if _state.activeOwners.contains(owner) { return }
        if let quarantineError = _state.quarantineError { throw quarantineError }
        guard _state.legacyOwners.isEmpty else {
            throw LiveKitError(
                .invalidState,
                message: "Protected playout cannot begin while legacy remote audio is active"
            )
        }

        if !_state.activeOwners.isEmpty {
            guard await driver.isPlaying(), await driver.isEngineRunning() else {
                let error = LiveKitError(
                    .audioEngine,
                    message: "Active remote-audio owners lost the process-global playout lifecycle"
                )
                quarantineAll(with: error)
                throw error
            }
            guard admissionIsCurrent() else {
                throw LiveKitError(.invalidState, message: "Remote-audio playout admission was revoked")
            }
            _state.mutate { state in
                state.activeOwners.insert(owner)
                state.quarantineHandlers[owner] = onGlobalQuarantine
            }
            return
        }

        let isPlaying = await driver.isPlaying()
        let isRecording = await driver.isRecording()
        let isEngineRunning = await driver.isEngineRunning()
        let isIdle = !isPlaying && !isRecording && !isEngineRunning
        let isRecordingOnly = !isPlaying && isRecording && isEngineRunning
        guard isIdle || isRecordingOnly else {
            throw LiveKitError(
                .audioEngine,
                message: "Protected playout requires a provably idle process-global audio lifecycle"
            )
        }
        let sessionHandle = try await driver.acquirePlaybackSession()
        var claimedPlayoutStart = false

        do {
            guard admissionIsCurrent() else {
                throw LiveKitError(.invalidState, message: "Remote-audio playout admission was revoked")
            }

            if !(await driver.isPlayoutInitialized()) {
                try await driver.initializePlayout()
                guard await driver.isPlayoutInitialized() else {
                    throw LiveKitError(.audioEngine, message: "Audio playout initialization was not acknowledged")
                }
            }

            claimedPlayoutStart = true
            try await driver.startPlayout()

            guard await driver.isPlaying(), await driver.isEngineRunning() else {
                throw LiveKitError(.audioEngine, message: "Audio playout start was not acknowledged")
            }
            guard admissionIsCurrent() else {
                throw LiveKitError(.invalidState, message: "Remote-audio playout admission was revoked during startup")
            }
            try sessionHandle.release()
            guard admissionIsCurrent() else {
                throw LiveKitError(.invalidState, message: "Remote-audio playout admission was revoked during startup")
            }

            _state.mutate { state in
                state.activeOwners.insert(owner)
                state.quarantineHandlers[owner] = onGlobalQuarantine
                state.startedByCoordinator = true
            }
        } catch {
            var playoutStopWasProven = !claimedPlayoutStart
            do {
                if claimedPlayoutStart {
                    try await stopPlayoutAndProveRecordingPreserved()
                    playoutStopWasProven = true
                }
                try sessionHandle.release()
            } catch let rollbackError {
                let quarantineError = LiveKitError(
                    .audioEngine,
                    message: "Unable to prove remote-audio playout rollback: \(rollbackError)"
                )
                _state.mutate { state in
                    state.failedOwners.insert(owner)
                    state.quarantineHandlers[owner] = onGlobalQuarantine
                    state.sessionHandle = sessionHandle
                    state.startedByCoordinator = claimedPlayoutStart && !playoutStopWasProven
                    state.quarantineError = quarantineError
                }
                throw quarantineError
            }
            throw error
        }
    }

    private func releaseSerialized(owner: RemoteAudioPlayoutOwner) async throws {
        let ownership = _state.mutate { state -> (
            wasOwned: Bool,
            shouldStop: Bool,
            handle: SessionRequirementHandle?,
            quarantineHandler: (@Sendable () -> Void)?
        ) in
            let wasActive = state.activeOwners.remove(owner) != nil
            let hadFailed = state.failedOwners.remove(owner) != nil
            let wasOwned = wasActive || hadFailed
            let noOwnersRemain = state.activeOwners.isEmpty && state.failedOwners.isEmpty
            return (
                wasOwned,
                noOwnersRemain && state.startedByCoordinator,
                noOwnersRemain ? state.sessionHandle : nil,
                state.quarantineHandlers[owner]
            )
        }
        guard ownership.wasOwned else { return }

        do {
            if ownership.shouldStop {
                try await stopPlayoutAndProveRecordingPreserved()
                _state.mutate { $0.startedByCoordinator = false }
            }
            try ownership.handle?.release()
            _state.mutate { state in
                state.quarantineHandlers[owner] = nil
                guard state.activeOwners.isEmpty, state.failedOwners.isEmpty else { return }
                if let handle = ownership.handle,
                   state.sessionHandle !== handle
                {
                    return
                }
                state.sessionHandle = nil
                state.startedByCoordinator = false
                state.mustPreserveRecordingAcrossStop = false
                state.quarantineError = nil
                state.quarantineHandlers[owner] = nil
            }
        } catch {
            let quarantineError = LiveKitError(
                .audioEngine,
                message: "Unable to prove remote-audio playout release: \(error)"
            )
            _state.mutate { state in
                state.failedOwners.insert(owner)
                if let quarantineHandler = ownership.quarantineHandler {
                    state.quarantineHandlers[owner] = quarantineHandler
                }
                state.sessionHandle = ownership.handle
                state.quarantineError = quarantineError
            }
            throw quarantineError
        }
    }

    private func stopPlayoutAndProveRecordingPreserved() async throws {
        let recordingBeforeStop = await driver.isRecording()
        _state.mutate {
            $0.mustPreserveRecordingAcrossStop =
                $0.mustPreserveRecordingAcrossStop || recordingBeforeStop
        }
        try await driver.stopPlayout()

        let isPlaying = await driver.isPlaying()
        let isRecording = await driver.isRecording()
        let isEngineRunning = await driver.isEngineRunning()
        let mustPreserveRecording = _state.mustPreserveRecordingAcrossStop
        guard !isPlaying,
              isEngineRunning == isRecording,
              !mustPreserveRecording || isRecording
        else {
            throw LiveKitError(
                .audioEngine,
                message: "Audio playout stop did not preserve the exact pre-stop recording lifecycle"
            )
        }
        _state.mutate { $0.mustPreserveRecordingAcrossStop = false }
    }

    private func quarantineAll(with error: LiveKitError) {
        let handlers = _state.mutate { state -> [@Sendable () -> Void] in
            state.quarantineError = error
            state.failedOwners.formUnion(state.activeOwners)
            state.activeOwners.removeAll()
            return Array(state.quarantineHandlers.values)
        }
        handlers.forEach { $0() }
    }
}
