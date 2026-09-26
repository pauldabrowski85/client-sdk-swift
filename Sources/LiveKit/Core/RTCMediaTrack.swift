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

/// Identifies one native libwebrtc track, independent of the ObjC wrapper that carried it.
///
/// libwebrtc hands out a new `LKRTCMediaStreamTrack` wrapper on every `LKRTCRtpReceiver.track`
/// read, so the wrapper delivered with `didAdd` and the one delivered with `didRemove` are
/// different objects for the same native track, and `===` never matches them. The wrapper's `hash`
/// is the native track pointer (what its `isEqual:` compares), and reading it is an ivar read, not a
/// proxy call, so it is safe on any thread. `trackId` alone is not enough: a resubscription can
/// deliver a new native track with the same id.
///
/// The identity retains the track, so its native pointer cannot be freed and reused by another
/// track while the identity can still be compared: equal identities are the same live track.
struct RTCMediaTrackIdentity: Hashable, Sendable {
    let trackId: String
    private let native: Int
    private let anchor: RTCBox<LKRTCMediaStreamTrack>

    /// Nonisolated: reads only `id()` (a `BYPASS` proxy member) and the wrapper's `hash`.
    init(_ raw: LKRTCMediaStreamTrack) {
        self.init(raw, anchor: RTCBox(raw))
    }

    fileprivate init(_ raw: LKRTCMediaStreamTrack, anchor: RTCBox<LKRTCMediaStreamTrack>) {
        trackId = raw.trackId
        native = raw.hash
        self.anchor = anchor
    }

    static func == (lhs: Self, rhs: Self) -> Bool {
        lhs.native == rhs.native && lhs.trackId == rhs.trackId
    }

    func hash(into hasher: inout Hasher) {
        hasher.combine(native)
        hasher.combine(trackId)
    }
}

/// A `@RTC`-confined handle on an `LKRTCMediaStreamTrack`.
///
/// The raw track is a libwebrtc proxy: `set_enabled`, the renderer sink attach and detach, `volume`
/// and the audio-processing options are all `BlockingCall`s onto WebRTC's signaling or worker
/// thread, and its last release runs a blocking destructor. `LKRTCMediaStreamTrack` is
/// intentionally **not** `Sendable`, so it cannot leave this type; the raw pointer is reachable
/// only through ``raw``, which is `@RTC`-isolated, or ``blocking(_:)``, which is the opt-in hop for
/// the public synchronous APIs that block their caller by contract.
struct RTCMediaTrack: Sendable {
    /// `trackId` and `kind` map to libwebrtc's `id()`/`kind()`, which are thread-safe (`BYPASS`
    /// proxy members). Captured once at construction so reading them later needs no actor hop.
    let trackId: String
    let kind: String
    /// The native track this wraps; see ``RTCMediaTrackIdentity``.
    let identity: RTCMediaTrackIdentity

    private let box: RTCBox<LKRTCMediaStreamTrack>

    /// Nonisolated: remote tracks arrive in peer-connection delegate callbacks on the signaling
    /// thread, and boxing a reference blocks nothing.
    init(_ raw: LKRTCMediaStreamTrack) {
        trackId = raw.trackId
        kind = raw.kind
        let box = RTCBox(raw)
        identity = RTCMediaTrackIdentity(raw, anchor: box)
        self.box = box
    }

    /// The underlying track. `@RTC`-isolated by design — every use is on the RTC executor.
    @RTC var raw: LKRTCMediaStreamTrack { box.value }

    /// Runs `body` with the raw track on the RTC executor, blocking the caller until it returns.
    /// For the public synchronous APIs that document that they block; async code uses ``raw``.
    func blocking<T>(_ body: (LKRTCMediaStreamTrack) throws -> T) rethrows -> T {
        try box.blocking(body)
    }

    /// Runs `body` with the raw track on the calling thread, for the media gates that enable or
    /// silence a remote track while holding a `StateSync` lock, so that a revoked admission can
    /// never race the flip. They must not wait on the serial RTC executor there: work on that
    /// executor takes `StateSync` locks itself (the video publish body reads `Room._state` inside
    /// `RTC.run`), so a lock holder waiting on it inverts the lock order and can deadlock. The raw
    /// track is a libwebrtc proxy that marshals `set_enabled` and the source volume onto WebRTC's
    /// own threads, so the direct call is thread-safe, as every caller made it before 2.17.0.
    func gate<T>(_ body: (LKRTCMediaStreamTrack) throws -> T) rethrows -> T {
        try box.unconfined(body)
    }

    /// Runs `teardown` with the raw track off both the cooperative pool and the RTC executor, for
    /// `deinit` paths whose detach blocks on a WebRTC thread.
    func park(_ teardown: @escaping @Sendable (LKRTCMediaStreamTrack) -> Void) {
        box.park(teardown)
    }
}
