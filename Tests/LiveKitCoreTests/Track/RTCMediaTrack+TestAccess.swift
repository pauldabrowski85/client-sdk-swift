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

/// Test-only synchronous access to the enabled flag of an RTC-confined track.
///
/// Since #1101 the raw `LKRTCMediaStreamTrack` is reachable only on the RTC executor. These go
/// through ``RTCMediaTrack/blocking(_:)``, the SDK's own hop for synchronous callers, so an
/// assertion reads the native track's state rather than a copy.
extension RTCMediaTrack {
    var isEnabledForTesting: Bool {
        blocking { $0.isEnabled }
    }

    func setEnabledForTesting(_ isEnabled: Bool) {
        blocking { $0.isEnabled = isEnabled }
    }
}
