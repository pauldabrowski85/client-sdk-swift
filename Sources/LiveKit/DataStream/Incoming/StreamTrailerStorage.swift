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

/// Holds what the sender put on a data stream's trailer.
///
/// The stream header is immutable and fixed when the reader is created, but the
/// protocol lets the sender attach more attributes to the trailer
/// (`DataStream.Trailer.attributes`). The JS and Rust SDKs merge those into the
/// stream info when the stream closes; this storage carries them to the reader
/// so ``TextStreamReader/trailerAttributes`` and
/// ``ByteStreamReader/trailerAttributes`` can expose them.
///
/// The manager records the attributes *before* it finishes the reader's
/// source, so a consumer that has seen the sequence end (or `readAll()` return)
/// always observes them.
final class StreamTrailerStorage: Sendable {
    private let state = StateSync<[String: String]>([:])

    init() {}

    /// The attributes from the trailer, or an empty dictionary when the
    /// stream has not closed yet or the trailer carried none.
    var attributes: [String: String] {
        state.copy()
    }

    /// Records the trailer's attributes. Called once by the manager when the
    /// trailer arrives from the stream's own sender.
    func record(_ attributes: [String: String]) {
        state.mutate { $0 = attributes }
    }
}
