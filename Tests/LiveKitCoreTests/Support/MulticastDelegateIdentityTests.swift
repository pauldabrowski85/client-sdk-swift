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
import Testing

/// Delegates are told apart by pointer. A publication hashes its whole state behind its own lock,
/// so a table that asked for that hash under the table's lock could deadlock against a thread
/// holding the publication's lock.
@Suite struct MulticastDelegateIdentityTests {
    private final class StatefulDelegate: NSObject, @unchecked Sendable {
        let state = StateSync(0)
        let hashCalls = StateSync(0)

        override var hash: Int {
            hashCalls.mutate { $0 += 1 }
            return state.copy()
        }

        override func isEqual(_ object: Any?) -> Bool {
            hashCalls.mutate { $0 += 1 }
            return (object as? StatefulDelegate)?.state.copy() == state.copy()
        }
    }

    @Test func tableNeverAsksADelegateForItsHash() {
        let delegates = MulticastDelegate<StatefulDelegate>(label: "identity")
        let delegate = StatefulDelegate()

        delegates.add(delegate: delegate)
        _ = delegates.allDelegates
        delegates.remove(delegate: delegate)

        #expect(delegate.hashCalls.copy() == 0)
    }

    @Test func equalDelegatesAreBothKept() {
        let delegates = MulticastDelegate<StatefulDelegate>(label: "identity")
        let (first, second) = (StatefulDelegate(), StatefulDelegate())

        delegates.add(delegate: first)
        delegates.add(delegate: second)

        #expect(delegates.allDelegates.count == 2)
    }
}
