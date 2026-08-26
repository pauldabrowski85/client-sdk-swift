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

@Suite
struct RoomLazyMemberConcurrencyTests {
    @Test func concurrentAccessReturnsTheEagerRoomMembers() {
        let room = Room()
        let observations = StateSync<[[ObjectIdentifier]]>([])

        DispatchQueue.concurrentPerform(iterations: 64) { _ in
            let identities = [
                ObjectIdentifier(room.localParticipant),
                ObjectIdentifier(room.metricsManager),
                ObjectIdentifier(room.subscriberDataChannel),
                ObjectIdentifier(room.publisherDataChannel),
                ObjectIdentifier(room.incomingStreamManager),
                ObjectIdentifier(room.outgoingStreamManager),
                ObjectIdentifier(room.preConnectBuffer),
            ]
            observations.mutate {
                $0.append(identities)
            }
        }

        let identitySnapshots = observations.copy()
        #expect(identitySnapshots.count == 64)
        #expect(identitySnapshots.dropFirst().allSatisfy { $0 == identitySnapshots.first })
    }
}
