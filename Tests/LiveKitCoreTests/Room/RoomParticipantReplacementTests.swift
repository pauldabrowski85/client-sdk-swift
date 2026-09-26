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

/// A known identity announced under a new SID is a new connection. The connection it replaces
/// must leave the room the way a disconnect does, even when the server never sends that SID's
/// disconnect, or sends it after the replacement when it no longer matches.
@Suite(.tags(.media))
struct RoomParticipantReplacementTests {
    @Test func newSidRetiresTheReplacedConnectionBeforeTheReplacementConnects() async throws {
        let room = Room()
        await room.rpcClient.attach(to: room)
        let identity = Participant.Identity(from: "agent")
        let original = RemoteParticipant(
            info: .with {
                $0.identity = identity.stringValue
                $0.sid = "PA_original"
                $0.tracks = [.with {
                    $0.sid = "TR_original"
                    $0.type = .audio
                    $0.source = .microphone
                }]
            },
            room: room,
            connectionState: .connected
        )
        room._state.mutate {
            $0.connectionState = .connected
            $0.remoteParticipants[identity] = original
        }
        let publication = try #require(original.trackPublications.values.first)
        let recorder = ReplacementRecorder()
        room.add(delegate: recorder)

        await room.signalClient(room.signalClient, didUpdateParticipants: [.with {
            $0.identity = identity.stringValue
            $0.sid = "PA_replacement"
            $0.state = .active
        }])
        await room.delegates.notifyAsync { _ in }

        let replacement = try #require(room.remoteParticipants[identity])
        #expect(replacement !== original)
        #expect(replacement.sid == Participant.Sid(from: "PA_replacement"))
        #expect(original.trackPublications.isEmpty)
        #expect(recorder.events.copy() == [
            .unpublished(ObjectIdentifier(original), publication.sid),
            .disconnected(ObjectIdentifier(original)),
            .connected(ObjectIdentifier(replacement)),
        ])

        // The replaced SID's own disconnect, arriving late, must leave the replacement alone.
        await room.signalClient(room.signalClient, didUpdateParticipants: [.with {
            $0.identity = identity.stringValue
            $0.sid = "PA_original"
            $0.state = .disconnected
        }])
        await room.delegates.notifyAsync { _ in }

        #expect(room.remoteParticipants[identity] === replacement)
        #expect(recorder.events.copy().count == 3)
    }
}

private final class ReplacementRecorder: NSObject, RoomDelegate, @unchecked Sendable {
    enum Event: Equatable {
        case unpublished(ObjectIdentifier, Track.Sid)
        case disconnected(ObjectIdentifier)
        case connected(ObjectIdentifier)
    }

    let events = StateSync<[Event]>([])

    func room(_: Room, participant: RemoteParticipant, didUnpublishTrack publication: RemoteTrackPublication) {
        events.mutate { $0.append(.unpublished(ObjectIdentifier(participant), publication.sid)) }
    }

    func room(_: Room, participantDidDisconnect participant: RemoteParticipant) {
        events.mutate { $0.append(.disconnected(ObjectIdentifier(participant))) }
    }

    func room(_: Room, participantDidConnect participant: RemoteParticipant) {
        events.mutate { $0.append(.connected(ObjectIdentifier(participant))) }
    }
}
