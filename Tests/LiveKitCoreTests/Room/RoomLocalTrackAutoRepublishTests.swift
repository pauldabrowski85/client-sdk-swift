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
import Testing

struct RoomLocalTrackAutoRepublishTests {
    @Test func automaticFullReconnectRepublishDefaultsOn() {
        #expect(RoomOptions().autoRepublishLocalTracksOnFullReconnect)
    }

    @Test func automaticFullReconnectRepublishCanBeDisabled() {
        let options = RoomOptions(autoRepublishLocalTracksOnFullReconnect: false)

        #expect(!options.autoRepublishLocalTracksOnFullReconnect)
    }

    @Test func disabledAutomaticRepublishLeavesRetainedTrackUntouched() async throws {
        let room = Room(roomOptions: RoomOptions(
            stopLocalTrackOnUnpublish: false,
            autoRepublishLocalTracksOnFullReconnect: false
        ))
        let publication = await installMutedRetainedTrack(in: room)
        var oldState = room._state.copy()
        oldState.connectionState = .reconnecting
        oldState.isReconnectingWithMode = .full
        var connectedState = oldState
        connectedState.connectionState = .connected

        room.engine(room, didMutateState: connectedState, oldState: oldState)
        try await Task.sleep(nanoseconds: 100_000_000)

        #expect(room.localParticipant.trackPublications[publication.sid] === publication)
    }

    @Test func roomMoveCannotRepublishBeforeFreshAdmission() async throws {
        let room = Room(roomOptions: RoomOptions(
            stopLocalTrackOnUnpublish: false,
            autoRepublishLocalTracksOnFullReconnect: false
        ))
        let publication = await installMutedRetainedTrack(in: room)

        await room.signalClient(
            room.signalClient,
            didReceiveRoomMoved: Livekit_RoomMovedResponse()
        )
        try await Task.sleep(nanoseconds: 100_000_000)

        #expect(room.localParticipant.trackPublications[publication.sid] === publication)
    }

    @Test func roomMoveQuarantinesSurvivingTransportsInsteadOfRelabelingThem() async throws {
        let room = Room(roomOptions: RoomOptions(
            autoRepublishLocalTracksOnFullReconnect: false
        ))
        let oldGeneration = room.dataPacketReceiveGeneration
        let connection = ConnectionDependencies(
            room: room,
            roomOptions: room._state.roomOptions
        )
        let join = try await JoinDependencies.make(
            room: room,
            connection: connection,
            joinResponse: .with { $0.subscriberPrimary = true },
            rtcConfiguration: .liveKitDefault(),
            singlePeerConnection: false
        )
        let publisher = join.transport.publisher
        let subscriber = join.transport.subscriber
        room._state.mutate {
            $0.stage = .connected(join)
        }

        await room.signalClient(
            room.signalClient,
            didReceiveRoomMoved: Livekit_RoomMovedResponse()
        )

        #expect(room.dataPacketReceiveGeneration == oldGeneration + 1)
        #expect(publisher.dataPacketReceiveGeneration == oldGeneration)
        #expect(subscriber.dataPacketReceiveGeneration == oldGeneration)
        #expect(room._state.transport == join.transport)
        await join.transport.close()
    }

    private func installMutedRetainedTrack(in room: Room) async -> LocalTrackPublication {
        let track = await LocalAudioTrack.createTrack(name: "retained-microphone")
        track.set(muted: true, notify: false)
        let publication = LocalTrackPublication(
            info: .with {
                $0.sid = "TR_retained"
                $0.name = "retained-microphone"
                $0.type = .audio
                $0.source = .microphone
            },
            participant: room.localParticipant
        )
        await publication.set(track: track)
        room.localParticipant.add(publication: publication)
        return publication
    }
}
