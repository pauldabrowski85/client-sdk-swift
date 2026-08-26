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

@Suite(.tags(.dataChannel))
struct RoomUserDataProvenanceTests {
    @Test func deprecatedInnerIdentityCannotSpoofUserDataPublisher() async throws {
        let room = Room()
        let authentic = installRemoteParticipant(
            in: room,
            identity: "authentic",
            sid: "PA_authentic"
        )
        let imposter = installRemoteParticipant(
            in: room,
            identity: "imposter",
            sid: "PA_imposter"
        )
        let recorder = UserDataRecorder()
        room.add(delegate: recorder)
        authentic.add(delegate: recorder)
        imposter.add(delegate: recorder)

        let packet = Livekit_DataPacket.with {
            $0.participantIdentity = "authentic"
            $0.participantSid = "PA_authentic"
            $0.user = Livekit_UserPacket.with {
                $0.participantIdentity = "imposter"
                $0.participantSid = "PA_imposter"
                $0.payload = Data("payload".utf8)
                $0.topic = "topic"
            }
        }
        room.dataChannel(
            MockDataChannelPair { _ in },
            didReceiveDataPacket: packet,
            encryptionType: .none,
            receiveGeneration: room.dataPacketReceiveGeneration,
            receivedAtContinuousTimeNanoseconds: RpcContinuousClock.nowNanoseconds()
        )

        await room.delegates.notifyAsync { _ in }
        await authentic.delegates.notifyAsync { _ in }
        await imposter.delegates.notifyAsync { _ in }

        let roomParticipants = recorder.roomParticipants.copy()
        let participantSenders = recorder.participantSenders.copy()
        #expect(roomParticipants.count == 1)
        let observedRoomParticipant = try #require(roomParticipants.first)
        let roomParticipant = try #require(observedRoomParticipant)
        #expect(roomParticipant === authentic)
        #expect(participantSenders.count == 1)
        #expect(participantSenders.first === authentic)
    }

    @Test func queuedUserDataCannotReachSameIdentityAndSidReplacement() async {
        let room = Room()
        let identity = Participant.Identity(from: "same-agent")
        let sid = Participant.Sid(from: "PA_reused")
        let original = installRemoteParticipant(
            in: room,
            identity: identity.stringValue,
            sid: sid.stringValue
        )
        let recorder = UserDataRecorder()
        room.add(delegate: recorder)
        original.add(delegate: recorder)

        let roomQueueGate = DelegateQueueGate()
        let participantQueueGate = DelegateQueueGate()
        room.delegates.notify { _ in roomQueueGate.block() }
        original.delegates.notify { _ in participantQueueGate.block() }
        await roomQueueGate.waitUntilBlocked()
        await participantQueueGate.waitUntilBlocked()

        let oldGeneration = room.dataPacketReceiveGeneration
        let packet = Livekit_DataPacket.with {
            $0.participantIdentity = identity.stringValue
            $0.participantSid = sid.stringValue
            $0.user = Livekit_UserPacket.with {
                $0.participantIdentity = identity.stringValue
                $0.participantSid = sid.stringValue
                $0.payload = Data("stale".utf8)
            }
        }
        room.dataChannel(
            MockDataChannelPair { _ in },
            didReceiveDataPacket: packet,
            encryptionType: .none,
            receiveGeneration: oldGeneration,
            receivedAtContinuousTimeNanoseconds: RpcContinuousClock.nowNanoseconds()
        )

        _ = room.incrementDataPacketReceiveGeneration()
        let replacement = RemoteParticipant(
            info: .with {
                $0.identity = identity.stringValue
                $0.sid = sid.stringValue
            },
            room: room,
            connectionState: .connected
        )
        room._state.mutate { $0.remoteParticipants[identity] = replacement }

        #expect(replacement !== original)
        #expect(replacement.dataPacketReceiveGeneration > oldGeneration)
        roomQueueGate.open()
        participantQueueGate.open()
        await room.delegates.notifyAsync { _ in }
        await original.delegates.notifyAsync { _ in }

        #expect(roomQueueGate.wasReleasedBeforeTimeout)
        #expect(participantQueueGate.wasReleasedBeforeTimeout)
        #expect(recorder.roomParticipants.copy().isEmpty)
        #expect(recorder.participantSenders.copy().isEmpty)
    }
}

private func installRemoteParticipant(
    in room: Room,
    identity: String,
    sid: String
) -> RemoteParticipant {
    let participant = RemoteParticipant(
        info: .with {
            $0.identity = identity
            $0.sid = sid
        },
        room: room,
        connectionState: .connected
    )
    room._state.mutate {
        $0.connectionState = .connected
        $0.remoteParticipants[Participant.Identity(from: identity)] = participant
    }
    return participant
}

private final class UserDataRecorder: NSObject, RoomDelegate, ParticipantDelegate, @unchecked Sendable {
    let roomParticipants = StateSync<[RemoteParticipant?]>([])
    let participantSenders = StateSync<[RemoteParticipant]>([])

    func room(
        _: Room,
        participant: RemoteParticipant?,
        didReceiveData _: Data,
        forTopic _: String,
        encryptionType _: EncryptionType
    ) {
        roomParticipants.mutate { $0.append(participant) }
    }

    func participant(
        _ participant: RemoteParticipant,
        didReceiveData _: Data,
        forTopic _: String,
        encryptionType _: EncryptionType
    ) {
        participantSenders.mutate { $0.append(participant) }
    }
}

private final class DelegateQueueGate: @unchecked Sendable {
    private let shouldBlock = StateSync(true)
    private let releasedBeforeTimeout = StateSync<Bool?>(nil)
    private let release = DispatchSemaphore(value: 0)
    private let enteredStream: AsyncStream<Void>
    private let enteredContinuation: AsyncStream<Void>.Continuation

    init() {
        (enteredStream, enteredContinuation) = AsyncStream.makeStream(of: Void.self)
    }

    func block() {
        let admitted = shouldBlock.mutate { shouldBlock -> Bool in
            defer { shouldBlock = false }
            return shouldBlock
        }
        guard admitted else { return }
        enteredContinuation.yield()
        enteredContinuation.finish()
        let result = release.wait(timeout: .now() + 5)
        releasedBeforeTimeout.mutate { $0 = result == .success }
    }

    func waitUntilBlocked() async {
        for await _ in enteredStream { return }
    }

    func open() {
        release.signal()
    }

    var wasReleasedBeforeTimeout: Bool {
        releasedBeforeTimeout.copy() == true
    }
}
