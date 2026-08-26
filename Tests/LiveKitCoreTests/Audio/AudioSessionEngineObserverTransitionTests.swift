#if os(iOS) || os(visionOS) || os(tvOS)

import AVFoundation
@testable import LiveKit
import Testing

@Suite(.serialized, .tags(.audio))
struct AudioSessionEngineObserverTransitionTests {
    @Test func downstreamDisableObservesPlaybackThenNoneAfterSDKTransition() throws {
        let transitions = StateSync(0)
        let observer = AudioSessionEngineObserver { _, _ in
            transitions.mutate { $0 += 1 }
        }
        let downstream = AudioSessionTransitionSpy(observer: observer)
        observer.next = downstream
        let engine = AVAudioEngine()

        #expect(observer.engineWillEnable(
            engine,
            isPlayoutEnabled: true,
            isRecordingEnabled: true
        ) == 0)
        #expect(matches(observer.effectiveSessionConfiguration, .playAndRecordSpeaker))

        #expect(observer.engineDidDisable(
            engine,
            isPlayoutEnabled: true,
            isRecordingEnabled: false
        ) == 0)
        #expect(matches(downstream.observedConfigurations.last ?? nil, .playback))

        #expect(observer.engineDidDisable(
            engine,
            isPlayoutEnabled: false,
            isRecordingEnabled: false
        ) == 0)
        #expect(downstream.observedConfigurations.count == 2)
        #expect(downstream.observedConfigurations[1] == nil)
        #expect(observer.effectiveSessionConfiguration == nil)
        #expect(transitions.copy() == 3)
    }

    @Test func redundantExternalRequirementDoesNotReconfigureEffectiveState() throws {
        let transitions = StateSync(0)
        let observer = AudioSessionEngineObserver { _, _ in
            transitions.mutate { $0 += 1 }
        }
        let engine = AVAudioEngine()

        #expect(observer.engineWillEnable(
            engine,
            isPlayoutEnabled: true,
            isRecordingEnabled: false
        ) == 0)
        #expect(transitions.copy() == 1)

        let handle = try observer.acquire(requirement: .playbackOnly)
        #expect(transitions.copy() == 1)
        try handle.release()
        #expect(transitions.copy() == 1)
        #expect(matches(observer.effectiveSessionConfiguration, .playback))
    }
}

private final class AudioSessionTransitionSpy: AudioEngineObserver, @unchecked Sendable {
    weak var observer: AudioSessionEngineObserver?
    var next: (any AudioEngineObserver)?
    private let configurations = StateSync<[AudioSessionConfiguration?]>([])

    init(observer: AudioSessionEngineObserver) {
        self.observer = observer
    }

    var observedConfigurations: [AudioSessionConfiguration?] { configurations.copy() }

    func engineDidDisable(
        _: AVAudioEngine,
        isPlayoutEnabled _: Bool,
        isRecordingEnabled _: Bool
    ) -> Int {
        configurations.mutate { $0.append(observer?.effectiveSessionConfiguration) }
        return 0
    }
}

private func matches(
    _ lhs: AudioSessionConfiguration?,
    _ rhs: AudioSessionConfiguration
) -> Bool {
    lhs?.isEqual(rhs) == true
}

#endif
