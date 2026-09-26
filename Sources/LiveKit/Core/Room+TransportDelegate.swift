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

extension LKRTCPeerConnectionState {
    var isConnected: Bool {
        self == .connected
    }

    var isDisconnected: Bool {
        [.disconnected, .failed].contains(self)
    }
}

extension Room: TransportDelegate {
    func transport(_ transport: Transport, didUpdateState pcState: LKRTCPeerConnectionState) {
        let receiveGeneration = transport.dataPacketReceiveGeneration
        guard isCurrentTransport(transport, receiveGeneration: receiveGeneration) else { return }
        log("target: \(transport.target), connectionState: \(pcState.description)")

        let pcError: LiveKitError? = _state.connectionState.isTearingDown ? nil : LiveKitError(
            .network, message: "Transport \(transport.target) state changed to \(pcState.description)",
        )

        // primary connected
        if transport.isPrimary {
            if pcState.isConnected {
                primaryTransportConnectedCompleter.resume(returning: ())
            } else if pcState.isDisconnected {
                primaryTransportConnectedCompleter.reset(throwing: pcError)
            }
        }

        // publisher connected
        if case .publisher = transport.target {
            if pcState.isConnected {
                publisherTransportConnectedCompleter.resume(returning: ())
            } else if pcState.isDisconnected {
                publisherTransportConnectedCompleter.reset(throwing: pcError)
            }
        }

        if _state.connectionState == .connected {
            // Attempt re-connect if primary or publisher transport failed
            if transport.isPrimary || (_state.hasPublished && transport.target == .publisher), pcState.isDisconnected {
                Task {
                    guard self.isCurrentTransport(
                        transport,
                        receiveGeneration: receiveGeneration
                    ) else { return }
                    do {
                        try await startReconnect(reason: .transport)
                    } catch {
                        log("Failed calling startReconnect, error: \(error)", .error)
                    }
                }
            }
        }
    }

    func transport(_ transport: Transport, didGenerateIceCandidate iceCandidate: IceCandidate) {
        let receiveGeneration = transport.dataPacketReceiveGeneration
        guard isCurrentTransport(transport, receiveGeneration: receiveGeneration) else { return }
        Task {
            guard self.isCurrentTransport(
                transport,
                receiveGeneration: receiveGeneration
            ) else { return }
            do {
                log("sending iceCandidate")
                try await signalClient.sendCandidate(candidate: iceCandidate, target: transport.target)
            } catch {
                log("Failed to send iceCandidate, error: \(error)", .error)
            }
        }
    }

    func transport(_ transport: Transport, didAddTrack track: RTCMediaTrack, rtpReceiver: RTCReceiver, streamIds: [String]) {
        // WebRTC delivers remote media enabled. `Transport` silences it on the
        // signaling thread, before the raw track is boxed and before any
        // execution-queue or Task hop can delay subscription admission. The
        // exact admitted publication re-enables the track only inside
        // `activateSubscribedTrack`.
        guard let streamId = streamIds.first else {
            log("Received onTrack with no streams!", .warning)
            return
        }

        let receiveGeneration = transport.dataPacketReceiveGeneration
        guard isCurrentTransport(
            transport,
            receiveGeneration: receiveGeneration,
            asSubscriber: true
        ) else { return }

        // execute block when connected
        execute(when: { state, _ in state.connectionState == .connected },
                // always remove this block when disconnected
                removeWhen: { state, _ in state.connectionState == .disconnected })
        { [weak self] in
            guard let self else { return }
            Task {
                await self.engine(
                    self,
                    didAddTrack: track,
                    rtpReceiver: rtpReceiver,
                    streamId: streamId,
                    sourceTransport: transport,
                    receiveGeneration: receiveGeneration
                )
            }
        }
    }

    func transport(_ transport: Transport, didRemoveTrack track: RTCMediaTrackIdentity) {
        let receiveGeneration = transport.dataPacketReceiveGeneration
        guard isCurrentTransport(
            transport,
            receiveGeneration: receiveGeneration,
            asSubscriber: true
        ) else { return }

        execute(when: { state, _ in state.connectionState == .connected },
                removeWhen: { state, _ in state.connectionState == .disconnected })
        { [weak self] in
            guard let self else { return }
            Task {
                do {
                    try await self.engine(
                        self,
                        didRemoveTrack: track,
                        sourceTransport: transport,
                        receiveGeneration: receiveGeneration
                    )
                } catch {
                    self.log("Failed to retire removed remote track: \(error)", .error)
                    await self.disconnect()
                }
            }
        }
    }

    func transport(_ transport: Transport, didOpenDataChannel dataChannel: LKRTCDataChannel) {
        log("Server opened data channel \(dataChannel.label)(\(dataChannel.readyState))")

        let receiveGeneration = transport.dataPacketReceiveGeneration
        guard isCurrentTransport(
            transport,
            receiveGeneration: receiveGeneration,
            asSubscriber: true
        ) else { return }

        switch dataChannel.label {
        case LKRTCDataChannel.Labels.reliable:
            subscriberDataChannel.set(
                reliable: dataChannel,
                receiveGeneration: receiveGeneration
            )
        case LKRTCDataChannel.Labels.lossy:
            subscriberDataChannel.set(
                lossy: dataChannel,
                receiveGeneration: receiveGeneration
            )
        case LKRTCDataChannel.Labels.dataTrack: dataTracks?.setSubscriberChannel(dataChannel)
        default: log("Unknown data channel label \(dataChannel.label)", .warning)
        }
    }

    func transportShouldNegotiate(_: Transport) {}

    func isCurrentTransport(
        _ transport: Transport,
        receiveGeneration: UInt64,
        asSubscriber: Bool = false
    ) -> Bool {
        guard receiveGeneration == dataPacketReceiveGeneration,
              transport.dataPacketReceiveGeneration == receiveGeneration
        else { return false }
        return _state.read { state in
            guard let current = state.transport else { return false }
            if asSubscriber { return current.subscriber === transport }
            return current.allTransports.contains { $0 === transport }
        }
    }
}
