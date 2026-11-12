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

internal import LiveKitFFI

/// Stops an outgoing stream whose header may not have gone out yet.
struct OutgoingStreamCancellation: Sendable {
    private let action: @Sendable () -> Void

    init(action: @escaping @Sendable () -> Void) {
        self.action = action
    }

    func cancel() async {
        action()
    }
}

/// What every packet of one outgoing stream is sent with.
///
/// The core builds a stream's packets, but it does not know which transport the stream was opened
/// on. The data channel's send generation is recorded here when the stream opens, and each packet
/// is then sent only while that generation is current, so a stream opened before a reconnect
/// cannot continue on the transport that replaced it.
final class OutgoingStreamSendContext: Sendable {
    let sendGeneration: UInt64
    let admission: DataChannelSendAdmission?

    private struct State {
        var isCancelled = false
        var failure: Error?
    }

    private let state = StateSync(State())

    init(sendGeneration: UInt64, admission: DataChannelSendAdmission?) {
        self.sendGeneration = sendGeneration
        self.admission = admission
    }

    var isCancelled: Bool { state.read { $0.isCancelled } }

    /// The error that stopped this stream's packets. The core reports every delivery failure as
    /// one error; this keeps the cause for the caller.
    var failure: Error? { state.read { $0.failure } }

    func cancel() {
        state.mutate { $0.isCancelled = true }
    }

    func fail(_ error: Error) {
        state.mutate { if $0.failure == nil { $0.failure = error } }
    }

    /// The error to report for a failed send or write on this stream.
    func sendFailure(for error: LiveKitFFI.DataStreamError) -> Error {
        if case .SendFailed = error, let failure { return failure }
        return StreamError(error)
    }
}

/// A step of an outgoing stream after its header.
enum OutgoingStreamOperationKind: Equatable, Sendable {
    case write
    case close
}

/// The send contexts of a connection's open outgoing streams, by stream ID.
final class OutgoingStreamSendContexts: Sendable {
    private let contexts = StateSync<[String: OutgoingStreamSendContext]>([:])

    /// Called before a chunk or a trailer is sent. For tests that change the room between two
    /// packets of one stream.
    let operationObserver = StateSync<(@Sendable (OutgoingStreamOperationKind) -> Void)?>(nil)

    var count: Int { contexts.read { $0.count } }

    func open(_ streamID: String, _ context: OutgoingStreamSendContext) throws {
        try contexts.mutate {
            guard $0[streamID] == nil else { throw StreamError.alreadyOpened }
            $0[streamID] = context
        }
    }

    func context(for streamID: String) -> OutgoingStreamSendContext? {
        contexts.read { $0[streamID] }
    }

    /// Removes `context`, unless a later stream has taken its ID.
    func close(_ streamID: String, _ context: OutgoingStreamSendContext) {
        contexts.mutate { if $0[streamID] === context { $0[streamID] = nil } }
    }

    func isOpen(_ streamID: String, _ context: OutgoingStreamSendContext) -> Bool {
        contexts.read { $0[streamID] === context }
    }
}
