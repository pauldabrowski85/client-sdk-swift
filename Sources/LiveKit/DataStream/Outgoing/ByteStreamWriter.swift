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

/// Asynchronously write to an open byte stream.
///
/// - Important: Call and await ``close(reason:)`` when you are done. Releasing a writer without
///   closing it does not send the end of the stream, so receivers keep waiting for it until you
///   disconnect.
@objcMembers
public final class ByteStreamWriter: NSObject, Sendable {
    /// Information about the outgoing byte stream.
    public let info: ByteStreamInfo

    private let writer: LiveKitFFI.ByteStreamWriter

    /// What this stream's packets are sent with; it also keeps the cause of a failed delivery.
    private let context: OutgoingStreamSendContext?

    /// Whether or not the stream is still open. Reflects the FFI writer's state, so it becomes
    /// `false` once the stream is closed locally or a send fails (e.g. the room disconnected).
    public var isOpen: Bool {
        get async { await writer.isOpen() }
    }

    /// Write data to the stream.
    ///
    /// - Parameter data: Data to be sent.
    /// - Throws: ``StreamError/alreadyClosed`` if the stream is closed, or
    ///   ``StreamError/sendFailed`` or another ``StreamError`` if the data cannot be delivered, for
    ///   example because the room disconnected. Then ``isOpen`` is `false`. `CancellationError` at
    ///   once if the calling task is cancelled, but the write can still go out.
    ///
    public func write(_ data: Data) async throws {
        try checkNotCancelled()
        do {
            try await abandoningOnCancel { [writer] in try await writer.write(data: data) }
        } catch let error as LiveKitFFI.DataStreamError {
            throw context?.sendFailure(for: error) ?? StreamError(error)
        }
    }

    /// Close the stream.
    ///
    /// - Parameter reason: A textual description of why the stream is being closed. Absense
    ///   of a reason indicates a normal closure.
    /// - Throws: ``StreamError/alreadyClosed`` if the stream is already closed, or another
    ///   ``StreamError`` if the end of the stream cannot be delivered. `CancellationError` at once
    ///   if the calling task is cancelled, but the stream can still close.
    ///
    public func close(reason: String? = nil) async throws {
        try checkNotCancelled()
        do {
            try await abandoningOnCancel { [writer] in
                if let reason {
                    try await writer.closeWithReason(reason: reason)
                } else {
                    try await writer.close()
                }
            }
        } catch let error as LiveKitFFI.DataStreamError {
            throw context?.sendFailure(for: error) ?? StreamError(error)
        }
    }

    init(_ writer: LiveKitFFI.ByteStreamWriter, encryptionType: EncryptionType, context: OutgoingStreamSendContext? = nil) {
        self.writer = writer
        self.context = context
        info = ByteStreamInfo(writer.info(), encryptionType: encryptionType)
    }

    /// A cancelled stream gave its ID back. The core does not know, so what it would still send
    /// could leave with the context of a later stream that took the ID.
    private func checkNotCancelled() throws {
        if context?.isCancelled == true { throw StreamError.terminated }
    }
}
