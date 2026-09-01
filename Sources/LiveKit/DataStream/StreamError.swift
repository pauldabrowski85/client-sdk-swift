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

public enum StreamError: Error, Equatable, Sendable {
    /// Unable to open a stream with the same ID more than once.
    case alreadyOpened

    /// Stream closed abnormally by remote participant.
    case abnormalEnd(reason: String)

    /// Incoming chunk data could not be decoded.
    case decodeFailed

    /// Read length exceeded total length specified in stream header.
    case lengthExceeded

    /// Stream length exceeded the receiver's configured resource limit.
    case streamSizeExceeded(maximumBytes: Int)

    /// Declared stream length cannot be represented safely on this platform.
    case invalidDeclaredLength

    /// The receiver already has the configured number of streams open.
    case tooManyOpenStreams(maximum: Int)

    /// A suspended handler allowed its bounded chunk buffer to fill.
    case bufferOverflow

    /// The room's bounded incoming packet-event buffer filled.
    case ingressBufferOverflow

    /// Read length less than total length specified in stream header.
    case incomplete

    /// Stream terminated before completion.
    case terminated

    /// Cannot perform operations on an unknown stream.
    case unknownStream

    /// Unable to register a stream handler more than once.
    case handlerAlreadyRegistered

    /// Given destination URL is not a directory.
    case notDirectory

    /// Unable to read information about the file to send.
    case fileInfoUnavailable

    /// Encryption type mismatch between stream header and chunk/trailer.
    case encryptionTypeMismatch(expected: EncryptionType, received: EncryptionType)

    /// A stream fragment was published by a different participant connection
    /// than its header. The payload names the header's sender and the
    /// offending fragment's, as `identity/sid@receiveGeneration`.
    case senderMismatch(expected: String, received: String)
}

public extension StreamError {
    /// Whether this error is a sender mismatch, regardless of the recorded senders.
    var isSenderMismatch: Bool {
        if case .senderMismatch = self { return true }
        return false
    }
}
