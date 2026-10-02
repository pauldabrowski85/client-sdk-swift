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

/// An asynchronous sequence of chunks read from a text data stream.
@objcMembers
public final class TextStreamReader: NSObject, AsyncSequence, Sendable {
    /// Information about the incoming text stream.
    public let info: TextStreamInfo

    let source: StreamReaderSource
    private let trailer: StreamTrailerStorage
    private let cancelSource: @Sendable () async -> Void

    /// Attributes the sender attached to the stream's trailer.
    ///
    /// ``info`` is fixed when the stream opens and holds only the header's
    /// attributes. A sender can add more when it closes the stream (for
    /// example a value it only knows once the content is complete); those
    /// arrive here. Read this after the sequence has finished or ``readAll()``
    /// has returned. Before then, and for a trailer with no attributes, it is
    /// empty. The header's attributes are never changed.
    public var trailerAttributes: [String: String] {
        trailer.attributes
    }

    init(
        info: TextStreamInfo,
        source: StreamReaderSource,
        trailer: StreamTrailerStorage = StreamTrailerStorage(),
        cancelSource: @escaping @Sendable () async -> Void = {}
    ) {
        self.info = info
        self.source = source
        self.trailer = trailer
        self.cancelSource = cancelSource
    }

    /// Reads incoming chunks from the text stream, concatenating them into a single string which is returned
    /// once the stream closes normally.
    ///
    /// - Returns: The string consisting of all concatenated chunks.
    /// - Throws: ``StreamError`` if an error occurs while reading the stream.
    ///
    public func readAll() async throws -> String {
        try await collect()
    }

    func cancel() async {
        await cancelSource()
    }

    /// An asynchronous iterator of incoming chunks.
    public struct AsyncChunks: AsyncIteratorProtocol {
        fileprivate var source: StreamReaderSource.Iterator

        public mutating func next() async throws -> String? {
            guard let data = try await source.next() else {
                return nil
            }
            guard let string = String(data: data, encoding: .utf8) else {
                throw StreamError.decodeFailed
            }
            return string
        }
    }

    public func makeAsyncIterator() -> AsyncChunks {
        AsyncChunks(source: source.makeAsyncIterator())
    }
}

// MARK: - Objective-C compatibility

public extension TextStreamReader {
    @objc
    @available(*, deprecated, message: "Use for/await on TextStreamReader reader instead.")
    func readChunks(onChunk: @Sendable @escaping (String) -> Void, onCompletion: (@Sendable (Error?) -> Void)?) {
        Task {
            do {
                for try await chunk in self {
                    onChunk(chunk)
                }
                onCompletion?(nil)
            } catch {
                onCompletion?(error)
            }
        }
    }
}
