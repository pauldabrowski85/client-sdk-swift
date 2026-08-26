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

/// Manages a map of AsyncCompleters
actor CompleterMapActor<T: Sendable> {
    // MARK: - Public

    nonisolated let label: String

    // MARK: - Private

    private let _defaultTimeout: TimeInterval
    private var _completerMap = [String: AsyncCompleter<T>]()

    init(label: String, defaultTimeout: TimeInterval) {
        self.label = label
        _defaultTimeout = defaultTimeout
    }

    func completer(for key: String) -> AsyncCompleter<T> {
        // Return completer if already exists...
        if let element = _completerMap[key] {
            return element
        }

        let newCompleter = AsyncCompleter<T>(label: label, defaultTimeout: _defaultTimeout)
        _completerMap[key] = newCompleter
        return newCompleter
    }

    func resume(returning value: T, for key: String) {
        let completer = completer(for: key)
        completer.resume(returning: value)
    }

    func resume(throwing error: any Error, for key: String) {
        guard let completer = _completerMap[key] else { return }
        completer.resume(throwing: error)
    }

    func reset(throwing error: Error? = nil) {
        // Reset call completers...
        for (_, value) in _completerMap {
            value.reset(throwing: error)
        }
        // Clear all completers...
        _completerMap.removeAll()
    }
}

final class AsyncCompleter<T: Sendable>: @unchecked Sendable, Loggable {
    //
    struct WaitEntry {
        let continuation: CheckedContinuation<T, Error>
        let timeoutBlock: DispatchWorkItem

        func cancel(throwing error: LiveKitError? = nil) {
            continuation.resume(throwing: error ?? LiveKitError(.cancelled))
            timeoutBlock.cancel()
        }

        func timeout() {
            continuation.resume(throwing: LiveKitError(.timedOut))
            timeoutBlock.cancel()
        }

        func resume(with result: Result<T, Error>) {
            continuation.resume(with: result)
            timeoutBlock.cancel()
        }
    }

    let label: String

    private let _timerQueue = DispatchQueue(label: "LiveKitSDK.AsyncCompleter", qos: .utility)

    // Internal states
    private var _defaultTimeout: DispatchTimeInterval
    private var _entries: [UUID: WaitEntry] = [:]
    private var _result: Result<T, Error>?
    private var _resetGeneration: UInt64 = 0
    private var _lastResetError = LiveKitError(.cancelled)
    private var _beforeWaitRegistration: (@Sendable () -> Void)?

    private let _lock: some Lock = createLock()

    var waiterCount: Int {
        _lock.sync { _entries.count }
    }

    init(label: String, defaultTimeout: TimeInterval) {
        self.label = label
        _defaultTimeout = defaultTimeout.toDispatchTimeInterval
    }

    deinit {
        reset()
    }

    func set(defaultTimeout: TimeInterval) {
        _lock.sync {
            _defaultTimeout = defaultTimeout.toDispatchTimeInterval
        }
    }

    func setBeforeWaitRegistrationForTests(_ hook: (@Sendable () -> Void)?) {
        _lock.sync { _beforeWaitRegistration = hook }
    }

    func reset(throwing error: Error? = nil) {
        let resetError = LiveKitError.from(error: error) ?? LiveKitError(.cancelled)
        _lock.sync {
            _resetGeneration &+= 1
            _lastResetError = resetError
            for entry in _entries.values {
                entry.cancel(throwing: resetError)
            }
            _entries.removeAll()
            _result = nil
        }
    }

    /// Clears a cached result so future `wait()` calls block again, without cancelling in-flight
    /// waiters — they keep waiting for the next `resume`.
    func rearm() {
        _lock.sync {
            _result = nil
        }
    }

    func resume(with result: Result<T, Error>) {
        _lock.sync {
            if let _result {
                log("\(label) already resolved \(_entries) with \(_result)", .debug)
            }

            for entry in _entries.values {
                entry.resume(with: result)
            }
            _entries.removeAll()
            _result = result
        }
    }

    func resume(returning value: T) {
        resume(with: .success(value))
    }

    func resume(throwing error: Error) {
        log("\(label)", .error)
        resume(with: .failure(error))
    }

    func wait(timeout: TimeInterval? = nil) async throws -> T {
        let initialState = _lock.sync {
            (_result, _resetGeneration, _beforeWaitRegistration)
        }
        if let result = initialState.0 {
            // Already resolved...
            if case let .success(value) = result {
                // resume(returning:) already called
                return value
            } else if case let .failure(error) = result {
                // resume(throwing:) already called
                log("\(label) throwing existing error")
                throw error
            }
        }

        initialState.2?()

        // Create ids for continuation & timeoutBlock
        let entryId = UUID()
        let cancellationObserved = StateSync(false)

        // Create a cancel-aware timed continuation
        return try await withTaskCancellationHandler {
            try await withCheckedThrowingContinuation { continuation in
                // Create time-out block
                let timeoutBlock = DispatchWorkItem { [weak self] in
                    guard let self else { return }
                    log("\(label) id: \(entryId) timed out")
                    _lock.sync {
                        if let entry = self._entries[entryId] {
                            entry.timeout()
                        }
                        self._entries.removeValue(forKey: entryId)
                    }
                }

                let immediateResult: Result<T, Error>? = _lock.sync {
                    if _resetGeneration != initialState.1 {
                        return .failure(_lastResetError)
                    }
                    if let result = _result { return result }
                    if cancellationObserved.copy() {
                        return .failure(LiveKitError(.cancelled))
                    }

                    let computedTimeout = timeout?.toDispatchTimeInterval ?? _defaultTimeout
                    _timerQueue.asyncAfter(deadline: .now() + computedTimeout, execute: timeoutBlock)
                    _entries[entryId] = WaitEntry(continuation: continuation, timeoutBlock: timeoutBlock)
                    log("\(label) id: \(entryId) waiting for \(computedTimeout)")
                    return nil
                }
                if let immediateResult { continuation.resume(with: immediateResult) }
            }
        } onCancel: {
            cancellationObserved.mutate { $0 = true }
            // Cancel only this completer when Task gets cancelled
            _lock.sync {
                if let entry = self._entries[entryId] {
                    entry.cancel()
                }
                self._entries.removeValue(forKey: entryId)
            }
        }
    }
}
