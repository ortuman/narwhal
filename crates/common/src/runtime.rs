// SPDX-License-Identifier: BSD-3-Clause

/// Type alias for runtime task handles.
///
/// This represents a spawned task that can be awaited or detached.
pub type Task = compio::runtime::Task<Result<(), Box<dyn std::any::Any + Send>>, ()>;
