# Contributing to Narwhal

Thank you for your interest in contributing to Narwhal! We welcome contributions from everyone and appreciate your effort to make this project better.

## Table of Contents

- [Code of Conduct](#code-of-conduct)
- [Getting Started](#getting-started)
- [Development Setup](#development-setup)
- [Building and Testing](#building-and-testing)
- [Making Changes](#making-changes)
- [Submitting a Pull Request](#submitting-a-pull-request)
- [Reporting Issues](#reporting-issues)
- [Code Style](#code-style)
- [Getting Help](#getting-help)

## Code of Conduct

This project adheres to a [Code of Conduct](CODE_OF_CONDUCT.md). By participating, you are expected to uphold this code. Please report unacceptable behavior to ortuman@gmail.com.

## Getting Started

There are many ways to contribute to Narwhal:

- **Report bugs**: If you find a bug, please open an issue
- **Suggest features**: Have an idea? We'd love to hear it
- **Improve documentation**: Help make our docs clearer and more comprehensive
- **Write code**: Fix bugs or implement new features
- **Review pull requests**: Help review and improve contributions from others

## Development Setup

### Prerequisites

- Rust (latest stable version recommended)
- Cargo (comes with Rust)
- Git

### Clone the Repository

```bash
git clone https://github.com/narwhal-io/narwhal.git
cd narwhal
```

### Install Dependencies

Cargo will automatically handle dependencies when you build the project.

## Building and Testing

### Building the Project

To build all workspace crates:

```bash
cargo build
```

To build in release mode:

```bash
cargo build --release
```

### Running Tests

Run all tests:

```bash
cargo test
```

Run tests for a specific crate:

```bash
cargo test -p <crate-name>
```

For example:

```bash
cargo test -p narwhal-server
```

### Running Clippy

We use Clippy for additional linting:

```bash
cargo clippy --all-targets --all-features -- -D warnings
```

### Checking Documentation

Build and check documentation:

```bash
cargo doc --no-deps --all-features
```

## Making Changes

### Creating a Branch

Create a new branch for your changes:

```bash
git checkout -b feature/your-feature-name
```

or for bug fixes:

```bash
git checkout -b fix/issue-description
```

### Writing Code

- Write clear, readable code
- Add tests for new functionality
- Update documentation as needed
- Ensure your code compiles without warnings
- Follow the existing code style

### Commit Messages

Write clear, descriptive commit messages:

- Use the present tense ("Add feature" not "Added feature")
- Use the imperative mood ("Move cursor to position X" not "Moves cursor to position X")
- Limit the first line to 72 characters or less
- Reference issues and pull requests when relevant

Example:
```
Add support for custom configuration files

- Implement config file parsing
- Add tests for new functionality
- Update documentation

Fixes #123
```

## Submitting a Pull Request

1. **Push your changes** to your fork:
   ```bash
   git push origin your-branch-name
   ```

2. **Open a Pull Request** on GitHub with:
   - A clear title and description
   - Reference to any related issues
   - Description of the changes made
   - Any breaking changes highlighted

3. **Ensure CI passes**: The automated checks must pass before merging

4. **Respond to feedback**: Be prepared to make changes based on review comments

5. **Keep it focused**: Try to keep PRs focused on a single feature or fix

## Reporting Issues

When reporting issues, please include:

- **Description**: Clear description of the problem
- **Steps to reproduce**: Detailed steps to reproduce the issue
- **Expected behavior**: What you expected to happen
- **Actual behavior**: What actually happened
- **Environment**: OS, Rust version, and any other relevant details
- **Logs**: Any relevant error messages or logs

### Bug Report Template

```
**Description**
A clear and concise description of the bug.

**To Reproduce**
Steps to reproduce the behavior:
1. ...
2. ...
3. ...

**Expected Behavior**
What you expected to happen.

**Actual Behavior**
What actually happened.

**Environment**
- OS: [e.g., macOS, Linux, Windows]
- Rust version: [output of `rustc --version`]
- Narwhal version/commit: [e.g., v0.1.0 or commit hash]

**Additional Context**
Add any other context about the problem here.
```

## Code Style

### Rust Style

- Follow the [Rust API Guidelines](https://rust-lang.github.io/api-guidelines/)
- Format your code with `rustfmt`:
  ```bash
  cargo fmt --all
  ```
- The project includes a `.rustfmt.toml` configuration file

### Documentation

- Document all public APIs with doc comments
- Include examples in doc comments where appropriate
- Use `///` for item documentation
- Use `//!` for module-level documentation

Example:
```rust
/// Processes a connection request.
///
/// # Arguments
///
/// * `request` - The incoming connection request
///
/// # Returns
///
/// Returns `Ok(())` on success, or an error if processing fails.
///
/// # Examples
///
/// ```
/// let result = process_request(request);
/// assert!(result.is_ok());
/// ```
pub fn process_request(request: Request) -> Result<()> {
    // Implementation
}
```

## Getting Help

If you need help or have questions:

- **Open a discussion**: Use GitHub Discussions for questions and ideas
- **Join the community**: Check if there's a chat room or forum
- **Read the docs**: Check the documentation in the `docs/` directory
- **Look at examples**: The `examples/` directory contains usage examples

## Recognition

Contributors who have their pull requests merged will be recognized in the project. We appreciate all contributions, big and small!

---

Thank you for contributing to Narwhal! 🎉