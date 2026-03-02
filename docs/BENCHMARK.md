# Benchmark Results

This document contains performance benchmarks for Narwhal's pub/sub messaging system. All benchmarks were conducted using the `narwhal-bench` tool included in the project.

## Test Environment

### Hardware

- **CPU**: Intel(R) Core(TM) Ultra 9 275HX (24 cores)
- **Memory**: 32 GB

### Configuration

- **Build Configuration**: Release mode (`--release`)
- **Server**: Local Narwhal instance (127.0.0.1:22622)
- **Test Duration**: 60 seconds per run
- **Producers**: 1
- **Consumers**: 1
- **Channels**: 1

## Benchmark Tool

The benchmark tool is located in the `crates/benchmark` directory and can be built and run as follows:

```bash
# Build the benchmark tool
cargo build --release -p narwhal-benchmark

# Run a benchmark
./target/release/narwhal-bench \
  --server 127.0.0.1:22622 \
  --producers 1 \
  --consumers 1 \
  --duration 1m \
  --max-payload-size 256
```

### Available Options

- `-s, --server <ADDR>`: Server address to connect to (default: 127.0.0.1:22622)
- `-p, --producers <N>`: Number of producer clients (default: 1)
- `-c, --consumers <N>`: Number of consumer clients (default: 10)
- `-n, --channels <N>`: Number of channels to create (default: 1)
- `-d, --duration <TIME>`: Duration to run the benchmark (supports: 30s, 5m, 1h)
- `--max-payload-size <BYTES>`: Maximum size of message payload in bytes (default: 16384)
- `-w, --worker-threads <N>`: Number of worker threads to use (default: 0, auto-detect)

## Results

### Performance Overview Table

All benchmarks were run with 1 producer and 1 consumer for 60 seconds.

| Payload Size | Throughput (msg/s) | Data Throughput | Mean Latency | P50 | P95 | P99 | Avg Total Messages | Runs used |
|--------------|-------------------:|----------------:|-------------:|----:|----:|----:|-------------------:|----------:|
| 256 B        |             93,026 |       23.8 MB/s |         1.07ms |   1.11ms |   1.20ms |   1.33ms |      5,600,142 | 4/5 |
| 512 B        |             92,684 |       47.5 MB/s |         1.08ms |   1.01ms |   1.25ms |   1.34ms |      5,579,668 | 4/5 |
| 1 KB         |             90,407 |       92.6 MB/s |         1.10ms |   1.10ms |   1.17ms |   1.30ms |      5,441,121 | 4/5 |
| 4 KB         |             83,150 |      340.6 MB/s |         1.20ms |   1.19ms |   1.27ms |   1.41ms |      5,004,638 | 4/5 |
| 8 KB         |             72,954 |      597.6 MB/s |         1.36ms |   1.36ms |   1.46ms |   1.69ms |      4,391,013 | 4/5 |
| 16 KB        |             59,675 |      977.7 MB/s |         1.67ms |   1.66ms |   1.78ms |   1.96ms |      3,591,555 | 4/5 |
| 32 KB        |             40,202 |       1.32 GB/s |         2.48ms |   2.47ms |   2.67ms |   3.01ms |      2,419,756 | 4/5 |
| 64 KB        |             24,315 |       1.59 GB/s |         4.10ms |   4.08ms |   4.50ms |   5.15ms |      1,463,814 | 4/5 |
| 128 KB       |             13,750 |       1.80 GB/s |         7.25ms |   7.18ms |   8.08ms |   9.67ms |        828,198 | 4/5 |

### Visual Performance Analysis

#### Message Throughput

![Message Throughput vs Payload Size](benchmark_msg_throughput.png)

This graph shows how message throughput (messages per second) varies with payload size. Error bars and individual run points reflect the 4 kept runs. Peak performance is achieved with small payloads, delivering over 93,026 messages per second on average.

#### Latency

![Message Latency vs Payload Size](benchmark_latency.png)

This graph compares Mean, P50, P95, and P99 latency across different payload sizes. Latency scales gradually with payload size, with P99 latency remaining in the single-digit millisecond range even at 128KB payloads.

#### Data Throughput (Bandwidth)

![Data Throughput vs Payload Size](benchmark_data_throughput.png)

This graph illustrates the actual data throughput. While message rate decreases with larger payloads, the overall bandwidth increases significantly, peaking at 1.80 GB/s with 128 KB payloads.

### Key Observations

1. **Peak Throughput**: Achieved with 256 B payloads at ~93,026 messages/second
2. **Consistent Low Latency**: Sub-2ms mean latency maintained across small to medium payload sizes (256B - 8KB)
3. **Payload Size Impact**: As payload size increases, message throughput decreases while data throughput increases, which is expected due to larger data transfers
4. **Latency Stability**: P99 latency remains very low across all payload sizes, indicating consistent performance
5. **Zero Errors**: All benchmarks completed without any message loss or errors
6. **Peak Bandwidth**: 1.80 GB/s achieved at 128 KB payload

## Notes

- All benchmarks were run on a local machine with the server and client on the same host
- Network latency in production environments will add to the observed latency values
- The benchmark tool generates random payloads up to the specified maximum size
- Results may vary based on hardware, OS, and system load
