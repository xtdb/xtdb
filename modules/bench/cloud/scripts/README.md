# Benchmark Tasks

Babashka scripts for processing and summarizing XTDB benchmark logs.

## Usage

### Summarize Benchmark Logs

```bash
# Table format (default)
bb summarize-log tpch path/to/benchmark.log

# Slack format (with code blocks)
bb summarize-log --format slack tpch path/to/benchmark.log

# GitHub markdown format
bb summarize-log --format github yakbench path/to/benchmark.log
```
**Supported benchmark types:**
- `tpch` - TPC-H benchmark logs
- `yakbench` - YakBench profile logs

**Supported output formats:**
- `table` - Human-readable ASCII table (default)
- `slack` - Slack-formatted code blocks
- `github` - GitHub-flavored markdown table

### Run Tests

```bash
bb test
```
