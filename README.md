# MongoDB Oplog Analyzer

A powerful command-line tool for analyzing MongoDB oplog (operations log) entries to identify large operations, track patterns, and monitor database activity in real-time or historically.

## Features

- **Historical Analysis**: Scan oplog entries within specific time ranges
- **Real-time Monitoring**: Tail the oplog as new operations occur
- **Size Threshold Filtering**: Focus on operations above a specified size threshold
- **Statistical Reporting**: Track operation counts, sizes, and patterns
- **ID Statistics**: Analyze frequently accessed document IDs with flexible thresholds
- **Batch Document Size Fetching**: Get accurate document sizes for update operations
- **Sharded Cluster Support**: Multi-threaded analysis across all shards simultaneously
- **BSON Export**: Export oplog entries to BSON format for further analysis

## Installation

### Prerequisites

- Java 8 or later
- Maven (for building from source)
- MongoDB replica set or sharded cluster with replication/oplog enabled

### Quick Install

```bash
# Clone the repository
git clone https://github.com/yourusername/oplog-analyzer.git
cd oplog-analyzer

# Run the install script
./install.sh
```

The install script will:
- Check Java installation
- Build the project if needed
- Install the `oplog-analyzer` command to `~/.local/bin`
- Configure your PATH if necessary

### Manual Build

```bash
# Build the project
mvn clean package

# The JAR will be created in bin/OplogAnalyzer.jar
java -jar bin/OplogAnalyzer.jar --help
```

## Usage

Oplog Analyzer provides two main commands for different analysis scenarios:

### Command Structure

```bash
oplog-analyzer <subcommand> [options]
```

### Available Subcommands

- `scan` - Analyze historical oplog entries within a time range
- `tail` - Monitor oplog entries in real-time

### Global Options

- `--help` - Show help message
- `--version` - Show version information

## Scan Command (Historical Analysis)

Analyze oplog entries between specific timestamps for retrospective analysis.

### Syntax

```bash
oplog-analyzer scan [options]
```

### Options

| Option | Description | Required | Default |
|--------|-------------|----------|---------|
| `-c, --uri <uri>` | MongoDB connection string | Yes | - |
| `-t, --threshold <bytes>` | Only show operations >= this size | No | MAX_VALUE |
| `-s, --startTime <timestamp>` | Start time (ISO 8601 format) | No | - |
| `-e, --endTime <timestamp>` | End time (ISO 8601 format) | No | - |
| `-d, --db <database>` | Database name to query | No | local |

### Examples

```bash
# Analyze oplog for the last 24 hours
oplog-analyzer scan -c mongodb://localhost:27017 \
  -s 2025-01-01T00:00:00Z \
  -e 2025-01-02T00:00:00Z

# Find large operations (>1MB) in the past hour
oplog-analyzer scan -c mongodb://localhost:27017 \
  -t 1048576 \
  -s 2025-01-01T12:00:00Z \
  -e 2025-01-01T13:00:00Z

# Analyze specific database operations
oplog-analyzer scan -c mongodb://localhost:27017 \
  -d myDatabase \
  -s 2025-01-01T00:00:00Z
```

## Tail Command (Real-time Monitoring)

Monitor oplog entries as they occur in real-time, useful for debugging and monitoring active operations.

### Syntax

```bash
oplog-analyzer tail [options]
```

### Options

| Option | Description | Required | Default |
|--------|-------------|----------|---------|
| `-c, --uri <uri>` | MongoDB connection string | Yes | - |
| `-t, --threshold <bytes>` | Only show operations >= this size for debug output | No | MAX_VALUE |
| `-l, --limit <count>` | Stop after examining this many entries | No | unlimited |
| `-d, --dump` | Dump BSON to output file | No | false |
| `--idStats` | Enable _id statistics tracking | No | false |
| `--idStatsThreshold <bytes>` | Minimum size for documents to include in ID statistics | No | 0 |
| `--fetchDocSizes` | Fetch actual document sizes for updates (slower but accurate) | No | false |
| `--topIdCount <n>` | Number of top frequent _id values to report | No | 20 |
| `--shardIndex <indices>` | Comma-separated shard indices to analyze (e.g., 0,1,2) | No | all shards |

### Examples

```bash
# Tail oplog in real-time
oplog-analyzer tail -c mongodb://localhost:27017

# Monitor large operations (>500KB) with basic ID statistics
oplog-analyzer tail -c mongodb://localhost:27017 \
  -t 512000 \
  --idStats

# Track all document IDs (default behavior)
oplog-analyzer tail -c mongodb://localhost:27017 \
  --idStats \
  --topIdCount 50

# Track only large documents in ID statistics
oplog-analyzer tail -c mongodb://localhost:27017 \
  --idStats \
  --idStatsThreshold 100000 \
  --topIdCount 20

# Get accurate document sizes for updates (slower but precise)
oplog-analyzer tail -c mongodb://localhost:27017 \
  --idStats \
  --fetchDocSizes \
  --idStatsThreshold 50000

# Different thresholds for debug vs statistics
oplog-analyzer tail -c mongodb://localhost:27017 \
  --threshold 1048576 \
  --idStats \
  --idStatsThreshold 10000 \
  --fetchDocSizes

# Analyze specific shards in a sharded cluster
oplog-analyzer tail -c mongodb+srv://user:pass@cluster.net/ \
  --shardIndex 0,2 \
  --idStats

# Dump oplog entries to BSON file
oplog-analyzer tail -c mongodb://localhost:27017 \
  -d \
  -l 5000
```

## Output Format

### Operation Summary

Both commands produce a summary table showing:

```
========== OPLOG ANALYSIS REPORT ==========
Namespace::Operation                                        Count  Total Size       Avg Size
========================================================================================
mydb.users::insert                                           245    15234567         62181
mydb.orders::update                                          189     8456789         44750
mydb.products::delete                                         45     1234567         27434
POCDB.POCCOLL::u                                           68032        258         17552256
POCDB.POCCOLL::i                                           88660       1616        143314980
```

Where:
- **Namespace::Operation**: Database.collection::operation_type (u=update, i=insert, d=delete, c=command)
- **Count**: Number of operations of this type  
- **Total Size**: Cumulative size of all operations in bytes
- **Avg Size**: Average operation size in bytes

### ID Statistics (tail command with --idStats)

When enabled, shows the most frequently accessed document IDs. The output format depends on whether `--fetchDocSizes` is used:

#### Basic Mode (fast, oplog sizes only)
```
Top 20 most frequent _id values:
Namespace::_id                             Count   Avg Oplog Size
=============================================================================
mydb.users::507f1f77bcf86cd799439011         125         2 KB
mydb.orders::507f191e810c19729de860ea         89         1 KB

Notes:
- Count: Total operations on this _id (inserts + updates + deletes)
- Avg Oplog Size: Average oplog entry size (small for updates/deletes, full for inserts)  
- ID Statistics Threshold: 0 bytes (only documents >= this size are tracked)
- Use --fetchDocSizes option to see actual document sizes (slower but more accurate)
```

#### Accurate Mode (with --fetchDocSizes)
```
Top 20 most frequent _id values:
Namespace::_id                             Count    Avg Doc Size   Avg Oplog Size  Doc/Oplog Ratio
========================================================================================================
mydb.users::507f1f77bcf86cd799439011         125          2 MB           2 KB         1000.0x
mydb.orders::507f191e810c19729de860ea         89         150 KB           1 KB          150.0x

Notes:
- Count: Total operations on this _id (inserts + updates + deletes)
- Avg Oplog Size: Average oplog entry size (small for updates/deletes, full for inserts)
- ID Statistics Threshold: 50000 bytes (only documents >= this size are tracked)  
- Avg Doc Size: Average actual document size (excludes deletes where size is unknown)
- Doc/Oplog Ratio: Higher ratio indicates documents much larger than their oplog entries
```

## Connection String Examples

```bash
# Local MongoDB
mongodb://localhost:27017

# MongoDB with authentication
mongodb://username:password@localhost:27017/admin

# Replica set
mongodb://host1:27017,host2:27017,host3:27017/?replicaSet=rs0

# MongoDB Atlas
mongodb+srv://username:password@cluster.mongodb.net/

# With additional options
mongodb://localhost:27017/?readPreference=secondaryPreferred&ssl=true
```

## Advanced Features

### Sharded Cluster Analysis

The oplog analyzer automatically detects sharded clusters and analyzes all shards simultaneously using multi-threading:

```bash
# Automatic shard detection and parallel analysis
oplog-analyzer tail -c mongodb+srv://user:pass@cluster.mongodb.net/

# Analyze specific shards only
oplog-analyzer tail -c mongodb+srv://user:pass@cluster.mongodb.net/ \
  --shardIndex 0,2,4
```

When connected to a sharded cluster, you'll see output like:
```
Detected sharded cluster - tailing all shards simultaneously
Tailing 4 shards in parallel
[atlas-shard-0] Processed 1,234 entries, Lag: 0s
[atlas-shard-1] Processed 1,456 entries, Lag: 0s
...
```

### Two-Threshold System

The tool provides separate thresholds for different purposes:

1. **Debug Threshold (`--threshold`)**: Controls which operations are displayed in real-time
2. **Statistics Threshold (`--idStatsThreshold`)**: Controls which documents are included in ID statistics

```bash
# Show debug messages for operations >1MB, but track all documents in statistics
oplog-analyzer tail -c mongodb://localhost:27017 \
  --threshold 1048576 \
  --idStats \
  --idStatsThreshold 0

# Track only large documents in both debug and statistics  
oplog-analyzer tail -c mongodb://localhost:27017 \
  --threshold 500000 \
  --idStats \
  --idStatsThreshold 500000
```

### Document Size Accuracy Modes

#### Fast Mode (Default)
- Uses oplog entry sizes only
- No additional database queries
- Good for identifying update patterns
- Misleading for update operations (shows small oplog entry size, not actual document size)

#### Accurate Mode (`--fetchDocSizes`)
- Fetches actual document sizes via batch queries
- Slower but provides true document sizes for updates
- Batches queries (max 10 docs, 100ms timeout) for efficiency
- Essential for accurate size-based analysis

```bash
# Fast mode - good for pattern analysis
oplog-analyzer tail -c mongodb://localhost:27017 \
  --idStats

# Accurate mode - essential for size analysis  
oplog-analyzer tail -c mongodb://localhost:27017 \
  --idStats \
  --fetchDocSizes
```

## Use Cases

### Performance Analysis
Identify operations that consume excessive resources:
```bash
oplog-analyzer scan -c mongodb://localhost:27017 \
  -t 10485760 \  # 10MB threshold
  -s 2025-01-01T00:00:00Z \
  -e 2025-01-02T00:00:00Z
```

### Real-time Debugging
Monitor operations as they happen during application testing:
```bash
# Fast debugging - track update patterns
oplog-analyzer tail -c mongodb://localhost:27017 \
  --idStats \
  --threshold 102400

# Accurate debugging - get true document sizes  
oplog-analyzer tail -c mongodb://localhost:27017 \
  --idStats \
  --fetchDocSizes \
  --threshold 102400
```

### Capacity Planning
Analyze operation patterns over time:
```bash
# Historical analysis
oplog-analyzer scan -c mongodb://localhost:27017 \
  -s 2025-01-01T00:00:00Z \
  -e 2025-01-08T00:00:00Z

# Real-time capacity monitoring with accurate sizes
oplog-analyzer tail -c mongodb://localhost:27017 \
  --idStats \
  --fetchDocSizes \
  --idStatsThreshold 10000 \
  -l 50000
```

### Hot Document Detection
Find documents that are frequently accessed:
```bash
# Track all document access patterns (fast)
oplog-analyzer tail -c mongodb://localhost:27017 \
  --idStats \
  --topIdCount 100 \
  -l 100000

# Track only large document access patterns (accurate)
oplog-analyzer tail -c mongodb://localhost:27017 \
  --idStats \
  --fetchDocSizes \
  --idStatsThreshold 50000 \
  --topIdCount 50 \
  -l 100000
```

### Sharded Cluster Monitoring
Monitor operations across a sharded cluster:
```bash
# Monitor all shards
oplog-analyzer tail -c mongodb+srv://user:pass@cluster.net/ \
  --idStats \
  --fetchDocSizes

# Monitor specific shards only
oplog-analyzer tail -c mongodb+srv://user:pass@cluster.net/ \
  --shardIndex 0,2 \
  --idStats
```

## Understanding the Output

- **Namespace**: Database and collection name (e.g., `mydb.users`)
- **Operation**: Type of operation (`insert`, `update`, `delete`, `command`)
- **Count**: Number of operations of this type
- **Total Size**: Cumulative size of all operations (in bytes)
- **Avg Size**: Average operation size (in bytes)
- **Min/Max**: Minimum and maximum operation sizes (when using --idStats)

## Troubleshooting

### Common Issues

1. **"Cannot connect to MongoDB"**
   - Verify your connection string
   - Ensure MongoDB is running
   - Check network connectivity and firewall rules

2. **"No oplog entries found"**
   - Ensure MongoDB is running as a replica set
   - Verify the time range contains operations
   - Check that the oplog hasn't rolled over

3. **"Java not found"**
   - Install Java 8 or later
   - Ensure Java is in your PATH

4. **"No ID statistics appear"**
   - Ensure you're using `--idStats` option
   - Check `--idStatsThreshold` - default is 0 (tracks all documents)
   - Verify operations are occurring that exceed your threshold
   - Use `--topIdCount` to increase the number of results shown

5. **Performance considerations**
   - Use `--threshold` to reduce debug output volume
   - Use `--idStatsThreshold` to limit statistics collection 
   - Avoid `--fetchDocSizes` for high-volume environments unless needed
   - Set reasonable time ranges for scan operations
   - Use `--limit` when tailing to prevent memory issues
   - For sharded clusters, use `--shardIndex` to analyze specific shards if needed

## Building from Source

```bash
# Clone the repository
git clone https://github.com/yourusername/oplog-analyzer.git
cd oplog-analyzer

# Build with Maven
mvn clean package

# Run directly
java -jar bin/OplogAnalyzer.jar --help

# Or install
./install.sh
```

## Requirements

- **Java**: Version 8 or later
- **MongoDB**: Version 3.0 or later with oplog enabled
- **Memory**: Minimum 512MB, recommended 2GB+ for large datasets
- **Disk Space**: For BSON dumps, ensure adequate space

## Uninstalling

To remove the installed command:

```bash
./uninstall.sh
```

## Contributing

Contributions are welcome! Please feel free to submit issues and pull requests.

## License

[Specify your license here]

## Authors

- [Your name/organization]

## Acknowledgments

- MongoDB Java Driver team
- Picocli for excellent command-line interface support
- Caffeine cache library for efficient statistics tracking