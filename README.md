# MongoDB Oplog Analyzer

A powerful command-line tool for analyzing MongoDB oplog (operations log) entries to identify large operations, track patterns, and monitor database activity in real-time or historically.

## Features

- **Historical Analysis**: Scan oplog entries within specific time ranges
- **Real-time Monitoring**: Tail the oplog as new operations occur
- **Size Threshold Filtering**: Focus on operations above a specified size threshold
- **Statistical Reporting**: Track operation counts, sizes, and patterns
- **ID Statistics**: Analyze frequently accessed document IDs
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
| `-x, --sheet <name>` | Excel sheet name for export | No | - |

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
| `-t, --threshold <bytes>` | Only show operations >= this size | No | MAX_VALUE |
| `-l, --limit <count>` | Stop after examining this many entries | No | unlimited |
| `-d, --dump` | Dump BSON to output file | No | false |
| `--idStats` | Enable _id statistics tracking | No | false |
| `--topIdCount <n>` | Number of top frequent _id values to report | No | 20 |

### Examples

```bash
# Tail oplog in real-time
oplog-analyzer tail -c mongodb://localhost:27017

# Monitor large operations (>500KB) with statistics
oplog-analyzer tail -c mongodb://localhost:27017 \
  -t 512000 \
  --idStats

# Analyze first 10000 operations with ID tracking
oplog-analyzer tail -c mongodb://localhost:27017 \
  -l 10000 \
  --idStats \
  --topIdCount 50

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

When enabled, shows the most frequently accessed document IDs:

```
Top 20 most frequent _id values:
Namespace::_id                                      count       min       max           avg
========================================================================================
mydb.users::507f1f77bcf86cd799439011                 125      1024      4096        2548.5
mydb.orders::507f191e810c19729de860ea                 89       512      8192        3276.8
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
oplog-analyzer tail -c mongodb://localhost:27017 \
  --idStats \
  -t 102400  # 100KB threshold
```

### Capacity Planning
Analyze operation patterns over time:
```bash
oplog-analyzer scan -c mongodb://localhost:27017 \
  -s 2025-01-01T00:00:00Z \
  -e 2025-01-08T00:00:00Z
```

### Hot Document Detection
Find documents that are frequently accessed:
```bash
oplog-analyzer tail -c mongodb://localhost:27017 \
  --idStats \
  --topIdCount 100 \
  -l 100000
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

4. **Performance considerations**
   - Use threshold filtering to reduce output volume
   - Set reasonable time ranges for scan operations
   - Use limits when tailing to prevent memory issues

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