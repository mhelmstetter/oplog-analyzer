# oplog-analyzer
MongoDB oplog analysis utility. Reads the MongoDB oplog up until the last operation and prints the count, min, max, average, and total bson size (in bytes) of each oplog entry by the namespace and op type. This can be useful to gather a better understanding of the write patterns of a given application, and also to analyze and troubleshoot excessive oplog usage.

# TailingOplogAnalyzer
`TailingOplogAnalyzer` is the recommened tool from this package to use, which may work better for very large/active oplogs. Rather than scanning the entire oplog, it
will tail the oplog when the program starts until the program is ended using CTRL-C. Before exiting, the program will print the final report.
## Usage
```
 -c,--uri <connection uri>                         mongodb connection
                                                   string uri
 -d,--dump                                         dump bson to output
                                                   file (mongodump format)
 -l,--limit <limit>                                only examine limit
                                                   number of oplog entries
 -t,--threshold <log ops size threshold (bytes)>   log operations >= this
                                                   size
```
* Example:
    `java -cp OplogAnalyzer.jar com.mongodb.oploganalyzer.TailingOplogAnalyzer -c mongodb://localhost:27017`

# OplogAnalyzer
* OplogAnalyzer.jar is included in the bin directory, or build the jar using `mvn package`
* To run:
    `java -jar OplogAnalyzer.jar -c mongodb://localhost:27017`

Optionally, start and end times can be specified:
    `java -cp OplogAnalyzer.jar com.mongodb.oploganalyzer.TailingOplogAnalyzer -c mongodb://localhost:27017 -s "2023-01-19T14:00:00.000Z" -e "2023-01-19T14:30:00.000Z"`

# Build
mvn package

# Sample Output
```
Namespace                                                  op      count        min        max        avg                total
foo.bar                                                     u     119402        258        258        258             30805716
foo.bar                                                     i     119643        471        512        498             59628050
foo.$cmd                                                    c          1        258        258        258                  258
POCDB.POCCOLL                                               u      68032        258        258        258             17552256
POCDB.POCCOLL                                               i      88660       1562       1659       1616            143314980
```
