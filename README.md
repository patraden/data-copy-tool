### Data Copy Tool
Simple file copy utility leveraging akka reactive streams in its core.
At the moment it implements single scenario of copying parquet files to PostgreSQL table.
In this scenario it re-uses full functionality of spark sql parquet readers (with partitioning support etc) as streaming source.
And efficient and fast PostgreSQL copy command for streaming sink.

### Prerequisites
JRE 1.8

### Usage
java -jar data-copy-tool-assembly-0.1.1-dev.jar --help