# flink-pcap
Apache Flink PCAP file parser 

The flink job sample in this repository serves as a starting point for developers interested in building custom source connectors for Apache Flink.The code uses Apache Flink 1.20.3 LTS release as the base version on which the code sample is built.
The code monitors a directory for predefined period and parses any PCAP file that lands into the said directory. This has been achieved by extending the FileSource API of Apache Flink. The output is printed to console but can be written to any sink, inbuilt or developed specifically by the user of this codebase.

I would appreciate it if the users can give this repoitory a "Star" if they find it useful and are able to leverage it in their day to day tasks.
