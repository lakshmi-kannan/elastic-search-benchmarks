elastic-search-benchmarks
=========================

mvn compile
mvn exec:java -Dexec.mainClass="com.rackspacecloud.esbenchmarks.ESBenchmarker" -Dlog4j.configuration=$ROOT/target/es-log4j.properties
