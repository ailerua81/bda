# Environment Summary

## Versions
- Python: 3.10.19
- Spark: 4.0.1
- PySpark: 4.0.1
- Java: openjdk version "21.0.6-internal" 2025-01-21
- OS: Linux-6.6.87.2-microsoft-standard-WSL2-x86_64-with-glibc2.39

## Spark Configuration
- spark.app.id = local-1764271127599
- spark.app.name = BDA-Assignment-Relational-Streaming
- spark.app.startTime = 1764271126377
- spark.app.submitTime = 1764271125831
- spark.driver.extraJavaOptions = -Djava.net.preferIPv6Addresses=false -XX:+IgnoreUnrecognizedVMOptions --add-modules=jdk.incubator.vector --add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.lang.invoke=ALL-UNNAMED --add-opens=java.base/java.lang.reflect=ALL-UNNAMED --add-opens=java.base/java.io=ALL-UNNAMED --add-opens=java.base/java.net=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/java.util=ALL-UNNAMED --add-opens=java.base/java.util.concurrent=ALL-UNNAMED --add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED --add-opens=java.base/jdk.internal.ref=ALL-UNNAMED --add-opens=java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/sun.nio.cs=ALL-UNNAMED --add-opens=java.base/sun.security.action=ALL-UNNAMED --add-opens=java.base/sun.util.calendar=ALL-UNNAMED --add-opens=java.security.jgss/sun.security.krb5=ALL-UNNAMED -Djdk.reflect.useDirectMethodHandle=false -Dio.netty.tryReflectionSetAccessible=true
- spark.driver.host = 10.255.255.254
- spark.driver.port = 41721
- spark.executor.extraJavaOptions = -Djava.net.preferIPv6Addresses=false -XX:+IgnoreUnrecognizedVMOptions --add-modules=jdk.incubator.vector --add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.lang.invoke=ALL-UNNAMED --add-opens=java.base/java.lang.reflect=ALL-UNNAMED --add-opens=java.base/java.io=ALL-UNNAMED --add-opens=java.base/java.net=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/java.util=ALL-UNNAMED --add-opens=java.base/java.util.concurrent=ALL-UNNAMED --add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED --add-opens=java.base/jdk.internal.ref=ALL-UNNAMED --add-opens=java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/sun.nio.cs=ALL-UNNAMED --add-opens=java.base/sun.security.action=ALL-UNNAMED --add-opens=java.base/sun.util.calendar=ALL-UNNAMED --add-opens=java.security.jgss/sun.security.krb5=ALL-UNNAMED -Djdk.reflect.useDirectMethodHandle=false -Dio.netty.tryReflectionSetAccessible=true
- spark.executor.id = driver
- spark.hadoop.fs.s3a.vectored.read.max.merged.size = 2M
- spark.hadoop.fs.s3a.vectored.read.min.seek.size = 128K
- spark.master = local[*]
- spark.rdd.compress = True
- spark.serializer.objectStreamReset = 100
- spark.sql.artifact.isolation.enabled = false
- spark.sql.session.timeZone = UTC
- spark.sql.shuffle.partitions = 4
- spark.sql.warehouse.dir = file:/home/aurel/bda_labs/bda_assignment04/spark-warehouse
- spark.submit.deployMode = client
- spark.submit.pyFiles = 
- spark.ui.showConsoleProgress = true

## Execution Commands

### Running the complete assignment:
```bash
spark-submit --master local[*] bda_assignment.py
```

### Individual query execution:
```bash
# Q1 with text format
spark-submit --master local[*] bda_assignment.py --query q1 --format text --date 1995-03-15

# Q1 with parquet format
spark-submit --master local[*] bda_assignment.py --query q1 --format parquet --date 1995-03-15
```

## Output Structure

```
outputs/
├── q1_results.txt
├── q2_clerks.txt
├── q3_parts_suppliers.txt
├── q4_nation_shipments.txt
├── q5_monthly_volumes.txt
├── q6_pricing_summary.txt
├── q7_shipping_priority.txt
├── hourly_trip_count/
├── region_trip_count/
└── trending_arrivals/

proof/
├── plan_parquet_queries.txt
└── streaming_evidence.txt

checkpoints/
├── hourly_trip_count/
├── region_trip_count/
└── trending_arrivals/
```

## Notes

### Part A - Relational Queries
- All queries implemented using RDD-only operations
- Text vs Parquet comparison shows parquet is more efficient
- Broadcast joins used for small dimension tables (part, supplier, customer, nation)
- Reduce-side joins used for large-large joins (lineitem ⨝ orders)
- Mixed join strategy for optimal performance in Q4

### Part B - Streaming Queries
- All streaming queries use Structured Streaming API
- Watermarks configured with 5-minute delay
- Trigger mode: once=True for batch-like execution
- Output modes: update for aggregations
- Checkpointing enabled for fault tolerance

### Performance Observations
- Parquet format provides better compression and columnar access
- Broadcast joins significantly faster than reduce-side for small tables
- Streaming queries handle late data with watermarks
- 10-minute windows capture meaningful traffic patterns
