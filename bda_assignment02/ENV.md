# Environment Configuration

## Java Version
```
openjdk version "21.0.6-internal" 2025-01-21
OpenJDK Runtime Environment (build 21.0.6-internal-adhoc.conda.src)
OpenJDK 64-Bit Server VM (build 21.0.6-internal-adhoc.conda.src, mixed mode, sharing)

```

## Spark Configuration

- `spark.app.id`: local-1763910217319
- `spark.app.name`: BDA-02
- `spark.app.startTime`: 1763910215811
- `spark.app.submitTime`: 1763910214991
- `spark.driver.extraJavaOptions`: -Djava.net.preferIPv6Addresses=false -XX:+IgnoreUnrecognizedVMOptions --add-modules=jdk.incubator.vector --add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.lang.invoke=ALL-UNNAMED --add-opens=java.base/java.lang.reflect=ALL-UNNAMED --add-opens=java.base/java.io=ALL-UNNAMED --add-opens=java.base/java.net=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/java.util=ALL-UNNAMED --add-opens=java.base/java.util.concurrent=ALL-UNNAMED --add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED --add-opens=java.base/jdk.internal.ref=ALL-UNNAMED --add-opens=java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/sun.nio.cs=ALL-UNNAMED --add-opens=java.base/sun.security.action=ALL-UNNAMED --add-opens=java.base/sun.util.calendar=ALL-UNNAMED --add-opens=java.security.jgss/sun.security.krb5=ALL-UNNAMED -Djdk.reflect.useDirectMethodHandle=false -Dio.netty.tryReflectionSetAccessible=true
- `spark.driver.host`: 10.255.255.254
- `spark.driver.port`: 45267
- `spark.executor.extraJavaOptions`: -Djava.net.preferIPv6Addresses=false -XX:+IgnoreUnrecognizedVMOptions --add-modules=jdk.incubator.vector --add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.lang.invoke=ALL-UNNAMED --add-opens=java.base/java.lang.reflect=ALL-UNNAMED --add-opens=java.base/java.io=ALL-UNNAMED --add-opens=java.base/java.net=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/java.util=ALL-UNNAMED --add-opens=java.base/java.util.concurrent=ALL-UNNAMED --add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED --add-opens=java.base/jdk.internal.ref=ALL-UNNAMED --add-opens=java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/sun.nio.cs=ALL-UNNAMED --add-opens=java.base/sun.security.action=ALL-UNNAMED --add-opens=java.base/sun.util.calendar=ALL-UNNAMED --add-opens=java.security.jgss/sun.security.krb5=ALL-UNNAMED -Djdk.reflect.useDirectMethodHandle=false -Dio.netty.tryReflectionSetAccessible=true
- `spark.executor.id`: driver
- `spark.hadoop.fs.s3a.vectored.read.max.merged.size`: 2M
- `spark.hadoop.fs.s3a.vectored.read.min.seek.size`: 128K
- `spark.master`: local[*]
- `spark.rdd.compress`: True
- `spark.serializer.objectStreamReset`: 100
- `spark.sql.artifact.isolation.enabled`: false
- `spark.sql.session.timeZone`: GMT+1
- `spark.sql.shuffle.partitions`: 4
- `spark.sql.warehouse.dir`: file:/home/aurel/bda_labs/bda_assignment02/spark-warehouse
- `spark.submit.deployMode`: client
- `spark.submit.pyFiles`: 
- `spark.ui.showConsoleProgress`: true

## System Information

- **OS**: Linux 6.6.87.2-microsoft-standard-WSL2
- **Python**: 3.10.19
- **Architecture**: x86_64
