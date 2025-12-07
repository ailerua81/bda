# Performance Study

## Shuffle Partitions Experiment

### Partitions: 8
- Execution time: 1.66s

### Partitions: 50
- Execution time: 0.82s

### Partitions: 200
- Execution time: 0.98s


## Pairs vs Stripes Trade-offs

### Pairs Approach
- **Pros**: Simple implementation, easy to understand
- **Cons**: More shuffling overhead (two passes needed)

### Stripes Approach
- **Pros**: Reduced shuffling (one pass), better memory locality
- **Cons**: More complex reducer logic, potential memory issues with large vocabularies


## Execution Plan (Pairs Approach)

```
== Physical Plan ==
* Scan ExistingRDD (1)


(1) Scan ExistingRDD [codegen id : 1]
Output [4]: [w1#63, w2#64, rel_freq#65, count#66L]
Arguments: [w1#63, w2#64, rel_freq#65, count#66L], MapPartitionsRDD[41] at applySchemaToPythonRDD at NativeMethodAccessorImpl.java:0, ExistingRDD, UnknownPartitioning(0)



```
