| Config | maxOffsetsPerTrigger | shuffle.partitions | trigger (s) | backlog (events) | throughput (events/s) | latency p50 (s) | latency p95 (s) |

|---|---|---|---|---|---|---|---|

| A | 1,000 | 8 | 5 | 20,000 | 94.6 | 52.4 | 72.3 |

| B | 10,000 | 8 | 5 | 20,000 | 376.7 | 32.0 | 44.4 |

| C | 1,000 | 200 | 5 | 2,000 \* | 4.5 | n/a \*\* | n/a \*\* |

| D | 10,000 | 200 | 5 | 2,000 \* | 9.1 | n/a \*\* | n/a \*\* |

| E | 10,000 | 8 | 2 | 20,000 | 367.0 | 31.1 | 43.6 |



\\\* Reduced backlog: with 20,000 events, config C drained only 1,994 rows in 600 s.

\\\*\\\* No alert observed in the measurement window (0 alerts measured): the job could not keep up with the 100 events/s steady load.

