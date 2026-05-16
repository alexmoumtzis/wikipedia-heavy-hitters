# Wikipedia Heavy Hitters

This project benchmarks two major families of streaming heavy hitter algorithms—sketch-based (e.g., Count-Min Sketch, Count Sketch) and insert-only/counter-based (e.g., Misra-Gries, SpaceSaving)—using the Wikimedia pageview dataset. The goal is to evaluate these algorithms under realistic large-scale streaming conditions, focusing on trade-offs between accuracy, memory consumption, throughput, scalability, and distributed mergeability.

The dataset consists of real-time and historical Wikipedia page requests, replayed as a simulated data stream in Apache Spark. This setup enables incremental processing and realistic benchmarking of heavy hitter detection, frequency estimation error, runtime, memory usage, and scalability. Both real and synthetic data distributions are used to assess algorithm robustness and performance under varying conditions.

The project aims to provide a nuanced comparison, highlighting when each algorithmic family is preferable depending on operational constraints and distributed system requirements.
