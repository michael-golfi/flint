# Flint
Welcome to Flint. Flint is a metagenomics profiling pipeline that is built on top of the [Apache Spark]()[1]() framework, and is designed for fast real-time profiling of metagenomic samples against a large collection of reference genomes. Flint takes advantage of Spark's built-in parallelism and streaming engine architecture to quickly map reads against a large reference collection of bacterial genomes.

Our computational framework is primarily implemented using the MapReduce model, and deployed in a cluster launched using the [Elastic Map Reduce]()[2]() service offered by AWS ([Amazon Web Services]()[3]()). The cluster consists of multiple commodity worker machines (computational nodes), and in the current configuration of the cluster that we use, each worker machine consists of 15 GB of RAM, 8 vCPUs (a hyperthread of a single Intel Xeon core), and 100 GB of EBS disk storage. Each of the worker nodes will work in parallel to align the input sequencing DNA reads to a partitioned shard of the reference database; after the alignment step is completed, each worker node acts as a regular Spark executor node.
