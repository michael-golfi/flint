"""
    spark.py contains functions for pipeline workflow
"""

from pyspark.streaming import StreamingContext

from common.logging import getLogger
from common.profile_sample import profile_sample
from common.run_config import (
    AbundanceAccumulator, BowtieConfig, FlintConfig, AbundanceTracker)


class FlintExecutor:
    def dispatch_stream_from_dir(self, stream_source_dir: str,
                                 sampleID,
                                 sample_format,
                                 output_file,
                                 save_to_local,
                                 save_to_s3,
                                 partition_size,
                                 ssc: StreamingContext,
                                 sensitive_align,
                                 annotations_dictionary,
                                 s3_output_bucket: str,
                                 number_of_shards: int,
                                 keep_shard_profiles,
                                 coalesce_output,
                                 sample_type,
                                 verbose_output,
                                 debug_mode,
                                 streaming_timeout):
        """
        Executes the requested Spark job in the cluster
        using a streaming strategy.

        Args:
            stream_source_dir:
                The directory to stream files from.
            number_of_shards:
                The number of shards that
                we'll be picking up from 'stream_source_dir'.
            keep_shard_profiles:
                Retains the rolling shard profiles in S3
                or the local filesystem.
            sampleID:
                The unique id of the sample.
            sample_format:
                What type of input format are
                the reads in (tab5, fastq, tab6, etc.).
            sample_type:
                Are the reads single-end or paired-end.
            output_file:
                The path to the output file.
            save_to_s3:
                Flag for storing output to AWS S3.
            save_to_local:
                Flag for storing output to the local filesystem.
            partition_size:
                Level of parallelization for RDDs
                that are not partitioned by the system.
            ssc:
                Spark Streaming Context.
            sensitive_align:        Sensitive Alignment Mode.
            annotations_dict:
                Dictionary of Annotations for reporting organism names.
            s3_output_bucket:       The S3 bucket to write files into.
            coalesce_output:        Merge output into a single file.
            verbose_output:         Flag for wordy terminal print statements.
            debug_mode:
                Flag for debug mode. Activates slow checkpoints.
            streaming_timeout:
                Time (in sec) after which streaming will stop.

        Returns:
            Nothing, if all goes well it should return cleanly.

        """

        log = getLogger("FlintExecutor")

        log.info('Starting streaming { \
            "source": "directory", \
            "sampleId": %s, \
            "sampleFormat": "%s", \
            "sampleType": "%s",  \
            "numShards": %d }', sampleID, sample_format,
                 sample_type, number_of_shards)
        flint_conf = BowtieConfig()
        ab_tracker = AbundanceTracker()
        run_conf = FlintConfig()

        # Set the number of shards so that we can safely exit
        # after we have analyzed the requested number of shards.
        run_conf.number_of_shards_all = int(number_of_shards)
        kinesis_decode = False

        #   Tab5-formatted FASTQ reads.
        if sample_format == "tab5":

            #
            #   Before we do anything, we have to move the data
            #   back to the Master. The way Spark Streaming works, is that
            #   the RDDs will be processed in the Worker in which
            #   they were received, which does not parallelize well.
            #   If we did not ship the input reads back to the master,
            #   then they would only be aligned in one Executor.
            #
            spark_ctx = ssc.sparkContext

            overall_abundance_accumulator = spark_ctx.accumulator(
                {}, AbundanceAccumulator())
            ab_tracker.abundances = overall_abundance_accumulator

            # In this approach, we'll stream the reads from
            # a S3 directory that we monitor with Spark.
            sample_dstream = ssc.textFileStream(stream_source_dir)

            sample_dstream.foreachRDD(lambda rdd: profile_sample(
                sampleReadsRDD=rdd,
                sc=spark_ctx,
                ssc=ssc,
                output_file=output_file,
                save_to_s3=save_to_s3,
                save_to_local=save_to_local,
                sample_type=sample_type,
                sensitive_align=sensitive_align,
                annotations_dictionary=annotations_dictionary,
                partition_size=partition_size,
                s3_output_bucket=s3_output_bucket,
                kinesis_decode=kinesis_decode,
                keep_shard_profiles=keep_shard_profiles,
                coalesce_output=coalesce_output,
                verbose_output=verbose_output,
                debug_mode=debug_mode,
                streaming_timeout=streaming_timeout,
                bowtie2_node_path=flint_conf.path,
                bowtie2_index_path=flint_conf.index_path,
                bowtie2_index_name=flint_conf.index_name,
                bowtie2_number_threads=flint_conf.thread_count))

            # Start to schedule the Spark job on the underlying Spark Context.
            ssc.start()
            # Wait for the streaming computations to finish.
            ssc.awaitTermination()
            ssc.stop()  # Stop the Streaming context
