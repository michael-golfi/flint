"""
    fs.py contains functions for accessing Hadoop files
"""
from pyspark import SparkContext, RDD


def load_tab5_file(spark_ctx: SparkContext, file_path: str) -> RDD:
    """
    Loads a Tab5-formatted file into an RDD.
    The file is loaded using the Hadoop File API so we can create an RDD
    based on the tab5 format: [name]\t[seq1]\t[qual1]\t[seq2]\t[qual2]\n
    Args:
        spark_ctx: A Spark context
        file_path: A valid Hadoop file path (S3, HDFS, etc.).

    Returns:
        An RDD with a record number as KEY, and reads as VALUE.
    """
    return spark_ctx.newAPIHadoopFile(file_path,
                                      'org.apache.hadoop.mapreduce.lib.input.TextInputFormat',
                                      'org.apache.hadoop.io.LongWritable',
                                      'org.apache.hadoop.io.Text',
                                      conf={'textinputformat.record.delimiter': '\n'})


def load_fastq_file(spark_ctx: SparkContext, file_path: str) -> RDD:
    """
    Loads a FASTQ file into an RDD.
    The file is loaded using the Hadoop File API so we can create an RDD based on the
    FASTQ file format, i.e., multiline parsing of the file.

    Args:
        v: A Spark context
        file_path: A valid Hadoop file path (S3, HDFS, etc.).

    Returns:
        An RDD with a record number as KEY, and read attributes as VALUE.
    """

    return spark_ctx.newAPIHadoopFile(file_path,
                                      'org.apache.hadoop.mapreduce.lib.input.TextInputFormat',
                                      'org.apache.hadoop.io.LongWritable',
                                      'org.apache.hadoop.io.Text',
                                      conf={'textinputformat.record.delimiter': '\n@'})
