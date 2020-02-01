"""
    This module contains the run configuration for the spark job
"""

import time
from dataclasses import dataclass

from pyspark.accumulators import AccumulatorParam


class RunConfiguration:
    """
        RunConfiguration
    """

    def __init__(self):
        self.bowtie2_path = ""
        self.bowtie2_index_path = ""
        self.bowtie2_index_name = ""
        self.bowtie2_threads = 2

        self.rdd_counter = 0
        self.number_of_shards_all = 0
        self.analysis_start_time = 0
        self.analysis_end_time = 0

        self.time_of_last_rdd = 0
        self.overall_abundances = {}

    def increment_rdd_count(self):
        """
        Increments the count that we use as an affix for the profile files.
        Returns:
                Nothing. It just increments the counter variable that we use
                to keep track of the incoming RDDs.
        """

        self.rdd_counter = self.rdd_counter + 1

    def set_number_of_shards(self, num_shards_from_run):
        """
        Sets global number of shards to the specified number of shards
        from an experimental run.
        Args:
            num_shards_from_run: Number of shards that we'll be streaming in.
        Returns:
            Nothing. This is a 'set()' function.
        """
        self.number_of_shards_all = num_shards_from_run

    def get_shard_counter(self):
        """
        Accessor for returning the current number of shards
        that we have processed.
        Returns:
                Integer containing the current count of processed shards.
        """
        return int(self.rdd_counter)

    def get_shards(self):
        """
        Accessor for returning the value of the overall number of shards
        we want to analyze.
        Returns:
                Integer containing the number of overall shards.
        """
        return int(self.number_of_shards_all)

    def shard_equals_counter(self):
        """
        Checks whether the number of shards processed equals
        the specified limit.
        Returns:
                True if its time to stop,
                the shards processed equal the number specified.
                False otherwise.
        """
        return self.get_shard_counter() == self.get_shards()

    def set_analysis_start_time(self):
        """
        Sets the time for the analysis when the first
        streamed shard is captured.
        Returns:
                Nothing, this is a 'setter' method.
        """
        self.analysis_start_time = time.time()

    def set_analysis_end_time(self):
        """
        Sets the time for when all the shards have been processed.
        Returns:
                Nothing, this is a 'setter' method.
        """
        self.analysis_end_time = time.time()

    def set_bowtie2_path(self, bowtie2_node_path):
        """
        Sets the path at which the Bowtie2 executable can be located.
        Args:
            bowtie2_node_path:  A path in the local nodes at which Bowtie2
            can be found at. Note that this should match
            the path that was given in the "app-setup.sh" bootstrap script.

        Returns:
            Nothing. This is a setter method.
        """
        self.bowtie2_path = bowtie2_node_path

    def get_bowtie2_path(self):
        """
        Retrieves the path at which the Bowtie2 executable can be called.

        Returns:
            A path in the local nodes at which Bowtie2 can be found at.
            Note that this will match the path that
            was given in the "app-setup.sh" bootstrap script.
        """
        return self.bowtie2_path

    def set_bowtie2_index_path(self, bowtie2_index_path):
        """
        Sets the path at which the Bowtie2 index can be found
        in each local node in the cluster.
        Args:
            bowtie2_index_path: A path in the local nodes at which Bowtie2
            can be found at. Note that this should match
            the path that was given in the "app-setup.sh" bootstrap script.

        Returns:
            Nothing.
        """
        self.bowtie2_index_path = bowtie2_index_path

    def get_bowtie2_index_path(self):
        """
        Retrieves the path at which the local Bowtie2 index can be found.
        Returns:
            A path in the local nodes at which Bowtie2 can be found at.
            Note that this will match the path that
            was given in the "app-setup.sh" bootstrap script.
        """
        return self.bowtie2_index_path

    def set_bowtie2_index_name(self, bowtie2_index_name):
        """
        Sets the name of the Bowtie2 index name we'll be using.
        Returns:
            Nothing.
        """
        self.bowtie2_index_name = bowtie2_index_name

    def get_bowtie2_index_name(self):
        """
        Retrieves the name of the Bowtie2 index name we'll be using.
        Returns:
            A string with the name of the Bowtie2 index.
        """
        return self.bowtie2_index_name

    def set_bowtie2_number_threads(self, bowtie2_number_threads):
        """
        Sets the number of threads in a Bowtie2 command.
        Args:
            bowtie2_number_threads: INT,
            the number of threads to give to bowtie2.

        Returns:
            Nothing.
        """
        self.bowtie2_threads = bowtie2_number_threads

    def get_bowtie2_number_threads(self):
        """
        Get the number of threads that Bowtie2 is currently using.
        Returns:
            INT The number of bowtie2 threads.
        """
        return self.bowtie2_threads

    def set_time_of_last_rdd(self, time_of_last_rdd_processed):
        """
        Sets the time at which the last RDD was processed.
        Returns:
            Nothing. Set() method.
        """
        self.time_of_last_rdd = time_of_last_rdd_processed

    def get_time_of_last_rdd(self):
        """
        Retrieves the time at which the last RDD was processed.
        Returns:
            time_of_last_rdd object that represents the time
            at which the last RDD ended processing.
        """
        return self.time_of_last_rdd

    def set_overall_abundances(self, abundance_acc):
        """
        Set the overal abundances found
        """
        self.overall_abundances = abundance_acc

    def get_overall_abundaces(self):
        """
        Get the overal abundances
        """
        return self.overall_abundances


class AbundanceAccumulator(AccumulatorParam):
    """
    Custom class for stockpiling the rolling abundance counts
    for a given bacterial strain. Big thanks go to
    StackOverflow and the following post:
    https://stackoverflow.com/questions/44640184/accumulator-in-pyspark-with-dict-as-global-variable

    """

    def zero(self, value=""):
        return dict()

    def addInPlace(self, value1, value2):
        value1.update(value2)
        return value1


@dataclass
class BowtieConfig:
    """
        Configuration for Bowtie command execution
    """
    path: str
    index_path: str
    index_name: str
    thread_count: int = 2


@dataclass
class FlintConfig:
    """
    FlintConfig
    """
    rdd_counter: int = 0
    number_of_shards_all: int = 0
    analysis_start_time: int = 0
    analysis_end_time: int = 0
    time_of_last_rdd: int = 0

    def increment_rdd_count(self):
        """
        Increment RDDs processed
        """
        self.rdd_counter = self.rdd_counter + 1

    def shard_equals_counter(self):
        """
        Checks whether the number of shards processed equals
        the specified limit.
        Returns:
                True if its time to stop, 
                the shards processed equal the number specified.
                False otherwise.
        """
        return self.rdd_counter == self.number_of_shards_all


@dataclass
class AbundanceTracker:
    abundances = {}
