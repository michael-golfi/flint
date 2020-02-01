import subprocess as sp
import sys
from common.bowtie import bowtie2_cmd
from common.logging import getLogger

log = getLogger("profile_sample.py")


def align_with_bowtie2(iterator):
    """
    Function that runs on ALL worker nodes (Executors).
    Dispatches a Bowtie2 command and handles read alignments.
    Args:
        iterator:   Iterator object from Spark

    Returns:    An iterable collection of read alignments in SAM format.

    """
    alignments = []

    #
    #   We pick up the RDD with reads that we set as a
    #   broadcast variable "previously" — The location of this action
    #   happens in the code below, which executes before 'this' code block.
    #
    reads_list = broadcast_sample_reads.value
    bowtie_cmd = bowtie2_cmd(node_path=bowtie2_node_path,
                             index_path=bowtie2_index_path,
                             index_name=bowtie2_index_name,
                             num_threads=bowtie2_number_threads,
                             sensitive=sensitive_align)

    try:
        align_subprocess = sp.Popen(
            bowtie_cmd, stdin=sp.PIPE, stdout=sp.PIPE, stderr=sp.PIPE)

        pickled_reads_list = pickle.dumps(reads_list)
        # no_reads = len(reads_list)

        alignment_output, alignment_error = align_subprocess.communicate(
            input=pickled_reads_list.decode('latin-1'))

        #   The output is returned as a 'bytes' object,
        #   so we'll convert it to a list. That way, 'this' worker node
        #   will return a list of the alignments it found.
        for a_read in alignment_output.strip().decode().splitlines():

            #   Each alignment (in SAM format) is parsed and broken down into two (2) pieces: the read name,
            #   and the genome reference the read aligns to. We do the parsing here so that it occurs in the
            #   worker node and not in the master node. A benefit of parsing alignments in the worker node is
            #   that it also brings down the size of the 'alignment' object that gets transmitted through the
            #   network. Note that networking costs are minimal for a 'few' alignments, but they do add up
            #   for large samples with many shards.
            #
            #   SAM format: [0] - QNAME (the read name)
            #               [1] - FLAG
            #               [2] - RNAME (the genome reference name that the read aligns to
            #
            alignment = a_read.split(
                "\t")[0] + "\t" + a_read.split("\t")[2]

            #   Once the alignment has been parsed, we add it to the return list of alignments that will be
            #   sent back to the master node.
            alignments.append(alignment)

    except sp.CalledProcessError as err:
        log.error("Alignment error: %s", str(err))
        sys.exit(-1)

    return iter(alignments)


def get_organism_name(gca_id):
    """
    Nested function for retrieving and constructing a proper organism name for the reports.
    Args:
        gca_id: The 'GCA' formatted ID to look up in the annotations.

    Returns:
        A properly formatted string for the organism name that contains a Taxa_ID, Genus-Species-Strain name,
        and the GCA_ID that was used for the query.
    """

    organism_name_string = ""

    if gca_id in annotations_dictionary:
        taxa_id = annotations_dictionary[gca_id]['taxa_id']
        organism_name = annotations_dictionary[gca_id]['organism_name']

        organism_name_string = str(
            taxa_id) + "\t" + str(gca_id) + "\t" + str(organism_name)

    else:
        organism_name_string = gca_id

    return organism_name_string


def accumulate_abundaces(a_strain):
    log.info("Accumulating abundances")
    strain_name = a_strain[0]
    abundance_count = a_strain[1]

    # abundances = get_overall_abundaces()
    global OVERALL_ABUNDANCES
    OVERALL_ABUNDANCES += {strain_name: abundance_count}

    return a_strain


def profile_sample(sampleReadsRDD, sc, ssc, output_file, save_to_s3,
                   save_to_local, sensitive_align, partition_size,
                   annotations_dictionary, s3_output_bucket,
                   keep_shard_profiles, coalesce_output, verbose_output,
                   bowtie2_node_path, bowtie2_index_path,
                   bowtie2_index_name, bowtie2_number_threads, sample_type,
                   debug_mode, streaming_timeout, kinesis_decode=None):
    try:
        if not sampleReadsRDD.isEmpty():

            if get_shard_counter() == 0:
                set_analysis_start_time()

            # -------------------------------------- Alignment --------------------------------------------------------
            #
            #   First, we'll convert the RDD to a list, which we'll then convert to a Spark.broadcast variable
            #   that will be shipped to all the Worker Nodes (and their Executors) for read alignment.
            #
            if kinesis_decode:
                print("[" + time.strftime('%d-%b-%Y %H:%M:%S',
                                          time.localtime()) + "] Decoding Kinesis Stream...")
                sample_reads_list = json.loads(sampleReadsRDD.collect())
            else:
                # collect returns <type 'list'> on the main driver.
                sample_reads_list = sampleReadsRDD.collect()

            number_input_reads = len(sample_reads_list)

            #
            #   The RDD with reads is set as a Broadcast variable that will be picked up by each worker node.
            #
            broadcast_sample_reads = sc.broadcast(sample_reads_list)

            #
            #   Run starts here.
            #
            print("[" + time.strftime('%d-%b-%Y %H:%M:%S', time.localtime()) + "] Shard: " +
                  str(get_shard_counter()) + " of " + str(get_shards()))

            read_noun = ""
            if sample_type.lower() == "paired":
                read_noun == "Paired-End"

            print("[" + time.strftime('%d-%b-%Y %H:%M:%S', time.localtime()) + "] Input: " +
                  '{:0,.0f}'.format(number_input_reads) + " " + read_noun + " Reads.")

            alignment_start_time = time.time()

            data = sc.parallelize(range(1, partition_size))
            data_num_partitions = data.getNumPartitions()

            if verbose_output:
                print("[" + time.strftime('%d-%b-%Y %H:%M:%S', time.localtime()) + "] No. RDD Partitions: " +
                      str(data_num_partitions))

            #
            #   Dispatch the Alignment job with Bowtie2
            #
            print("[" + time.strftime('%d-%b-%Y %H:%M:%S', time.localtime()) +
                  "] Aligning reads with Bowtie2 (" + str(bowtie2_number_threads) + ")...")

            if sensitive_align:
                print("[" + time.strftime('%d-%b-%Y %H:%M:%S', time.localtime()) +
                      "] Using Sensitive Alignment Mode...")

            alignments_RDD = data.mapPartitions(align_with_bowtie2)
            number_of_alignments = alignments_RDD.count()

            alignment_end_time = time.time()
            alignment_total_time = alignment_end_time - alignment_start_time

            print("[" + time.strftime('%d-%b-%Y %H:%M:%S', time.localtime()) + "] Bowtie2 - Complete. " +
                  "(" + str(timedelta(seconds=alignment_total_time)) + ")")
            print("[" + time.strftime('%d-%b-%Y %H:%M:%S', time.localtime()) + "]" + " Found: " +
                  '{:0,.0f}'.format(number_of_alignments) + " Alignments.")

            print("[" + time.strftime('%d-%b-%Y %H:%M:%S', time.localtime()) +
                  "] Analyzing...")

            # ------------------------------------------- Map 1 -------------------------------------------------------
            #
            #   The Map step sets up the basic data structure that we start with — a map of reads to the genomes they
            #   align to. We'll Map a read (QNAME) with a genome reference name (RNAME).
            #   The REDUCE 1 step afterwards will collect all the genomes and key them to a unique read name.
            #
            if verbose_output:
                print("[" + time.strftime('%d-%b-%Y %H:%M:%S', time.localtime()) +
                      "] Mapping Reads to Genomes (QNAME-RNAME).")

            map_reads_to_genomes = alignments_RDD.map(lambda line: (
                line.split("\t")[0], [line.split("\t")[1]])).cache()

            if debug_mode:
                print("[" + time.strftime('%d-%b-%Y %H:%M:%S', time.localtime()) +
                      "] • Checkpoint 1: Map 1, map_reads_to_genomes")
                chk_1_s = time.time()
                checkpoint_1 = map_reads_to_genomes.count()
                chk_1_e = time.time()
                print("[" + time.strftime('%d-%b-%Y %H:%M:%S', time.localtime()) +
                      "]   TIME: " + str(timedelta(seconds=(chk_1_e - chk_1_s))))

            if verbose_output:
                #   We use the following block to get the Overall Mapping Rate for 'this' shard.
                list_unique_reads = map_reads_to_genomes.flatMap(
                    lambda x: x).keys().distinct()
                number_of_reads_aligned = list_unique_reads.count()

                overall_mapping_rate = float(
                    number_of_reads_aligned) / number_input_reads
                overall_mapping_rate = overall_mapping_rate * 100

                print("[" + time.strftime('%d-%b-%Y %H:%M:%S', time.localtime()) + "] Shard Mapping Rate: " +
                      '{:.2f}'.format(overall_mapping_rate) + "%")

            # ------------------------------------------ Reduce 1 -----------------------------------------------------
            #
            #   Reduce will operate first by calculating the read contributions, and then using these contributions
            #   to calculate an abundance.
            #   Note the "+" operator can be used to concatenate two lists. :)
            #

            #   'Reduce by Reads' will give us a 'dictionary-like' data structure that contains a Read Name (QNAME) as
            #   the KEY, and a list of genome references (RNAME) as the VALUE. This allows us to calculate
            #   each read's contribution.
            #
            if verbose_output:
                print("[" + time.strftime('%d-%b-%Y %H:%M:%S', time.localtime()) +
                      "] Reducing Reads to list of Genomes...")

            reads_to_genomes_list = map_reads_to_genomes.reduceByKey(
                lambda l1, l2: l1 + l2)

            if debug_mode:
                print("[" + time.strftime('%d-%b-%Y %H:%M:%S', time.localtime()) +
                      "] • Checkpoint 2: Reduce 1, reads_to_genomes_list")
                chk_2_s = time.time()
                checkpoint_2 = reads_to_genomes_list.count()
                chk_2_e = time.time()
                print("[" + time.strftime('%d-%b-%Y %H:%M:%S', time.localtime()) +
                      "]   TIME: " + str(timedelta(seconds=(chk_2_e - chk_2_s))))

            # ------------------------------------ Map 2, Fractional Reads --------------------------------------------
            #
            #   Read Contributions.
            #   Each read is normalized by the number of genomes it maps to. The idea is that reads that align to
            #   multiple genomes will contribute less (have a hig denominator) than reads that align to fewer genomes.
            #

            if verbose_output:
                print("[" + time.strftime('%d-%b-%Y %H:%M:%S', time.localtime()) +
                      "] Calculating Read Contributions...")

            read_contributions = reads_to_genomes_list.mapValues(
                lambda l1: 1 / float(len(l1)))

            if debug_mode:
                print("[" + time.strftime('%d-%b-%Y %H:%M:%S', time.localtime()) +
                      "] • Checkpoint 3: Map 2, read_contributions")
                chk_3_s = time.time()
                checkpoint_3 = read_contributions.count()
                chk_3_e = time.time()
                print("[" + time.strftime('%d-%b-%Y %H:%M:%S', time.localtime()) +
                      "]   TIME: " + str(timedelta(seconds=(chk_3_e - chk_3_s))))

            #
            #   Once we have the read contributions, we'll JOIN them with the starting 'map_reads_to_genomes' RDD to
            #   get an RDD that will map the read contribution to the Genome it aligns to.
            #   Note: l[0] is the Read name (key), and l[1] is the VALUE (a list) after the join.
            #
            if verbose_output:
                print("[" + time.strftime('%d-%b-%Y %H:%M:%S', time.localtime()) +
                      "] Joining Read Contributions to Genomes...")

            read_contribution_to_genome = read_contributions.join(map_reads_to_genomes)\
                                                            .map(lambda l: (l[1][0], "".join(l[1][1])))

            if debug_mode:
                print("[" + time.strftime('%d-%b-%Y %H:%M:%S', time.localtime()) +
                      "] • Checkpoint 4: read_contribution_to_genome")
                chk_4_s = time.time()
                checkpoint_4 = read_contribution_to_genome.count()
                chk_4_e = time.time()
                print("[" + time.strftime('%d-%b-%Y %H:%M:%S', time.localtime()) +
                      "]   TIME: " + str(timedelta(seconds=(chk_4_e - chk_4_s))))

            #
            #   Now have an RDD mapping the Read Contributions to Genome Names, we'll flip the (KEY,VALUE) pairings
            #   so that we have Genome Name as KEY and Read Contribution as Value.
            #
            if verbose_output:
                print("[" + time.strftime('%d-%b-%Y %H:%M:%S', time.localtime()) +
                      "] Flipping Genomes and Read Contributions...")

            genomes_to_read_contributions = read_contribution_to_genome.map(
                lambda x: (x[1], x[0]))

            if debug_mode:
                print("[" + time.strftime('%d-%b-%Y %H:%M:%S', time.localtime()) +
                      "] • Checkpoint 5: genomes_to_read_contributions")
                chk_5_s = time.time()
                checkpoint_5 = genomes_to_read_contributions.count()
                chk_5_e = time.time()
                print("[" + time.strftime('%d-%b-%Y %H:%M:%S', time.localtime()) +
                      "]   TIME: " + str(timedelta(seconds=(chk_5_e - chk_5_s))))

            # --------------------------------------- Reduce 2, Abundances --------------------------------------------
            #
            #   Following the KEY-VALUE inversion, we do a reduceByKey() to aggregate the fractional counts for a
            #   given genomic assembly and calculate its abundance.
            #
            if verbose_output:
                print("[" + time.strftime('%d-%b-%Y %H:%M:%S', time.localtime()) +
                      "] Calculating Genome Abundances...")

            genomic_assembly_abundances = genomes_to_read_contributions.reduceByKey(
                lambda l1, l2: l1 + l2)

            if debug_mode:
                print("[" + time.strftime('%d-%b-%Y %H:%M:%S', time.localtime()) +
                      "] • Checkpoint 6: Reduce 2, genomic_assembly_abundances")
                chk_6_s = time.time()
                checkpoint_6 = genomic_assembly_abundances.count()
                chk_6_e = time.time()
                print("[" + time.strftime('%d-%b-%Y %H:%M:%S', time.localtime()) +
                      "]   TIME: " + str(timedelta(seconds=(chk_6_e - chk_6_s))))

            #
            #   Strain level abundances
            #   At this point we have abundances at the genomic-assembly level (chromosomes, contigs, etc.), but what
            #   we are after is abundances one level higher, i.e., at the Strain level. So we'll do one more map to
            #   to set a key that we can reduce with at the Strain level.
            #
            #   Note: The key to map here is critical. The assembly FASTA files need to be in a format that tells us
            #   how to 'fold up' the assemblies into a parent taxa. The FASTA files we indexed had a Taxonomic ID
            #   that tells us the Organism name (at the Strain level), and it is delimited by a period and located
            #   at the beginning of the FASTA record.
            #
            strain_map = genomic_assembly_abundances.map(
                lambda x: ("GCA_" + x[0].split(".")[0], x[1]))

            if debug_mode:
                print("[" + time.strftime('%d-%b-%Y %H:%M:%S', time.localtime()) +
                      "] • Checkpoint 7: strain_map")
                chk_7_s = time.time()
                checkpoint_7 = strain_map.count()
                chk_7_e = time.time()
                print("[" + time.strftime('%d-%b-%Y %H:%M:%S', time.localtime()) +
                      "]   TIME: " + str(timedelta(seconds=(chk_7_e - chk_7_s))))

            #
            #   Once the Mapping of organism names at the Strain level is complete, we can just Reduce them to
            #   aggregate the Strain-level abundances.
            #
            strain_abundances = strain_map.reduceByKey(
                lambda l1, l2: l1 + l2).cache()

            if debug_mode:
                print("[" + time.strftime('%d-%b-%Y %H:%M:%S', time.localtime()) +
                      "] • Checkpoint 8: strain_abundances")
                chk_8_s = time.time()
                checkpoint_8 = strain_abundances.count()
                chk_8_e = time.time()
                print("[" + time.strftime('%d-%b-%Y %H:%M:%S', time.localtime()) +
                      "]   TIME: " + str(timedelta(seconds=(chk_8_e - chk_8_s))))

            # --------------------------------------- Abundance Coalescing --------------------------------------------
            #
            #   If requested, we'll continously update the rolling count of abundances for the strains that we've
            #   seen, or add new ones. Note that this involves a call to 'collect()' which brings back everything
            #   to the Master node, so there is a slight hit on performance.
            #
            if coalesce_output:
                print("[" + time.strftime('%d-%b-%Y %H:%M:%S',
                                          time.localtime()) + "] Updating abundance counts...")

                #   Use 'collect()' to aggregate the counts for 'this' Shard and accumulate the rolling count.
                #   We are violating the prime directive of Spark design patterns by using 'collect()', but in
                #   practice, the call adds a negligible amount to the running time.
                # list_strain_abundances = strain_abundances.collect()
                #
                # for a_strain in list_strain_abundances:
                #     strain_name     = a_strain[0]
                #     abundance_count = a_strain[1]
                #
                #     if strain_name in OVERALL_ABUNDANCES:
                #         OVERALL_ABUNDANCES[strain_name] += abundance_count
                #     else:
                #         OVERALL_ABUNDANCES[strain_name] = abundance_count

                abundace_rdd = strain_abundances.map(
                    lambda x: accumulate_abundaces(x)).cache()
                abundance_count = abundace_rdd.count()

            else:
                if save_to_s3:
                    output_file = output_file.replace("/abundances.txt", "")
                    output_dir_s3_path = "s3a://" + s3_output_bucket + "/" + output_file + "/shard_" + \
                                         str(RDD_COUNTER) + "/"

                    strain_abundances.map(lambda x: "%s\t%s" % (get_organism_name(x[0]), x[1])) \
                        .saveAsTextFile(output_dir_s3_path)

                #   Careful with this. This will cause the individual files to be stored in Worker nodes, and
                #   not in the Master node.
                #   TODO: Refactor so that it sends it back to the Master, and stores it in the 'local' master path.
                # if save_to_local:
                #     output_dir_local_path = output_file.replace("abundances.txt", "/shard_" + str(RDD_COUNTER))
                #     abundances_list = strain_abundances.map(lambda x: "%s\t%s" % (get_organism_name(x[0]), x[1])) \
                #         .saveAsTextFile("file://" + output_dir_local_path)

            # ------------------------------------------ Shard Profiles -----------------------------------------------
            #
            #   The user can specify whether to retain individual Shard profiles. The flag that controls this
            #   'keep_shard_profiles' works in conjuction with the '' and '' flags. So the rolling shard prolies
            #   will be stored in the location that the user requested.
            #
            if keep_shard_profiles:
                # -------------------------------------- S3 Rolling Output --------------------------------------------
                if save_to_s3:
                    if verbose_output:
                        print("[" + time.strftime('%d-%b-%Y %H:%M:%S', time.localtime()) +
                              "] Saving to S3 bucket...")

                    #   Pointer to S3 filesystem.
                    sc._jsc.hadoopConfiguration().set("mapred.output.committer.class",
                                                      "org.apache.hadoop.mapred.FileOutputCommitter")
                    URI = sc._gateway.jvm.java.net.URI
                    Path = sc._gateway.jvm.org.apache.hadoop.fs.Path
                    FileSystem = sc._gateway.jvm.org.apache.hadoop.fs.FileSystem
                    fs_uri_string = "s3a://" + s3_output_bucket + ""
                    fs = FileSystem.get(URI(fs_uri_string),
                                        sc._jsc.hadoopConfiguration())

                    shard_sub_dir = "shard-profiles"
                    output_file = output_file.replace("/abundances.txt", "")
                    shard_sub_dir_path = "s3a://" + s3_output_bucket + \
                        "/" + output_file + "/" + shard_sub_dir

                    #   Create the 1st temporary output file through the 'saveAsTextFile()' method.
                    tmp_output_file = shard_sub_dir_path + "-tmp"

                    strain_abundances.map(lambda x: "%s\t%s" % (get_organism_name(x[0]), x[1])) \
                                     .coalesce(1) \
                                     .saveAsTextFile(tmp_output_file)

                    #   This is the "tmp" file created by "saveAsTextFile()", by default its named "part-00000").
                    created_file_path = Path(tmp_output_file + "/part-00000")

                    #   The gimmick here is to move the tmp file into a final location so that "saveAsTextFile()"
                    #   can write again.
                    str_for_rename = shard_sub_dir_path + \
                        "/abundances-" + str(RDD_COUNTER) + ".txt"
                    renamed_file_path = Path(str_for_rename)

                    #   Create the directory in which we'll be storing the shard profiles.
                    fs.mkdirs(Path(shard_sub_dir_path))

                    #   The "rename()" function can be used as a "move()" if the second path is different.
                    fs.rename(created_file_path, renamed_file_path)

                    #   Remove the "tmp" directory we created when we used "saveAsTextFile()".
                    fs.delete(Path(tmp_output_file))

                # ------------------------------------ Local Rolling Output -------------------------------------------
                if save_to_local:
                    if verbose_output:
                        print("[" + time.strftime('%d-%b-%Y %H:%M:%S', time.localtime()) +
                              "] Saving to local filesystem...")

                    rdd_counter_str = "-" + str(RDD_COUNTER) + ".txt"
                    output_file = output_file.replace(
                        "abundances", "/shard_profiles/abundances")
                    output_file = output_file.replace(".txt", rdd_counter_str)

                    writer = csv.writer(open(output_file, "wb"), delimiter='|', lineterminator="\n", quotechar='',
                                        quoting=csv.QUOTE_NONE)

                    abundances_list = strain_abundances.map(lambda x: "%s\t%s" % (get_organism_name(x[0]), x[1])) \
                        .repartition(1).collect()
                    for a_line in abundances_list:
                        writer.writerow([a_line])

            # -------------------------------------------- End of Run -------------------------------------------------
            #
            #   Housekeeping tasks go here. This completes the processing of a single streamed shard.

            #   Increment the counter that we use to keep track of, and also use as an affix for a RDDs profile count.
            increment_rdd_count()

            #   Set the time at which 'this' RDD (a sample shard) was last processed.
            set_time_of_last_rdd(time.time())

            print(
                "[" + time.strftime('%d-%b-%Y %H:%M:%S', time.localtime()) + "] Done.")
            print("[" + time.strftime('%d-%b-%Y %H:%M:%S', time.localtime()) + "]")

        # ---------------------------------------------- Empty RDD Case -----------------------------------------------
        #
        else:
            #   If we've analyzed the same number of shards as those that were requested, we stop the streaming.
            #   How we stop streaming, it depends on what flags were set. We'll stop the streaming if the time
            #   between the last RDD processed and now is greater than a user-defined timeout, or 3 seconds for
            #   the default. Another way to stop is to have processed a certain number of shards. If this number
            #   has been reached, then we'll go ahead and stop.

            time_of_last_check = get_time_of_last_rdd()
            time_now = time.time()
            check_delta = timedelta(seconds=(time_now - time_of_last_check))
            check_delta_int = int(check_delta.seconds)

            if time_of_last_check != 0:
                if shard_equals_counter() or check_delta_int > streaming_timeout:
                    print("[" + time.strftime('%d-%b-%Y %H:%M:%S', time.localtime()) +
                          "] All Requested Sample Shards Finished. (" + str(get_shards()) + " shards)")

                    print("[" + time.strftime('%d-%b-%Y %H:%M:%S',
                                              time.localtime()) + "] Stopping Streaming.")

                    set_analysis_end_time()

                    #   The last thing we do is to stop the streaming context. Once this command finishes, we are
                    #   jumped back-out into the code-block that called us — the 'flint.py' script.
                    ssc.stop()

    except Exception as ex:
        template = "[Flint - ERROR] An exception of type {0} occurred. Arguments:\n{1!r}"
        message = template.format(type(ex).__name__, ex.args)
        print(message)
