"""
    Module for bowtie helpers
"""

import shlex
from typing import List


def bowtie2_cmd(node_path: str,
                index_path: str,
                index_name: str,
                num_threads: int,
                sensitive: bool = False) -> List[str]:
    """
    Constructs a properly formatted shell Bowtie2 command by performing
    a simple lexical analysis using 'shlex.split()'.
    Returns:
        An array with the bowtie 2 command call split into an array
        that can be used by the popen() function.
    """
    index = index_path + "/" + index_name

    if sensitive:
        cmd = node_path + '/bowtie2 \
                                    --threads ' + str(num_threads) + ' \
                                    --local \
                                    -D 20 \
                                    -R 3 \
                                    -N 0 \
                                    -L 20 \
                                    -i \'"S,1,0.50"\' \
                                    --no-sq \
                                    --no-hd \
                                    --no-unal \
                                    -q \
                                    -x ' + index + ' --tab5 -'
    else:
        cmd = node_path + '/bowtie2 \
                                    --threads ' + str(num_threads) + ' \
                                    --local \
                                    -D 5 \
                                    -R 1 \
                                    -N 0 \
                                    -L 25 \
                                    -i \'"S,0,2.75"\' \
                                    --no-discordant \
                                    --no-mixed \
                                    --no-contain \
                                    --no-overlap \
                                    --no-sq \
                                    --no-hd \
                                    --no-unal \
                                    -q \
                                    -x ' + index + ' --tab5 -'
    return shlex.split(cmd)
