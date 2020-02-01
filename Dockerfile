FROM amazoncorretto:8 as python_base

ENV PYTHON_VERSION 3.7.6
ENV BOWTIE_VERSION 2.3.4.1
ENV SAMTOOLS_VERSION 1.3.1
ENV PYSPARK_DRIVER_PYTHON python3
ENV PYSPARK_PYTHON python3

# Install Conda
RUN curl -fsSL https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh -o miniconda.sh && \
    /bin/bash miniconda.sh -b -p /opt/conda && \
    rm -f miniconda.sh && \
    /opt/conda/bin/conda clean -tipsy

RUN ln -s /opt/conda/etc/profile.d/conda.sh /etc/profile.d/conda.sh && \
    echo ". /opt/conda/etc/profile.d/conda.sh" >> ~/.bashrc && \
    echo "conda activate flint-worker" >> ~/.bashrc

# Install Dependencies
ADD environment.yml /tmp/environment.yml
RUN /opt/conda/bin/conda env create -f /tmp/environment.yml