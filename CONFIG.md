# Flint Operation

## Dependency Management

Flint uses a Dockerfile wrapped around BioConda to manage dependencies for various tools such as bowtie2.

The code can run without Docker using Conda on its own for dependency management.

## Configuration

```yaml
bowtie2:
  threads: The number of threads for bowtie to use
  indexPath: The path to the index files for bowtie to align against
  path: The path to the bowtie2 command executable. Omission will try to resolve it on the PATH.

partitionSize: 64

annotations: |
  The annotations path to resolve. This path should be a URL that can be a local path, an S3 path or a plain web file.

sampleSource:
  - id: A unique ID for the source
    format: tab5
    type: paired
    location: The path to resolve for the sample files. This path should be a local path, an S3 path or a plain web file. In the case of local path or S3 path it should also be able to use glob path and recursive directories
    batching:
      duration: 0.25
      shards: 2
    output: The path to resolve for the sample files. This path should be a local path, an S3 path or a plain web file. The results will be a flat directory with output files.
    
```