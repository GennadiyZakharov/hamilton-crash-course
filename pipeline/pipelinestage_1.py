"""
This is the example of code defining pipeline stage
It defined the set of functions
used as pipeline steps and dependencies between them
"""
import gzip
import shutil
from pathlib import Path
import urllib.request

from hamilton.function_modifiers import extract_fields

"""
all pipeline step functions should follow the pattern
  - accept parameters that are eiter values form config, 
  or dependencies from previous steps
  Use str for config parametyers and Path for file dependencies
  return dict[str, Any] that contains all output parameters
"""

# Defining a pipelien step
@extract_fields({"genome_path_gz": Path})
# This annotation informs Hamilton how to name output parameters
# See the explanation below
def pipelinestep_download_genome(
        # Hamilton matches all parameters by their names
        # from the config dictionary
        # passed to the Hamilton driver
        genome_url: str,
        genome_name: str,
        data_root: str,
) -> dict[str, Path]:
    # Creating data folder if not exists
    data_root_path = Path(data_root)
    data_root_path.mkdir(parents=True, exist_ok=True)
    genome_path_gz = data_root_path / Path(genome_name + '.fa.gz')
    if not genome_path_gz.exists(): # Checking the existence of the output file
        urllib.request.urlretrieve(genome_url, genome_path_gz)
    # This is a Hamilton trick
    # to make meaningful names for output dependencies
    # We return all values as a dict[str, Any]
    # From the annotation at the beginning of the function
    # Hamilton knows, that function returns the parameter
    # Named "genome_path_gz" and matches is with the real value
    # We can return any Python objects, not only file paths
    return {"genome_path_gz": genome_path_gz}

@extract_fields({"genome_path": Path})
def pipelinestep_unzip_reference(
        # We use for the first parameter the same name,
        # as in the annotation dictionary of the previous function
        # By thi Hamilton knows that to get this parameter
        # it needs to execute the previous function
        genome_path_gz: Path,
        genome_name: str,
        data_root: str,
) -> dict[str, Path]:
    genome_path = Path(data_root) / Path(genome_name + '.fa')

    with gzip.open(genome_path_gz, 'rb') as f_in:
        with open(genome_path, 'wb') as f_out:
            shutil.copyfileobj(f_in, f_out)
    # Same thing, returning the new path as a dictionary
    return {"genome_path": genome_path}

@extract_fields({"chromosomes_path": Path})
def pipelinestep_extract_chromosome_names(
        genome_path: Path,
        genome_name: str,
        data_root: str,
) -> dict[str, Path]:
    """
    Generated random data and saves it to the file
    :param input_data:
    :return:
    """
    chromosomes_path = Path(data_root) / Path(genome_name + '.chromosomes.txt')

    with open(genome_path, "r") as f:
        with open(chromosomes_path, 'w') as q:
            for line in f.readlines():
                if line.startswith(">"):
                    q.write(line)

    return {"chromosomes_path": chromosomes_path}

# Declaring the list of final files that we need to obtain
final_outputs = ["chromosomes_path"]