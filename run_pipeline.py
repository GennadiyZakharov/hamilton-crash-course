"""
This is a small project explaining the key ideas of
Hamilton workflow manager

Based on the investigation made by Eric Hidari: https://gist.github.com/Eric-Kobayashi/

To run the code, install the following packages
```shell
mamba install -c hamilton-opensource sf-hamilton
mamba install pyyaml
mamba install graphviz
```
"""

import argparse
import yaml

from hamilton import driver
from hamilton import lifecycle
from pipeline import pipelinestage_1  # Importing modules that contain real pipeline code
from pipeline import pipelinestage_2

pipeline_stages = [pipelinestage_1, pipelinestage_2]

final_outputs = pipelinestage_1.final_outputs


def get_options() -> argparse.Namespace:
    """
    Get options from the command line
    """
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", default="default-config.yaml", help="Config file name")
    args = parser.parse_args()
    return args


def main() -> None:
    # set up

    args = get_options()
    # This is the dictionary of parameters
    config = yaml.safe_load(open(args.config))

    dr = (
        driver
        .Builder()
        .with_modules(pipelinestage_1)  # Including modules with functions
        .with_modules(pipelinestage_2)
        .with_config(config)  # Adding configuration parameters
        .with_adapters(lifecycle.PrintLn(verbosity=2))
        .build()
    )
    dr.display_all_functions("execution_graph.png", orient="TB", show_legend=True)
    dr.execute(final_outputs)  # Requesting final outputs from all stages


if __name__ == "__main__":
    main()
