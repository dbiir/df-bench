# modin

Modin has three backend: 

* [Ray](https://www.ray.io/)
* [Dask](https://dask.org/)
* [unidist](unidist.readthedocs.io)

## Installation

If you use backends like Ray or Dask, just follow the [modin installation documentation](https://modin.readthedocs.io/en/stable/getting_started/installation.html).

If you use unidist MPI backend, please refer to the [unidist documentation](https://unidist.readthedocs.io/en/latest/installation.html) and install a MPI implementation.

## SLURM deployments

Use the scripts with `slurm_` prefix to run on SLURM clusters.