# Dask

Dask has a big ecosystem with various plugins. 

## Installation

Dask can run on dedicated cluster, kubernetes, or slurm. It support High-Performance Computing hardware like InfiniBand (MPI) and GPU (rapids dask-cuDF). You may need to install specific packages if you want to enable one of the above features.

### General

Just follow the [Dask installation documentation](https://docs.dask.org/en/stable/install.html) to install `dask` and `dask-distributed`.

### MPI

You will need to install [Dask-MPI](http://mpi.dask.org/en/latest/index.html) if you have RDMA networks like InfiniBand or RoCE. Note that Daks-MPI is based on [mpi4py](https://mpi4py.readthedocs.io/en/stable/). So you should follow the [mpi4py installation guide](https://mpi4py.readthedocs.io/en/stable/install.html) to install a working MPI and mpi4py.

### GPU and Dask-cuDF