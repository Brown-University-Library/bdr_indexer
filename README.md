# BDR Indexer

Code for BDR solr indexing.


## Local Development Installation

For local development, install the virtualenv in the outer directory that
contains this repository, then point an `env` symlink at it. For example, from
an outer directory shaped like this:

```bash
bdr_indexer_stuff/
  bdr_indexer/
  env -> ./venv_indexer
  venv_indexer/
```

create and populate the environment with `uv`:

```bash
cd /path/to/bdr_indexer_stuff/
uv venv --python 3.8 ./venv_indexer
ln -sfn ./venv_indexer ./env

cd ./bdr_indexer
source ../env/bin/activate
(venv_indexer) uv pip sync ./requirements/local.txt
```

Run tests from the `bdr_indexer` directory:

```bash
source ../env/bin/activate
(venv_indexer) python ./run_tests.py
```
