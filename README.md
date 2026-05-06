# BDR Indexer

Code for BDR solr indexing.

## Overview

`bdr_indexer` turns Brown Digital Repository OCFL objects into Solr update
documents. Jobs are queued with RQ/Redis, then workers call the Solrizer for a
PID and action such as add, delete, ZIP indexing, or image-parent updates.

The main indexing path loads an OCFL-backed `StorageObject`, gathers active file
metadata, relationships, rights, technical metadata, collection info, extracted
text, and descriptive metadata, then posts a JSON update to Solr. Descriptive
metadata is handled by focused indexers for MODS, Darwin Core, TEI, IR metadata,
RELS-EXT, rights metadata, FITS, and collection-info JSON. MODS, DWC, and TEI
can fall back to related ancestor objects when direct metadata is unavailable,
while image accessibility alt text is restricted to direct-object sources.

The repository also includes command-line helpers for queuing PIDs, starting
workers, requeueing or inspecting failed jobs, clearing ZIP jobs, generating the
resource-type SQLite lookup database, and running the unit test suite.


## Local Development Installation

For local development, install the virtualenv in the outer directory that contains this repository, then point an `env` symlink at it. For example, from an outer directory shaped like this:

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
