# BDR Indexer

Code for BDR solr indexing.


## Overview

`bdr_indexer` turns Brown Digital Repository OCFL objects into Solr update documents. Jobs are queued with RQ/Redis, then workers call the Solrizer for a PID and action such as add, delete, ZIP indexing, or image-parent updates.

The main indexing path: 
- loads an OCFL-backed `StorageObject`, 
- then gathers active file metadata, relationships, rights, technical metadata, collection info, extracted text, and descriptive metadata, 
- then posts a JSON update to Solr. 

Descriptive metadata is handled by focused indexers for MODS, Darwin Core, TEI, IR metadata, RELS-EXT, rights metadata, FITS, and collection-info JSON. 

MODS, DWC, and TEI can fall back to related ancestor objects when direct metadata is unavailable, while image accessibility alt text is restricted to direct-object sources.

The repository also includes command-line helpers for queuing PIDs, starting workers, requeueing or inspecting failed jobs, clearing ZIP jobs, generating the resource-type SQLite lookup database, and running the unit test suite.


## Requirements

- [uv](https://docs.astral.sh/uv/) for Python installation, dependency management, and command execution.


## Local Development Installation

From an outer/stuff directory:

```shell
git clone <repository-url> bdr_indexer
cd bdr_indexer
```

From the `bdr_indexer` directory, create the uv-managed environment and install the application and development dependencies:

```bash
uv sync --group local
```

Run tests from the `bdr_indexer` directory:

```bash
uv run ./run_tests.py
```

Our code update scripts specify the uv syntax for updating our servers.

---
