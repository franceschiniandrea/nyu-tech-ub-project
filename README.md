# TECH-UB 24 Project - Village Idiots

## Quickstart

### Running the Project

We are using Docker to spin up the Tardis Machine, and `uv` to handle the dependencies of the project.

First clone the repo, and then run

```bash
uv sync
uv run pip install -e . 
```

This will create a virtual environment, sync the dependencies to the ones we are using and install the project locally.

Then spin up the Tardis Machine with

```bash
docker compose up
```

Then you can run the main loop using 

```bash
uv run ./crypto_hft/main_loop.py
```

### Devs Quickstart

Follow the same instructions as above to clone the project and install the dependencies. 

If you use a new dependency, remember to add it to the dependencies list using

```bash
uv add <package>
```

This makes sure our dependencies versions are synced. To run any file, you can run

```bash
uv run <path-to-file>
```