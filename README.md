# tap-logic4

`tap-logic4` is a Singer tap for Logic4.

## Installation
```bash
pipx install tap-logic4
```

## Configuration

### Accepted Config Options

A full list of supported settings and capabilities for this
tap is available by running:

```bash
tap-logic4 --about
```

### Configure using environment variables

This Singer tap will automatically import any environment variables within the working directory's
`.env` if the `--config=ENV` is provided, such that config values will be considered if a matching
environment variable is set either in the terminal context or in the `.env` file.

## Usage

You can easily run `tap-logic4` by itself or in a pipeline.

### Executing the Tap Directly

```bash
tap-logic4 --version
tap-logic4 --help
tap-logic4 --config CONFIG --discover > ./catalog.json
```

### Initialize your Development Environment

```bash
pipx install poetry
poetry install
```

### Create and Run Tests

Create tests within the `tap_logic4/tests` subfolder and
  then run:

```bash
poetry run pytest
```

You can also test the `tap-logic4` CLI interface directly using `poetry run`:

```bash
poetry run tap-logic4 --help
```
