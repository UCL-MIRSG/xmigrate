# xmigrate

[![pre-commit](https://img.shields.io/badge/pre--commit-enabled-brightgreen?logo=pre-commit&logoColor=white)](https://github.com/pre-commit/pre-commit)
[![Tests status][tests-badge]][tests-link]
[![Linting status][linting-badge]][linting-link]
[![Documentation status][documentation-badge]][documentation-link]
[![License][license-badge]](./LICENSE.md)
[![Coverage Status](https://coveralls.io/repos/github/UCL-MIRSG/xmigrate/badge.svg?branch=main)](https://coveralls.io/github/UCL-MIRSG/xmigrate?branch=main)

<!-- prettier-ignore-start -->
[tests-badge]:              https://github.com/UCL-MIRSG/xmigrate/actions/workflows/tests.yml/badge.svg
[tests-link]:               https://github.com/UCL-MIRSG/xmigrate/actions/workflows/tests.yml
[linting-badge]:            https://github.com/UCL-MIRSG/xmigrate/actions/workflows/linting.yml/badge.svg
[linting-link]:             https://github.com/UCL-MIRSG/xmigrate/actions/workflows/linting.yml
[documentation-badge]:      https://github.com/UCL-MIRSG/xmigrate/actions/workflows/docs.yml/badge.svg
[documentation-link]:       https://github.com/UCL-MIRSG/xmigrate/actions/workflows/docs.yml
[license-badge]:            https://img.shields.io/badge/License-MIT-yellow.svg
<!-- prettier-ignore-end -->

A Python package to migrate projects from one XNAT to another

## Getting Started

### Installation

<!-- How to build or install the application. -->

We recommend installing in a project specific virtual environment created using
a environment management tool such as
[uv](https://docs.astral.sh/uv/#installation).

First, create a local clone of the repository:

```sh
git clone https://github.com/UCL-MIRSG/xmigrate.git
cd xmigrate
```

Then install `xmigrate` in editable mode using `uv`:

```sh
uv venv --python=3.12
source .venv/bin/activate
uv sync --group test
```

### Running `xmigrate`

First, configure `xmigrate` using `xmigrate.toml`. See `xmigrate.toml.sample`
for an example.

Then run the on the command line the migration for all projects on the source
using:

```sh
xmigrate migrate_all_projects
```

You may only want to migrate a subset of projects on the source using:

```sh
xmigrate migrate_project_list
```

For testing, you may also want to set your environment variables using mise.

N.B. Having a project specific .netrc can be helpful instead of the default
~/.netrc which may contain localhost. This can override the specified ports in
xnat4tests so source and destination XNATs are on same port rather than the
necessary different ports.

You can set up a mise.toml as below:

```sh
[env]
XNAT4TEST_KEEP_INSTANCE=true #This speeds up developer time by keeping the docker containers live
NETRC="path/to/your/.netrc" #Project specific .netrc can be helpful instead of default ~/.netrc
```

and activate it as below:

```sh
mise en
```

For development if you want to install a new package, add it to `pyproject.toml`
dependency-groups and then to update the `uv.lock` file run:

```sh
uv lock
```

To run the integration tests from the command line:

```sh
cd xmigrate
pytest
```

N.B. Currently, the submission object of the custom forms PUT API call is
hardcoded so if the custom forms API changes then this will also need to be
changed.
