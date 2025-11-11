# xmigrate

[![pre-commit](https://img.shields.io/badge/pre--commit-enabled-brightgreen?logo=pre-commit&logoColor=white)](https://github.com/pre-commit/pre-commit)
[![Tests status][tests-badge]][tests-link]
[![Linting status][linting-badge]][linting-link]
[![Documentation status][documentation-badge]][documentation-link]
[![License][license-badge]](./LICENSE.md)

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

Then install `xmigrate` in editable mode:

```sh
uv pip install -e .
```
