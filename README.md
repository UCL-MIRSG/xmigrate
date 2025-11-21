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

To use direnv, you need to create a `.envrc` file with 
`layout python-venv python3.13` which is specified in `.direnvrc`:

```sh
layout_python-venv() {
    local python=${1:-python3}
    [[ $# -gt 0 ]] && shift
    unset PYTHONHOME
    if [[ -n $VIRTUAL_ENV ]]; then
        VIRTUAL_ENV=$(realpath "${VIRTUAL_ENV}")
    else
        local python_version
        python_version=$("$python" -c "import platform; print(platform.python_version())")
        if [[ -z $python_version ]]; then
            log_error "Could not detect Python version"
            return 1
        fi
        VIRTUAL_ENV=$PWD/.direnv/python-venv-$python_version
    fi
    export VIRTUAL_ENV
    if [[ ! -d $VIRTUAL_ENV ]]; then
        log_status "no venv found; creating $VIRTUAL_ENV"
        "$python" -m venv "$VIRTUAL_ENV"
    fi

    PATH="${VIRTUAL_ENV}/bin:${PATH}"
    export PATH
}
```

Then run

```sh
direnv allow
xmigrate migrate
```

`eval "$(direnv hook zsh)"`
