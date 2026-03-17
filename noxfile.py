"""Nox config."""

import nox
import nox_uv

# Options to modify nox behaviour
nox.options.default_venv_backend = "uv"
nox.options.reuse_existing_virtualenvs = True
nox.options.sessions = [
    "lint",
    "tests",
]

ALL_PYTHON = [
    "3.12",
    "3.13",
    "3.14",
]


@nox_uv.session(
    uv_no_install_project=True,
    uv_only_groups=["build"],
)
def build(session: nox.Session) -> None:
    """Build an SDist and wheel."""
    session.run("python", "-m", "build")


@nox_uv.session(
    python=ALL_PYTHON,
    uv_groups=["test"],
)
def coverage(session: nox.Session) -> None:
    """Run tests and compute coverage for the tests."""
    session.run("pytest", "--cov", "--cov-report=xml", *session.posargs)


@nox_uv.session(uv_groups=["docs"])
def docs(session: nox.Session) -> None:
    """Build the docs. Pass "serve" to serve."""
    session.run("zensical", "build", "--strict")

    if session.posargs:
        if "serve" in session.posargs:
            session.run("zensical", "serve", "--strict")
        else:
            session.log("Unsupported argument to docs")


@nox_uv.session(
    uv_no_install_project=True,
    uv_only_groups=["lint"],
)
def lint(session: nox.Session) -> None:
    """Run the linter."""
    session.run("prek", "run", "--all-files", *session.posargs)


@nox_uv.session(
    python=ALL_PYTHON,
    uv_groups=["test"],
)
def tests(session: nox.Session) -> None:
    """Run the unit tests."""
    session.run("pytest", *session.posargs)


@nox_uv.session
def version(session: nox.Session) -> None:
    """
    Check the current version of the package.

    The intent of this check is to ensure that the package
    is installed without any additional dependencies
    through optional dependencies nor dependency groups.

    """
    session.run("python", "-c", "import xmigrate; print(xmigrate.__version__)")
