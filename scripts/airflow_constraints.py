# /// script
# requires-python = ">=3.12"
# dependencies = ["packaging==26.3"]  # the version in uv.lock, so a bump never runs a release nobody has vetted
# ///
"""Set aside the Airflow constraints that fall below a minimum in pyproject.toml.

Usage: uv run --no-project scripts/airflow_constraints.py CONSTRAINTS_FILE PYPROJECT_FILE

make bump-airflow runs this on Airflow's constraints file before resolving. Airflow's constraints pin every
package, sometimes to a version with a known vulnerability; [tool.uv] constraint-dependencies in pyproject.toml
holds the minimums that must win. A constraint that satisfies its minimum is kept, so Airflow's pin applies again
once it catches up.
"""

from pathlib import Path
import sys
import tomllib

from packaging.requirements import Requirement
from packaging.utils import canonicalize_name


def read_minimums(pyproject_text: str) -> dict[str, Requirement]:
    """Return pyproject.toml's constraint-dependencies, keyed by normalised package name."""
    entries = tomllib.loads(pyproject_text).get("tool", {}).get("uv", {}).get("constraint-dependencies", [])
    requirements = [Requirement(entry) for entry in entries]
    return {canonicalize_name(requirement.name): requirement for requirement in requirements}


def apply_minimums(constraints: str, minimums: dict[str, Requirement]) -> tuple[str, list[tuple[str, str]]]:
    """Drop the `name==version` lines whose version is outside that package's minimum.

    Returns the remaining text and, for each dropped line, the line and the requirement that overrode it.
    Comments, blank lines and anything else that is not a plain pin are kept as they are.
    """
    kept = []
    set_aside = []
    for line in constraints.splitlines(keepends=True):
        name, separator, version = line.strip().partition("==")
        requirement = minimums.get(canonicalize_name(name)) if separator else None
        if requirement and not requirement.specifier.contains(version, prereleases=True):
            set_aside.append((line.strip(), str(requirement)))
        else:
            kept.append(line)
    return "".join(kept), set_aside


def main(argv: list[str]) -> None:
    """Rewrite the constraints file in place and print what was set aside."""
    constraints_path, pyproject_path = (Path(argument) for argument in argv)
    kept, set_aside = apply_minimums(constraints_path.read_text(), read_minimums(pyproject_path.read_text()))
    constraints_path.write_text(kept)
    for line, requirement in set_aside:
        print(f"Setting aside Airflow's {line}: pyproject.toml requires {requirement}")


if __name__ == "__main__":
    main(sys.argv[1:])
