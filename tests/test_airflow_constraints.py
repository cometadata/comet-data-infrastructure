import importlib.util
from pathlib import Path

SCRIPT = Path(__file__).resolve().parents[1] / "scripts" / "airflow_constraints.py"

spec = importlib.util.spec_from_file_location("airflow_constraints", SCRIPT)
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)

PYPROJECT = '[tool.uv]\nconstraint-dependencies = ["dnspython>=2.9.2", "Flask_Login>=0.6"]\n'
CONSTRAINTS = "# generated\n\ndnspython==2.9.1\nFlask-Login==0.6.3\ntornado==6.6\n"


def test_sets_aside_only_lines_below_a_minimum():
    kept, set_aside = module.apply_minimums(CONSTRAINTS, module.read_minimums(PYPROJECT))

    assert kept == "# generated\n\nFlask-Login==0.6.3\ntornado==6.6\n"
    assert set_aside == [("dnspython==2.9.1", "dnspython>=2.9.2")]


def test_no_constraint_dependencies_keeps_everything():
    assert module.apply_minimums(CONSTRAINTS, module.read_minimums("[tool.uv]\n")) == (CONSTRAINTS, [])


def test_main_rewrites_the_file_and_reports(tmp_path, capsys):
    constraints = tmp_path / "constraints.txt"
    constraints.write_text(CONSTRAINTS)
    pyproject = tmp_path / "pyproject.toml"
    pyproject.write_text(PYPROJECT)

    module.main([str(constraints), str(pyproject)])

    assert "dnspython" not in constraints.read_text()
    assert (
        "Setting aside Airflow's dnspython==2.9.1: pyproject.toml requires dnspython>=2.9.2" in capsys.readouterr().out
    )


def test_names_match_whatever_their_case_or_punctuation():
    minimums = module.read_minimums('[tool.uv]\nconstraint-dependencies = ["Flask_Login>=0.7"]\n')

    kept, set_aside = module.apply_minimums("Flask-Login==0.6.3\nflask.login==0.6.3\n", minimums)

    assert kept == ""
    assert [line for line, _ in set_aside] == ["Flask-Login==0.6.3", "flask.login==0.6.3"]
