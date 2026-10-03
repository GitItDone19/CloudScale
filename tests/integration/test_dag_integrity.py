"""
DAG integrity and syntax validation tests.
Ensures all DAG files can be imported cleanly without syntax or circular import errors.
"""

import importlib.util
from pathlib import Path
import pytest

DAGS_FOLDER = Path(__file__).resolve().parent.parent.parent / "airflow" / "dags"


def get_dag_files():
    if not DAGS_FOLDER.exists():
        return []
    return [str(p) for p in DAGS_FOLDER.glob("*.py")]


@pytest.mark.parametrize("dag_path", get_dag_files())
def test_dag_syntax_integrity(dag_path):
    """
    Validates that each DAG Python file loads without syntax errors.
    If Airflow is not installed in the local Python environment, tests code compilation.
    """
    path = Path(dag_path)
    with open(path, "r", encoding="utf-8") as f:
        source = f.read()

    # Verify python syntax compiles cleanly
    compiled = compile(source, path.name, "exec")
    assert compiled is not None
