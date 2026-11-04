"""The chart ships exactly what the pipeline code compiles to."""

from compile_pipelines import CHART_FILES, compile_all


def test_chart_ships_exactly_the_compiled_pipelines(tmp_path):
    """Fails when a compiled pipeline in the chart is missing, extra or out of date."""
    built = compile_all(tmp_path)
    shipped = {
        str(path.relative_to(CHART_FILES)): path.read_text() for path in CHART_FILES.rglob("*.yaml")
    }
    assert shipped == built
