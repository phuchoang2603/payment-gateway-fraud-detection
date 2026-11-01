from compile_pipelines import CHART_FILES, compile_all


def test_chart_ships_exactly_the_compiled_pipelines(tmp_path):
    built = compile_all(tmp_path)
    shipped = {path.name: path.read_text() for path in CHART_FILES.glob("*.yaml")}
    assert shipped == {f"{name}.yaml": text for name, text in built.items()}
