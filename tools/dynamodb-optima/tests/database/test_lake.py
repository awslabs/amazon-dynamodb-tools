def test_get_lake_dir_under_data(tmp_path, monkeypatch):
    from dynamodb_optima import paths
    monkeypatch.setattr(paths, "_project_root_override", tmp_path)
    lake = paths.get_lake_dir()
    assert lake == tmp_path / "data" / "metrics_lake"
