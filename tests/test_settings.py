import pytest

from osm_spark_import.settings import Settings


def test_from_env(monkeypatch):
    monkeypatch.setenv("OSM_SRID", "4326")
    monkeypatch.setenv("OSM_STORAGE_ROOT", "hdfs://nn:8020/osm/")
    monkeypatch.setenv("OSM_WEBHDFS_KERBEROS", "true")
    monkeypatch.setenv("OSM_NAMESPACE", "")  # empty: default
    s = Settings.from_env()
    assert s.srid == 4326 and s.storage_root == "hdfs://nn:8020/osm" and s.webhdfs_kerberos is True
    assert s.namespace == "osm" and s.outbox == "hdfs://nn:8020/osm/outbox/osm"
    monkeypatch.setenv("OSM_WEBHDFS_KERBEROS", "0")
    assert Settings.from_env().webhdfs_kerberos is False


@pytest.mark.parametrize("name", ["Planet", "1x", "a-b", "a.b", "a;drop"])
def test_namespace_is_validated(name):
    with pytest.raises(SystemExit):
        Settings(namespace=name)


def test_mapping_from_path_or_url(tmp_path):
    from test_osm import DEFAULT_MAPPING

    for uri in (DEFAULT_MAPPING, f"file://{DEFAULT_MAPPING}"):
        mp, text = Settings(mapping=uri).load_mapping()
        assert "planet_osm_point" in mp.columns and text
    with pytest.raises(SystemExit, match="not found"):
        Settings(mapping=str(tmp_path / "missing.yml")).load_mapping()
