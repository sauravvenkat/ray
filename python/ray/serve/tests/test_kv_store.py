import pytest

from ray.serve._private.storage.kv_store import get_storage_key, RayInternalKVStore


def test_get_storage_key(serve_instance):  # noqa: F811
    assert get_storage_key("", "my_key") == "-my_key"
    assert get_storage_key("my_key", "test_ns") == "test_ns-my_key"


def test_ray_internal_kv(serve_instance):  # noqa: F811
    with pytest.raises(TypeError):
        RayInternalKVStore(namespace=1)
        RayInternalKVStore(namespace=b"")

    kv = RayInternalKVStore()

    with pytest.raises(TypeError):
        kv.put(1, b"1")
    with pytest.raises(TypeError):
        kv.put("1", 1)
    with pytest.raises(TypeError):
        kv.put("1", "1")

    kv.put("1", b"2")
    assert kv.get("1") == b"2"
    kv.put("2", b"4")
    assert kv.get("2") == b"4"
    kv.put("1", b"3")
    assert kv.get("1") == b"3"
    assert kv.get("2") == b"4"


def test_ray_internal_kv_collisions(serve_instance):  # noqa: F811
    kv1 = RayInternalKVStore()
    kv1.put("1", b"1")
    assert kv1.get("1") == b"1"

    kv2 = RayInternalKVStore("namespace")

    assert kv2.get("1") is None

    kv2.put("1", b"-1")
    assert kv2.get("1") == b"-1"
    assert kv1.get("1") == b"1"


def test_ray_internal_kv_delete_type_errors(serve_instance):  # noqa: F811
    kv = RayInternalKVStore()
    with pytest.raises(TypeError):
        kv.delete(1)
    with pytest.raises(TypeError):
        kv.delete(b"1")


def test_ray_internal_kv_delete_basic(serve_instance):  # noqa: F811
    kv = RayInternalKVStore()
    kv.put("del_test", b"v")
    assert kv.get("del_test") == b"v"
    # delete should remove the entry
    kv.delete("del_test")
    assert kv.get("del_test") is None


def test_ray_internal_kv_delete_collisions(serve_instance):  # noqa: F811
    kv1 = RayInternalKVStore()
    kv2 = RayInternalKVStore("namespace")

    kv1.put("k", b"1")
    kv2.put("k", b"2")

    # Deleting in kv2 (different namespace) should not affect kv1
    kv2.delete("k")
    assert kv2.get("k") is None
    assert kv1.get("k") == b"1"

    # Deleting in kv1 now should remove its value
    kv1.delete("k")
    assert kv1.get("k") is None


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", "-s", __file__]))
