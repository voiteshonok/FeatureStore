def test_import() -> None:
    import schemas

    assert schemas.__name__ == "schemas"
