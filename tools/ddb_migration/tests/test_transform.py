from transform import transform


def test_identity_passes_item_unchanged() -> None:
    item = {"pk": "a", "sk": "1", "amount": 5}
    assert transform(item) == item


def test_returns_none_means_skip() -> None:
    """Caller contract: returning None signals 'skip this item.'

    The default identity transform never returns None, but a user-supplied
    transform can. This test documents the contract via a sample.
    """

    def filter_deleted(item, source_event=None):
        if item.get("status") == "DELETED":
            return None
        return item

    assert filter_deleted({"pk": "a", "status": "ACTIVE"}) is not None
    assert filter_deleted({"pk": "a", "status": "DELETED"}) is None


def test_source_event_argument_is_optional() -> None:
    transform({"pk": "a"}, source_event=None)
    transform({"pk": "a"}, source_event={"eventName": "INSERT"})
