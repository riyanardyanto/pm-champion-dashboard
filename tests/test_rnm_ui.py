from src.utils.rnm_helpers import sanitize_linkup


def test_sanitize_linkup_strips_leading_LU():
    assert sanitize_linkup("LU12") == "12"
    assert sanitize_linkup("Lu99") == "99"
    assert sanitize_linkup("luabc") == "abc"


def test_sanitize_linkup_leaves_other_occurences():
    # should only remove leading LU
    assert sanitize_linkup("FOOLU12") == "FOOLU12"
    assert sanitize_linkup("LUXLU") == "XLU"


def test_sanitize_linkup_none_or_empty():
    assert sanitize_linkup("") == ""
    assert sanitize_linkup(None) == ""
