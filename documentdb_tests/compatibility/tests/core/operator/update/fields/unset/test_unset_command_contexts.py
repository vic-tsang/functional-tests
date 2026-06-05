"""
Tests for $unset update operator - command contexts.

Covers updateOne, updateMany, findOneAndUpdate, upsert, and nModified wiring.
"""

import pytest

from documentdb_tests.compatibility.tests.core.operator.update.utils.update_test_case import (
    UpdateTestCase,
)
from documentdb_tests.framework.assertions import assertSuccess, assertSuccessPartial
from documentdb_tests.framework.executor import execute_command
from documentdb_tests.framework.parametrize import pytest_params

UNSET_COMMAND_CONTEXT_TESTS: list[UpdateTestCase] = [
    UpdateTestCase(
        id="upsert_no_match",
        setup_docs=[],
        query={"_id": 1},
        update={"$unset": {"missing": ""}},
        upsert=True,
        expected=[{"_id": 1}],
        msg="Upsert with $unset should create doc without field",
    ),
]


@pytest.mark.parametrize("test", pytest_params(UNSET_COMMAND_CONTEXT_TESTS))
def test_unset_command_contexts(collection, test):
    """Test $unset in various command contexts."""
    if test.setup_docs:
        collection.insert_many(test.setup_docs)
    execute_command(
        collection,
        {
            "update": collection.name,
            "updates": [{"q": test.query, "u": test.update, "upsert": test.upsert}],
        },
    )
    result = execute_command(collection, {"find": collection.name, "filter": {"_id": 1}})
    assertSuccess(result, test.expected, msg=test.msg)


def test_unset_update_one(collection):
    """Test $unset in updateOne removes field from one doc."""
    collection.insert_many([{"_id": 1, "a": 1, "b": 2}, {"_id": 2, "a": 1, "b": 2}])
    result = execute_command(
        collection,
        {
            "update": collection.name,
            "updates": [{"q": {"a": 1}, "u": {"$unset": {"b": ""}}}],
        },
    )
    assertSuccessPartial(result, {"n": 1, "nModified": 1, "ok": 1.0}, msg="Should unset in one doc")


def test_unset_nmodified_zero_when_field_missing(collection):
    """Test $unset on non-existent field returns nModified: 0."""
    collection.insert_one({"_id": 1, "a": 1})
    result = execute_command(
        collection,
        {
            "update": collection.name,
            "updates": [{"q": {"_id": 1}, "u": {"$unset": {"missing": ""}}}],
        },
    )
    assertSuccessPartial(
        result,
        {"n": 1, "nModified": 0, "ok": 1.0},
        msg="Unset non-existent field should have nModified: 0",
    )


def test_unset_update_many(collection):
    """Test $unset in updateMany removes field from all matching."""
    collection.insert_many([{"_id": 1, "a": 1, "b": 2}, {"_id": 2, "a": 1, "b": 3}])
    result = execute_command(
        collection,
        {
            "update": collection.name,
            "updates": [{"q": {"a": 1}, "u": {"$unset": {"b": ""}}, "multi": True}],
        },
    )
    assertSuccessPartial(
        result, {"n": 2, "nModified": 2, "ok": 1.0}, msg="Should unset from all matching"
    )


def test_unset_find_and_modify(collection):
    """Test $unset in findAndModify removes field and returns document."""
    collection.insert_one({"_id": 1, "a": 1, "b": 2})
    result = execute_command(
        collection,
        {
            "findAndModify": collection.name,
            "query": {"_id": 1},
            "update": {"$unset": {"b": ""}},
            "new": True,
        },
    )
    assertSuccessPartial(
        result, {"value": {"_id": 1, "a": 1}}, msg="Should return doc without unset field"
    )
