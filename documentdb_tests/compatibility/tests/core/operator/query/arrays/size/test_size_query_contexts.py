"""
Tests for $size query contexts.

Covers negation ($not), query contexts (find, aggregate $match,
find with sort/limit/projection), and $pull context.
"""

import pytest

from documentdb_tests.compatibility.tests.core.operator.query.utils.query_test_case import (
    QueryTestCase,
)
from documentdb_tests.framework.assertions import assertSuccess
from documentdb_tests.framework.executor import execute_command
from documentdb_tests.framework.parametrize import pytest_params

NEGATION_TESTS: list[QueryTestCase] = [
    QueryTestCase(
        id="not_size_2_matches_other_sizes",
        filter={"a": {"$not": {"$size": 2}}},
        doc=[
            {"_id": 1, "a": [1]},
            {"_id": 2, "a": [1, 2]},
            {"_id": 3, "a": [1, 2, 3]},
        ],
        expected=[{"_id": 1, "a": [1]}, {"_id": 3, "a": [1, 2, 3]}],
        msg="$not $size 2 matches arrays of size != 2",
    ),
    QueryTestCase(
        id="not_size_includes_null",
        filter={"a": {"$not": {"$size": 2}}},
        doc=[
            {"_id": 1, "a": [1, 2]},
            {"_id": 2, "a": None},
        ],
        expected=[{"_id": 2, "a": None}],
        msg="$not $size 2 matches docs where field is null",
    ),
    QueryTestCase(
        id="not_size_includes_missing",
        filter={"a": {"$not": {"$size": 2}}},
        doc=[
            {"_id": 1, "a": [1, 2]},
            {"_id": 2, "b": 1},
        ],
        expected=[{"_id": 2, "b": 1}],
        msg="$not $size 2 matches docs where field is missing",
    ),
    QueryTestCase(
        id="not_size_includes_scalar",
        filter={"a": {"$not": {"$size": 2}}},
        doc=[
            {"_id": 1, "a": [1, 2]},
            {"_id": 2, "a": "scalar"},
        ],
        expected=[{"_id": 2, "a": "scalar"}],
        msg="$not $size 2 matches docs where field is scalar",
    ),
    QueryTestCase(
        id="not_size_0_matches_nonempty_and_nonarray",
        filter={"a": {"$not": {"$size": 0}}},
        doc=[
            {"_id": 1, "a": []},
            {"_id": 2, "a": [1]},
            {"_id": 3, "a": "x"},
            {"_id": 4, "b": 1},
        ],
        expected=[
            {"_id": 2, "a": [1]},
            {"_id": 3, "a": "x"},
            {"_id": 4, "b": 1},
        ],
        msg="$not $size 0 matches non-empty arrays and non-array fields",
    ),
]


@pytest.mark.parametrize("test", pytest_params(NEGATION_TESTS))
def test_size_query_contexts(collection, test):
    """Parametrized test for $size negation query contexts."""
    if test.doc:
        collection.insert_many(test.doc)
    result = execute_command(collection, {"find": collection.name, "filter": test.filter})
    assertSuccess(result, test.expected, ignore_doc_order=True)


def test_size_in_aggregate_match(collection):
    """Test $size in aggregate $match stage."""
    collection.insert_many(
        [
            {"_id": 1, "a": [1, 2]},
            {"_id": 2, "a": [1]},
            {"_id": 3, "a": [1, 2, 3]},
        ]
    )
    result = execute_command(
        collection,
        {
            "aggregate": collection.name,
            "pipeline": [{"$match": {"a": {"$size": 2}}}],
            "cursor": {},
        },
    )
    assertSuccess(result, [{"_id": 1, "a": [1, 2]}])


def test_size_find_with_sort(collection):
    """Test $size in find() with sort."""
    collection.insert_many(
        [
            {"_id": 2, "a": [3, 4]},
            {"_id": 1, "a": [1, 2]},
            {"_id": 3, "a": [5]},
        ]
    )
    result = execute_command(
        collection,
        {
            "find": collection.name,
            "filter": {"a": {"$size": 2}},
            "sort": {"_id": 1},
        },
    )
    assertSuccess(result, [{"_id": 1, "a": [1, 2]}, {"_id": 2, "a": [3, 4]}])


def test_size_find_with_limit(collection):
    """Test $size in find() with limit."""
    collection.insert_many(
        [
            {"_id": 1, "a": [1]},
            {"_id": 2, "a": [2]},
            {"_id": 3, "a": [1, 2]},
        ]
    )
    result = execute_command(
        collection,
        {
            "find": collection.name,
            "filter": {"a": {"$size": 1}},
            "limit": 1,
        },
    )
    assertSuccess(result, [{"_id": 1, "a": [1]}])


def test_size_find_with_projection(collection):
    """Test $size in find() with projection."""
    collection.insert_many(
        [
            {"_id": 1, "a": [1, 2], "b": "x"},
            {"_id": 2, "a": [1], "b": "y"},
        ]
    )
    result = execute_command(
        collection,
        {
            "find": collection.name,
            "filter": {"a": {"$size": 2}},
            "projection": {"a": 1},
        },
    )
    assertSuccess(result, [{"_id": 1, "a": [1, 2]}])


def test_size_in_pull_context(collection):
    """Test $pull with $size condition removes matching array elements."""
    collection.insert_one({"_id": 1, "a": [[1, 2], [3], [4, 5]]})
    execute_command(
        collection,
        {
            "update": collection.name,
            "updates": [{"q": {"_id": 1}, "u": {"$pull": {"a": {"$size": 2}}}}],
        },
    )
    find_result = execute_command(collection, {"find": collection.name, "filter": {"_id": 1}})
    assertSuccess(find_result, [{"_id": 1, "a": [[3]]}])
