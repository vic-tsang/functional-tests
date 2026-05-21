"""Tests for $near core functionality — GeoJSON, distance, field paths, interactions."""

import pytest

from documentdb_tests.compatibility.tests.core.operator.query.utils.query_test_case import (
    QueryTestCase,
)
from documentdb_tests.framework.assertions import assertSuccess
from documentdb_tests.framework.executor import execute_command
from documentdb_tests.framework.parametrize import pytest_params
from documentdb_tests.framework.test_constants import INT64_MAX

GEOJSON_CORE_TESTS: list[QueryTestCase] = [
    QueryTestCase(
        id="basic_nearest_first",
        filter={
            "loc": {
                "$near": {
                    "$geometry": {"type": "Point", "coordinates": [0, 0]},
                }
            }
        },
        doc=[
            {"_id": 1, "loc": {"type": "Point", "coordinates": [5, 5]}},
            {"_id": 2, "loc": {"type": "Point", "coordinates": [0, 0]}},
            {"_id": 3, "loc": {"type": "Point", "coordinates": [1, 1]}},
        ],
        expected=[
            {"_id": 2, "loc": {"type": "Point", "coordinates": [0, 0]}},
            {"_id": 3, "loc": {"type": "Point", "coordinates": [1, 1]}},
            {"_id": 1, "loc": {"type": "Point", "coordinates": [5, 5]}},
        ],
        msg="Should return documents sorted nearest to farthest",
    ),
    QueryTestCase(
        id="maxDistance_filters",
        filter={
            "loc": {
                "$near": {
                    "$geometry": {"type": "Point", "coordinates": [0, 0]},
                    "$maxDistance": 200000,
                }
            }
        },
        doc=[
            {"_id": 1, "loc": {"type": "Point", "coordinates": [0, 0]}},
            {"_id": 2, "loc": {"type": "Point", "coordinates": [1, 1]}},
            {"_id": 3, "loc": {"type": "Point", "coordinates": [50, 50]}},
        ],
        expected=[
            {"_id": 1, "loc": {"type": "Point", "coordinates": [0, 0]}},
            {"_id": 2, "loc": {"type": "Point", "coordinates": [1, 1]}},
        ],
        msg="Should filter documents beyond $maxDistance",
    ),
    QueryTestCase(
        id="minDistance_excludes_close",
        filter={
            "loc": {
                "$near": {
                    "$geometry": {"type": "Point", "coordinates": [0, 0]},
                    "$minDistance": 100000,
                }
            }
        },
        doc=[
            {"_id": 1, "loc": {"type": "Point", "coordinates": [0, 0]}},
            {"_id": 2, "loc": {"type": "Point", "coordinates": [5, 5]}},
        ],
        expected=[
            {"_id": 2, "loc": {"type": "Point", "coordinates": [5, 5]}},
        ],
        msg="Should exclude documents closer than $minDistance",
    ),
    QueryTestCase(
        id="ring_query",
        filter={
            "loc": {
                "$near": {
                    "$geometry": {"type": "Point", "coordinates": [0, 0]},
                    "$minDistance": 100000,
                    "$maxDistance": 1000000,
                }
            }
        },
        doc=[
            {"_id": 1, "loc": {"type": "Point", "coordinates": [0, 0]}},
            {"_id": 2, "loc": {"type": "Point", "coordinates": [3, 3]}},
            {"_id": 3, "loc": {"type": "Point", "coordinates": [50, 50]}},
        ],
        expected=[
            {"_id": 2, "loc": {"type": "Point", "coordinates": [3, 3]}},
        ],
        msg="Should return only documents in ring between $minDistance and $maxDistance",
    ),
    QueryTestCase(
        id="empty_result",
        filter={
            "loc": {
                "$near": {
                    "$geometry": {"type": "Point", "coordinates": [0, 0]},
                    "$maxDistance": 1,
                }
            }
        },
        doc=[
            {"_id": 1, "loc": {"type": "Point", "coordinates": [50, 50]}},
        ],
        expected=[],
        msg="Should return empty when no documents within distance",
    ),
    QueryTestCase(
        id="cross_antimeridian",
        filter={
            "loc": {
                "$near": {
                    "$geometry": {"type": "Point", "coordinates": [179, 0]},
                    "$maxDistance": 300000,
                }
            }
        },
        doc=[
            {"_id": 1, "loc": {"type": "Point", "coordinates": [179, 0]}},
            {"_id": 2, "loc": {"type": "Point", "coordinates": [-179, 0]}},
            {"_id": 3, "loc": {"type": "Point", "coordinates": [0, 0]}},
        ],
        expected=[
            {"_id": 1, "loc": {"type": "Point", "coordinates": [179, 0]}},
            {"_id": 2, "loc": {"type": "Point", "coordinates": [-179, 0]}},
        ],
        msg="Should find nearby point across antimeridian (2 degrees apart, not 358)",
    ),
]

NULL_MISSING_TESTS: list[QueryTestCase] = [
    QueryTestCase(
        id="null_location_not_matched",
        filter={
            "loc": {
                "$near": {
                    "$geometry": {"type": "Point", "coordinates": [0, 0]},
                    "$maxDistance": 1000000,
                }
            }
        },
        doc=[
            {"_id": 1, "loc": {"type": "Point", "coordinates": [0, 0]}},
            {"_id": 2, "loc": None},
        ],
        expected=[
            {"_id": 1, "loc": {"type": "Point", "coordinates": [0, 0]}},
        ],
        msg="Should not match documents with null location",
    ),
    QueryTestCase(
        id="missing_location_not_matched",
        filter={
            "loc": {
                "$near": {
                    "$geometry": {"type": "Point", "coordinates": [0, 0]},
                    "$maxDistance": 1000000,
                }
            }
        },
        doc=[
            {"_id": 1, "loc": {"type": "Point", "coordinates": [0, 0]}},
            {"_id": 2, "other": "value"},
        ],
        expected=[
            {"_id": 1, "loc": {"type": "Point", "coordinates": [0, 0]}},
        ],
        msg="Should not match documents with missing location field",
    ),
]

VALID_INTERACTION_TESTS: list[QueryTestCase] = [
    QueryTestCase(
        id="near_inside_and",
        filter={
            "$and": [
                {"loc": {"$near": {"$geometry": {"type": "Point", "coordinates": [0, 0]}}}},
                {"category": "A"},
            ]
        },
        doc=[
            {"_id": 1, "loc": {"type": "Point", "coordinates": [0, 0]}, "category": "A"},
            {"_id": 2, "loc": {"type": "Point", "coordinates": [1, 1]}, "category": "B"},
            {"_id": 3, "loc": {"type": "Point", "coordinates": [2, 2]}, "category": "A"},
        ],
        expected=[
            {"_id": 1, "loc": {"type": "Point", "coordinates": [0, 0]}, "category": "A"},
            {"_id": 3, "loc": {"type": "Point", "coordinates": [2, 2]}, "category": "A"},
        ],
        msg="Should work inside $and with additional filter",
    ),
    QueryTestCase(
        id="near_with_equality_other_field",
        filter={
            "loc": {"$near": {"$geometry": {"type": "Point", "coordinates": [0, 0]}}},
            "category": "A",
        },
        doc=[
            {"_id": 1, "loc": {"type": "Point", "coordinates": [0, 0]}, "category": "A"},
            {"_id": 2, "loc": {"type": "Point", "coordinates": [1, 1]}, "category": "B"},
        ],
        expected=[
            {"_id": 1, "loc": {"type": "Point", "coordinates": [0, 0]}, "category": "A"},
        ],
        msg="Should combine with equality on another field",
    ),
]


DISTANCE_TESTS: list[QueryTestCase] = [
    QueryTestCase(
        id="spherical_distance_excludes",
        filter={
            "loc": {
                "$near": {
                    "$geometry": {"type": "Point", "coordinates": [0, 0]},
                    "$maxDistance": 50000,
                }
            }
        },
        doc=[
            {"_id": 1, "loc": {"type": "Point", "coordinates": [0, 0]}},
            {"_id": 2, "loc": {"type": "Point", "coordinates": [1, 0]}},
        ],
        expected=[{"_id": 1, "loc": {"type": "Point", "coordinates": [0, 0]}}],
        msg="Should use spherical distance (meters) — 50km excludes point at 1 degree",
    ),
    QueryTestCase(
        id="spherical_distance_includes",
        filter={
            "loc": {
                "$near": {
                    "$geometry": {"type": "Point", "coordinates": [0, 0]},
                    "$maxDistance": 120000,
                }
            }
        },
        doc=[
            {"_id": 1, "loc": {"type": "Point", "coordinates": [0, 0]}},
            {"_id": 2, "loc": {"type": "Point", "coordinates": [1, 0]}},
        ],
        expected=[
            {"_id": 1, "loc": {"type": "Point", "coordinates": [0, 0]}},
            {"_id": 2, "loc": {"type": "Point", "coordinates": [1, 0]}},
        ],
        msg="Should include point within 120km",
    ),
    QueryTestCase(
        id="minDistance_greater_than_maxDistance_empty",
        filter={
            "loc": {
                "$near": {
                    "$geometry": {"type": "Point", "coordinates": [0, 0]},
                    "$minDistance": 500000,
                    "$maxDistance": 100000,
                }
            }
        },
        doc=[
            {"_id": 1, "loc": {"type": "Point", "coordinates": [0, 0]}},
            {"_id": 2, "loc": {"type": "Point", "coordinates": [1, 0]}},
        ],
        expected=[],
        msg="Should return empty when $minDistance > $maxDistance",
    ),
    QueryTestCase(
        id="very_large_maxDistance",
        filter={
            "loc": {
                "$near": {
                    "$geometry": {"type": "Point", "coordinates": [0, 0]},
                    "$maxDistance": 50000000,
                }
            }
        },
        doc=[
            {"_id": 1, "loc": {"type": "Point", "coordinates": [0, 0]}},
            {"_id": 2, "loc": {"type": "Point", "coordinates": [90, 45]}},
            {"_id": 3, "loc": {"type": "Point", "coordinates": [-120, -30]}},
        ],
        expected=[
            {"_id": 1, "loc": {"type": "Point", "coordinates": [0, 0]}},
            {"_id": 2, "loc": {"type": "Point", "coordinates": [90, 45]}},
            {"_id": 3, "loc": {"type": "Point", "coordinates": [-120, -30]}},
        ],
        msg="Should return all documents with very large $maxDistance",
    ),
    QueryTestCase(
        id="maxDistance_int64_max",
        filter={
            "loc": {
                "$near": {
                    "$geometry": {"type": "Point", "coordinates": [0, 0]},
                    "$maxDistance": INT64_MAX,
                }
            }
        },
        doc=[
            {"_id": 1, "loc": {"type": "Point", "coordinates": [0, 0]}},
            {"_id": 2, "loc": {"type": "Point", "coordinates": [180, 0]}},
        ],
        expected=[
            {"_id": 1, "loc": {"type": "Point", "coordinates": [0, 0]}},
            {"_id": 2, "loc": {"type": "Point", "coordinates": [180, 0]}},
        ],
        msg="Should return all documents with $maxDistance=INT64_MAX",
    ),
    QueryTestCase(
        id="very_small_maxDistance",
        filter={
            "loc": {
                "$near": {
                    "$geometry": {"type": "Point", "coordinates": [0, 0]},
                    "$maxDistance": 0.001,
                }
            }
        },
        doc=[
            {"_id": 1, "loc": {"type": "Point", "coordinates": [0, 0]}},
            {"_id": 2, "loc": {"type": "Point", "coordinates": [0.001, 0]}},
        ],
        expected=[{"_id": 1, "loc": {"type": "Point", "coordinates": [0, 0]}}],
        msg="Should return only exact point with very small $maxDistance",
    ),
    QueryTestCase(
        id="zero_maxDistance",
        filter={
            "loc": {
                "$near": {
                    "$geometry": {"type": "Point", "coordinates": [0, 0]},
                    "$maxDistance": 0,
                }
            }
        },
        doc=[
            {"_id": 1, "loc": {"type": "Point", "coordinates": [0, 0]}},
            {"_id": 2, "loc": {"type": "Point", "coordinates": [0.001, 0]}},
        ],
        expected=[{"_id": 1, "loc": {"type": "Point", "coordinates": [0, 0]}}],
        msg="Should return only exact-match point with $maxDistance=0",
    ),
]


@pytest.mark.usefixtures("geo_2dsphere")
@pytest.mark.parametrize(
    "test",
    pytest_params(
        GEOJSON_CORE_TESTS + NULL_MISSING_TESTS + VALID_INTERACTION_TESTS + DISTANCE_TESTS
    ),
)
def test_near_core(collection, test):
    """Verifies $near core functionality."""
    collection.insert_many(test.doc)
    result = execute_command(collection, {"find": collection.name, "filter": test.filter})
    assertSuccess(result, test.expected, msg=test.msg)


def test_near_nested_field(collection):
    """Verifies $near works on nested field path."""
    collection.create_index([("address.location", "2dsphere")])
    collection.insert_many(
        [
            {"_id": 1, "address": {"location": {"type": "Point", "coordinates": [0, 0]}}},
            {"_id": 2, "address": {"location": {"type": "Point", "coordinates": [5, 5]}}},
        ]
    )
    result = execute_command(
        collection,
        {
            "find": collection.name,
            "filter": {
                "address.location": {
                    "$near": {"$geometry": {"type": "Point", "coordinates": [0, 0]}}
                }
            },
        },
    )
    assertSuccess(
        result,
        [
            {"_id": 1, "address": {"location": {"type": "Point", "coordinates": [0, 0]}}},
            {"_id": 2, "address": {"location": {"type": "Point", "coordinates": [5, 5]}}},
        ],
        msg="Should work on nested field path",
    )
