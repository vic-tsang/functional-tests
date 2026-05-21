"""Tests for $near argument handling — valid GeoJSON structures, distance combinations."""

import pytest

from documentdb_tests.compatibility.tests.core.operator.query.utils.query_test_case import (
    QueryTestCase,
)
from documentdb_tests.framework.assertions import assertSuccess
from documentdb_tests.framework.executor import execute_command
from documentdb_tests.framework.parametrize import pytest_params

pytestmark = pytest.mark.usefixtures("geo_2dsphere")

NYC_POINT = [-73.9667, 40.78]


GEOJSON_STRUCTURE_SUCCESS_TESTS: list[QueryTestCase] = [
    QueryTestCase(
        id="missing_type_defaults_to_point",
        filter={"loc": {"$near": {"$geometry": {"coordinates": NYC_POINT}}}},
        doc=[{"_id": 1, "loc": {"type": "Point", "coordinates": NYC_POINT}}],
        expected=[{"_id": 1, "loc": {"type": "Point", "coordinates": NYC_POINT}}],
        msg="Should accept $geometry without type field (defaults to Point)",
    ),
    QueryTestCase(
        id="valid_geojson_point",
        filter={"loc": {"$near": {"$geometry": {"type": "Point", "coordinates": NYC_POINT}}}},
        doc=[{"_id": 1, "loc": {"type": "Point", "coordinates": NYC_POINT}}],
        expected=[{"_id": 1, "loc": {"type": "Point", "coordinates": NYC_POINT}}],
        msg="Should accept valid GeoJSON Point",
    ),
    QueryTestCase(
        id="three_coordinates_altitude",
        filter={
            "loc": {"$near": {"$geometry": {"type": "Point", "coordinates": [-73.9667, 40.78, 0]}}}
        },
        doc=[{"_id": 1, "loc": {"type": "Point", "coordinates": NYC_POINT}}],
        expected=[{"_id": 1, "loc": {"type": "Point", "coordinates": NYC_POINT}}],
        msg="Should accept three coordinates (with altitude)",
    ),
    QueryTestCase(
        id="extra_field_in_geometry",
        filter={
            "loc": {
                "$near": {
                    "$geometry": {
                        "type": "Point",
                        "coordinates": [0, 0],
                        "extra": "field",
                    }
                }
            }
        },
        doc=[{"_id": 1, "loc": {"type": "Point", "coordinates": [0, 0]}}],
        expected=[{"_id": 1, "loc": {"type": "Point", "coordinates": [0, 0]}}],
        msg="Should ignore extra fields inside $geometry",
    ),
]

DISTANCE_COMBINATION_TESTS: list[QueryTestCase] = [
    QueryTestCase(
        id="both_min_and_max",
        filter={
            "loc": {
                "$near": {
                    "$geometry": {"type": "Point", "coordinates": [0, 0]},
                    "$minDistance": 0,
                    "$maxDistance": 1000000,
                }
            }
        },
        doc=[
            {"_id": 1, "loc": {"type": "Point", "coordinates": [0, 0]}},
            {"_id": 2, "loc": {"type": "Point", "coordinates": [5, 5]}},
        ],
        expected=[
            {"_id": 1, "loc": {"type": "Point", "coordinates": [0, 0]}},
            {"_id": 2, "loc": {"type": "Point", "coordinates": [5, 5]}},
        ],
        msg="Should accept both $minDistance and $maxDistance",
    ),
    QueryTestCase(
        id="only_minDistance",
        filter={
            "loc": {
                "$near": {
                    "$geometry": {"type": "Point", "coordinates": [0, 0]},
                    "$minDistance": 0,
                }
            }
        },
        doc=[{"_id": 1, "loc": {"type": "Point", "coordinates": [0, 0]}}],
        expected=[{"_id": 1, "loc": {"type": "Point", "coordinates": [0, 0]}}],
        msg="Should accept only $minDistance",
    ),
]


@pytest.mark.parametrize(
    "test", pytest_params(GEOJSON_STRUCTURE_SUCCESS_TESTS + DISTANCE_COMBINATION_TESTS)
)
def test_near_valid_argument_handling(collection, test):
    """Verifies $near accepts valid GeoJSON structures and distance combinations."""
    collection.insert_many(test.doc)
    result = execute_command(collection, {"find": collection.name, "filter": test.filter})
    assertSuccess(result, test.expected, msg=test.msg)
