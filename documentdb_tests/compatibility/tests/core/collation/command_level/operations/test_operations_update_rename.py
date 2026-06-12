"""Tests for collation behavior in update with $rename operator.

Tests collation-aware filter matching (case-insensitive, accent-insensitive),
collection-level collation inheritance, and explicit collation override.
"""

import pytest

from documentdb_tests.compatibility.tests.core.utils.command_test_case import (
    CommandContext,
    CommandTestCase,
)
from documentdb_tests.framework.assertions import assertResult
from documentdb_tests.framework.executor import execute_command
from documentdb_tests.framework.parametrize import pytest_params
from documentdb_tests.framework.target_collection import CustomCollection

# Property [Rename Filter with Collation]: collation on the update statement
# affects which documents are matched by the query filter for $rename.
COLLATION_RENAME_FILTER_TESTS: list[CommandTestCase] = [
    CommandTestCase(
        "rename_case_insensitive_filter",
        docs=[
            {"_id": 1, "name": "apple", "old_field": "v1"},
            {"_id": 2, "name": "Apple", "old_field": "v2"},
            {"_id": 3, "name": "banana", "old_field": "v3"},
        ],
        command=lambda ctx: {
            "update": ctx.collection,
            "updates": [
                {
                    "q": {"name": "apple"},
                    "u": {"$rename": {"old_field": "new_field"}},
                    "multi": True,
                    "collation": {"locale": "en", "strength": 2},
                }
            ],
        },
        expected={"ok": 1.0, "n": 2, "nModified": 2},
        msg="$rename with collation should match filter case-insensitively",
    ),
    CommandTestCase(
        "rename_accent_insensitive_filter",
        docs=[
            {"_id": 1, "name": "cafe", "src": 1},
            {"_id": 2, "name": "café", "src": 2},
            {"_id": 3, "name": "other", "src": 3},
        ],
        command=lambda ctx: {
            "update": ctx.collection,
            "updates": [
                {
                    "q": {"name": "cafe"},
                    "u": {"$rename": {"src": "dst"}},
                    "multi": True,
                    "collation": {"locale": "en", "strength": 1},
                }
            ],
        },
        expected={"ok": 1.0, "n": 2, "nModified": 2},
        msg="$rename with strength 1 should match accent variants in filter",
    ),
]

# Property [Rename with Collection Default Collation]: $rename inherits
# collection-level collation for filter matching when no explicit collation
# is specified on the update statement.
COLLATION_RENAME_COLLECTION_DEFAULT_TESTS: list[CommandTestCase] = [
    CommandTestCase(
        "rename_inherits_collection_collation",
        target_collection=CustomCollection(options={"collation": {"locale": "en", "strength": 2}}),
        docs=[
            {"_id": 1, "name": "apple", "a": 1},
            {"_id": 2, "name": "Apple", "a": 2},
        ],
        command=lambda ctx: {
            "update": ctx.collection,
            "updates": [
                {
                    "q": {"name": "apple"},
                    "u": {"$rename": {"a": "b"}},
                    "multi": True,
                }
            ],
        },
        expected={"ok": 1.0, "n": 2, "nModified": 2},
        msg="$rename should inherit collection default collation for filter",
    ),
    CommandTestCase(
        "rename_explicit_overrides_collection_collation",
        target_collection=CustomCollection(options={"collation": {"locale": "en", "strength": 2}}),
        docs=[
            {"_id": 1, "name": "apple", "a": 1},
            {"_id": 2, "name": "Apple", "a": 2},
        ],
        command=lambda ctx: {
            "update": ctx.collection,
            "updates": [
                {
                    "q": {"name": "apple"},
                    "u": {"$rename": {"a": "b"}},
                    "multi": True,
                    "collation": {"locale": "en", "strength": 3},
                }
            ],
        },
        expected={"ok": 1.0, "n": 1, "nModified": 1},
        msg="$rename explicit collation should override collection default",
    ),
]

COLLATION_RENAME_TESTS = COLLATION_RENAME_FILTER_TESTS + COLLATION_RENAME_COLLECTION_DEFAULT_TESTS


@pytest.mark.parametrize("test", pytest_params(COLLATION_RENAME_TESTS))
def test_collation_update_rename(database_client, collection, test):
    """Test collation behavior in $rename update operator."""
    collection = test.prepare(database_client, collection)
    ctx = CommandContext.from_collection(collection)
    result = execute_command(collection, test.build_command(ctx))
    assertResult(
        result,
        expected=test.build_expected(ctx),
        error_code=test.error_code,
        msg=test.msg,
        raw_res=True,
    )
