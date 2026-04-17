"""
Tests for $anyElementTrue core behavior.

Covers truthiness evaluation (falsy/truthy values), argument handling,
empty/nested arrays, BSON type distinction, shorthand syntax,
boolean result type, order independence, and data type coverage.
"""

from datetime import datetime

import pytest
from bson import Binary, Code, Decimal128, Int64, MaxKey, MinKey, ObjectId, Regex, Timestamp

from documentdb_tests.compatibility.tests.core.operator.expressions.set.anyElementTrue.utils.anyElementTrue_utils import (  # noqa: E501
    AnyElementTrueTest,
    build_expr,
)
from documentdb_tests.compatibility.tests.core.operator.expressions.utils.utils import (
    assert_expression_result,
    execute_expression,
    execute_expression_with_insert,
)
from documentdb_tests.framework.parametrize import pytest_params
from documentdb_tests.framework.test_constants import (
    DECIMAL128_INFINITY,
    DECIMAL128_NAN,
    DECIMAL128_NEGATIVE_INFINITY,
    DECIMAL128_NEGATIVE_ZERO,
    DOUBLE_MIN_NEGATIVE_SUBNORMAL,
    DOUBLE_MIN_SUBNORMAL,
    DOUBLE_NEAR_MAX,
    DOUBLE_NEAR_MIN,
    DOUBLE_NEGATIVE_ZERO,
    FLOAT_INFINITY,
    FLOAT_NAN,
    FLOAT_NEGATIVE_INFINITY,
    INT32_MAX,
    INT32_MIN,
    NUMERIC,
    ZERO_NUMERIC,
)

ANYELEMENTTRUE_CORE_TESTS: list[AnyElementTrueTest] = [
    # --- Falsy values — sole element returns false ---
    AnyElementTrueTest("false", array=[False], expected=False, msg="Should return false for false"),
    AnyElementTrueTest("int_zero", array=[0], expected=False, msg="Should return false for int 0"),
    AnyElementTrueTest(
        "long_zero", array=[Int64(0)], expected=False, msg="Should return false for long 0"
    ),
    AnyElementTrueTest(
        "double_zero", array=[0.0], expected=False, msg="Should return false for double 0.0"
    ),
    AnyElementTrueTest(
        "decimal_zero",
        array=[Decimal128("0")],
        expected=False,
        msg="Should return false for Decimal128 0",
    ),
    AnyElementTrueTest("null", array=[None], expected=False, msg="Should return false for null"),
    AnyElementTrueTest(
        "neg_zero_double",
        array=[DOUBLE_NEGATIVE_ZERO],
        expected=False,
        msg="Should return false for -0.0",
    ),
    AnyElementTrueTest(
        "neg_zero_decimal",
        array=[DECIMAL128_NEGATIVE_ZERO],
        expected=False,
        msg="Should return false for Decimal128 -0",
    ),
    AnyElementTrueTest(
        "decimal_0_0",
        array=[Decimal128("0.0")],
        expected=False,
        msg="Should return false for Decimal128 0.0",
    ),
    AnyElementTrueTest(
        "decimal_0_00",
        array=[Decimal128("0.00")],
        expected=False,
        msg="Should return false for Decimal128 0.00",
    ),
    AnyElementTrueTest(
        "decimal_0E6144",
        array=[Decimal128("0E+6144")],
        expected=False,
        msg="Should return false for Decimal128 0E+6144",
    ),
    AnyElementTrueTest(
        "decimal_0E_neg6176",
        array=[Decimal128("0E-6176")],
        expected=False,
        msg="Should return false for Decimal128 0E-6176",
    ),
    AnyElementTrueTest(
        "decimal_neg0E6144",
        array=[Decimal128("-0E+6144")],
        expected=False,
        msg="Should return false for Decimal128 -0E+6144",
    ),
    AnyElementTrueTest(
        "decimal_neg0E_neg6176",
        array=[Decimal128("-0E-6176")],
        expected=False,
        msg="Should return false for Decimal128 -0E-6176",
    ),
    AnyElementTrueTest(
        "decimal_neg0_many_zeros",
        array=[Decimal128("-0.00000000000000000000000000000000")],
        expected=False,
        msg="Should return false for Decimal128 -0 with trailing zeros",
    ),
    # --- Truthy values — sole element returns true ---
    AnyElementTrueTest("true", array=[True], expected=True, msg="Should return true for true"),
    AnyElementTrueTest("int_1", array=[1], expected=True, msg="Should return true for 1"),
    AnyElementTrueTest("int_neg1", array=[-1], expected=True, msg="Should return true for -1"),
    AnyElementTrueTest(
        "long_1", array=[Int64(1)], expected=True, msg="Should return true for long 1"
    ),
    AnyElementTrueTest(
        "long_neg1", array=[Int64(-1)], expected=True, msg="Should return true for long -1"
    ),
    AnyElementTrueTest("double_1_5", array=[1.5], expected=True, msg="Should return true for 1.5"),
    AnyElementTrueTest(
        "double_neg1_5", array=[-1.5], expected=True, msg="Should return true for -1.5"
    ),
    AnyElementTrueTest(
        "decimal_1",
        array=[Decimal128("1")],
        expected=True,
        msg="Should return true for Decimal128 1",
    ),
    AnyElementTrueTest(
        "decimal_neg1",
        array=[Decimal128("-1")],
        expected=True,
        msg="Should return true for Decimal128 -1",
    ),
    AnyElementTrueTest(
        "double_0_001", array=[0.001], expected=True, msg="Should return true for 0.001"
    ),
    AnyElementTrueTest(
        "nan_double", array=[FLOAT_NAN], expected=True, msg="Should return true for NaN"
    ),
    AnyElementTrueTest(
        "nan_decimal",
        array=[DECIMAL128_NAN],
        expected=True,
        msg="Should return true for Decimal128 NaN",
    ),
    AnyElementTrueTest(
        "inf", array=[FLOAT_INFINITY], expected=True, msg="Should return true for Infinity"
    ),
    AnyElementTrueTest(
        "neg_inf",
        array=[FLOAT_NEGATIVE_INFINITY],
        expected=True,
        msg="Should return true for -Infinity",
    ),
    AnyElementTrueTest(
        "decimal_inf",
        array=[DECIMAL128_INFINITY],
        expected=True,
        msg="Should return true for Decimal128 Infinity",
    ),
    AnyElementTrueTest(
        "decimal_neg_inf",
        array=[DECIMAL128_NEGATIVE_INFINITY],
        expected=True,
        msg="Should return true for Decimal128 -Infinity",
    ),
    AnyElementTrueTest(
        "subnormal",
        array=[DOUBLE_MIN_SUBNORMAL],
        expected=True,
        msg="Should return true for min subnormal",
    ),
    AnyElementTrueTest(
        "neg_subnormal",
        array=[DOUBLE_MIN_NEGATIVE_SUBNORMAL],
        expected=True,
        msg="Should return true for min negative subnormal",
    ),
    AnyElementTrueTest(
        "near_max",
        array=[DOUBLE_NEAR_MAX],
        expected=True,
        msg="Should return true for near max double",
    ),
    AnyElementTrueTest(
        "near_min",
        array=[DOUBLE_NEAR_MIN],
        expected=True,
        msg="Should return true for near min double",
    ),
    AnyElementTrueTest(
        "decimal_tiny_pos",
        array=[Decimal128("0.00000000000000000000000000000001")],
        expected=True,
        msg="Should return true for tiny positive decimal",
    ),
    AnyElementTrueTest(
        "decimal_tiny_neg",
        array=[Decimal128("-0.00000000000000000000000000000001")],
        expected=True,
        msg="Should return true for tiny negative decimal",
    ),
    AnyElementTrueTest(
        "empty_string", array=[""], expected=True, msg="Should return true for empty string"
    ),
    AnyElementTrueTest(
        "string", array=["hello"], expected=True, msg="Should return true for string"
    ),
    AnyElementTrueTest(
        "empty_nested_array",
        array=[[]],
        expected=True,
        msg="Should return true for empty nested array",
    ),
    AnyElementTrueTest(
        "nested_array", array=[[1, 2]], expected=True, msg="Should return true for nested array"
    ),
    AnyElementTrueTest(
        "empty_object", array=[{}], expected=True, msg="Should return true for empty object"
    ),
    AnyElementTrueTest(
        "object", array=[{"a": 1}], expected=True, msg="Should return true for object"
    ),
    AnyElementTrueTest(
        "objectid",
        array=[ObjectId("507f1f77bcf86cd799439011")],
        expected=True,
        msg="Should return true for ObjectId",
    ),
    AnyElementTrueTest(
        "date", array=[datetime(2017, 1, 1)], expected=True, msg="Should return true for date"
    ),
    AnyElementTrueTest(
        "timestamp", array=[Timestamp(0, 1)], expected=True, msg="Should return true for Timestamp"
    ),
    AnyElementTrueTest(
        "bindata", array=[Binary(b"", 0)], expected=True, msg="Should return true for BinData"
    ),
    AnyElementTrueTest(
        "regex", array=[Regex("pattern")], expected=True, msg="Should return true for regex"
    ),
    AnyElementTrueTest(
        "javascript",
        array=[Code("function(){}")],
        expected=True,
        msg="Should return true for JavaScript",
    ),
    AnyElementTrueTest(
        "timestamp_zero",
        array=[Timestamp(0, 0)],
        expected=True,
        msg="Should return true for Timestamp(0,0)",
    ),
    AnyElementTrueTest(
        "int32_max", array=[INT32_MAX], expected=True, msg="Should return true for INT32_MAX"
    ),
    AnyElementTrueTest(
        "int32_min", array=[INT32_MIN], expected=True, msg="Should return true for INT32_MIN"
    ),
    AnyElementTrueTest(
        "double_0_0000001", array=[0.0000001], expected=True, msg="Should return true for 0.0000001"
    ),
    AnyElementTrueTest(
        "double_neg0_0000001",
        array=[-0.0000001],
        expected=True,
        msg="Should return true for -0.0000001",
    ),
    # --- All falsy combined ---
    AnyElementTrueTest(
        "false_0_null",
        array=[False, 0, None],
        expected=False,
        msg="Should return false when all elements are falsy",
    ),
    AnyElementTrueTest(
        "all_falsy_types",
        array=[False, 0, Int64(0), 0.0, Decimal128("0"), None],
        expected=False,
        msg="Should return false when all falsy types are combined",
    ),
    AnyElementTrueTest(
        "all_nulls",
        array=[None, None, None],
        expected=False,
        msg="Should return false when all elements are null",
    ),
    AnyElementTrueTest(
        "all_false",
        array=[False, False, False],
        expected=False,
        msg="Should return false when all elements are false",
    ),
    AnyElementTrueTest(
        "all_zeros",
        array=[0, 0, 0],
        expected=False,
        msg="Should return false when all elements are zero",
    ),
    # --- Mixed falsy and truthy ---
    AnyElementTrueTest(
        "false_true",
        array=[False, True],
        expected=True,
        msg="Should return true for one true among false",
    ),
    AnyElementTrueTest(
        "0_1", array=[0, 1], expected=True, msg="Should return true for one 1 among 0"
    ),
    AnyElementTrueTest(
        "null_hello",
        array=[None, "hello"],
        expected=True,
        msg="Should return true for string among null",
    ),
    AnyElementTrueTest(
        "falsy_plus_1",
        array=[False, 0, None, 1],
        expected=True,
        msg="Should return true for one truthy among many falsy",
    ),
    AnyElementTrueTest(
        "falsy_plus_empty_str",
        array=[False, 0, None, ""],
        expected=True,
        msg="Should return true for empty string among falsy",
    ),
    AnyElementTrueTest(
        "falsy_plus_empty_arr",
        array=[False, 0, None, []],
        expected=True,
        msg="Should return true for empty array among falsy",
    ),
    AnyElementTrueTest(
        "falsy_plus_empty_obj",
        array=[False, 0, None, {}],
        expected=True,
        msg="Should return true for empty object among falsy",
    ),
    AnyElementTrueTest(
        "falsy_plus_objectid",
        array=[False, 0, None, ObjectId("507f1f77bcf86cd799439011")],
        expected=True,
        msg="Should return true for ObjectId among falsy",
    ),
    AnyElementTrueTest(
        "falsy_plus_date",
        array=[False, 0, None, datetime(2017, 1, 1)],
        expected=True,
        msg="Should return true for date among falsy",
    ),
    AnyElementTrueTest(
        "falsy_plus_timestamp",
        array=[False, 0, None, Timestamp(0, 1)],
        expected=True,
        msg="Should return true for Timestamp among falsy",
    ),
    AnyElementTrueTest(
        "falsy_plus_bindata",
        array=[False, 0, None, Binary(b"", 0)],
        expected=True,
        msg="Should return true for BinData among falsy",
    ),
    AnyElementTrueTest(
        "falsy_plus_regex",
        array=[False, 0, None, Regex("regex")],
        expected=True,
        msg="Should return true for regex among falsy",
    ),
    AnyElementTrueTest(
        "falsy_plus_object",
        array=[False, 0, None, {"a": 1}],
        expected=True,
        msg="Should return true for object among falsy",
    ),
    AnyElementTrueTest(
        "all_falsy_plus_empty_str",
        array=[False, 0, Int64(0), 0.0, Decimal128("0"), None, ""],
        expected=True,
        msg="Should return true when empty string added to falsy",
    ),
    # --- Empty array and nested arrays ---
    AnyElementTrueTest(
        "empty_array", array=[], expected=False, msg="Should return false for empty array"
    ),
    AnyElementTrueTest(
        "nested_false",
        array=[[False]],
        expected=True,
        msg="Should return true for nested false element",
    ),
    AnyElementTrueTest(
        "nested_zero", array=[[0]], expected=True, msg="Should return true for nested zero element"
    ),
    AnyElementTrueTest(
        "nested_null",
        array=[[None]],
        expected=True,
        msg="Should return true for nested null element",
    ),
    AnyElementTrueTest(
        "nested_all_falsy",
        array=[[False, 0, None]],
        expected=True,
        msg="Should return true for nested array with all falsy",
    ),
    AnyElementTrueTest(
        "empty_arr_and_false",
        array=[[], False],
        expected=True,
        msg="Should return true for empty array element",
    ),
    AnyElementTrueTest(
        "nested_false_and_false",
        array=[[False], False],
        expected=True,
        msg="Should return true for nested false array element",
    ),
    AnyElementTrueTest(
        "multiple_nested",
        array=[[False], [0], [None]],
        expected=True,
        msg="Should return true for multiple nested arrays",
    ),
    AnyElementTrueTest(
        "deeply_nested_false",
        array=[[[[[[False]]]]]],
        expected=True,
        msg="Should return true for deeply nested false",
    ),
    AnyElementTrueTest(
        "deeply_nested_zero",
        array=[[[[[[0]]]]]],
        expected=True,
        msg="Should return true for deeply nested zero",
    ),
    AnyElementTrueTest(
        "deeply_nested_empty",
        array=[[[[[[]]]]]],
        expected=True,
        msg="Should return true for deeply nested empty",
    ),
    AnyElementTrueTest(
        "nested_nan",
        array=[[DECIMAL128_NAN]],
        expected=True,
        msg="Should return true for nested NaN array",
    ),
    AnyElementTrueTest(
        "nested_inf",
        array=[[FLOAT_INFINITY]],
        expected=True,
        msg="Should return true for nested Infinity array",
    ),
    AnyElementTrueTest(
        "mixed_nesting_depths",
        array=[[False], [[None]], [[[0]]]],
        expected=True,
        msg="Should return true for mixed nesting depths",
    ),
    # --- Order independence ---
    AnyElementTrueTest(
        "false_true_order",
        array=[False, True],
        expected=True,
        msg="Should return true regardless of element order",
    ),
    AnyElementTrueTest(
        "true_false_order",
        array=[True, False],
        expected=True,
        msg="Should return true regardless of reversed element order",
    ),
    AnyElementTrueTest(
        "0_1_null",
        array=[0, 1, None],
        expected=True,
        msg="Should return true when array has mixed falsy and truthy",
    ),
    AnyElementTrueTest(
        "null_0_1",
        array=[None, 0, 1],
        expected=True,
        msg="Should return true when truthy element is at end",
    ),
    AnyElementTrueTest(
        "1_null_0",
        array=[1, None, 0],
        expected=True,
        msg="Should return true when truthy element is at start",
    ),
    # --- BSON type distinction ---
    AnyElementTrueTest(
        "false_and_0", array=[False, 0], expected=False, msg="Should return false for false and 0"
    ),
    AnyElementTrueTest(
        "false_and_null",
        array=[False, None],
        expected=False,
        msg="Should return false for false and null",
    ),
    AnyElementTrueTest(
        "empty_str_and_null",
        array=["", None],
        expected=True,
        msg="Should return true for empty string with null",
    ),
    AnyElementTrueTest(
        "neg_zero_and_zero",
        array=[DOUBLE_NEGATIVE_ZERO, 0.0],
        expected=False,
        msg="Should return false for -0.0 and 0.0",
    ),
]


@pytest.mark.parametrize("test", pytest_params(ANYELEMENTTRUE_CORE_TESTS))
def test_anyElementTrue_core(collection, test):
    """Test $anyElementTrue core behavior with literal arrays."""
    result = execute_expression(collection, build_expr(test))
    assert_expression_result(result, expected=test.expected, msg=test.msg)


# ---------------------------------------------------------------------------
# Boolean result type verification
# ---------------------------------------------------------------------------
def test_anyElementTrue_returns_boolean_true(collection):
    """Test $anyElementTrue returns exactly boolean true, not 1."""
    result = execute_expression(collection, {"$anyElementTrue": [[True]]})
    assert_expression_result(result, expected=True, msg="Should return boolean true")


def test_anyElementTrue_returns_boolean_false(collection):
    """Test $anyElementTrue returns exactly boolean false, not 0."""
    result = execute_expression(collection, {"$anyElementTrue": [[False]]})
    assert_expression_result(result, expected=False, msg="Should return boolean false")


def test_anyElementTrue_empty_returns_boolean_false(collection):
    """Test $anyElementTrue on empty array returns exactly boolean false."""
    result = execute_expression(collection, {"$anyElementTrue": [[]]})
    assert_expression_result(result, expected=False, msg="Empty array should return boolean false")


# ---------------------------------------------------------------------------
# Shorthand syntax
# ---------------------------------------------------------------------------
def test_anyElementTrue_shorthand_field(collection):
    """Test $anyElementTrue shorthand syntax without outer array wrapper."""
    result = execute_expression_with_insert(
        collection, {"$anyElementTrue": "$arr"}, {"arr": [True, False]}
    )
    assert_expression_result(result, expected=True, msg="Shorthand syntax should work")


def test_anyElementTrue_shorthand_field_all_falsy(collection):
    """Test $anyElementTrue shorthand syntax with all falsy array."""
    result = execute_expression_with_insert(
        collection, {"$anyElementTrue": "$arr"}, {"arr": [0, False]}
    )
    assert_expression_result(
        result, expected=False, msg="Shorthand with all falsy should return false"
    )


# ---------------------------------------------------------------------------
# Argument handling
# ---------------------------------------------------------------------------
def test_anyElementTrue_single_arg_literal(collection):
    """Test $anyElementTrue with single literal array argument."""
    result = execute_expression(collection, {"$anyElementTrue": [[True, False]]})
    assert_expression_result(result, expected=True, msg="Single literal array argument should work")


def test_anyElementTrue_single_arg_field(collection):
    """Test $anyElementTrue with single field path argument."""
    result = execute_expression_with_insert(
        collection, {"$anyElementTrue": ["$arr"]}, {"arr": [True]}
    )
    assert_expression_result(result, expected=True, msg="Single field path argument should work")


# ---------------------------------------------------------------------------
# Numeric boundary values from NUMERIC dataset
# ---------------------------------------------------------------------------
ANYELEMENTTRUE_NUMERIC_TESTS: list[AnyElementTrueTest] = [
    AnyElementTrueTest(
        f"numeric_{repr(v).replace('.', '_')[:50]}",
        array=[v],
        expected=v not in ZERO_NUMERIC,
        msg=f"NUMERIC value {repr(v)} truthiness",
    )
    for v in NUMERIC
]


@pytest.mark.parametrize("test", pytest_params(ANYELEMENTTRUE_NUMERIC_TESTS))
def test_anyElementTrue_numeric_boundary(collection, test):
    """Test $anyElementTrue with each NUMERIC boundary value."""
    result = execute_expression(collection, build_expr(test))
    assert_expression_result(result, expected=test.expected, msg=test.msg)


# ---------------------------------------------------------------------------
# Field path resolution
# ---------------------------------------------------------------------------
def test_anyElementTrue_field_all_true(collection):
    """Test $anyElementTrue with field path resolving to all-true array."""
    result = execute_expression_with_insert(
        collection, {"$anyElementTrue": ["$arr"]}, {"arr": [True, True]}
    )
    assert_expression_result(result, expected=True, msg="All true field should return true")


def test_anyElementTrue_field_some_true(collection):
    """Test $anyElementTrue with field path resolving to mixed array."""
    result = execute_expression_with_insert(
        collection, {"$anyElementTrue": ["$arr"]}, {"arr": [True, False]}
    )
    assert_expression_result(result, expected=True, msg="Some true field should return true")


def test_anyElementTrue_field_none_true(collection):
    """Test $anyElementTrue with field path resolving to all-false array."""
    result = execute_expression_with_insert(
        collection, {"$anyElementTrue": ["$arr"]}, {"arr": [0, False]}
    )
    assert_expression_result(result, expected=False, msg="None true field should return false")


def test_anyElementTrue_field_null_input(collection):
    """Test $anyElementTrue with field containing [null]."""
    result = execute_expression_with_insert(
        collection, {"$anyElementTrue": ["$arr"]}, {"arr": [None]}
    )
    assert_expression_result(
        result, expected=False, msg="Should return false for array with only null"
    )


def test_anyElementTrue_field_null_true(collection):
    """Test $anyElementTrue with field containing [null, true]."""
    result = execute_expression_with_insert(
        collection, {"$anyElementTrue": ["$arr"]}, {"arr": [None, True]}
    )
    assert_expression_result(
        result, expected=True, msg="Should return true for array with null and true"
    )


def test_anyElementTrue_field_empty(collection):
    """Test $anyElementTrue with field containing empty array."""
    result = execute_expression_with_insert(collection, {"$anyElementTrue": ["$arr"]}, {"arr": []})
    assert_expression_result(result, expected=False, msg="Empty array field should return false")


# ---------------------------------------------------------------------------
# MinKey/MaxKey truthy — must use field reference (literals conflict with $project)
# ---------------------------------------------------------------------------
def test_anyElementTrue_minkey_truthy(collection):
    """MinKey is truthy — must use field reference (literals conflict with $project)."""
    result = execute_expression_with_insert(
        collection, {"$anyElementTrue": ["$arr"]}, {"arr": [MinKey()]}
    )
    assert_expression_result(result, expected=True, msg="Should return true for MinKey")


def test_anyElementTrue_maxkey_truthy(collection):
    """MaxKey is truthy — must use field reference (literals conflict with $project)."""
    result = execute_expression_with_insert(
        collection, {"$anyElementTrue": ["$arr"]}, {"arr": [MaxKey()]}
    )
    assert_expression_result(result, expected=True, msg="Should return true for MaxKey")
