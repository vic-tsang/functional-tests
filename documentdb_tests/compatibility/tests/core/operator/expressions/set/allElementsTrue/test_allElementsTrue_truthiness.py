"""
Tests for $allElementsTrue truthiness evaluation.

Covers falsy values, truthy values, NaN, Infinity, negative zero,
Decimal128 zero variants, numeric boundary values, empty array,
and per-BSON-type truthiness.
"""

from datetime import datetime

import pytest
from bson import Binary, Code, Decimal128, Int64, ObjectId, Regex, Timestamp

from documentdb_tests.compatibility.tests.core.operator.expressions.set.allElementsTrue.utils.allElementsTrue_utils import (  # noqa: E501
    AllElementsTrueTest,
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
    DECIMAL128_LARGE_EXPONENT,
    DECIMAL128_MAX,
    DECIMAL128_MAX_NEGATIVE,
    DECIMAL128_MIN,
    DECIMAL128_MIN_POSITIVE,
    DECIMAL128_NAN,
    DECIMAL128_NEGATIVE_INFINITY,
    DECIMAL128_NEGATIVE_ZERO,
    DECIMAL128_SMALL_EXPONENT,
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
    INT64_MAX,
    INT64_MIN,
    NUMERIC,
    ZERO_NUMERIC,
)

# ---------------------------------------------------------------------------
# Falsy values — sole element returns false
# ---------------------------------------------------------------------------
FALSY_TESTS: list[AllElementsTrueTest] = [
    AllElementsTrueTest(
        "false", array=[False], expected=False, msg="Should return false for false"
    ),
    AllElementsTrueTest("int_zero", array=[0], expected=False, msg="Should return false for int 0"),
    AllElementsTrueTest(
        "long_zero", array=[Int64(0)], expected=False, msg="Should return false for long 0"
    ),
    AllElementsTrueTest(
        "double_zero", array=[0.0], expected=False, msg="Should return false for double 0.0"
    ),
    AllElementsTrueTest(
        "decimal_zero",
        array=[Decimal128("0")],
        expected=False,
        msg="Should return false for Decimal128 0",
    ),
    AllElementsTrueTest("null", array=[None], expected=False, msg="Should return false for null"),
]


@pytest.mark.parametrize("test", pytest_params(FALSY_TESTS))
def test_allElementsTrue_falsy(collection, test):
    """Test $allElementsTrue returns false for sole falsy element."""
    result = execute_expression(collection, build_expr(test))
    assert_expression_result(result, expected=test.expected, msg=test.msg)


# ---------------------------------------------------------------------------
# Truthy values — sole element returns true
# ---------------------------------------------------------------------------
TRUTHY_TESTS: list[AllElementsTrueTest] = [
    AllElementsTrueTest("true", array=[True], expected=True, msg="Should return true for true"),
    AllElementsTrueTest("int_1", array=[1], expected=True, msg="Should return true for 1"),
    AllElementsTrueTest("int_neg1", array=[-1], expected=True, msg="Should return true for -1"),
    AllElementsTrueTest(
        "long_1", array=[Int64(1)], expected=True, msg="Should return true for long 1"
    ),
    AllElementsTrueTest(
        "long_neg1", array=[Int64(-1)], expected=True, msg="Should return true for long -1"
    ),
    AllElementsTrueTest("double_1_5", array=[1.5], expected=True, msg="Should return true for 1.5"),
    AllElementsTrueTest(
        "double_neg1_5", array=[-1.5], expected=True, msg="Should return true for -1.5"
    ),
    AllElementsTrueTest(
        "decimal_1",
        array=[Decimal128("1")],
        expected=True,
        msg="Should return true for Decimal128 1",
    ),
    AllElementsTrueTest(
        "decimal_neg1",
        array=[Decimal128("-1")],
        expected=True,
        msg="Should return true for Decimal128 -1",
    ),
    AllElementsTrueTest(
        "empty_string", array=[""], expected=True, msg="Should return true for empty string"
    ),
    AllElementsTrueTest(
        "string", array=["hello"], expected=True, msg="Should return true for string"
    ),
    AllElementsTrueTest(
        "empty_nested_array",
        array=[[]],
        expected=True,
        msg="Should return true for empty nested array",
    ),
    AllElementsTrueTest(
        "empty_object", array=[{}], expected=True, msg="Should return true for empty object"
    ),
    AllElementsTrueTest(
        "object", array=[{"a": 1}], expected=True, msg="Should return true for object"
    ),
    AllElementsTrueTest(
        "objectid",
        array=[ObjectId("507f1f77bcf86cd799439011")],
        expected=True,
        msg="Should return true for ObjectId",
    ),
    AllElementsTrueTest(
        "date", array=[datetime(2017, 1, 1)], expected=True, msg="Should return true for date"
    ),
    AllElementsTrueTest(
        "timestamp", array=[Timestamp(1, 1)], expected=True, msg="Should return true for Timestamp"
    ),
    AllElementsTrueTest(
        "bindata", array=[Binary(b"", 0)], expected=True, msg="Should return true for BinData"
    ),
    AllElementsTrueTest(
        "regex", array=[Regex("pattern")], expected=True, msg="Should return true for regex"
    ),
    AllElementsTrueTest(
        "javascript",
        array=[Code("function(){}")],
        expected=True,
        msg="Should return true for JavaScript",
    ),
    AllElementsTrueTest(
        "timestamp_zero",
        array=[Timestamp(0, 0)],
        expected=True,
        msg="Should return true for Timestamp(0,0)",
    ),
]


@pytest.mark.parametrize("test", pytest_params(TRUTHY_TESTS))
def test_allElementsTrue_truthy(collection, test):
    """Test $allElementsTrue returns true for sole truthy element."""
    result = execute_expression(collection, build_expr(test))
    assert_expression_result(result, expected=test.expected, msg=test.msg)


# ---------------------------------------------------------------------------
# MinKey/MaxKey truthy — must use field reference (literals conflict with $project)
# ---------------------------------------------------------------------------
def test_allElementsTrue_minkey_truthy(collection):
    """Test $allElementsTrue with MinKey element is truthy."""
    from bson import MinKey

    result = execute_expression_with_insert(
        collection, {"$allElementsTrue": ["$arr"]}, {"arr": [MinKey()]}
    )
    assert_expression_result(result, expected=True, msg="Should return true for MinKey")


def test_allElementsTrue_maxkey_truthy(collection):
    """Test $allElementsTrue with MaxKey element is truthy."""
    from bson import MaxKey

    result = execute_expression_with_insert(
        collection, {"$allElementsTrue": ["$arr"]}, {"arr": [MaxKey()]}
    )
    assert_expression_result(result, expected=True, msg="Should return true for MaxKey")


# ---------------------------------------------------------------------------
# NaN is truthy
# ---------------------------------------------------------------------------
NAN_TESTS: list[AllElementsTrueTest] = [
    AllElementsTrueTest(
        "nan_double", array=[FLOAT_NAN], expected=True, msg="Should return true for NaN"
    ),
    AllElementsTrueTest(
        "nan_decimal",
        array=[DECIMAL128_NAN],
        expected=True,
        msg="Should return true for Decimal128 NaN",
    ),
    AllElementsTrueTest(
        "nan_both",
        array=[FLOAT_NAN, DECIMAL128_NAN],
        expected=True,
        msg="Should return true for both NaN types",
    ),
    AllElementsTrueTest(
        "nan_with_zero",
        array=[FLOAT_NAN, 0],
        expected=False,
        msg="Should return false for NaN combined with zero",
    ),
    AllElementsTrueTest(
        "nan_with_null",
        array=[FLOAT_NAN, None],
        expected=False,
        msg="Should return false for NaN combined with null",
    ),
]


@pytest.mark.parametrize("test", pytest_params(NAN_TESTS))
def test_allElementsTrue_nan(collection, test):
    """Test $allElementsTrue NaN truthiness."""
    result = execute_expression(collection, build_expr(test))
    assert_expression_result(result, expected=test.expected, msg=test.msg)


# ---------------------------------------------------------------------------
# Infinity is truthy
# ---------------------------------------------------------------------------
INFINITY_TESTS: list[AllElementsTrueTest] = [
    AllElementsTrueTest(
        "inf_double", array=[FLOAT_INFINITY], expected=True, msg="Should return true for Infinity"
    ),
    AllElementsTrueTest(
        "neg_inf_double",
        array=[FLOAT_NEGATIVE_INFINITY],
        expected=True,
        msg="Should return true for -Infinity",
    ),
    AllElementsTrueTest(
        "inf_decimal",
        array=[DECIMAL128_INFINITY],
        expected=True,
        msg="Should return true for Decimal128 Infinity",
    ),
    AllElementsTrueTest(
        "neg_inf_decimal",
        array=[DECIMAL128_NEGATIVE_INFINITY],
        expected=True,
        msg="Should return true for Decimal128 -Infinity",
    ),
    AllElementsTrueTest(
        "inf_both",
        array=[FLOAT_INFINITY, FLOAT_NEGATIVE_INFINITY],
        expected=True,
        msg="Should return true for both infinities",
    ),
    AllElementsTrueTest(
        "decimal_inf_both",
        array=[DECIMAL128_INFINITY, DECIMAL128_NEGATIVE_INFINITY],
        expected=True,
        msg="Should return true for both decimal infinities",
    ),
    AllElementsTrueTest(
        "inf_with_zero",
        array=[FLOAT_INFINITY, 0],
        expected=False,
        msg="Should return false for Infinity combined with zero",
    ),
]


@pytest.mark.parametrize("test", pytest_params(INFINITY_TESTS))
def test_allElementsTrue_infinity(collection, test):
    """Test $allElementsTrue Infinity truthiness."""
    result = execute_expression(collection, build_expr(test))
    assert_expression_result(result, expected=test.expected, msg=test.msg)


# ---------------------------------------------------------------------------
# Negative zero is falsy
# ---------------------------------------------------------------------------
NEG_ZERO_TESTS: list[AllElementsTrueTest] = [
    AllElementsTrueTest(
        "neg_zero_double",
        array=[DOUBLE_NEGATIVE_ZERO],
        expected=False,
        msg="Should return false for -0.0",
    ),
    AllElementsTrueTest(
        "neg_zero_decimal",
        array=[DECIMAL128_NEGATIVE_ZERO],
        expected=False,
        msg="Should return false for Decimal128 -0",
    ),
    AllElementsTrueTest(
        "neg_zero_decimal_0_0",
        array=[Decimal128("-0.0")],
        expected=False,
        msg="Should return false for Decimal128 -0.0",
    ),
    AllElementsTrueTest(
        "neg_zero_decimal_E1",
        array=[Decimal128("-0E+1")],
        expected=False,
        msg="Should return false for Decimal128 -0E+1",
    ),
    AllElementsTrueTest(
        "neg_zero_decimal_E_1",
        array=[Decimal128("-0E-1")],
        expected=False,
        msg="Should return false for Decimal128 -0E-1",
    ),
    AllElementsTrueTest(
        "neg_zero_both",
        array=[DOUBLE_NEGATIVE_ZERO, DECIMAL128_NEGATIVE_ZERO],
        expected=False,
        msg="Should return false for both neg zeros",
    ),
    AllElementsTrueTest(
        "truthy_with_neg_zero",
        array=[1, DOUBLE_NEGATIVE_ZERO],
        expected=False,
        msg="Should return false for truthy with -0.0",
    ),
]


@pytest.mark.parametrize("test", pytest_params(NEG_ZERO_TESTS))
def test_allElementsTrue_negative_zero(collection, test):
    """Test $allElementsTrue negative zero is falsy."""
    result = execute_expression(collection, build_expr(test))
    assert_expression_result(result, expected=test.expected, msg=test.msg)


# ---------------------------------------------------------------------------
# Decimal128 zero variants
# ---------------------------------------------------------------------------
DECIMAL_ZERO_TESTS: list[AllElementsTrueTest] = [
    AllElementsTrueTest(
        "decimal_0_0",
        array=[Decimal128("0.0")],
        expected=False,
        msg="Should return false for Decimal128 0.0",
    ),
    AllElementsTrueTest(
        "decimal_0_many_zeros",
        array=[Decimal128("0.00000000000000000000000000000000")],
        expected=False,
        msg="Should return false for Decimal128 0 with trailing zeros",
    ),
    AllElementsTrueTest(
        "decimal_0E6144",
        array=[Decimal128("0E+6144")],
        expected=False,
        msg="Should return false for Decimal128 0E+6144",
    ),
    AllElementsTrueTest(
        "decimal_neg0E_6143",
        array=[Decimal128("-0E-6143")],
        expected=False,
        msg="Should return false for Decimal128 -0E-6143",
    ),
    AllElementsTrueTest(
        "decimal_1_0_truthy",
        array=[Decimal128("1.0")],
        expected=True,
        msg="Should return true for Decimal128 1.0",
    ),
    AllElementsTrueTest(
        "decimal_1_many_zeros_truthy",
        array=[Decimal128("1.00000000000000000000000000000000")],
        expected=True,
        msg="Should return true for Decimal128 1 with trailing zeros",
    ),
]


@pytest.mark.parametrize("test", pytest_params(DECIMAL_ZERO_TESTS))
def test_allElementsTrue_decimal_zero_variants(collection, test):
    """Test $allElementsTrue Decimal128 zero variants."""
    result = execute_expression(collection, build_expr(test))
    assert_expression_result(result, expected=test.expected, msg=test.msg)


# ---------------------------------------------------------------------------
# Numeric boundary values from NUMERIC dataset
# ---------------------------------------------------------------------------
@pytest.mark.parametrize("value", NUMERIC)
def test_allElementsTrue_numeric_boundary(collection, value):
    """Test $allElementsTrue with each NUMERIC boundary value."""
    expected = value not in ZERO_NUMERIC
    result = execute_expression_with_insert(
        collection, {"$allElementsTrue": ["$arr"]}, {"arr": [value]}
    )
    assert_expression_result(
        result, expected=expected, msg=f"NUMERIC value {repr(value)} truthiness"
    )


# ---------------------------------------------------------------------------
# Additional non-zero boundaries are truthy
# ---------------------------------------------------------------------------
BOUNDARY_TRUTHY_TESTS: list[AllElementsTrueTest] = [
    AllElementsTrueTest(
        "int32_min", array=[INT32_MIN], expected=True, msg="Should return true for INT32_MIN"
    ),
    AllElementsTrueTest(
        "int32_max", array=[INT32_MAX], expected=True, msg="Should return true for INT32_MAX"
    ),
    AllElementsTrueTest(
        "int64_min", array=[INT64_MIN], expected=True, msg="Should return true for INT64_MIN"
    ),
    AllElementsTrueTest(
        "int64_max", array=[INT64_MAX], expected=True, msg="Should return true for INT64_MAX"
    ),
    AllElementsTrueTest(
        "double_near_max",
        array=[DOUBLE_NEAR_MAX],
        expected=True,
        msg="Should return true for DOUBLE_NEAR_MAX",
    ),
    AllElementsTrueTest(
        "double_min_subnormal",
        array=[DOUBLE_MIN_SUBNORMAL],
        expected=True,
        msg="Should return true for DOUBLE_MIN_SUBNORMAL",
    ),
    AllElementsTrueTest(
        "double_min_neg_subnormal",
        array=[DOUBLE_MIN_NEGATIVE_SUBNORMAL],
        expected=True,
        msg="Should return true for DOUBLE_MIN_NEGATIVE_SUBNORMAL",
    ),
    AllElementsTrueTest(
        "double_near_min",
        array=[DOUBLE_NEAR_MIN],
        expected=True,
        msg="Should return true for DOUBLE_NEAR_MIN",
    ),
    AllElementsTrueTest(
        "decimal_max",
        array=[DECIMAL128_MAX],
        expected=True,
        msg="Should return true for DECIMAL128_MAX",
    ),
    AllElementsTrueTest(
        "decimal_min",
        array=[DECIMAL128_MIN],
        expected=True,
        msg="Should return true for DECIMAL128_MIN",
    ),
    AllElementsTrueTest(
        "decimal_large_exp",
        array=[DECIMAL128_LARGE_EXPONENT],
        expected=True,
        msg="Should return true for DECIMAL128_LARGE_EXPONENT",
    ),
    AllElementsTrueTest(
        "decimal_small_exp",
        array=[DECIMAL128_SMALL_EXPONENT],
        expected=True,
        msg="Should return true for DECIMAL128_SMALL_EXPONENT",
    ),
    AllElementsTrueTest(
        "decimal_min_positive",
        array=[DECIMAL128_MIN_POSITIVE],
        expected=True,
        msg="Should return true for DECIMAL128_MIN_POSITIVE",
    ),
    AllElementsTrueTest(
        "decimal_max_negative",
        array=[DECIMAL128_MAX_NEGATIVE],
        expected=True,
        msg="Should return true for DECIMAL128_MAX_NEGATIVE",
    ),
]


@pytest.mark.parametrize("test", pytest_params(BOUNDARY_TRUTHY_TESTS))
def test_allElementsTrue_boundary_truthy(collection, test):
    """Test $allElementsTrue with non-zero numeric boundary values."""
    result = execute_expression(collection, build_expr(test))
    assert_expression_result(result, expected=test.expected, msg=test.msg)


# ---------------------------------------------------------------------------
# Empty array — vacuous truth
# ---------------------------------------------------------------------------
def test_allElementsTrue_empty_array(collection):
    """Test $allElementsTrue with empty array returns true (vacuous truth)."""
    result = execute_expression(collection, {"$allElementsTrue": [[]]})
    assert_expression_result(result, expected=True, msg="Empty array should return true")


# ---------------------------------------------------------------------------
# Data type coverage — double
# ---------------------------------------------------------------------------
DOUBLE_TESTS: list[AllElementsTrueTest] = [
    AllElementsTrueTest(
        "double_zero", array=[0.0], expected=False, msg="Should return false for double zero"
    ),
    AllElementsTrueTest(
        "double_neg_zero", array=[-0.0], expected=False, msg="Should return false for double -0"
    ),
    AllElementsTrueTest(
        "double_small_pos",
        array=[0.0000001],
        expected=True,
        msg="Should return true for small positive double",
    ),
    AllElementsTrueTest(
        "double_small_neg",
        array=[-0.0000001],
        expected=True,
        msg="Should return true for small negative double",
    ),
    AllElementsTrueTest(
        "double_int32_max",
        array=[2147483647.1],
        expected=True,
        msg="Should return true for INT32_MAX as double",
    ),
    AllElementsTrueTest(
        "double_int32_min",
        array=[-2147483648.1],
        expected=True,
        msg="Should return true for INT32_MIN as double",
    ),
]


@pytest.mark.parametrize("test", pytest_params(DOUBLE_TESTS))
def test_allElementsTrue_double(collection, test):
    """Test $allElementsTrue with double values."""
    result = execute_expression(collection, build_expr(test))
    assert_expression_result(result, expected=test.expected, msg=test.msg)


# ---------------------------------------------------------------------------
# Data type coverage — int
# ---------------------------------------------------------------------------
INT_TESTS: list[AllElementsTrueTest] = [
    AllElementsTrueTest(
        "int_zero_explicit", array=[0], expected=False, msg="Should return false for NumberInt(0)"
    ),
    AllElementsTrueTest(
        "int_1_explicit", array=[1], expected=True, msg="Should return true for NumberInt(1)"
    ),
    AllElementsTrueTest(
        "int_neg1_explicit", array=[-1], expected=True, msg="Should return true for NumberInt(-1)"
    ),
    AllElementsTrueTest(
        "int_max", array=[INT32_MAX], expected=True, msg="Should return true for INT32_MAX"
    ),
    AllElementsTrueTest(
        "int_min", array=[INT32_MIN], expected=True, msg="Should return true for INT32_MIN"
    ),
]


@pytest.mark.parametrize("test", pytest_params(INT_TESTS))
def test_allElementsTrue_int(collection, test):
    """Test $allElementsTrue with int values."""
    result = execute_expression(collection, build_expr(test))
    assert_expression_result(result, expected=test.expected, msg=test.msg)


# ---------------------------------------------------------------------------
# Data type coverage — long
# ---------------------------------------------------------------------------
LONG_TESTS: list[AllElementsTrueTest] = [
    AllElementsTrueTest(
        "long_zero", array=[Int64(0)], expected=False, msg="Should return false for NumberLong(0)"
    ),
    AllElementsTrueTest(
        "long_1", array=[Int64(1)], expected=True, msg="Should return true for NumberLong(1)"
    ),
    AllElementsTrueTest(
        "long_neg1", array=[Int64(-1)], expected=True, msg="Should return true for NumberLong(-1)"
    ),
    AllElementsTrueTest(
        "long_max", array=[INT64_MAX], expected=True, msg="Should return true for INT64_MAX"
    ),
    AllElementsTrueTest(
        "long_min", array=[INT64_MIN], expected=True, msg="Should return true for INT64_MIN"
    ),
]


@pytest.mark.parametrize("test", pytest_params(LONG_TESTS))
def test_allElementsTrue_long(collection, test):
    """Test $allElementsTrue with long values."""
    result = execute_expression(collection, build_expr(test))
    assert_expression_result(result, expected=test.expected, msg=test.msg)


# ---------------------------------------------------------------------------
# Data type coverage — decimal128
# ---------------------------------------------------------------------------
DECIMAL_TESTS: list[AllElementsTrueTest] = [
    AllElementsTrueTest(
        "decimal_zero_val",
        array=[Decimal128("0")],
        expected=False,
        msg="Should return false for NumberDecimal(0)",
    ),
    AllElementsTrueTest(
        "decimal_neg_zero_val",
        array=[Decimal128("-0")],
        expected=False,
        msg="Should return false for NumberDecimal(-0)",
    ),
    AllElementsTrueTest(
        "decimal_small_pos",
        array=[Decimal128("0.0000001")],
        expected=True,
        msg="Should return true for small positive decimal",
    ),
    AllElementsTrueTest(
        "decimal_small_neg",
        array=[Decimal128("-0.0000001")],
        expected=True,
        msg="Should return true for small negative decimal",
    ),
    AllElementsTrueTest(
        "decimal_int32_max",
        array=[Decimal128("2147483647")],
        expected=True,
        msg="Should return true for INT32_MAX as decimal",
    ),
    AllElementsTrueTest(
        "decimal_int32_min",
        array=[Decimal128("-2147483648")],
        expected=True,
        msg="Should return true for INT32_MIN as decimal",
    ),
]


@pytest.mark.parametrize("test", pytest_params(DECIMAL_TESTS))
def test_allElementsTrue_decimal128(collection, test):
    """Test $allElementsTrue with Decimal128 values."""
    result = execute_expression(collection, build_expr(test))
    assert_expression_result(result, expected=test.expected, msg=test.msg)


# ---------------------------------------------------------------------------
# undefined/null in array is falsy (via field reference)
# ---------------------------------------------------------------------------
def test_allElementsTrue_undefined_falsy(collection):
    """Test $allElementsTrue with array containing null returns false."""
    result = execute_expression_with_insert(
        collection, {"$allElementsTrue": ["$arr"]}, {"arr": [None]}
    )
    assert_expression_result(result, expected=False, msg="Array with null should return false")


# ---------------------------------------------------------------------------
# Duplicates
# ---------------------------------------------------------------------------
DUPLICATE_TESTS: list[AllElementsTrueTest] = [
    AllElementsTrueTest(
        "all_true",
        array=[True, True, True],
        expected=True,
        msg="Should return true for all true duplicates",
    ),
    AllElementsTrueTest(
        "all_false",
        array=[False, False, False],
        expected=False,
        msg="Should return false for all false duplicates",
    ),
    AllElementsTrueTest(
        "all_ones",
        array=[1, 1, 1, 1],
        expected=True,
        msg="Should return true for all one duplicates",
    ),
    AllElementsTrueTest(
        "all_zeros",
        array=[0, 0, 0],
        expected=False,
        msg="Should return false for all zero duplicates",
    ),
    AllElementsTrueTest(
        "all_nulls",
        array=[None, None],
        expected=False,
        msg="Should return false for all null duplicates",
    ),
]


@pytest.mark.parametrize("test", pytest_params(DUPLICATE_TESTS))
def test_allElementsTrue_duplicates(collection, test):
    """Test $allElementsTrue with duplicate elements."""
    result = execute_expression(collection, build_expr(test))
    assert_expression_result(result, expected=test.expected, msg=test.msg)
