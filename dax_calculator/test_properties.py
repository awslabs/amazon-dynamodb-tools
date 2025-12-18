"""
Property-based tests for DAX Cluster Calculator

These tests verify universal properties that should hold across all valid inputs.
Each test runs a minimum of 100 iterations with randomly generated inputs.
"""

import math
from hypothesis import given, strategies as st, settings
from dax_calculator import (
    round_up_kb, 
    calculate_normalized_rps,
    calculate_utilization_multiplier,
    calculate_node_loss_multiplier,
    calculate_target_rps,
    validate_positive,
    validate_range,
    validate_node_count,
    get_node_capacity,
    find_node_recommendations,
    check_memory_fit,
    CAPACITY_TABLE
)


# Feature: dax-cluster-calculator, Property 2: Item size rounding
# Validates: Requirements 1.2
@settings(max_examples=100)
@given(size=st.floats(min_value=0.01, max_value=1000, allow_nan=False, allow_infinity=False))
def test_size_rounding(size):
    """
    Property: For any fractional item size value, the rounded size should be 
    the smallest integer greater than or equal to the input value.
    """
    rounded = round_up_kb(size)
    
    # The rounded value should be >= the original size
    assert rounded >= size, f"Rounded value {rounded} should be >= original {size}"
    
    # The rounded value should equal math.ceil(size)
    assert rounded == math.ceil(size), f"Rounded value {rounded} should equal ceil({size}) = {math.ceil(size)}"
    
    # The rounded value should be an integer
    assert isinstance(rounded, int), f"Rounded value {rounded} should be an integer"


# Feature: dax-cluster-calculator, Property 1: Normalized RPS calculation correctness
# Validates: Requirements 1.1
@settings(max_examples=100)
@given(
    cache_hits=st.floats(min_value=0, max_value=1000000, allow_nan=False, allow_infinity=False),
    cache_misses=st.floats(min_value=0, max_value=100000, allow_nan=False, allow_infinity=False),
    writes=st.floats(min_value=0, max_value=50000, allow_nan=False, allow_infinity=False),
    size=st.integers(min_value=1, max_value=400),
    nodes=st.integers(min_value=1, max_value=11)
)
def test_normalized_rps_formula(cache_hits, cache_misses, writes, size, nodes):
    """
    Property: For any valid set of cache hit RPS, cache miss RPS, write RPS, 
    item size, and node count values, the calculated Normalized RPS should equal 
    (cache_hits * size) + (cache_misses * size * 10) + (writes * 25 * size * node_count).
    """
    result = calculate_normalized_rps(cache_hits, cache_misses, writes, size, nodes)
    expected = (cache_hits * size) + (cache_misses * size * 10) + (writes * 25 * size * nodes)
    
    # Allow for floating point precision errors
    assert abs(result - expected) < 0.01, \
        f"Normalized RPS {result} should equal expected {expected}"


# Feature: dax-cluster-calculator, Property 4: Utilization multiplier calculation
# Validates: Requirements 2.1
@settings(max_examples=100)
@given(target_utilization=st.floats(min_value=1, max_value=100, allow_nan=False, allow_infinity=False))
def test_utilization_multiplier(target_utilization):
    """
    Property: For any target utilization percentage between 1 and 100, 
    the utilization multiplier should equal (100 / target_utilization).
    """
    result = calculate_utilization_multiplier(target_utilization)
    expected = 100 / target_utilization
    
    # Allow for floating point precision errors
    assert abs(result - expected) < 0.0001, \
        f"Utilization multiplier {result} should equal {expected}"


# Feature: dax-cluster-calculator, Property 5: Node loss multiplier calculation
# Validates: Requirements 2.2
@settings(max_examples=100)
@given(node_count=st.integers(min_value=2, max_value=100))
def test_node_loss_multiplier(node_count):
    """
    Property: For any node count greater than 1, the node loss multiplier 
    should equal (node_count / (node_count - 1)).
    """
    result = calculate_node_loss_multiplier(node_count)
    expected = node_count / (node_count - 1)
    
    # Allow for floating point precision errors
    assert abs(result - expected) < 0.0001, \
        f"Node loss multiplier {result} should equal {expected}"


# Feature: dax-cluster-calculator, Property 6: Maximum multiplier selection
# Validates: Requirements 2.3
@settings(max_examples=100)
@given(
    normalized_rps=st.floats(min_value=1, max_value=10000000, allow_nan=False, allow_infinity=False),
    utilization_mult=st.floats(min_value=1, max_value=100, allow_nan=False, allow_infinity=False),
    node_loss_mult=st.floats(min_value=1, max_value=100, allow_nan=False, allow_infinity=False)
)
def test_maximum_multiplier_selection(normalized_rps, utilization_mult, node_loss_mult):
    """
    Property: For any pair of utilization multiplier and node loss multiplier values, 
    the Target RPS calculation should use the maximum of the two multipliers.
    """
    result = calculate_target_rps(normalized_rps, utilization_mult, node_loss_mult)
    max_mult = max(utilization_mult, node_loss_mult)
    expected = normalized_rps * max_mult
    
    # Allow for floating point precision errors
    assert abs(result - expected) < 0.01, \
        f"Target RPS {result} should equal normalized_rps * max_multiplier = {expected}"


# Feature: dax-cluster-calculator, Property 7: Target RPS calculation
# Validates: Requirements 2.4
@settings(max_examples=100)
@given(
    normalized_rps=st.floats(min_value=1, max_value=10000000, allow_nan=False, allow_infinity=False),
    multiplier=st.floats(min_value=1, max_value=100, allow_nan=False, allow_infinity=False)
)
def test_target_rps_calculation(normalized_rps, multiplier):
    """
    Property: For any normalized RPS and multiplier values, 
    the Target RPS should equal (normalized_rps * multiplier).
    """
    # Use the same multiplier for both to test the calculation formula
    result = calculate_target_rps(normalized_rps, multiplier, multiplier)
    expected = normalized_rps * multiplier
    
    # Allow for floating point precision errors
    assert abs(result - expected) < 0.01, \
        f"Target RPS {result} should equal {expected}"


# Feature: dax-cluster-calculator, Property 3: Negative input rejection
# Validates: Requirements 1.3
@settings(max_examples=100)
@given(value=st.floats(max_value=-0.01, allow_nan=False, allow_infinity=False))
def test_negative_input_rejection(value):
    """
    Property: For any input parameter that receives a negative value, 
    the validation function should return False and prevent calculation.
    """
    # Test validate_positive with negative values
    result = validate_positive(value, "test_param")
    assert result is False, \
        f"validate_positive should return False for negative value {value}"
    
    # Test validate_range with negative values outside range
    result_range = validate_range(value, 0, 100, "test_param")
    assert result_range is False, \
        f"validate_range should return False for negative value {value} outside range [0, 100]"
    
    # Test validate_node_count with negative integer values
    if value == int(value):  # Only test with integer values
        result_node = validate_node_count(int(value))
        assert result_node is False, \
            f"validate_node_count should return False for negative value {int(value)}"


# Feature: dax-cluster-calculator, Property 8: Capacity table lookup
# Validates: Requirements 3.1
@settings(max_examples=100)
@given(
    node_type=st.sampled_from(list(CAPACITY_TABLE.keys())),
    node_count=st.sampled_from([1, 3, 5, 11])
)
def test_capacity_table_lookup(node_type, node_count):
    """
    Property: For any valid node count (1, 3, 5, or 11), looking up a node type's 
    capacity should return the correct value from the capacity table.
    """
    result = get_node_capacity(node_type, node_count)
    expected = CAPACITY_TABLE[node_type]['capacity'][node_count]
    
    assert result == expected, \
        f"Capacity lookup for {node_type} with {node_count} nodes should return {expected}, got {result}"
    
    # Also verify that the result is a positive integer
    assert result > 0, \
        f"Capacity should be positive, got {result}"


# Feature: dax-cluster-calculator, Property 9: Optimal node type recommendation
# Validates: Requirements 3.3, 3.5
@settings(max_examples=100)
@given(
    target_rps=st.floats(min_value=1000, max_value=500000, allow_nan=False, allow_infinity=False),
    node_count=st.sampled_from([1, 3, 5, 11])
)
def test_optimal_node_recommendation(target_rps, node_count):
    """
    Property: For any Target RPS value and node count, the recommended node type should be 
    the smallest (by memory) node type whose capacity meets or exceeds the Target RPS, 
    and all node types with sufficient capacity should be included in recommendations.
    """
    recommendations = find_node_recommendations(target_rps, node_count)
    
    # If there are recommendations, verify they are sorted by memory (smallest first)
    if len(recommendations) > 0:
        for i in range(len(recommendations) - 1):
            assert recommendations[i].memory_gb <= recommendations[i + 1].memory_gb, \
                f"Recommendations should be sorted by memory size (smallest first)"
        
        # Verify all recommendations meet or exceed the target RPS
        for rec in recommendations:
            assert rec.capacity >= target_rps, \
                f"Recommendation {rec.node_type} capacity {rec.capacity} should be >= target RPS {target_rps}"
        
        # Verify the first recommendation is the smallest that meets the requirement
        smallest_rec = recommendations[0]
        
        # Check that no smaller node type (not in recommendations) could meet the requirement
        for node_type, node_info in CAPACITY_TABLE.items():
            capacity = get_node_capacity(node_type, node_count)
            if capacity is not None and capacity >= target_rps:
                # This node type meets the requirement, so it should have memory >= smallest recommendation
                assert node_info['memory_gb'] >= smallest_rec.memory_gb, \
                    f"Node type {node_type} with capacity {capacity} >= target {target_rps} should have memory >= smallest recommendation {smallest_rec.memory_gb}"
    
    # If there are no recommendations, verify that no node type can meet the target RPS
    if len(recommendations) == 0:
        for node_type, node_info in CAPACITY_TABLE.items():
            capacity = get_node_capacity(node_type, node_count)
            if capacity is not None:
                assert capacity < target_rps, \
                    f"If no recommendations, all node types should have capacity < target RPS, but {node_type} has {capacity}"


# Feature: dax-cluster-calculator, Property 11: Memory capacity comparison
# Validates: Requirements 6.1
@settings(max_examples=100)
@given(
    node_type=st.sampled_from(list(CAPACITY_TABLE.keys())),
    dataset_size_gb=st.floats(min_value=0.1, max_value=1000, allow_nan=False, allow_infinity=False)
)
def test_memory_capacity_comparison(node_type, dataset_size_gb):
    """
    Property: For any dataset size and node type, the memory comparison should 
    correctly determine whether dataset_size <= node_memory.
    """
    result = check_memory_fit(dataset_size_gb, node_type)
    node_memory_gb = CAPACITY_TABLE[node_type]['memory_gb']
    expected = dataset_size_gb <= node_memory_gb
    
    assert result == expected, \
        f"Memory fit check for {dataset_size_gb}GB on {node_type} ({node_memory_gb}GB) should return {expected}, got {result}"


# Feature: dax-cluster-calculator, Property 12: Optional dataset handling
# Validates: Requirements 6.5
@settings(max_examples=100)
@given(node_type=st.sampled_from(list(CAPACITY_TABLE.keys())))
def test_optional_dataset_handling(node_type):
    """
    Property: For any calculation where dataset size is not provided (None), 
    the calculation should complete successfully and memory validation should be skipped.
    """
    # When dataset size is None, check_memory_fit should return True (skip validation)
    result = check_memory_fit(None, node_type)
    
    assert result is True, \
        f"Memory fit check with None dataset size should return True (skip validation), got {result}"


# Feature: dax-cluster-calculator, Property 10: Output completeness
# Validates: Requirements 5.1, 5.2, 5.3, 5.4
@settings(max_examples=100)
@given(
    cache_hits=st.floats(min_value=0, max_value=1000000, allow_nan=False, allow_infinity=False),
    cache_misses=st.floats(min_value=0, max_value=100000, allow_nan=False, allow_infinity=False),
    writes=st.floats(min_value=0, max_value=50000, allow_nan=False, allow_infinity=False),
    item_size_kb=st.floats(min_value=0.01, max_value=400, allow_nan=False, allow_infinity=False),
    node_count=st.sampled_from([1, 3, 5, 11]),
    target_utilization=st.floats(min_value=1, max_value=100, allow_nan=False, allow_infinity=False),
    dataset_size_gb=st.one_of(st.none(), st.floats(min_value=0.1, max_value=1000, allow_nan=False, allow_infinity=False))
)
def test_output_completeness(cache_hits, cache_misses, writes, item_size_kb, 
                             node_count, target_utilization, dataset_size_gb):
    """
    Property: For any successful calculation, the output data structure should contain 
    all input parameters, normalized RPS, target RPS, selected multiplier with reasoning, 
    and node recommendations.
    """
    from io import StringIO
    import sys
    from dax_calculator import (
        ClusterInputs, 
        print_input_summary,
        print_calculation_details,
        print_recommendations,
        format_number
    )
    
    # Create ClusterInputs with the generated values
    inputs = ClusterInputs(
        cache_hit_rps=cache_hits,
        cache_miss_rps=cache_misses,
        write_rps=writes,
        item_size_kb=item_size_kb,
        node_count=node_count,
        target_utilization=target_utilization,
        dataset_size_gb=dataset_size_gb
    )
    
    # Perform calculations
    size = round_up_kb(item_size_kb)
    normalized_rps = calculate_normalized_rps(cache_hits, cache_misses, writes, size, node_count)
    utilization_mult = calculate_utilization_multiplier(target_utilization)
    node_loss_mult = calculate_node_loss_multiplier(node_count) if node_count > 1 else 1.0
    target_rps = calculate_target_rps(normalized_rps, utilization_mult, node_loss_mult)
    
    # Determine which multiplier was selected
    selected_mult = max(utilization_mult, node_loss_mult)
    if selected_mult == utilization_mult:
        multiplier_reason = "utilization"
    else:
        multiplier_reason = "node loss tolerance"
    
    # Get recommendations
    recommendations = find_node_recommendations(target_rps, node_count)
    
    # Capture output from print functions
    captured_output = StringIO()
    sys.stdout = captured_output
    
    try:
        # Call the output functions
        print_input_summary(inputs)
        print_calculation_details(normalized_rps, target_rps, utilization_mult, 
                                  node_loss_mult, selected_mult, multiplier_reason)
        print_recommendations(recommendations, node_count, target_rps)
        
        # Get the captured output
        output = captured_output.getvalue()
        
        # Verify all input parameters are present in output
        assert str(cache_hits) in output or format_number(cache_hits) in output, \
            "Output should contain cache hit RPS"
        assert str(cache_misses) in output or format_number(cache_misses) in output, \
            "Output should contain cache miss RPS"
        assert str(writes) in output or format_number(writes) in output, \
            "Output should contain writes RPS"
        assert str(item_size_kb) in output, \
            "Output should contain item size"
        assert str(node_count) in output, \
            "Output should contain node count"
        assert str(target_utilization) in output, \
            "Output should contain target utilization"
        
        # Verify calculated values are present
        assert format_number(normalized_rps) in output or str(normalized_rps) in output, \
            "Output should contain normalized RPS"
        assert format_number(target_rps) in output or str(target_rps) in output, \
            "Output should contain target RPS"
        
        # Verify multiplier information is present
        assert f"{utilization_mult:.2f}" in output, \
            "Output should contain utilization multiplier"
        assert f"{selected_mult:.2f}" in output, \
            "Output should contain selected multiplier"
        assert multiplier_reason in output.lower(), \
            "Output should contain multiplier reasoning"
        
        # Verify recommendations section is present
        if recommendations:
            # At least one node type should be mentioned
            assert any(rec.node_type in output for rec in recommendations), \
                "Output should contain at least one recommended node type"
        else:
            # Should have warning about exceeding capacity
            assert "warning" in output.lower() or "exceed" in output.lower(), \
                "Output should contain warning when no recommendations available"
        
    finally:
        # Restore stdout
        sys.stdout = sys.__stdout__
