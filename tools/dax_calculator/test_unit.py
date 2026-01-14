"""
Unit tests for DAX Cluster Calculator

These tests verify specific examples, edge cases, and error conditions.
Tests focus on concrete scenarios from AWS documentation and boundary conditions.
"""

import pytest
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
    ClusterInputs,
    NodeRecommendation,
    CAPACITY_TABLE
)


class TestAWSDocumentationExample:
    """Test the exact example from AWS DAX cluster sizing documentation."""
    
    def test_aws_example_normalized_rps(self):
        """
        Test AWS documentation example:
        50,000 cache hits, 1,000 misses, 100 writes, 2KB, 3 nodes = 135,000 Normalized RPS
        
        Requirements: 1.1
        """
        normalized = calculate_normalized_rps(50000, 1000, 100, 2, 3)
        assert normalized == 135000.0, \
            f"AWS example should produce 135,000 Normalized RPS, got {normalized}"
    
    def test_aws_example_full_calculation(self):
        """
        Test complete AWS example with Target RPS calculation.
        
        Requirements: 1.1, 2.1, 2.2, 2.3, 2.4
        """
        # Given: AWS example inputs
        cache_hits = 50000
        cache_misses = 1000
        writes = 100
        size = 2
        node_count = 3
        target_utilization = 80
        
        # Calculate Normalized RPS
        normalized_rps = calculate_normalized_rps(cache_hits, cache_misses, writes, size, node_count)
        assert normalized_rps == 135000.0
        
        # Calculate multipliers
        utilization_mult = calculate_utilization_multiplier(target_utilization)
        assert utilization_mult == 1.25
        
        node_loss_mult = calculate_node_loss_multiplier(node_count)
        assert node_loss_mult == 1.5
        
        # Calculate Target RPS
        target_rps = calculate_target_rps(normalized_rps, utilization_mult, node_loss_mult)
        expected_target = 135000 * 1.5  # 202,500
        assert target_rps == expected_target


class TestEdgeCases:
    """Test edge cases and boundary conditions."""
    
    def test_single_node_cluster(self):
        """
        Test single node cluster where node loss multiplier doesn't apply.
        
        Requirements: 2.2
        """
        # For a single node, node loss multiplier would be 1/(1-1) = division by zero
        # The application should handle this by using utilization multiplier only
        normalized_rps = 100000
        utilization_mult = 1.25
        
        # For single node, we expect the application to use utilization multiplier
        # The calculate_node_loss_multiplier function should not be called for single node
        # or should be handled specially in the main flow
        
        # Test that we can still calculate target RPS with node_loss_mult = 1.0
        target_rps = calculate_target_rps(normalized_rps, utilization_mult, 1.0)
        assert target_rps == 125000.0
    
    def test_zero_cache_misses(self):
        """
        Test workload with zero cache misses (all cache hits).
        
        Requirements: 1.1
        """
        normalized = calculate_normalized_rps(
            cache_hits=100000,
            cache_misses=0,
            writes=500,
            size=3,
            node_count=5
        )
        # Expected: (100000 * 3) + (0 * 3 * 10) + (500 * 25 * 3 * 5)
        # = 300000 + 0 + 187500 = 487500
        expected = 487500.0
        assert normalized == expected
    
    def test_zero_writes(self):
        """
        Test read-only workload with zero writes.
        
        Requirements: 1.1
        """
        normalized = calculate_normalized_rps(
            cache_hits=50000,
            cache_misses=5000,
            writes=0,
            size=2,
            node_count=3
        )
        # Expected: (50000 * 2) + (5000 * 2 * 10) + (0 * 25 * 2 * 3)
        # = 100000 + 100000 + 0 = 200000
        expected = 200000.0
        assert normalized == expected
    
    def test_fractional_item_size_rounding(self):
        """
        Test that fractional item sizes are rounded up correctly.
        
        Requirements: 1.2
        """
        # Test various fractional sizes
        assert round_up_kb(1.1) == 2
        assert round_up_kb(1.9) == 2
        assert round_up_kb(2.0) == 2
        assert round_up_kb(2.01) == 3
        assert round_up_kb(0.5) == 1
    
    def test_minimum_node_count(self):
        """
        Test cluster with minimum node count (1 node).
        
        Requirements: 1.4
        """
        # Single node should be valid
        assert validate_node_count(1) is True
        
        # Calculate normalized RPS for single node
        normalized = calculate_normalized_rps(10000, 1000, 100, 2, 1)
        # Expected: (10000 * 2) + (1000 * 2 * 10) + (100 * 25 * 2 * 1)
        # = 20000 + 20000 + 5000 = 45000
        assert normalized == 45000.0
    
    def test_maximum_supported_node_count(self):
        """
        Test cluster with maximum supported node count (11 nodes).
        
        Requirements: 3.1
        """
        # 11 nodes is the maximum supported in the capacity table
        node_count = 11
        
        # Verify capacity lookup works for 11 nodes
        capacity = get_node_capacity('dax.r5.large', node_count)
        assert capacity == 825000
        
        # Calculate normalized RPS for 11 nodes
        normalized = calculate_normalized_rps(10000, 1000, 100, 2, 11)
        # Expected: (10000 * 2) + (1000 * 2 * 10) + (100 * 25 * 2 * 11)
        # = 20000 + 20000 + 55000 = 95000
        assert normalized == 95000.0
    
    def test_high_utilization_target(self):
        """
        Test with high utilization target (close to 100%).
        
        Requirements: 2.1
        """
        # At 99% utilization, multiplier should be very close to 1
        utilization_mult = calculate_utilization_multiplier(99)
        expected = 100 / 99
        assert abs(utilization_mult - expected) < 0.001
        assert utilization_mult > 1.0
    
    def test_low_utilization_target(self):
        """
        Test with low utilization target (more headroom).
        
        Requirements: 2.1
        """
        # At 50% utilization, multiplier should be 2.0
        utilization_mult = calculate_utilization_multiplier(50)
        assert utilization_mult == 2.0


class TestErrorConditions:
    """Test error conditions and input validation."""
    
    def test_negative_cache_hits(self):
        """
        Test that negative cache hit values are rejected.
        
        Requirements: 1.3
        """
        assert validate_positive(-100, "cache_hits") is False
        assert validate_positive(-0.1, "cache_hits") is False
    
    def test_negative_cache_misses(self):
        """
        Test that negative cache miss values are rejected.
        
        Requirements: 1.3
        """
        assert validate_positive(-50, "cache_misses") is False
    
    def test_negative_writes(self):
        """
        Test that negative write values are rejected.
        
        Requirements: 1.3
        """
        assert validate_positive(-25, "writes") is False
    
    def test_negative_item_size(self):
        """
        Test that negative item sizes are rejected.
        
        Requirements: 1.3
        """
        assert validate_positive(-2.5, "item_size") is False
    
    def test_zero_node_count(self):
        """
        Test that zero node count is rejected.
        
        Requirements: 1.4
        """
        assert validate_node_count(0) is False
    
    def test_negative_node_count(self):
        """
        Test that negative node count is rejected.
        
        Requirements: 1.4
        """
        assert validate_node_count(-1) is False
        assert validate_node_count(-5) is False
    
    def test_invalid_utilization_below_range(self):
        """
        Test that utilization below 1% is rejected.
        
        Requirements: 1.3
        """
        assert validate_range(0, 1, 100, "utilization") is False
        assert validate_range(-10, 1, 100, "utilization") is False
    
    def test_invalid_utilization_above_range(self):
        """
        Test that utilization above 100% is rejected.
        
        Requirements: 1.3
        """
        assert validate_range(101, 1, 100, "utilization") is False
        assert validate_range(150, 1, 100, "utilization") is False
    
    def test_valid_utilization_range(self):
        """
        Test that valid utilization values (1-100) are accepted.
        
        Requirements: 1.3
        """
        assert validate_range(1, 1, 100, "utilization") is True
        assert validate_range(50, 1, 100, "utilization") is True
        assert validate_range(100, 1, 100, "utilization") is True



class TestNodeRecommendations:
    """Test node type recommendation logic."""
    
    def test_recommendation_for_small_workload(self):
        """
        Test recommendations for a small workload that fits in smallest node type.
        
        Requirements: 3.3, 3.5
        """
        # Target RPS of 50,000 with 3 nodes
        # dax.r5.large has capacity of 225,000 for 3 nodes
        recommendations = find_node_recommendations(50000, 3)
        
        # Should have recommendations
        assert len(recommendations) > 0
        
        # First recommendation should be the smallest that meets requirement
        first_rec = recommendations[0]
        assert first_rec.capacity >= 50000
        
        # All recommendations should meet the requirement
        for rec in recommendations:
            assert rec.capacity >= 50000
    
    def test_recommendation_for_large_workload(self):
        """
        Test recommendations for a large workload requiring bigger node types.
        
        Requirements: 3.3, 3.5
        """
        # Target RPS of 2,000,000 with 3 nodes
        # Only the largest node types can handle this
        recommendations = find_node_recommendations(2000000, 3)
        
        # Should have recommendations
        assert len(recommendations) > 0
        
        # All recommendations should meet the requirement
        for rec in recommendations:
            assert rec.capacity >= 2000000
        
        # Smallest recommendation should be dax.r5.8xlarge or larger
        first_rec = recommendations[0]
        assert first_rec.memory_gb >= 256
    
    def test_no_recommendation_when_exceeds_capacity(self):
        """
        Test that no recommendations are returned when Target RPS exceeds all capacities.
        
        Requirements: 3.2
        """
        # Target RPS of 20,000,000 with 3 nodes exceeds all node type capacities
        recommendations = find_node_recommendations(20000000, 3)
        
        # Should have no recommendations
        assert len(recommendations) == 0
    
    def test_recommendations_sorted_by_memory(self):
        """
        Test that recommendations are sorted by memory size (smallest first).
        
        Requirements: 3.3
        """
        # Get recommendations for a moderate workload
        recommendations = find_node_recommendations(500000, 3)
        
        # Should have multiple recommendations
        assert len(recommendations) > 1
        
        # Verify sorted by memory (ascending)
        for i in range(len(recommendations) - 1):
            assert recommendations[i].memory_gb <= recommendations[i + 1].memory_gb
    
    def test_unsupported_node_count(self):
        """
        Test that unsupported node counts return no recommendations.
        
        Requirements: 3.1
        """
        # Node count of 7 is not supported (only 1, 3, 5, 11 are supported)
        recommendations = find_node_recommendations(100000, 7)
        
        # Should have no recommendations for unsupported node count
        assert len(recommendations) == 0


class TestMemoryValidation:
    """Test memory capacity validation."""
    
    def test_dataset_fits_in_memory(self):
        """
        Test that dataset smaller than node memory is validated correctly.
        
        Requirements: 6.1
        """
        # dax.r5.large has 16 GB memory
        # 10 GB dataset should fit
        assert check_memory_fit(10.0, 'dax.r5.large') is True
    
    def test_dataset_exceeds_memory(self):
        """
        Test that dataset larger than node memory is detected.
        
        Requirements: 6.2
        """
        # dax.r5.large has 16 GB memory
        # 20 GB dataset should not fit
        assert check_memory_fit(20.0, 'dax.r5.large') is False
    
    def test_dataset_exactly_fits_memory(self):
        """
        Test boundary condition where dataset exactly equals node memory.
        
        Requirements: 6.1
        """
        # dax.r5.large has 16 GB memory
        # 16 GB dataset should fit (equal is acceptable)
        assert check_memory_fit(16.0, 'dax.r5.large') is True
    
    def test_optional_dataset_size(self):
        """
        Test that None dataset size skips memory validation.
        
        Requirements: 6.5
        """
        # When dataset size is None, validation should be skipped (return True)
        assert check_memory_fit(None, 'dax.r5.large') is True
        assert check_memory_fit(None, 'dax.r5.24xlarge') is True


class TestEndToEndFlow:
    """Test end-to-end calculation flows with sample inputs."""
    
    def test_complete_calculation_flow_small_cluster(self):
        """
        Test complete calculation flow for a small cluster configuration.
        
        Requirements: 1.1, 2.1, 2.2, 2.3, 2.4, 3.3
        """
        # Given: Small workload inputs
        inputs = ClusterInputs(
            cache_hit_rps=10000,
            cache_miss_rps=500,
            write_rps=50,
            item_size_kb=1.5,
            node_count=3,
            target_utilization=75,
            dataset_size_gb=5.0
        )
        
        # Step 1: Round item size
        size = round_up_kb(inputs.item_size_kb)
        assert size == 2
        
        # Step 2: Calculate Normalized RPS
        normalized_rps = calculate_normalized_rps(
            inputs.cache_hit_rps,
            inputs.cache_miss_rps,
            inputs.write_rps,
            size,
            inputs.node_count
        )
        # Expected: (10000 * 2) + (500 * 2 * 10) + (50 * 25 * 2 * 3)
        # = 20000 + 10000 + 7500 = 37500
        assert normalized_rps == 37500.0
        
        # Step 3: Calculate multipliers
        utilization_mult = calculate_utilization_multiplier(inputs.target_utilization)
        assert abs(utilization_mult - (100/75)) < 0.001
        
        node_loss_mult = calculate_node_loss_multiplier(inputs.node_count)
        assert node_loss_mult == 1.5
        
        # Step 4: Calculate Target RPS
        target_rps = calculate_target_rps(normalized_rps, utilization_mult, node_loss_mult)
        # Max multiplier is 1.5, so Target RPS = 37500 * 1.5 = 56250
        assert target_rps == 56250.0
        
        # Step 5: Get recommendations
        recommendations = find_node_recommendations(target_rps, inputs.node_count)
        assert len(recommendations) > 0
        
        # Step 6: Validate memory
        first_rec = recommendations[0]
        memory_ok = check_memory_fit(inputs.dataset_size_gb, first_rec.node_type)
        # 5 GB should fit in any node type (smallest is 16 GB)
        assert memory_ok is True
    
    def test_complete_calculation_flow_large_cluster(self):
        """
        Test complete calculation flow for a large cluster configuration.
        
        Requirements: 1.1, 2.1, 2.2, 2.3, 2.4, 3.3
        """
        # Given: Large workload inputs
        inputs = ClusterInputs(
            cache_hit_rps=500000,
            cache_miss_rps=10000,
            write_rps=5000,
            item_size_kb=4.0,
            node_count=11,
            target_utilization=80,
            dataset_size_gb=100.0
        )
        
        # Step 1: Round item size
        size = round_up_kb(inputs.item_size_kb)
        assert size == 4
        
        # Step 2: Calculate Normalized RPS
        normalized_rps = calculate_normalized_rps(
            inputs.cache_hit_rps,
            inputs.cache_miss_rps,
            inputs.write_rps,
            size,
            inputs.node_count
        )
        # Expected: (500000 * 4) + (10000 * 4 * 10) + (5000 * 25 * 4 * 11)
        # = 2000000 + 400000 + 5500000 = 7900000
        assert normalized_rps == 7900000.0
        
        # Step 3: Calculate multipliers
        utilization_mult = calculate_utilization_multiplier(inputs.target_utilization)
        assert utilization_mult == 1.25
        
        node_loss_mult = calculate_node_loss_multiplier(inputs.node_count)
        expected_node_loss = 11 / 10
        assert abs(node_loss_mult - expected_node_loss) < 0.001
        
        # Step 4: Calculate Target RPS
        target_rps = calculate_target_rps(normalized_rps, utilization_mult, node_loss_mult)
        # Max multiplier is 1.25, so Target RPS = 7900000 * 1.25 = 9875000
        assert target_rps == 9875000.0
        
        # Step 5: Get recommendations
        recommendations = find_node_recommendations(target_rps, inputs.node_count)
        assert len(recommendations) > 0
        
        # Step 6: Validate memory
        # 100 GB requires at least dax.r5.4xlarge (128 GB)
        first_rec = recommendations[0]
        memory_ok = check_memory_fit(inputs.dataset_size_gb, first_rec.node_type)
        if memory_ok:
            # If first recommendation has sufficient memory, verify it
            node_memory = CAPACITY_TABLE[first_rec.node_type]['memory_gb']
            assert node_memory >= 100
    
    def test_complete_calculation_flow_without_dataset_size(self):
        """
        Test complete calculation flow when dataset size is not provided.
        
        Requirements: 1.1, 2.1, 2.2, 2.3, 2.4, 3.3, 6.5
        """
        # Given: Inputs without dataset size
        inputs = ClusterInputs(
            cache_hit_rps=25000,
            cache_miss_rps=2500,
            write_rps=250,
            item_size_kb=3.0,
            node_count=5,
            target_utilization=70,
            dataset_size_gb=None  # No dataset size provided
        )
        
        # Step 1: Round item size
        size = round_up_kb(inputs.item_size_kb)
        assert size == 3
        
        # Step 2: Calculate Normalized RPS
        normalized_rps = calculate_normalized_rps(
            inputs.cache_hit_rps,
            inputs.cache_miss_rps,
            inputs.write_rps,
            size,
            inputs.node_count
        )
        # Expected: (25000 * 3) + (2500 * 3 * 10) + (250 * 25 * 3 * 5)
        # = 75000 + 75000 + 93750 = 243750
        assert normalized_rps == 243750.0
        
        # Step 3: Calculate multipliers
        utilization_mult = calculate_utilization_multiplier(inputs.target_utilization)
        expected_util = 100 / 70
        assert abs(utilization_mult - expected_util) < 0.001
        
        node_loss_mult = calculate_node_loss_multiplier(inputs.node_count)
        expected_node_loss = 5 / 4
        assert abs(node_loss_mult - expected_node_loss) < 0.001
        
        # Step 4: Calculate Target RPS
        target_rps = calculate_target_rps(normalized_rps, utilization_mult, node_loss_mult)
        max_mult = max(utilization_mult, node_loss_mult)
        expected_target = 243750 * max_mult
        assert abs(target_rps - expected_target) < 0.01
        
        # Step 5: Get recommendations
        recommendations = find_node_recommendations(target_rps, inputs.node_count)
        assert len(recommendations) > 0
        
        # Step 6: Memory validation should be skipped (return True)
        for rec in recommendations:
            memory_ok = check_memory_fit(inputs.dataset_size_gb, rec.node_type)
            assert memory_ok is True  # Should skip validation when dataset_size_gb is None


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
