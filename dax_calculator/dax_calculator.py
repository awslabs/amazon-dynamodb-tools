"""DAX Cluster Calculator - AWS DynamoDB Accelerator cluster sizing tool."""

from dataclasses import dataclass
from typing import Optional, List
import math


@dataclass
class ClusterInputs:
    cache_hit_rps: float
    cache_miss_rps: float
    write_rps: float
    item_size_kb: float
    node_count: int
    target_utilization: float
    dataset_size_gb: Optional[float] = None


@dataclass
class NodeRecommendation:
    node_type: str
    memory_gb: int
    capacity: int
    memory_sufficient: bool
    monthly_cost: float


# DAX node type capacity table
# Maps node types to their memory, capacity, and hourly cost (us-east-1 pricing)
CAPACITY_TABLE = {
    'dax.r5.24xlarge': {
        'memory_gb': 768,
        'capacity': {1: 1000000, 3: 3000000, 5: 5000000, 11: 11000000},
        'hourly_cost': 8.208
    },
    'dax.r5.16xlarge': {
        'memory_gb': 512,
        'capacity': {1: 1000000, 3: 3000000, 5: 5000000, 11: 11000000},
        'hourly_cost': 5.472
    },
    'dax.r5.12xlarge': {
        'memory_gb': 384,
        'capacity': {1: 1000000, 3: 3000000, 5: 5000000, 11: 11000000},
        'hourly_cost': 4.104
    },
    'dax.r5.8xlarge': {
        'memory_gb': 256,
        'capacity': {1: 1000000, 3: 3000000, 5: 5000000, 11: 11000000},
        'hourly_cost': 2.736
    },
    'dax.r5.4xlarge': {
        'memory_gb': 128,
        'capacity': {1: 600000, 3: 1800000, 5: 3000000, 11: 6600000},
        'hourly_cost': 1.368
    },
    'dax.r5.2xlarge': {
        'memory_gb': 64,
        'capacity': {1: 300000, 3: 900000, 5: 1500000, 11: 3300000},
        'hourly_cost': 0.684
    },
    'dax.r5.xlarge': {
        'memory_gb': 32,
        'capacity': {1: 150000, 3: 450000, 5: 750000, 11: 1650000},
        'hourly_cost': 0.342
    },
    'dax.r5.large': {
        'memory_gb': 16,
        'capacity': {1: 75000, 3: 225000, 5: 375000, 11: 825000},
        'hourly_cost': 0.171
    }
}


# Core calculation functions
def round_up_kb(size: float) -> int:
    return math.ceil(size)


def calculate_normalized_rps(cache_hits: float, cache_misses: float, writes: float, 
                             size: int, node_count: int) -> float:
    return (cache_hits * size) + (cache_misses * size * 10) + (writes * 25 * size * node_count)


def calculate_utilization_multiplier(target_utilization: float) -> float:
    return 100 / target_utilization


def calculate_node_loss_multiplier(node_count: int) -> float:
    return node_count / (node_count - 1)


def calculate_target_rps(normalized_rps: float, utilization_mult: float, 
                         node_loss_mult: float) -> float:
    return normalized_rps * max(utilization_mult, node_loss_mult)


# Input validation
def validate_positive(value: float, param_name: str) -> bool:
    return value >= 0


def validate_range(value: float, min_value: float, max_value: float, param_name: str) -> bool:
    return min_value <= value <= max_value


def validate_node_count(node_count: int) -> bool:
    return node_count >= 1


# Input prompts
def prompt_float(prompt: str, min_value: float = 0) -> float:
    while True:
        try:
            value = float(input(prompt))
            if value >= min_value:
                return value
            print(f"Error: Value must be at least {min_value}. You entered: {value}")
        except ValueError:
            print("Error: Please enter a valid number.")


def prompt_int(prompt: str, min_value: int = 1) -> int:
    while True:
        try:
            value = int(input(prompt))
            if value >= min_value:
                return value
            print(f"Error: Value must be at least {min_value}. You entered: {value}")
        except ValueError:
            print("Error: Please enter a valid integer.")


def prompt_optional_float(prompt: str) -> Optional[float]:
    while True:
        user_input = input(prompt).strip()
        if user_input == "":
            return None
        try:
            value = float(user_input)
            if value >= 0:
                return value
            print(f"Error: Value must be positive. You entered: {value}")
        except ValueError:
            print("Error: Please enter a valid number or press Enter to skip.")


# Recommendation engine
def get_node_capacity(node_type: str, node_count: int) -> Optional[int]:
    if node_type not in CAPACITY_TABLE:
        return None
    return CAPACITY_TABLE[node_type]['capacity'].get(node_count)


def find_node_recommendations(target_rps: float, node_count: int) -> List[NodeRecommendation]:
    recommendations = []
    for node_type, node_info in CAPACITY_TABLE.items():
        capacity = get_node_capacity(node_type, node_count)
        if capacity and capacity >= target_rps:
            # Calculate monthly cost: hourly_cost * 24 hours * 30 days * node_count
            monthly_cost = node_info['hourly_cost'] * 24 * 30 * node_count
            recommendations.append(NodeRecommendation(
                node_type=node_type,
                memory_gb=node_info['memory_gb'],
                capacity=capacity,
                memory_sufficient=True,
                monthly_cost=monthly_cost
            ))
    recommendations.sort(key=lambda x: x.memory_gb)
    return recommendations


def check_memory_fit(dataset_size_gb: Optional[float], node_type: str) -> bool:
    if dataset_size_gb is None or node_type not in CAPACITY_TABLE:
        return True
    return dataset_size_gb <= CAPACITY_TABLE[node_type]['memory_gb']


# Output formatting
def format_number(num: float) -> str:
    return f"{int(num):,}" if isinstance(num, int) or num == int(num) else f"{num:,.2f}"


def print_header(text: str) -> None:
    print(f"\n{'=' * 60}\n{text}\n{'=' * 60}")


def print_input_summary(inputs: ClusterInputs) -> None:
    print_header("Input Summary")
    print(f"Cache Hit Reads/sec:      {format_number(inputs.cache_hit_rps)}")
    print(f"Cache Miss Reads/sec:     {format_number(inputs.cache_miss_rps)}")
    print(f"Writes/sec:               {format_number(inputs.write_rps)}")
    print(f"Item Size:                {inputs.item_size_kb} KB")
    print(f"Node Count:               {inputs.node_count}")
    print(f"Target Utilization:       {inputs.target_utilization}%")
    print(f"Dataset Size:             {inputs.dataset_size_gb or 'Not provided'} {'GB' if inputs.dataset_size_gb else ''}")


def print_calculation_details(normalized_rps: float, target_rps: float, 
                              utilization_mult: float, node_loss_mult: float,
                              selected_multiplier: float, multiplier_reason: str) -> None:
    print_header("Calculation Details")
    print(f"Normalized RPS:           {format_number(normalized_rps)}")
    print(f"\nMultipliers:")
    print(f"  Utilization Multiplier: {utilization_mult:.2f}")
    print(f"  Node Loss Multiplier:   {node_loss_mult:.2f}")
    print(f"  Selected Multiplier:    {selected_multiplier:.2f} ({multiplier_reason})")
    print(f"\nTarget RPS:               {format_number(target_rps)}")


def print_recommendations(recommendations: List[NodeRecommendation], 
                         node_count: int, target_rps: float) -> None:
    print_header("Node Type Recommendations")
    if not recommendations:
        print(f"\nWarning: Target RPS ({format_number(target_rps)}) exceeds maximum capacity")
        print(f"for {node_count} nodes. Consider:")
        print(f"  - Increasing the node count to 5 or 11 nodes")
        print(f"  - Using multiple DAX clusters")
        return
    
    print(f"\nFor {node_count} node(s), the following node types meet your requirements:")
    print(f"(Target RPS: {format_number(target_rps)})\n")
    for i, rec in enumerate(recommendations, 1):
        print(f"{i}. {rec.node_type}")
        print(f"   Memory:        {rec.memory_gb} GB")
        print(f"   Capacity:      {format_number(rec.capacity)} RPS")
        print(f"   Monthly Cost:  ${rec.monthly_cost:,.2f}")
        if not rec.memory_sufficient:
            print(f"   ⚠️  Warning: Insufficient memory for dataset")
        print()


def print_memory_warning(dataset_size_gb: float, node_type: str, 
                        node_memory_gb: int, fits: bool) -> None:
    print_header("Memory Validation")
    print(f"Dataset Size:     {dataset_size_gb} GB")
    print(f"Node Type:        {node_type}")
    print(f"Node Memory:      {node_memory_gb} GB")
    print(f"\n{'✓ Dataset fits in node memory' if fits else f'⚠️  Warning: Dataset ({dataset_size_gb} GB) exceeds node memory ({node_memory_gb} GB)'}")
    if not fits:
        print(f"Consider using a larger node type or reducing dataset size.")


def collect_inputs() -> ClusterInputs:
    print("\n=== DAX Cluster Calculator ===")
    print("Please provide the following information about your workload:\n")
    
    cache_hit_rps = prompt_float("Cache hit reads per second: ")
    cache_miss_rps = prompt_float("Cache miss reads per second: ")
    write_rps = prompt_float("Writes per second: ")
    item_size_kb = prompt_float("Item size in KB: ", min_value=0.01)
    node_count = prompt_int("Number of nodes in cluster: ")
    
    while True:
        target_utilization = prompt_float("Target utilization percentage (1-100): ", min_value=1)
        if 1 <= target_utilization <= 100:
            break
        print(f"Error: Target utilization must be between 1 and 100. You entered: {target_utilization}")
    
    dataset_size_gb = prompt_optional_float("Dataset size in GB (press Enter to skip): ")
    
    return ClusterInputs(cache_hit_rps, cache_miss_rps, write_rps, item_size_kb, 
                        node_count, target_utilization, dataset_size_gb)


def main() -> None:
    print("\n" + "=" * 60)
    print("DAX Cluster Sizing Calculator")
    print("=" * 60)
    print("\nThis tool helps you determine the appropriate DAX cluster")
    print("configuration based on your workload characteristics.")
    print("\nBased on AWS DAX cluster sizing guide.")
    
    inputs = collect_inputs()
    rounded_size = round_up_kb(inputs.item_size_kb)
    normalized_rps = calculate_normalized_rps(inputs.cache_hit_rps, inputs.cache_miss_rps, 
                                              inputs.write_rps, rounded_size, inputs.node_count)
    
    utilization_mult = calculate_utilization_multiplier(inputs.target_utilization)
    node_loss_mult = calculate_node_loss_multiplier(inputs.node_count) if inputs.node_count > 1 else 1.0
    
    selected_multiplier = max(utilization_mult, node_loss_mult)
    multiplier_reason = ("Utilization" if utilization_mult > node_loss_mult 
                        else "Node Loss Tolerance" if node_loss_mult > utilization_mult 
                        else "Both Equal")
    
    target_rps = calculate_target_rps(normalized_rps, utilization_mult, node_loss_mult)
    recommendations = find_node_recommendations(target_rps, inputs.node_count)
    
    if inputs.dataset_size_gb:
        for rec in recommendations:
            rec.memory_sufficient = check_memory_fit(inputs.dataset_size_gb, rec.node_type)
    
    print_input_summary(inputs)
    print_calculation_details(normalized_rps, target_rps, utilization_mult, 
                             node_loss_mult, selected_multiplier, multiplier_reason)
    print_recommendations(recommendations, inputs.node_count, target_rps)
    
    if inputs.dataset_size_gb and recommendations:
        first_rec = recommendations[0]
        node_memory = CAPACITY_TABLE[first_rec.node_type]['memory_gb']
        fits = check_memory_fit(inputs.dataset_size_gb, first_rec.node_type)
        print_memory_warning(inputs.dataset_size_gb, first_rec.node_type, node_memory, fits)
    
    print("\n" + "=" * 60)
    print("Calculation complete!")
    print("=" * 60 + "\n")


if __name__ == "__main__":
    main()
