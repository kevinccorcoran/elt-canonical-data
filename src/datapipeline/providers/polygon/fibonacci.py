from itertools import accumulate


def fibonacci(n):
    """Generate the first `n` Fibonacci numbers."""
    sequence = [0, 1]
    for _ in range(2, n):
        sequence.append(sequence[-1] + sequence[-2])
    return sequence[:n]

def cumulative_fibonacci(n, limit=2**63 - 1):
    """
    Generate the cumulative sum of the first `n` Fibonacci numbers 
    up to a given limit.
    """
    fib_sequence = fibonacci(n)
    cumulative_sum, cumulative_fib_sequence = 0, []
    for num in fib_sequence:
        cumulative_sum += num
        if cumulative_sum > limit:
            break
        cumulative_fib_sequence.append(cumulative_sum)
    return cumulative_fib_sequence

# Example usage
if __name__ == "__main__":
    # Set parameters
    n = 10  # Number of Fibonacci numbers to generate
    limit = 100  # Optional cumulative sum limit (default is very large)
    
    # Generate Fibonacci sequence
    fib_sequence = fibonacci(n)
    print(f"The first {n} Fibonacci numbers:")
    print(fib_sequence)
    
    # Generate cumulative Fibonacci sequence
    cumulative_fib_sequence = cumulative_fibonacci(n, limit)
    print(f"\nCumulative Fibonacci sequence for n={n} with limit={limit}:")
    print(cumulative_fib_sequence)

