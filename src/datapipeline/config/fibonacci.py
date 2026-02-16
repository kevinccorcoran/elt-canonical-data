from itertools import accumulate


def fibonacci(n):
    # Generate the first n Fibonacci numbers
    sequence = [0, 1]
    for _ in range(2, n):
        sequence.append(sequence[-1] + sequence[-2])
    return sequence[:n]


def cumulative_fibonacci(n, limit=2**63 - 1):
    # Compute cumulative sums of Fibonacci numbers with an upper bound
    fib_sequence = fibonacci(n)

    cumulative_sum = 0
    cumulative_fib_sequence = []

    for num in fib_sequence:
        cumulative_sum += num
        if cumulative_sum > limit:
            break
        cumulative_fib_sequence.append(cumulative_sum)

    return cumulative_fib_sequence


# Local test / sanity check
if __name__ == "__main__":
    n = 10
    limit = 100

    fib_sequence = fibonacci(n)
    print(fib_sequence)

    cumulative_fib_sequence = cumulative_fibonacci(n, limit)
    print(cumulative_fib_sequence)
