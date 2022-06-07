import os
from typing import Iterable
import time
from concurrent.futures import ProcessPoolExecutor
import numpy as np
import ray


def generate_file(nums_count: int, filename: str):
    uint32_size = 4
    uint32_right_border = 2**(uint32_size*8-1)
    data = np.random.randint(uint32_right_border, size=nums_count)

    with open(filename, 'w', encoding='UTF-8') as file:
        for entry in data:
            file.write(f'{entry}\n')


# def read_file(filename: str):
#     with open(filename, 'r', encoding='UTF-8') as file:
#         return [int(line) for line in file.readlines()]


def generate_numbers(filename: str):
    with open(filename, 'r', encoding='UTF-8') as file:
        for line in file:
            yield int(line)


def get_factors_count(number: int):
    '''computes factors count'''
    simple_candidate = 2
    simple_dividers = []

    while simple_candidate*simple_candidate <= number:
        while number % simple_candidate == 0:
            simple_dividers.append(simple_candidate)
            number = int(number / simple_candidate)

        simple_candidate += 1

    if number > 1:
        simple_dividers.append(number)

    return len(simple_dividers)


def count_factors_sum_streightforward(filename: str):
    '''computes factors count number by number'''
    factors_count_sum = 0
    for number in generate_numbers(filename):
        factors_count_sum += get_factors_count(number)
    return factors_count_sum


def count_factors_sum_multiproc(filename: str):
    '''computes factors count concurrently'''
    factors_count_sum = 0

    with ProcessPoolExecutor(max_workers=4) as executor:
        for factors_count in executor.map(get_factors_count, generate_numbers(filename)):
            factors_count_sum += factors_count

    return factors_count_sum


@ray.remote
def get_factors_count_ray(number: int):
    return get_factors_count(number)


def count_factors_sum_ray(filename: str):
    '''computes factors count in tasks'''
    obj_refs = []
    for number in generate_numbers(filename):
        obj_refs.append(get_factors_count_ray.remote(number))

    factors_count = 0
    for obj_ref in obj_refs:
        factors_count += ray.get(obj_ref)

    return factors_count


def measure(label, func):
    '''measures func execution time'''
    print(f'Measuring {label}')
    start_time = time.time()
    result = func()
    time_score = (time.time() - start_time)
    print(f'Time: {time_score} seconds')
    print("Result: " + str(result))
    return result, time_score


if __name__ == '__main__':
    filename = 'data'
    nums_count = 2000

    if not os.path.isfile(filename):
        print('Generating random numbers...')
        generate_file(nums_count, filename)

    #array = read_file(filename)

    measure(
        'count_factors_sum_streightforward',
        lambda: count_factors_sum_streightforward(filename),
    )
    measure(
        'count_factors_sum_multiproc',
        lambda: count_factors_sum_multiproc(filename),
    )

    ray.init()
    measure(
        'count_factors_sum_ray',
        lambda: count_factors_sum_ray(filename),
    )
