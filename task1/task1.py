import random
from tqdm import tqdm
import os
import time
import mmap
from concurrent.futures import ThreadPoolExecutor
import math

def generate_file(size_gb: int, filename):
    bytes_in_gb = int(1e9)
    uint32_size = 4
    total_steps = int(size_gb*bytes_in_gb/uint32_size)
    uint32_right_border = 2**(uint32_size*8)

    with open(filename, 'wb') as f:
        for i in tqdm(range(total_steps)):
            f.write(random.randint(0, uint32_right_border).to_bytes(uint32_size, byteorder='big', signed=False))

def get_sum(descriptor):
    uint32_size = 4
    sum = 0

    bytes = descriptor.read(uint32_size)
    while bytes:
        sum += int.from_bytes(bytes, byteorder='big', signed=False)
        bytes = descriptor.read(uint32_size)

    return sum
    

def get_sum_streightforward(filename):
    with open(filename, 'rb') as f:
        return get_sum(f)


def mmap_file_parts(descriptor, max_workers):
    bytes_total = os.stat(filename).st_size
    page_size = mmap.ALLOCATIONGRANULARITY
    pages_total = math.ceil(bytes_total / page_size)
    pages_per_worker = math.ceil(pages_total / max_workers)

    bytes_per_worker = page_size*pages_per_worker
    for i in range(max_workers):
        offset = i*pages_per_worker*page_size
        if offset + bytes_per_worker < bytes_total:
            length = bytes_per_worker
        else:
            length = bytes_total - offset

        yield mmap.mmap(descriptor.fileno(), length=length, offset=offset, prot=mmap.PROT_READ)

def get_sum_concurrently(filename):
    sum = 0
    with ThreadPoolExecutor() as executor:
        with open(filename, 'rb') as f:
            for result in executor.map(get_sum, mmap_file_parts(f, max_workers=executor._max_workers)):
                    sum+=result
        
    return sum

def measure(label, func):
    print('Measuring %s' % label)
    start_time = time.time()
    result = func()
    time_score = (time.time() - start_time)
    print("Time: %s seconds" % (time_score))
    print("Result: " + str(result))
    return result, time_score

if __name__ == '__main__':
    filename = 'data'
    size_gb = 2

    if not os.path.isfile(filename):
        print('Generating random numbers...')
        generate_file(size_gb, filename)

    streightforward_result, streightforward_time = measure('streightforward sum', lambda: get_sum_streightforward(filename))
    concurrent_result, concurrent_time = measure('mm concurrent sum', lambda: get_sum_concurrently(filename))
    
    print('Results are the same: %s' % (streightforward_result == concurrent_result))

    if concurrent_time > streightforward_time:
        print('Streightforward is faster on %s seconds' % (concurrent_time-streightforward_time))
    else:
        print('Concurrent is faster on %s seconds' % (streightforward_time-concurrent_time))
