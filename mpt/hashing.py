import hashlib
from typing import List

from .paths import fix_path


def hash_files(file_list: List, algorithm: str = None, blocksize: int = None):
    """
    Hash all files in a list using the algorithm and blocksize specified
    :param file_list: list of files to hash
    :param algorithm: the algorithm to use
    :param blocksize: block size to use
    :return: a list of tuples in the form (path, hash value)
    """
    result = []
    for f in file_list:
        cs = hash_file(f, algorithm, blocksize)
        next_file = (f, cs)
        result.append(next_file)
    return result


def hash_file(in_path: str, algorithm: str = "sha256", blocksize: int = 131072):
    """ Return checksum value for a given file
    :param in_path: file to hash
    :param algorithm: hash algorithm to use
    :param blocksize: block size to use for file read
    :param return_size: optionl, return number of bytes hashed
    :return: the hash value of the file
    """
    hasher = hashlib.new(algorithm)
    path = fix_path(in_path)
    size = 0
    with open(path.encode('utf-8'), 'rb') as f:
        for block in iter(lambda: f.read(blocksize), b""):
            hasher.update(block)
            size += len(block)
    return hasher.hexdigest(), size
