# extraction pattern: ngram TAB year TAB match_count TAB volume_count NEWLINE
# out: unique_ngram TAB sum(match_count) NEWLINE
import gzip
import mmap
from concurrent.futures import ThreadPoolExecutor

files = ['../Data/ru_meta_part_1.jsonl.gz']


def process_file(file_name):
    print(f"process file {file_name}")
    with gzip.open(file_name, "rt", encoding='UTF-8') as j_file:
        with mmap.mmap(j_file.fileno(), length=0, access=mmap.ACCESS_READ) as mmap_obj:
            for line in iter(mmap_obj.readline, b""):
                print(line.decode("utf-8"))
        with ThreadPoolExecutor() as executor:
            executor.map(write_to_meta, j_file.readlines())


def write_to_meta(line):
    with open("../Data/ru_meta.jsonl", 'a', encoding="UTF-8") as out:
        out.write(line)


process_file(files[0])
