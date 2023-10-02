import os

from eren import fs
from .spark import spark

def test_read(tmp_path):
    with open(os.path.join(tmp_path, "test_read.txt"), "w") as f_:
        f_.write("testing_string")

    file_system = fs.HadoopFileSystem(spark, "file:///")
    read_result = file_system.read_utf8(os.path.join(tmp_path, "test_read.txt"))

    assert read_result == "testing_string"


def test_read_multiline(tmp_path):
    with open(os.path.join(tmp_path, "test_read_mult.txt"), "w") as f_:
        f_.write("first_line\nsecond_line")

    file_system = fs.HadoopFileSystem(spark, "file:///")
    read_result = file_system.read_utf8(os.path.join(tmp_path, "test_read_mult.txt"))
    lines = read_result.split("\n")

    assert len(lines) == 2
    assert lines[0] == "first_line"
    assert lines[1] == "second_line"


def test_write(tmp_path):
    file_system = fs.HadoopFileSystem(spark, "file:///")
    file_system.write_utf8(
        os.path.join(tmp_path, "test_write.txt"), "testing_string", "w"
    )

    with open(os.path.join(tmp_path, "test_write.txt"), "r") as f_:
        res = f_.read()

    assert res == "testing_string"


def test_glob():
    file_system = fs.HadoopFileSystem(spark, "file:///")
    list_of_files = file_system.glob(os.path.join(".", "*"))
    assert len(list_of_files) > 0
    assert [f for f in list_of_files if f.name == "tests"][0].is_dir
