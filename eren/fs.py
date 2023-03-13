import enum
import re
from dataclasses import dataclass
from typing import List, Literal, Tuple

from py4j.java_gateway import JavaObject
from pyspark.sql import SparkSession

_FS_PATTERN = r"(s3\w*://|hdfs://|dbfs://|file://|file:/).(.*)"


class FS_TYPES(enum.Enum):
    DBFS = "DBFS"
    HDFS = "HDFS"
    S3 = "S3"
    LOCAL = "LOCAL"
    UNKNOWN = "UNKNOWN"

    @classmethod
    def _from_pattern(cls, pattern: str):
        return {
            "s3://": cls.S3,
            "s3a://": cls.S3,
            "dbfs://": cls.DBFS,
            "hdfs://": cls.HDFS,
            "file://": cls.LOCAL,
        }.get(pattern, cls.UNKNOWN)


@dataclass
class HDFSFile:
    name: str
    path: str
    mod_time: int
    is_dir: bool
    fs_type: FS_TYPES


def _get_hdfs(
    spark: SparkSession, pattern: str
) -> Tuple[JavaObject, JavaObject, FS_TYPES]:
    match = re.match(_FS_PATTERN, pattern)
    if match is None:
        raise ValueError(
            f"Bad pattern or path. Got {pattern} but should be"
            " one of `s3://`, `s3a://`, `dbfs://`, `hdfs://`, `file://`"
        )

    fs_type = FS_TYPES._from_pattern(match.groups()[0])

    # Java is accessible in runtime only and it is impossible to infer types here
    hadoop = spark.sparkContext._jvm.org.apache.hadoop  # type: ignore
    hadoop_conf = spark._jsc.hadoopConfiguration()  # type: ignore
    uri = hadoop.fs.Path(pattern).toUri()  # type: ignore
    hdfs = hadoop.fs.FileSystem.get(uri, hadoop_conf)  # type: ignore

    return (hadoop, hdfs, fs_type)  # type: ignore


class HadoopFileSystem(object):
    def __init__(self: "HadoopFileSystem", spark: SparkSession, pattern: str) -> None:
        """Helper class for working with FileSystem.

        :param spark: SparkSession object
        :param pattern: Any pattern related to FileSystem.
                        We should provide it to choose the right implementation of org.apache.hadoop.fs.FileSystem under the hood.
                        Pattern here should have a form of URI-like string like `s3a:///my-bucket/my-prefix` or `file:///home/user/`.
        """
        hadoop, hdfs, fs_type = _get_hdfs(spark, pattern)
        self._hdfs = hdfs
        self._fs_type = fs_type
        self._hadoop = hadoop
        self._jvm = spark.sparkContext._jvm

    def write_utf8(
        self: "HadoopFileSystem", path: str, data: str, mode: Literal["a", "w"]
    ) -> None:
        """Write a given string in UTF-16BE to the given path.
        Do not use this method to write the data!
        It is fantastically slow compared to `spark.write`.

        :param path: Path of file
        :param data: String to write
        :param mode: Mode. `w` means overwrite but `a` means append.
        """
        if mode == "w":
            # org.apache.hadoop.fs.FileSystem.create(Path f, boolean overwrite)
            output_stream = self._hdfs.create(self._hadoop.fs.Path(path), True)  # type: ignore
        elif mode == "a":
            # org.apache.hadoop.fs.FileSystem.append(Path f)
            output_stream = self._hdfs.append(self._hadoop.fs.Path(path))  # type: ignore

        # org.apache.hadoop.fs.FSDataOutputStream
        try:
            for b in data.encode("utf-8"):
                output_stream.write(b)
            output_stream.flush()
            output_stream.close()
        except Exception as e:
            output_stream.close()
            raise e

    def read_utf8(self: "HadoopFileSystem", path: str) -> str:
        """Read string from given path.
        Do not use this method to read the data!
        It is fantastically slow compared to `spark.read`.

        :param path: Path of file
        :return: Decoded from UTF-8 string
        :rtype: str
        """
        res = []
        # org.apache.hadoop.fs.FileSystem.open
        in_stream = self._hdfs.open(self._hadoop.fs.Path(path))  # type: ignore

        # open returns us org.apache.hadoop.fs.FSDataInputStream
        try:
            while True:
                if in_stream.available() > 0:
                    res.append(in_stream.readByte())
                else:
                    in_stream.close()
                    break
        except Exception as e:
            in_stream.close()
            raise e

        return bytes(res).decode("utf-8")

    def glob(self, pattern: str) -> List[HDFSFile]:
        statuses = self._hdfs.globStatus(self._hadoop.fs.Path(pattern))

        res = []
        for file_status in statuses:
            # org.apache.hadoop.fs.FileStatus
            res.append(
                HDFSFile(
                    name=file_status.getPath().getName(),
                    path=file_status.getPath().toString(),
                    mod_time=file_status.getModificationTime(),
                    is_dir=file_status.isDirectory(),
                    fs_type=self._fs_type,
                )
            )

        return res
