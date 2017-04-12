import luigi
import nose
from hash_checking_tasks import (
    HashableLocalTarget,
    TaskWithCheckingInputHash
)


class HashFileInputTask(luigi.ExternalTask):
    """docstring for HashFileInputTask"""

    filename = luigi.Parameter()

    def output(self):
        return HashableLocalTarget(path=self.filename)


class SumupTask(TaskWithCheckingInputHash):
    """Task has to be run already."""

    filename = luigi.Parameter()

    def output(self):
        return HashableLocalTarget("test/test_result.txt")

    def requires(self):
        yield HashFileInputTask(self.filename)

    def run(self):
        with open(self.input()[0].fn) as in_file:
            result = sum(int(l) for l in in_file.readlines())
        with open(self.output().fn, "wb") as out_file:
            out_file.write(str(result).encode())


def setup_complete():
    luigi.run(
        main_task_cls=SumupTask,
        cmdline_args=[
            "--filename", "test/test_input1.csv",
            "--hash-db-path", "test/.hash_db"],
        local_scheduler=True)


@nose.with_setup(setup_complete)
def test_complete():
    assert SumupTask(
        filename="test/test_input2.csv",
        hash_db_path="test/.hash_db").complete()
    assert not SumupTask(
        filename="test/test_input2.csv",
        hash_db_path="test/.hash_db2").complete()
    assert not SumupTask(
        filename="test/test_input3.csv",
        hash_db_path="test/.hash_db").complete()
