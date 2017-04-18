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
        ],
        local_scheduler=True)


@nose.with_setup(setup_complete)
def test_complete():
    # The contents of `test_input1.csv` and `test_input2` are identical.
    # `SumupTask` with `filename=test_input1.csv` is already run in setup,
    # so `SumupTask` for the both has to be considered as completed.
    assert SumupTask(
        filename="test/test_input1.csv",
    ).complete()
    assert SumupTask(
        filename="test/test_input2.csv",
    ).complete()
    # `test_input3` is different. Task should not be considered as completed.
    assert not SumupTask(
        filename="test/test_input3.csv",
    ).complete()
