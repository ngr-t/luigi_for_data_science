"""Test for `TaskWithCheckingInputHash` and `HashableLocalTarget`."""
import os

import luigi
import nose
from hash_checking_tasks import (
    HashableLocalTarget,
    TaskWithCheckingInputHash
)


class HashFileInputTask(luigi.ExternalTask):
    """`ExternalTask` whose output is a `HashableLocalTarget(path=self.filename).`"""

    filename = luigi.Parameter()

    def output(self):
        return HashableLocalTarget(path=self.filename)


class SumupTask(TaskWithCheckingInputHash):
    """Sum up the integers in the input file."""

    filename = luigi.Parameter()
    result_fn = "test/test_result.txt"

    def output(self):
        return HashableLocalTarget(self.result_fn)

    def requires(self):
        yield HashFileInputTask(self.filename)

    def run(self):
        with open(self.input()[0].fn) as in_file:
            result = sum(int(l) for l in in_file.readlines())
        with open(self.output().fn, "wb") as out_file:
            out_file.write(str(result).encode())


def setup_complete():
    """Setup function for `test_complete()`.

    Run `SumupTask` whose input is "test/test_input1.csv" before test."""

    # Remove previous results to run the task.
    try:
        os.remove(SumupTask.result_fn)
    except FileNotFoundError:
        pass
    luigi.run(
        main_task_cls=SumupTask,
        cmdline_args=[
            "--filename", "test/test_input1.csv",
        ],
        local_scheduler=True)


@nose.with_setup(setup_complete)
def test_complete():
    """Test for `TaskWithCheckingInputHash` and `HashableLocalTarget`."""
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
