"""Extensions of :class:`luigi.Task`s more suitable for data science works.
"""
from hashlib import md5
import abc
from warnings import warn

import luigi
import portalocker


def _calc_md5_of_file(filename):
    hash_obj = md5()
    with open(filename, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hash_obj.update(chunk)
    return hash_obj.hexdigest()


def _to_iterable_if_not(x):
    try:
        return iter(x)
    except TypeError:
        # Manage a case with single input.
        return iter([x])


class HashableTargetException(Exception):
    pass


class HashableTarget(luigi.Target):
    """Metaclass of `Target` to be used with `TaskWithCheckingInputHash`."""

    __metaclass__ = abc.ABCMeta

    @abc.abstractmethod
    def store_input_hash(self, input_hash):
        # type: (list[str]) -> None
        """Store the hash value of the Task instance (not the hash of output)."""
        raise NotImplementedError

    @abc.abstractmethod
    def get_current_input_hash(self):
        """Get the hash value of the Task instance who made the current output.

        This method should throw `HashableTargetException` if you can't get
        input hash value for some reason but not want to halt the entire workflow.
        If `HashableTargetException` is thrown, `TaskWithCheckingInputHash.complete()`
        return False but the entire workflow is halt if other exception is thrown.
        """
        raise NotImplementedError


class HashableLocalTarget(HashableTarget, luigi.LocalTarget):
    """:class:`luigi.LocalTarget` with :class:`HashableTarget` interface.

    The hash values are stored as a pickle.
    The filename of the json file is the string which is added the suffix
    '.input.pickle' after the target filename.
    """

    import pickle

    def _get_hash_path(self):
        fn = self.fn
        return fn + ".input.pickle"

    def hash_content(self):
        """Get md5 sum of target file (if exists)."""
        return _calc_md5_of_file(self.fn)

    def store_input_hash(self, input_hash):
        """Store the hash value.

        Hash value is `pickle`d at the location `self._get_hash_path()`.
        """
        hash_path = self._get_hash_path()
        with portalocker.Lock(hash_path, mode="wb", timeout=5) as hash_file:
            self.pickle.dump(input_hash, hash_file)

    def get_current_input_hash(self):
        """Get the hash value of the Task instance who made the current output.

        Hash value is `pickle`d at the location `self._get_hash_path()`.
        """
        try:
            hash_path = self._get_hash_path()
            with portalocker.Lock(hash_path, mode="rb", timeout=5) as hash_file:
                return self.pickle.load(hash_file)
        except FileNotFoundError:
            # It's thrown when pickled hash file does not exist.
            raise HashableTargetException
        except AttributeError:
            # It's thrown if the shelved task class isn't imported.
            raise HashableTargetException


class HashableExternalFile(luigi.ExternalTask):
    """Task for external file whose target is hashable."""

    fn = luigi.Parameter()

    def output(self):
        return HashableLocalTarget(path=self.fn)


class TaskWithCheckingInputHash(luigi.Task):
    """Task which checks hash code of inputs.

    Return value of `output()` must be a single Target."""

    def _hash_input(self):
        target_hashes = [
            target.hash_content()
            for target
            in _to_iterable_if_not(self.input())]
        return [self.__class__] + target_hashes + list(self.param_args)

    def on_success(self):
        """Update hash values on success."""
        (
            self
            .output()
            .store_input_hash(
                self._hash_input())
        )

    def validate(self):
        pass

    def complete(self):
        """Check the completeness of `Task` more carefully than the default."""
        if not self.output().exists():
            # Check if output exists.
            return False

        for task in _to_iterable_if_not(self.requires()):
            # Check the completeness of dependent :class:`~luigi.Task`s.
            if not task.complete():
                return False
        try:
            # Check if the hash value of the input of the previous run
            # is equal to that of this instance.
            stored_input_hash = (
                self
                .output()
                .get_current_input_hash()
            )
            current_input_hash = self._hash_input()
            if stored_input_hash == current_input_hash:
                # If the hash values are the same,
                # this task is considered as completed.
                try:
                    self.validate()
                    return True
                except:
                    return False
        except HashableTargetException:
            return False

        return False


class WrapperTask(luigi.WrapperTask):

    def complete(self):
        return all(task.complete() for task in self.requires())
