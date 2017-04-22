"""Extensions of :class:`luigi.Task`s more suitable for data science works.
"""
import shelve
from hashlib import md5
import abc

import luigi
import portalocker


def _calc_md5_of_file(filename):
    hash_obj = md5()
    with open(filename, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hash_obj.update(chunk)
    return hash_obj.hexdigest()


class HashableTargetException(Exception):
    pass


class HashableTarget(luigi.Target):
    """Metaclass of `Target` to be used with `TaskWithCheckingInputHash`.

    Of course you don't have to inherit this when you implement new `Target`
    for `TaskWithCheckingInputHash` and just implement the methods below."""

    __metaclass__ = abc.ABCMeta

    @abc.abstractmethod
    def hash_content(self):
        """The hash value of content of the current output."""
        raise NotImplementedError

    @abc.abstractmethod
    def hash_container(self):
        """The hash value of where output will be created."""
        raise NotImplementedError

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
    """:class:`luigi.LocalTarget` with
    :meth:`HashableLocalTarget.hash()` method."""

    def _get_shelve_path(self):
        fn = self.fn
        return fn + ".shelf"

    def hash_container(self):
        return md5(self.fn.encode("utf-8")).hexdigest()

    def hash_content(self):
        return _calc_md5_of_file(self.fn)

    def store_input_hash(self, content_hash):
        shelve_path = self._get_shelve_path()
        container_hash = self.hash_container()
        with shelve.open(shelve_path, flag="c") as shelf:
            portalocker.Lock(shelve_path, timeout=5)
            shelf[container_hash] = content_hash
            shelf.close()

    def get_current_input_hash(self):
        try:
            shelve_path = self._get_shelve_path()
            container_hash = self.hash_container()
            with shelve.open(shelve_path, flag="c") as shelf:
                portalocker.Lock(shelve_path, timeout=5)
                content_hash = shelf[container_hash]
                shelf.close()
                return content_hash
        except KeyError:
            # It's thrown when hash key is not in cache_db.
            raise HashableTargetException
        except AttributeError:
            # It's thrown if the shelved task class isn't imported.
            raise HashableTargetException


class TaskWithCheckingInputHash(luigi.Task):
    """Task which checks hash code of inputs.

    Return value of `output()` must be a single Target."""

    def _iterable_input(self):
        try:
            return iter(self.input())
        except TypeError:
            # Manage a case with single input.
            return iter([self.input()])

    def _iterable_requires(self):
        try:
            return iter(self.requires())
        except TypeError:
            # Manage a case with single requires.
            return iter([self.requires()])

    def hash_input(self):
        target_hashes = [
            target.hash_content()
            for target
            in self._iterable_input()]
        return [self.__class__] + target_hashes

    def on_success(self):
        """Update hash values on success."""
        (
            self
            .output()
            .store_input_hash(
                self.hash_input())
        )

    def complete(self):
        """Check the completeness of `Task` more carefully than the default."""
        if not self.output().exists():
            # Check if output exists.
            return False

        for task in self._iterable_requires():
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
            current_input_hash = self.hash_input()
            if stored_input_hash == current_input_hash:
                # If the hash values are the same,
                # this task is considered as completed.
                return True
        except HashableTargetException:
            return False

        return False
