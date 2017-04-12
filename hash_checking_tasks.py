"""Extensions of :class:`luigi.Task`s more suitable for data science works.
"""
import luigi
import shelve
import portalocker
from hashlib import md5

_SHELVE_PATH = ""


def get_default_shelve_path():
    return _SHELVE_PATH


def set_default_shelve_path(path):
    global _SHELVE_PATH
    _SHELVE_PATH = path


def store_hash(container_hash, content_hash, shelve_path=None):
    if shelve_path is None:
        shelve_path = get_default_shelve_path()
    with shelve.open(shelve_path, flag="c") as shelf:
        portalocker.Lock(shelve_path, timeout=5)
        shelf[container_hash] = content_hash
        shelf.close()


def get_hash(container_hash, shelve_path=None):
    if shelve_path is None:
        shelve_path = get_default_shelve_path()
    with shelve.open(shelve_path, flag="c") as shelf:
        portalocker.Lock(shelve_path, timeout=5)
        content_hash = shelf[container_hash]
        shelf.close()
        return content_hash


class HashableTarget(luigi.Target):
    """Base class for `Target` to be used with
    `TaskWithCheckingInputHash`.

    This is just for the explaination and you need not to inherit this
    when you implement new `Target`."""

    def hash_content(self):
        """The hash value of content of the current output."""
        raise NotImplementedError

    def hash_container(self):
        """The hash value of where output will be created."""
        raise NotImplementedError


class HashableLocalTarget(luigi.LocalTarget):
    """:class:`luigi.LocalTarget` with
    :meth:`HashableLocalTarget.hash()` method."""

    def hash_container(self):
        return md5(self.fn.encode("utf-8")).hexdigest()

    def hash_content(self):
        hash_obj = md5()
        with open(self.fn, "rb") as f:
            for chunk in iter(lambda: f.read(4096), b""):
                hash_obj.update(chunk)
        return hash_obj.hexdigest()


class TaskWithCheckingInputHash(luigi.Task):
    """Task which checks hash code of inputs.

    Return value of `output()` must be a single Target."""

    hash_db_path = luigi.Parameter()

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

    def hash_container(self):
        return self.output().hash_container()

    def on_success(self):
        """Update hash values on success."""
        store_hash(
            self.hash_container(),
            self.hash_input(),
            self.hash_db_path)

    def complete(self):
        """Check the completeness of `Task` more carefully than the default."""
        if not self.output().exists():
            return False

        for task in self._iterable_requires():
            # Check the completeness of dependent :class:`~luigi.Task`s.
            if not task.complete():
                return False
        try:
            stored_input_hash = get_hash(
                self.hash_container(),
                self.hash_db_path)
            current_input_hash = self.hash_input()
            if stored_input_hash == current_input_hash:
                return True
        except KeyError:
            # It's thrown when hash key is not in cache_db.
            return False
        except AttributeError:
            # It's thrown if the shelved task class isn't imported.
            return False

        return False
