Luigi for reproducible data analysis workflow
========

`luigi` is a very powerful DAG workflow manager with strong extensibility.
It's useful to build data analysis pipelines, but some part of its default operation is unfavorable from the view of reproducibility and consistency.
The point is that `luigi.Task` by default checks only the existence of output object, therefore it's considered as completed if inputs changed but output object exists. 

Here I present an extension of `luigi.Task` more suitable for reproducable data analysis workflows. It override `complete` method of `luigi.Task` as to compare the hash values of inputs to those of previous run.

Thanks to the `luigi` team.


How to use?
-----------

1. Make your tasks inherit `hash_checking_tasks.TaskWithCheckingInputHash`
2. Make the task's output and all the input inherit `hash_checking_tasks.HashableTarget`.
3. Run.


How does it work?
-----------------

`TaskWithCheckingInputHash` is an extension of `luigi.Task` with below operation:

  - check the dependent tasks' completeness in `complete()` method.
  - check if the input of previous run is equal to that of the current run.
  - if the run is successful, store the information about the task.

`TaskWithCheckingInputHash` rely on `HashableTarget` that:

  - we can check the equality of the content of targets by comparing the values of `hash_content()`.
  - we can retrieve the information about the `Task` which made the current output (if exists) by `get_current_input_hash()`
  - we can store the information about the `Task` which made the output by `store_input_hash()`


TODO:
  - [ ] Docstrings for the whole public methods.
