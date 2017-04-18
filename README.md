Luigi for reproducible data analysis workflow
========

`luigi` is a very powerful DAG workflow manager with strong extensibility.
It's useful to build data analysis pipelines, but some part of its default operation is unfavorable from the view of reproducibility and consistency.
The point is that `luigi.Task` by default checks only the existence of output object, therefore it's considered as completed if inputs changed but output object exists. 

Here I present an extension of `luigi.Task` more suitable for reproducable data analysis workflows. It override `complete` method of `luigi.Task` as to compare the hash values of inputs to those of previous run.

Thanks to the `luigi` team.


TODO:
  [ ] Tutorial
  [ ] Docstrings for the whole public methods.