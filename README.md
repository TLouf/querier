# Querier

[![](https://readthedocs.org/projects/querier/badge/?version=latest)](https://querier.readthedocs.io/en/latest/?version=latest)

Querier is a simple library to extract data from MongoDB databases, wrapping [`pymongo`](pymongo.readthedocs.io/).

This branch contains the first version of the library. Everything is yet untested and subject to changes


# Installation


It is advised to install Querier in a `conda` environment (other than the base environment).
To do so, a conda environment with `python 3` must be activated

To install the package from the source repository, execute the
following command::

```
    pip install git+https://github.com/TLouf/querier.git#egg=querier
```

To test that the library is installed, execute the following python script:

```python
    import querier as qr
```
