# Querier

[![docs](https://gitlab.ifisc.uib-csic.es/socio-physics/querier/badges/master/pipeline.svg?job=pages&key_text=docs&key_width=50)](http://socio-physics.pages.ifisc.uib-csic.es/querier)

Querier is a simple library to extract data from MongoDB databases, wrapping [`pymongo`](pymongo.readthedocs.io/).

This branch contains the first version of the library. Everything is yet untested and subject to changes


# Installation


It is advised to install Querier in a `conda` environment (other than the base environment).
To do so, a conda environment with `python 3` must be activated

To install the package from the source repository, install git and execute the
following command::

```
    pip install git+https://gitlab.ifisc.uib-csic.es/socio-physics/querier.git
```

To test that the library is installed, execute the following python script:

```python
    import querier as qr
```
