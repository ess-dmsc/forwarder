[![Build Status](https://jenkins.esss.dk/dm/job/ess-dmsc/job/forwarder/job/master/badge/icon)](https://jenkins.esss.dk/dm/job/ess-dmsc/job/forwarder/job/master/) [![codecov](https://codecov.io/gh/ess-dmsc/forwarder/branch/master/graph/badge.svg)](https://codecov.io/gh/ess-dmsc/forwarder)


# Forwarder

## Installing dependencies

Python 3.6 or higher is required. https://www.python.org/downloads/

Runtime Python dependencies are listed in requirements.txt at the root of the
repository. They can be installed from a terminal by running
```
pip install -r requirements.txt
```

### Development dependencies

Development dependencies (including all runtime dependencies) can be installed by using the following command 

```
pip install -r requirements-dev.txt
```

`black`, `flake8` and `mypy` can be used as a pre-commit hook (installed by [pre-commit](https://pre-commit.com/)).
You need to run
```
pre-commit install
```
once to activate the pre-commit check.
To test the hooks run
```
pre-commit run --all-files
```
This command can also be used to run the hooks manually.

## Running the application

Run the python script `forwarder_launch.py`.
For run options do
```
forwarder_launch.py --help
```
Optional arguments match the C++ codebase with the exception of the additional `--output-broker` option.
This sets where the data is forwarded to as it is not configurable separately for each EPICS channel,
see [C++ Forwarder features not replicated here](#c++-forwarder-features-not-replicated-here). 


## Docker

To build a docker image
```
docker build . -t forwarder
```

Use environment variables to pass command line arguments, for example
```
docker run -e CONFIG_TOPIC="localhost/config" -e STATUS_TOPIC="localhost/status" forwarder
```

## Making a release

Version should be set in `setup.cfg`. The application picks up its version from that file.
