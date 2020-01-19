
# Python Forwarder

## Installing dependencies

This project is developed for Python 3.6, so an install of 3.6 or higher
is required. https://www.python.org/downloads/

Runtime Python dependencies are listed in requirements.txt at the root of the
repository. They can be installed from a terminal by running
```
pip install -r requirements.txt
```

### Development dependencies

Development dependencies (including all runtime dependencies) can be installed by using the following command: 

```
pip install -r requirements-dev.txt
```

The black pre-commit hook (installed by [pre-commit](https://pre-commit.com/)) requires Python 3.6 or above.
You need to once run
```
pre-commit install
```
to activate the pre-commit check.

## Running the application

Run the python script `forwarder.py` located in the root of the repository.
