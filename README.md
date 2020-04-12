
# Forwarder

## Installing dependencies

Python 3.7 or higher is required. https://www.python.org/downloads/

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

The black pre-commit hook (installed by [pre-commit](https://pre-commit.com/)) requires Python 3.6 or above.
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

Run the python script `forwarder.py` located in the root of the repository.

## Docker

To build a docker image
```
docker build . -t forwarder-python
```

Use environment variables to pass command line arguments, for example
```
docker run -e CONFIG_TOPIC="localhost/config" -e STATUS_TOPIC="localhost/status" forwarder-python
```

## C++ Forwarder features not replicated here

Won't do:
- Forwarding to multiple Kafka clusters
