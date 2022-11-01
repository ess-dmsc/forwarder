## Integration tests

There are two sets of tests:

- contract tests - tests that the contract/API of third-party services haven't changed
- smoke tests - tests that the system works at a basic level

See the respective README files for more information.

### Dockerfile

Creates an image of an Ubuntu system with Python installed which is used for running the integration tests.

### docker-compose.yml

Starts up the systems required for running the integration tests.
