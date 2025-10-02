# Dev Container

The dev container uses the devtools image as a base for a full-featured development environment. Using the dev container will provide you with all of our needed dependencies in an isolated environment as well as a preconfigured VS Code with our recommended settings and extensions for development, formatting, and testing.

Like all LSB images, no database is included. If you have a local database running, you should be able to connect to it normally from within the dev container. You can also launch a database container for testing using the `database` service in the [dev.docker-compose.yml](../dev.docker-compose.yml) file.

This does not include the Clang compiler or other LLVM build tools (other than clang-format). If you wish to build with clang-tidy you should use the `clang-tidy` service in [dev.docker-compose.yml](../dev.docker-compose.yml) or install the needed tools with `apt` (these changes will be discarded when the container is removed).
