# Contributing to Atomicbase

Thank you for helping make Atomicbase a better software for everyone.

This document describes how to run Atomicbase locally and make changes to commit to the repository.

It is recommended that you
1. Submit an issue for the requested feature or fix
2. Create a new branch for each of your changes

## Prerequisites

- [Go 1.22 +](https://go.dev/doc/install)
- [Templ cli (for making changes to the UI)](https://templ.guide/quick-start/installation)
- [Gcc compiler (for CGO for the sqlite driver)](https://jmeubank.github.io/tdm-gcc/articles/2021-05/10.3.0-release)

If you have not already, fork the main repository and clone your fork so that you can work locally:

```
    git clone https://github.com/your_username/atomicbase.git
```

## Local development

Once you have forked and cloned the repository, use `make run` to build and start the go server.

Also make sure you have CGO enabled. use `export CGO_ENABLED=1` on Linux or `set CGO_ENABLED=1` on Windows.

this will start a server at `http://localhost:3000` with the full project. That's pretty much it!