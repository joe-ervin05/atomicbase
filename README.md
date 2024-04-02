
## What is Atomicbase?

> [!IMPORTANT]  
> **Atomicbase is in very early stages of development.** It is not ready for use in projects yet.
> The more help we can get from the community, the faster it can be launched! Help of any form is greatly appreciated.

Atomicbase is a rest api that makes managing and querying turso databases significantly easier. It is especially useful for when your databases do not all share one schema.

Atomicbase provides a thin abstraction over your queries and maintains a separate schema cache for each database so that queries can be made efficiently and safely.

Through the combination of parameterizing values and checking table and column names against the schema cache, no unchecked sql should ever be executed when querying a database.

This does not always make sql injection impossible though because sqlite allows for table and column names to be anything including malicious sql queries as long as it is quoted. This means any changes to a database's schema based on user input must be sanitized before they are executed.

atomicbase also has a [javascript SDK](https://github.com/joe-ervin05/atomicbase-js).

## Contributing

Atomicbase is fully free and open source under the MIT license. It is free for any kind of use.

The only thing that I ask is that if you profit off of it significantly please consider contributing to its development.

All contributions are appreciated including:
- [Contributing to the source code](https://github.com/joe-ervin05/atomicbase/blob/main/CONTRIBUTING.MD)
- [Reporting issues and suggesting new features](https://github.com/joe-ervin05/atomicbase/issues)