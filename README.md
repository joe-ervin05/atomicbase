# Atomicbase

Atomicbase is a scalable Libsql-based backend in a single file.

## Development

Atomicbase is still in the very early stages of development. It is not ready for use in projects yet.

Here is a checklist for development of Atomicbase:
- [ ] REST API
- [ ] CLI for server and DB management
- [ ] File Storage
- [ ] User management
- [ ] Admin dashboard
- [ ] Client SDK
- [ ] Realtime

### Plan

The plan is for each project to have one central database which is the default database for queries to come through. Then if you want to access another database you can send the database name and token in the request. This makes it very easy to manage as many databases as you need all through one interface.

This makes a pattern like database per user significantly easier because you can just hold the name of the user's database in a JWT and then when they sign in use the api to retrieve data from their database.

Aside from that the vision is to create something similar to Supabase in that it provides an sql-like interface that is much simpler and faster to use than writing everything yourself.

The plan for UI is to write everything using Templ and HTMX so that it is all 100% golang.

## Why Atomicbase?

Atomicbase is small but incredibly powerful. It gives you the freedom to either embed your sqlite database or manage unlimited databases with ease using Turso. This makes multi-tenant backends easier than ever before.

Atomicbase is also very fast because it is written 100% in go.

Atomicbase is fully open source and it's single executable includes:
- A central sqlite database
- Ability to connect to any libsql database over the network
- RESTful sql-like API to simplify communicating with your databases
- File storage that syncs with your database
- Simple admin dashboard UI

## Notes

Atomicbase is heavily inspired by Pocketbase and Supabase but with the twist of Libsql to be incredibly flexible. 

The other big difference between Atomicbase and Pocketbase is that Atomicbase attempts to be more sql-like in its API so that it feels more familiar and easy to control. This is very similar to Supabase but without quite as many features because Sqlite has less features on purpose.

## Contributing

Atomicbase is fully free and open source under the MIT license. It is free for any kind of use.

The only thing that I ask is that if you profit off of it significantly please consider contributing to its development.

All contributions are appreciated including:
- [Contributing to the source code](https://github.com/joe-ervin05/atomicbase/blob/main/CONTRIBUTING.MD)
- [Reporting issues and suggesting new features](https://github.com/joe-ervin05/atomicbase/issues)
- Contributing to the buymeacoffee (coming soon)