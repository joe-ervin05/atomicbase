
## What is Atomicbase?

> [!IMPORTANT]  
> **Atomicbase is in very early stages of development.** It is not ready for use in projects yet.
> The more help we can get from the community, the faster it can be launched! Help of any form is greatly appreciated.

Atomicbase is small but incredibly powerful. It is a scalable libsql & go backend in a single file.

With Atomicbase and Turso, you can scale any application globally with ease.

> Atomicbase is not affiliated with Turso, I just think its awesome new tech to build a project around.

Atomicbase is fully open source and it's single executable will include:
- A central sqlite database
- Support for accessing unlimited databases with Turso
- RESTful sql-like API to simplify communicating with your databases
- File storage that syncs with your database
- Simple admin dashboard UI

## Development

Here is a checklist for development of Atomicbase:
- [ ] REST API
- [ ] CLI for server and DB management
- [ ] File Storage
- [ ] User management
- [ ] Admin dashboard
- [ ] Client SDK
- [ ] Realtime

### Plan

The plan is for each project to have one central database which is the default database for queries to come through. Then if you want to access another database you can send the database name in the request. This makes it very easy to manage as many databases as you need all through one interface. The db name is essentially just a pointer to the database you want to access.

This makes a pattern like database per user significantly easier because you can just hold the name of the user's database in a JWT and then when they sign in use the api to retrieve data from their database.

Aside from that the vision is to create something similar to Supabase in that it provides an sql-like interface that is much simpler, faster, and complete than writing everything yourself.

The plan for UI is to write everything using Templ and HTMX so that it is all 100% golang.

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