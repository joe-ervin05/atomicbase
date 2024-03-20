# Atomic Base

Atomic Base is a scalable Libsql-based backend in a single file.

## Development

Atomic Base still has a lot of work to be done before it is complete. It is not ready for use in projects yet.

Here is the checklist for development of Atomic Base:
- [ ] REST API
- [ ] File Storage
- [ ] User management
- [ ] Admin dashboard
- [ ] Client SDK
- [ ] Realtime

## Why Atomic Base?

Atomic Base is small but incredibly powerful. It gives you the freedom to either embed your sqlite database or manage unlimited databases with ease using Turso. This makes multi-tenant backends easier than ever before.

Atomic Base is also very fast because it is written 100% in go.

Atomic base is fully open source and it's single executable includes:
- RESTful sql-like API to simplify communicating with your databases
- File storage that syncs with your database
- Simple admin dashboard UI

## Notes

Atomic Base is heavily inspired by Pocketbase and Supabase but with the twist of Libsql to be incredibly flexible. 

The other big difference between Atomic Base and Pocketbase is that Atomic Base attempts to be more sql-like in its API so that it feels more familiar and easy to control. This is very similar to Supabase but without quite as many features because Sqlite has less features on purpose.

## Contributing

Atomic Base is fully free and open source under the MIT license. It is free for any kind of use.

The only thing that I ask is that if you profit off of it significantly please consider contributing to its development.

All contributions are appreciated including:
- Contributing to the source code
- Reporting issues and suggesting new features
- Contributing to the buymeacoffee (coming on release)