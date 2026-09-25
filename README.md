# Hotel Booking Lab

A hands-on learning project that rebuilds the core of a hotel booking platform (think Agoda / Booking.com) small enough to read in an afternoon, but with the real hard parts left in: atomic inventory under concurrency, a payment lock with timeouts, the transactional Outbox pattern into Kafka, role-based access with admin impersonation, and a built-in load simulator that shows you where the bottlenecks are.

Everything runs locally with Docker Compose or Podman Compose. Nothing is production-ready on purpose: passwords are plain text and shown on the login page so anyone can try every role.

![Seller dashboard](docs/seller-dashboard.png)

## Stack

| Layer | Choice | Why it is here |
|---|---|---|
| API | Node.js 22, TypeScript, Fastify 5 | small, fast, typed REST API |
| Web | React 19, Vite 8, TypeScript | single-file SPA, inline SVG charts |
| Source of truth | PostgreSQL 16 | users, hotels, rooms, bookings, audit log, outbox, simulation runs |
| Inventory / locks | Redis 7 + Lua | one counter per room per night, atomic all-or-nothing reservation |
| Events | Apache Kafka 3.9 (KRaft) + Kafka UI | domain events published from the outbox |
| Auth | JWT | roles CUSTOMER / SELLER / ADMIN, admin impersonation with audit trail |
| Containers | Docker Compose / Podman Compose | healthchecks, dependency ordering |

## Run it

```bash
docker compose up --build
# or
podman compose up --build
```

Then open:

| What | Where |
|---|---|
| Web app | http://localhost:5173 |
| API | http://localhost:3010/api/health |
| Kafka UI | http://localhost:8080 |
| PostgreSQL | `localhost:5433`, user / password / db `booking` |
| Redis | `redis://localhost:6379` |

The login page lists every account with its password; click a row to log in. On a fresh database only the admin exists (`admin@example.com` / `admin123`). Log in as admin and press **Generate Sample Data** to create 20 hotels, two sellers and two customers. Re-running it is safe.

After changing code, rebuild and recreate the containers (podman-compose does not recreate on image change by itself):

```bash
podman compose up --build -d --force-recreate --no-deps api web
```

## What you can do

**As a customer**
- Search hotels by city, pick check-in / check-out dates, open a hotel to see per-night availability and the total price for the stay.
- Book a room: the nights are locked for you immediately and the booking is *Awaiting payment*. Pay within 60 seconds in **My bookings** or the status becomes *Payment timed out* and the room is released.
- My bookings shows a live countdown, a Pay button, a Cancel button for upcoming stays, a stay-phase badge (Upcoming / Staying now / Completed) and a warning when two of your stays overlap.

**As a seller**
- **Dashboard**: stat tiles and charts (booked vs free rooms per night, occupancy per hotel) for the next 7 / 14 / 30 days, with a table view.
- **My hotels**: only the hotels you own, with availability and the bookings on each. Search is scoped the same way, enforced by the API.

**As an admin**
- Generate / delete sample data, create sample bookings for a chosen customer, rebuild the Redis availability cache from PostgreSQL.
- **Concurrency demo**: every customer books the same room at the same instant; see exactly who wins.
- **Load simulation** with a report that ranks the slowest steps (p95) and a Cancel-all button:
  - *Random rooms this week*: N customers book random rooms at once; a share pays, a share lets the lock expire and rebooks, the rest abandon.
  - *Same room, 7 nights*: everyone wants one room; rejected customers cascade to another room type in the hotel, then to another hotel.

![Admin load simulation report](docs/admin-simulation.png)

## How the booking flow works

1. `POST /api/bookings` runs one Lua script over every night of the stay. If any night is sold out it returns -1 and changes nothing; otherwise it decrements all nights atomically. No database lock is needed and concurrent requests cannot oversell.
2. The booking row is inserted as `PENDING` with `expires_at = now() + 60s`, together with `booking.created` and `room.availability.changed` rows in the `outbox_events` table, in the same transaction.
3. `POST /api/bookings/:id/pay` is a single conditional `UPDATE ... WHERE status='PENDING' AND expires_at > now()`, so a late payment and the expiry worker can never both win.
4. A worker moves expired `PENDING` rows to `PAYMENT_TIMEOUT` (`FOR UPDATE SKIP LOCKED`), gives the nights back to Redis and writes `booking.payment_timeout`.
5. The outbox publisher sends unpublished events to Kafka and stamps `published_at`. Watch the topics in Kafka UI: `booking.created`, `booking.confirmed`, `booking.payment_timeout`, `booking.cancelled`, `room.availability.changed`.
6. Redis is treated as a cache: at startup (and from the admin panel) the per-night counters are rebuilt from the bookings table.

More detail in [ARCHITECTURE.md](ARCHITECTURE.md).

## What the load simulation showed

100 customers booking at once on a laptop, single API instance:

| Step | p95 |
|---|---|
| Outbox to Kafka publish lag | ~12 s |
| Payment-timeout worker delay | ~1 s |
| PostgreSQL booking transaction | ~77 ms |
| Booking request end to end | ~78 ms |
| Redis Lua reservation | ~3 ms |

The publisher (1.5 s poll, one Kafka send per event) is the bottleneck by two orders of magnitude; Redis is negligible. Obvious next steps: batch the Kafka sends, replace polling with `LISTEN/NOTIFY`, shorten the worker interval.

## API overview

| Method | Path | Role |
|---|---|---|
| GET | `/api/auth/demo-accounts` | public, lists every account (learning project) |
| POST | `/api/auth/login` | public |
| GET | `/api/hotels?city=&page=&limit=` | public, sellers see only their own |
| GET | `/api/hotels/:id?checkIn=&checkOut=` | public, per-night availability |
| POST | `/api/bookings` `{ roomId, checkIn, checkOut }` | customer, locks the room |
| POST | `/api/bookings/:id/pay` | customer |
| POST | `/api/bookings/:id/cancel` | customer |
| GET | `/api/bookings/me` | customer |
| GET | `/api/seller/dashboard?days=7` | seller |
| GET | `/api/seller/hotels`, `/api/seller/hotels/:id/bookings` | seller |
| GET | `/api/admin/users` | admin |
| POST | `/api/admin/impersonate/:userId` | admin |
| POST / DELETE | `/api/admin/sample-data` | admin |
| POST | `/api/admin/sample-bookings` `{ userId }` | admin |
| POST | `/api/admin/concurrent-booking` `{ roomId, checkIn, checkOut, confirm? }` | admin |
| POST | `/api/admin/simulation` `{ mode, customers, payRatio, lateRatio, windowSeconds, roomId? }` | admin, then `GET /api/admin/simulation/:id`, `POST .../cancel-all` |
| POST | `/api/admin/rebuild-availability` | admin |

## Project layout

```
apps/api/src/server.ts   Fastify API, workers, simulation (single file on purpose)
apps/web/src/main.tsx    React SPA
apps/web/src/style.css
database/init.sql        schema for a fresh database (the API also migrates older ones)
docker-compose.yml
docs/                    screenshots
```

## Not production-ready, by design

Plain-text passwords, no rate limiting, no tests, one API instance, in-process workers. The [architecture notes](ARCHITECTURE.md) end with the list of upgrades that would turn this into something real.
