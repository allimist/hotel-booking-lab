# Architecture Notes

## PostgreSQL
Source of truth for users, hotels, rooms, bookings, audit logs and outbox events.

## Redis
Redis is a cache of PostgreSQL truth. At API startup, and via `POST /api/admin/rebuild-availability`, every room's availability for every night from today to `AVAILABILITY_DAYS` (default 365) ahead is recomputed from `rooms.total_rooms` minus active bookings and written back in one `MULTI`, so a restarted or flushed Redis cannot cause overselling. Keys for past nights or deleted rooms are dropped.

`availability:<roomId>:<YYYY-MM-DD>` stores the remaining inventory of a room for one night. Every night of every room must already be in Redis: keys are never created on the fly, and a missing key is an error (409 "Availability for these dates is not loaded in Redis"), never treated as "fully available". Sample data seeds its new rooms right after creating them. Stays ending beyond the horizon are rejected with a 400.

### Availability window (`apps/availability-worker`)
Redis holds exactly `AVAILABILITY_DAYS` (365) nights per room: today through today + 364, no more and no less. A separate worker service keeps that window moving. It runs once at startup and then just after every UTC midnight, and each run:

1. adds every missing night inside the window (normally just the one night that entered it) with `SET NX`, using `total_rooms` minus any active bookings, so a live counter is never overwritten;
2. removes every key outside the window (normally yesterday) by scanning `availability:*`.

Every run is written to the `availability_window_log` table (created by the worker): when it ran, why (`startup`, `daily` or `retry`), OK/FAILED, the window, and how many keys were added and removed per night. The admin UI has two tabs for this: **Redis records** lists every `availability:*` key with its hotel, room and remaining count, and checks that the window is exact (key count, first/last night, keys of deleted rooms). **Availability log** shows the worker's runs.

Both steps are idempotent: a run that is late, repeated, or follows days of downtime catches up in one go. A failed run is retried after a minute. The API uses the same `AVAILABILITY_DAYS` to reject stays ending beyond the window.

Booking a date range runs one Lua script over every night of the stay (check-in inclusive, check-out exclusive): if any night is missing it returns -2, if any night is sold out it returns -1, and in both cases changes nothing; otherwise it decrements all nights atomically. Cancelling runs the inverse script on the nights that are not yet past, incrementing each night but never above `total_rooms`; it also refuses (-2) if any of those nights is missing. This is what prevents overselling under concurrent requests without a database lock.

## Bookings and the payment lock
`bookings` stores `check_in`, `check_out`, the total `price` for the stay, a `status`, `expires_at` and `paid_at`.

Booking is two-step. `POST /api/bookings` reserves the nights in Redis and inserts a `PENDING` row with `expires_at = now() + 60s` (`PAYMENT_WINDOW_SECONDS`). Other customers already see the room as taken. `POST /api/bookings/:id/pay` flips it to `CONFIRMED` only if the window has not passed; the check is a single conditional `UPDATE`, so a late payment and the expiry worker cannot both win.

A background loop runs every 2 seconds and, in one transaction, moves due `PENDING` rows to `PAYMENT_TIMEOUT` (`FOR UPDATE SKIP LOCKED`, so several API instances could run it), gives the nights back to Redis and writes `booking.payment_timeout` + `room.availability.changed` outbox events. Customers can cancel a `PENDING` or `CONFIRMED` booking whose stay has not started; the nights are released the same way. Admin sample bookings are inserted directly as `CONFIRMED`.

Events: `booking.created`, `booking.confirmed`, `booking.payment_timeout`, `booking.cancelled`, `room.availability.changed`. On startup the API runs an idempotent migration that adds the date columns to databases created before they existed.

## Kafka
The API writes domain events to PostgreSQL first using the Outbox pattern. A background publisher sends unpublished events to Kafka.

## Lazy loading
Hotel search returns a lightweight list and a thumbnail. Hotel details are fetched only after opening a hotel. Browser images use `loading="lazy"`.

## Load simulation and bottlenecks
`POST /api/admin/simulation` creates N throw-away customers (`sim-<run>-<n>@example.com`, `is_sample=true`) and runs, inside the API process: a booking burst (every customer books a random room in the coming week at the same instant), immediate payments for a share, a wait for the payment-timeout worker, rebooking by the "late" share, and a wait for the outbox to drain. Each step is timed (avg/p50/p95/max) and the run is stored in `simulation_runs`, polled by the admin UI. The report ranks steps by p95. A second scenario (`same-room-fallback`) sends every customer at one room for 7 nights and, on a 409, walks them through the other room types of that hotel and then up to three other hotels, all concurrently; its report shows how many customers ended in each tier and which rooms filled up. Typical result on this stack: the outbox publisher (1.5s poll, one Kafka send per event) dominates at several seconds, the expiry worker adds up to 2s, a PostgreSQL booking transaction costs tens of ms under a 100-request burst, and the Redis Lua reservation costs about 2 ms. Obvious upgrades: batch the Kafka sends, LISTEN/NOTIFY instead of polling, and a shorter worker interval.

## Seller dashboard
`GET /api/seller/dashboard` computes, from PostgreSQL only, per-day booked/locked/free rooms (`generate_series` over the window, counting CONFIRMED and PENDING bookings that cover each night) and per-hotel booked room-nights, free room-nights, occupancy and revenue. Charts are inline SVG in the React app.

## Seller scoping
Hotels carry a `seller_id`. Seller endpoints filter by the caller's id, and the public hotel list and detail endpoints identify the caller when a token is present (optional auth) and restrict sellers to their own hotels. Customers and anonymous visitors see everything.

## Impersonation
Admins receive a JWT representing the target user plus `impersonatedBy`. An audit record is created.

## Next learning upgrades
- Proper password hashing with Argon2/bcrypt
- Refresh tokens and session revocation

- Seller CRUD UI
- Admin user/booking screens
- Kafka consumer services
- OpenTelemetry
- Prometheus/Grafana
- S3/CloudFront image storage
- Integration and concurrency tests
- CI/CD GitHub Actions
