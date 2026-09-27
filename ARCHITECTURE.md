# Architecture Notes

## PostgreSQL
Source of truth for users, hotels, rooms, bookings, audit logs and outbox events.

## Redis
Redis holds only **booked counts**. Per room type and month there is one small hash, `av:{<roomId>}:<YYYY-MM>`, with a field for each night that has bookings or payment holds (field = day of the month, value = rooms booked). A night with nothing booked has no entry, and "no entry" means every room is free. Capacity (`rooms.total_rooms`) stays in PostgreSQL and is passed to the scripts. So Redis memory grows with bookings, not with the size of the catalogue: 10,000 hotels with nothing booked cost about 20 KB. `{roomId}` is a hash tag, so all months of one room type map to the same Redis Cluster slot.

Small hashes use Redis's compact listpack encoding. Measured with the capacity test, per hotel (3 room types, 365 nights): about 7.9 KB at 10% occupancy and 11 KB at 90%, against about 120 KB for the previous model of one key per room per night (about 110 bytes × 1,095 keys) whatever the occupancy.

Every month key gets `EXPIREAT` two days after its month ends, so past nights clean themselves up.

**Booking** runs one Lua script over every night of the stay (check-in inclusive, check-out exclusive). If any night already has `booked >= total_rooms` it returns -1 and changes nothing; otherwise it adds 1 to every night and returns the rooms left on the fullest night. A stay of up to 30 nights touches at most two month keys. **Releasing** (payment timeout, cancel) runs the inverse script on the nights that are not yet past: -1 per night, and a night that reaches 0 is deleted, which keeps the hashes sparse (an empty hash disappears).

**Loaded check.** Because "no entry" means "free", an empty Redis (restarted without persistence, or flushed) would look fully available. So both scripts first check the key `av:loaded`, which only the rebuild writes. Without it they return -2 and the API answers "availability is not loaded yet" instead of selling rooms.

**Rebuild.** At API startup, and via `POST /api/admin/rebuild-availability`, Redis is rebuilt from PostgreSQL: the booked count of every night with active (confirmed or pending) bookings in the window is written with `HSET` (which only overwrites, so a booking running at that moment never sees its night vanish), entries no booking backs any more and keys of deleted rooms are dropped, keys of the old one-key-per-night format are deleted, and `av:loaded` is set. The work grows with the number of bookings: milliseconds for the sample data. Capacity-test rooms keep their synthetic occupancy, which exists only in Redis.

**PostgreSQL guard.** Redis is the fast gate that turns away almost every sold-out request, but it is not the last word. Inside the booking transaction the API locks the room type row (`SELECT … FOR UPDATE`) and counts the active bookings on each night of the stay; if any night is full it rolls back, gives the Redis hold back, and answers "sold out". So if Redis is ever wrong (writes lost in a failover to a lagging replica, or a rebuild racing a booking), the result is a rejected booking, never an overbooking. The lock serializes bookings of the same room type only, and costs a few milliseconds.

### Availability cleanup (`apps/availability-worker`)
A separate worker runs at startup and just after every UTC midnight. It removes nights that are already past from the current and previous month's keys (month keys also expire by themselves). It never adds anything: bookings write their own nights. Every run is written to `availability_window_log` and shown in the admin **Availability log** tab. Runs are idempotent and catch up after downtime; a failed run is retried after a minute.

The admin **Redis records** tab shows every room × night of the 365-night window with its booked count in Redis (or "no entry"), the Redis key and field, Redis memory, whether Redis is loaded, and a consistency check that compares the booked count of every booked night with the active bookings in PostgreSQL. The API uses `AVAILABILITY_DAYS` (365) as the booking horizon: stays ending later are rejected with a 400.

## Pricing
A night's price is built in layers, all in PostgreSQL:

1. **Base**: the room type's `rooms.price`.
2. **Holiday** (`price_rules.kind='HOLIDAY'`): a fixed price or base +/- %. It replaces steps 3 and 4.
3. **Season** (`'SEASON'`): a fixed price or base +/- %.
4. **Day of week**: multiplied by that weekday's %. The hotel's own value for the day (`hotels.weekday_pct`, 7 values Sunday..Saturday, NULL = inherit) wins over the country default (`country_pricing.weekday_pct`, set by the admin), e.g. Israel Mon −15%, Thu +30%, Fri +40%, Sat +10%.
5. **Discount** (`'DISCOUNT'`, hotel or room type only): minus the single largest active discount %. Discounts do not stack.

Seasons and holidays exist at three levels: country (admin), hotel and room type (seller). Inside one layer the most specific rule wins (room type > hotel > country), then the newest. Example: an Israeli Friday in high season with a 10% hotel discount on a ฿1,000 room costs 1,000 × 1.40 × 1.40 × 0.90 = ฿1,764. Prices are whole baht.

Prices are rules, not one row per room per night, so the pricing data stays tiny even for a large catalogue. Redis only answers "is a room free", so pricing never touches the atomic Lua scripts. `priceStay()` in the API is the only place prices are computed: the hotel page, search ("from" = the cheapest room's average per night for the selected dates), the seller's 60-night price calendar and booking all use it. Each night comes with its `parts` (layer, source, change) and a readable `label` such as "High season +40% · Fri +20% · Winter deal −10%".

A booking is priced before its nights are reserved in Redis, so a pricing error never leaves a lock behind. The booking stores the total in `price` and every night's price and label in `price_breakdown`, so later price changes, by the seller or the admin, never alter an existing booking.

## Bookings and the payment lock
`bookings` stores `check_in`, `check_out`, the total `price` for the stay, the per-night `price_breakdown`, a `status`, `expires_at` and `paid_at`.

Booking is two-step. `POST /api/bookings` reserves the nights in Redis and inserts a `PENDING` row with `expires_at = now() + 60s` (`PAYMENT_WINDOW_SECONDS`). Other customers already see the room as taken. `POST /api/bookings/:id/pay` flips it to `CONFIRMED` only if the window has not passed; the check is a single conditional `UPDATE`, so a late payment and the expiry worker cannot both win.

A background loop runs every 2 seconds and, in one transaction, moves due `PENDING` rows to `PAYMENT_TIMEOUT` (`FOR UPDATE SKIP LOCKED`, so several API instances could run it), gives the nights back to Redis and writes `booking.payment_timeout` + `room.availability.changed` outbox events. Customers can cancel a `PENDING` or `CONFIRMED` booking whose stay has not started; the nights are released the same way. Admin sample bookings are inserted directly as `CONFIRMED`.

Events: `booking.created`, `booking.confirmed`, `booking.payment_timeout`, `booking.cancelled`, `room.availability.changed`. On startup the API runs an idempotent migration that adds the date columns to databases created before they existed.

## Kafka
The API writes domain events to PostgreSQL first using the Outbox pattern. A background publisher sends unpublished events to Kafka.

## Lazy loading
Hotel search returns a lightweight list and a thumbnail. Hotel details are fetched only after opening a hotel. Browser images use `loading="lazy"`.

## Load simulation and bottlenecks
`POST /api/admin/simulation` creates N throw-away customers (`sim-<run>-<n>@example.com`, `is_sample=true`) and runs, inside the API process: a booking burst (every customer books a random room in the coming week at the same instant), immediate payments for a share, a wait for the payment-timeout worker, rebooking by the "late" share, and a wait for the outbox to drain. Each step is timed (avg/p50/p95/max) and the run is stored in `simulation_runs`, polled by the admin UI. The report ranks steps by p95. A second scenario (`same-room-fallback`) sends every customer at one room for 7 nights and, on a 409, walks them through the other room types of that hotel and then up to three other hotels, all concurrently; its report shows how many customers ended in each tier and which rooms filled up. Typical result on this stack: the outbox publisher (1.5s poll, one Kafka send per event) dominates at several seconds, the expiry worker adds up to 2s, a PostgreSQL booking transaction costs tens of ms under a 100-request burst, and the Redis Lua reservation costs about 2 ms. Obvious upgrades: batch the Kafka sends, LISTEN/NOTIFY instead of polling, and a shorter worker interval. The PostgreSQL guard adds a per-room-type row lock to each booking; in the same-room scenario that lock is where the crowd queues.

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
