// Availability window roller.
// Keeps Redis holding exactly AVAILABILITY_DAYS nights per room: today .. today+AVAILABILITY_DAYS-1.
// Once a day (just after UTC midnight) it adds the night that just entered the window and removes the night
// that just left it (yesterday). Each run is idempotent and catches up on missed days, so it is safe to
// restart, run late, or run more than once. Every run is recorded in availability_window_log (shown in the admin UI).
import { Pool } from 'pg';
import Redis from 'ioredis';

const pool = new Pool({ connectionString: process.env.DATABASE_URL });
const redis = new Redis(process.env.REDIS_URL || 'redis://localhost:6379');

const AVAILABILITY_DAYS = Number(process.env.AVAILABILITY_DAYS || 365);
const RETRY_MS = 60_000;

// Same key format and calendar (UTC YYYY-MM-DD) as the API.
const nightKey = (roomId: string, night: string) => `availability:${roomId}:${night}`;
function todayStr() { return new Date().toISOString().slice(0, 10); }
function addDays(d: string, n: number) {
  const t = new Date(d + 'T00:00:00Z'); t.setUTCDate(t.getUTCDate() + n); return t.toISOString().slice(0, 10);
}
function log(msg: string, extra: object = {}) { console.log(JSON.stringify({ time: new Date().toISOString(), msg, ...extra })); }

type Trigger = 'startup' | 'daily' | 'retry';
type NightCount = { night: string; keys: number };

/** Groups key counts per night, oldest first, for the log. */
function perNight(counts: Map<string, number>): NightCount[] {
  return [...counts].sort(([a], [b]) => a.localeCompare(b)).map(([night, keys]) => ({ night, keys }));
}
function bump(counts: Map<string, number>, night: string) { counts.set(night, (counts.get(night) || 0) + 1); }

async function ensureLogTable() {
  await pool.query(`CREATE TABLE IF NOT EXISTS availability_window_log (
    id BIGSERIAL PRIMARY KEY, ran_at TIMESTAMPTZ NOT NULL DEFAULT now(), trigger TEXT NOT NULL, status TEXT NOT NULL,
    window_first DATE, window_last DATE, added_keys INT NOT NULL DEFAULT 0, removed_keys INT NOT NULL DEFAULT 0,
    added_nights JSONB NOT NULL DEFAULT '[]'::jsonb, removed_nights JSONB NOT NULL DEFAULT '[]'::jsonb,
    duration_ms INT, error TEXT)`);
}

async function writeLog(e: { trigger: Trigger; status: 'OK' | 'FAILED'; first: string; last: string; added: NightCount[]; removed: NightCount[]; ms: number; error?: string }) {
  const sum = (x: NightCount[]) => x.reduce((a, n) => a + n.keys, 0);
  await pool.query(
    `INSERT INTO availability_window_log(trigger,status,window_first,window_last,added_keys,removed_keys,added_nights,removed_nights,duration_ms,error)
     VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)`,
    [e.trigger, e.status, e.first, e.last, sum(e.added), sum(e.removed), JSON.stringify(e.added), JSON.stringify(e.removed), e.ms, e.error ?? null]);
}

/** Adds every missing night inside the window. SET NX never touches a live counter. */
async function addMissingNights(first: string, last: string) {
  const added = new Map<string, number>();
  let after = '00000000-0000-0000-0000-000000000000';
  // 200 rooms (73k room-nights) per query, so a large catalogue (the API's Redis capacity test) never has to fit in memory at once.
  while (true) {
    // A missing night gets total_rooms minus the active bookings covering it (normally none that far ahead).
    const r = await pool.query(`
      WITH rs AS (SELECT id, total_rooms FROM rooms WHERE id > $3 ORDER BY id LIMIT 200)
      SELECT rs.id AS room_id, rs.total_rooms, to_char(d.night,'YYYY-MM-DD') AS night, count(b.id)::int AS booked
      FROM rs
      CROSS JOIN generate_series($1::date, $2::date, '1 day') AS d(night)
      LEFT JOIN bookings b ON b.room_id=rs.id AND b.status IN ('CONFIRMED','PENDING') AND b.check_in <= d.night AND b.check_out > d.night
      GROUP BY rs.id, rs.total_rooms, d.night`, [first, last, after]);
    if (!r.rows.length) break;
    const pipe = redis.pipeline();
    for (const row of r.rows) {
      pipe.set(nightKey(row.room_id, row.night), String(Math.max(0, row.total_rooms - row.booked)), 'NX');
      if (row.room_id > after) after = row.room_id;
    }
    const res = await pipe.exec();
    const failed = res!.find(([err]) => err);
    if (failed) throw failed[0]; // e.g. OOM: Redis is at maxmemory
    res!.forEach(([, v], i) => { if (v === 'OK') bump(added, r.rows[i].night); });
  }
  return perNight(added);
}

/** Removes every night outside the window: yesterday (and older, if a day was missed) and anything too far ahead. */
async function removeNightsOutside(first: string, last: string) {
  const removed = new Map<string, number>();
  const stream = redis.scanStream({ match: 'availability:*', count: 1000 });
  for await (const keys of stream as AsyncIterable<string[]>) {
    const outside = keys.filter(k => { const night = k.slice(-10); return night < first || night > last; });
    if (!outside.length) continue;
    await redis.unlink(...outside);
    for (const k of outside) bump(removed, k.slice(-10));
  }
  return perNight(removed);
}

async function roll(trigger: Trigger) {
  const t0 = performance.now();
  const first = todayStr();
  const last = addDays(first, AVAILABILITY_DAYS - 1);
  let added: NightCount[] = [], removed: NightCount[] = [];
  try {
    await ensureLogTable();
    added = await addMissingNights(first, last);
    removed = await removeNightsOutside(first, last);
  } catch (err) {
    // Best effort: PostgreSQL itself may be what failed.
    await writeLog({ trigger, status: 'FAILED', first, last, added, removed, ms: Math.round(performance.now() - t0), error: String(err) }).catch(() => {});
    throw err;
  }
  const ms = Math.round(performance.now() - t0);
  await writeLog({ trigger, status: 'OK', first, last, added, removed, ms });
  log('availability window rolled', { trigger, first, last, addedKeys: added.reduce((a, n) => a + n.keys, 0), removedKeys: removed.reduce((a, n) => a + n.keys, 0), ms });
}

function msUntilNextUtcMidnight() {
  const now = new Date();
  // +1s so the API's todayStr() has already moved to the new day.
  return Date.UTC(now.getUTCFullYear(), now.getUTCMonth(), now.getUTCDate() + 1) - now.getTime() + 1000;
}

async function loop(trigger: Trigger) {
  try {
    await roll(trigger);
    setTimeout(() => loop('daily'), msUntilNextUtcMidnight());
  } catch (err) {
    log('roll failed, retrying', { error: String(err), retryInMs: RETRY_MS });
    setTimeout(() => loop('retry'), RETRY_MS);
  }
}

async function shutdown() {
  await Promise.allSettled([pool.end(), redis.quit()]);
  process.exit(0);
}
process.on('SIGTERM', shutdown);
process.on('SIGINT', shutdown);

log('availability worker started', { days: AVAILABILITY_DAYS });
loop('startup');
