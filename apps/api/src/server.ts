import Fastify from 'fastify';
import cors from '@fastify/cors';
import jwt from '@fastify/jwt';
import { Pool } from 'pg';
import Redis from 'ioredis';
import { Kafka } from 'kafkajs';
import crypto from 'crypto';

const app = Fastify({ logger: true });
const pool = new Pool({ connectionString: process.env.DATABASE_URL });
const redis = new Redis(process.env.REDIS_URL || 'redis://localhost:6379');
const kafka = new Kafka({ clientId: 'hotel-booking-lab-api', brokers: (process.env.KAFKA_BROKERS || 'localhost:9092').split(',') });
const producer = kafka.producer();

app.register(cors, { origin: process.env.CORS_ORIGIN || true });
app.register(jwt, { secret: process.env.JWT_SECRET || 'dev-secret' });

type Role = 'CUSTOMER'|'SELLER'|'ADMIN';

declare module 'fastify' {
  interface FastifyRequest {
    userCtx?: { id: string; role: Role; impersonatedBy?: string };
  }
}

const MAX_NIGHTS = 30;
const PAYMENT_WINDOW_SECONDS = Number(process.env.PAYMENT_WINDOW_SECONDS || 60);
// How many nights ahead (from today) every room's availability is loaded into Redis.
const AVAILABILITY_DAYS = Number(process.env.AVAILABILITY_DAYS || 365);

function id() { return crypto.randomUUID(); }

async function auth(req: any, roles?: Role[]) {
  try {
    const p: any = await req.jwtVerify();
    req.userCtx = { id: p.id, role: p.role, impersonatedBy: p.impersonatedBy };
    if (roles && !roles.includes(p.role)) throw new Error('FORBIDDEN');
  } catch {
    throw { statusCode: 401, message: 'Unauthorized' };
  }
}

async function tryAuth(req: any) {
  if (!req.headers?.authorization) return;
  try {
    const p: any = await req.jwtVerify();
    req.userCtx = { id: p.id, role: p.role, impersonatedBy: p.impersonatedBy };
  } catch { /* anonymous */ }
}

async function addOutbox(client: any, topic: string, key: string, payload: any) {
  await client.query(
    `INSERT INTO outbox_events(id,topic,event_key,payload) VALUES($1,$2,$3,$4)`,
    [id(), topic, key, JSON.stringify(payload)]
  );
}

// ---- Date helpers -------------------------------------------------------
// Dates are plain YYYY-MM-DD strings (hotel-local calendar days, no timezone math).
const DATE_RE = /^\d{4}-\d{2}-\d{2}$/;
function todayStr() { return new Date().toISOString().slice(0, 10); }
function addDays(d: string, n: number) {
  const t = new Date(d + 'T00:00:00Z'); t.setUTCDate(t.getUTCDate() + n); return t.toISOString().slice(0, 10);
}
/** Returns each night (check-in inclusive, check-out exclusive) of a stay. */
function nightsOf(checkIn: string, checkOut: string) {
  const out: string[] = [];
  for (let d = checkIn; d < checkOut; d = addDays(d, 1)) out.push(d);
  return out;
}
/** Validates a stay range; returns the nights or an error message. */
function parseRange(checkIn: any, checkOut: any): { nights: string[] } | { error: string } {
  if (!DATE_RE.test(String(checkIn)) || !DATE_RE.test(String(checkOut))) return { error: 'checkIn and checkOut must be YYYY-MM-DD' };
  if (isNaN(Date.parse(checkIn)) || isNaN(Date.parse(checkOut))) return { error: 'Invalid date' };
  if (checkIn < todayStr()) return { error: 'checkIn cannot be in the past' };
  if (checkOut <= checkIn) return { error: 'checkOut must be after checkIn' };
  const nights = nightsOf(checkIn, checkOut);
  if (nights.length > MAX_NIGHTS) return { error: `Stay cannot exceed ${MAX_NIGHTS} nights` };
  if (checkOut > addDays(todayStr(), AVAILABILITY_DAYS)) return { error: `Bookings are open up to ${AVAILABILITY_DAYS} days ahead` };
  return { nights };
}

// ---- Pricing -------------------------------------------------------------
// A night's price is built in layers (whole baht, never below 0):
//   1. base: rooms.price
//   2. HOLIDAY rule, if any: fixed price or base +/- %; skips steps 3-4
//   3. SEASON rule: fixed price or base +/- %
//   4. weekday %: the hotel's value for that day of the week, else the country's (country_pricing), else 0
//   5. DISCOUNT: minus the single largest active hotel/room discount %
// Inside one layer a room rule beats a hotel rule beats a country rule, then the newest wins.
type PriceRule = { id: string; hotel_id: string | null; country: string | null; room_id: string | null; kind: 'SEASON'|'HOLIDAY'|'DISCOUNT';
  name: string; start_date: string; end_date: string; adjust_type: 'PERCENT'|'FIXED'; adjust_value: number; created_at: Date };
type Pricing = { hotels: Map<string, { country: string; weekdayPct: (number|null)[] }>; countryPct: Map<string, number[]>; rules: PriceRule[];
  byHotel: Map<string, PriceRule[]>; byCountry: Map<string, PriceRule[]> };
type PricedRoom = { id: string; hotel_id: string; price: any };
const WEEKDAY_SHORT = ['Sun', 'Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat'];
const toPct = (a: any) => Array.from({ length: 7 }, (_, i) => a?.[i] == null ? null : Number(a[i]));
const RULE_COLUMNS = `id, hotel_id, country, room_id, kind, name, to_char(start_date,'YYYY-MM-DD') AS start_date, to_char(end_date,'YYYY-MM-DD') AS end_date,
  adjust_type, adjust_value::float AS adjust_value, created_at`;

/** Everything needed to price rooms of these hotels: their country, weekday patterns and every applicable rule. */
async function loadPricing(hotelIds: string[]): Promise<Pricing> {
  const hotels = hotelIds.length ? (await pool.query(`SELECT id, country, weekday_pct FROM hotels WHERE id = ANY($1)`, [hotelIds])).rows : [];
  const countries = [...new Set(hotels.map((h:any) => h.country as string))];
  const [cp, rules] = await Promise.all([
    pool.query(`SELECT country, weekday_pct FROM country_pricing WHERE country = ANY($1)`, [countries]),
    pool.query(`SELECT ${RULE_COLUMNS} FROM price_rules WHERE hotel_id = ANY($1) OR country = ANY($2)`, [hotelIds, countries])]);
  const group = (key: (r: PriceRule) => string | null) => rules.rows.reduce((m: Map<string, PriceRule[]>, r: PriceRule) => {
    const k = key(r); if (k) m.set(k, [...(m.get(k) || []), r]); return m; }, new Map());
  return { hotels: new Map(hotels.map((h:any) => [h.id, { country: h.country, weekdayPct: toPct(h.weekday_pct) }])),
    countryPct: new Map(cp.rows.map((c:any) => [c.country, toPct(c.weekday_pct).map(x => x ?? 0)])), rules: rules.rows,
    byHotel: group(r => r.hotel_id), byCountry: group(r => r.country) };
}
const ruleSource = (r: PriceRule) => r.room_id ? 'room' : r.hotel_id ? 'hotel' : 'country';
const SOURCE_RANK = { room: 3, hotel: 2, country: 1 } as const;
function pickRule(rules: PriceRule[]) {
  let win: PriceRule | null = null;
  for (const r of rules) if (!win || (SOURCE_RANK[ruleSource(r)] - SOURCE_RANK[ruleSource(win)] || r.created_at.getTime() - win.created_at.getTime()) > 0) win = r;
  return win;
}
const signed = (n: number) => `${n > 0 ? '+' : n < 0 ? '−' : ''}${Math.abs(n)}%`;

function priceNight(room: PricedRoom, night: string, P: Pricing) {
  const hotel = P.hotels.get(room.hotel_id), country = hotel?.country;
  const base = Number(room.price);
  const candidates = [...(P.byHotel.get(room.hotel_id) || []), ...(country ? P.byCountry.get(country) || [] : [])];
  const active = candidates.filter(r => night >= r.start_date && night <= r.end_date &&
    (r.room_id ? r.room_id === room.id : r.hotel_id ? r.hotel_id === room.hotel_id : r.country === country));
  const of = (kind: PriceRule['kind']) => active.filter(r => r.kind === kind);
  const apply = (r: PriceRule) => r.adjust_type === 'FIXED' ? r.adjust_value : base * (1 + r.adjust_value / 100);
  const describe = (r: PriceRule) => r.adjust_type === 'FIXED' ? `฿${r.adjust_value.toLocaleString('en')}` : signed(r.adjust_value);
  const parts: { layer: string; source: string; name: string; change: string }[] = [];
  let price = base;
  const holiday = pickRule(of('HOLIDAY'));
  if (holiday) { price = apply(holiday); parts.push({ layer: 'HOLIDAY', source: ruleSource(holiday), name: holiday.name, change: describe(holiday) }); }
  else {
    const season = pickRule(of('SEASON'));
    if (season) { price = apply(season); parts.push({ layer: 'SEASON', source: ruleSource(season), name: season.name, change: describe(season) }); }
    const dow = new Date(night + 'T00:00:00Z').getUTCDay();
    const own = hotel?.weekdayPct[dow] ?? null, pct = own ?? (country ? P.countryPct.get(country)?.[dow] : 0) ?? 0;
    if (pct) { price *= 1 + pct / 100; parts.push({ layer: 'WEEKDAY', source: own != null ? 'hotel' : 'country', name: WEEKDAY_SHORT[dow], change: signed(pct) }); }
  }
  const discount = of('DISCOUNT').sort((a, b) => b.adjust_value - a.adjust_value)[0];
  if (discount) { price *= 1 - discount.adjust_value / 100; parts.push({ layer: 'DISCOUNT', source: ruleSource(discount), name: discount.name, change: signed(-discount.adjust_value) }); }
  const label = parts.map(p => `${p.name} ${p.change}`).join(' · ');
  // `rule` keeps the shape older clients and stored bookings use: the strongest layer, named by the full label.
  return { night, price: Math.max(0, Math.round(price)), parts, label, rule: parts[0] ? { kind: parts[0].layer, name: label } : null };
}
/** Price of every night of a stay and the total. */
function priceStay(room: PricedRoom, nights: string[], P: Pricing) {
  const nightly = nights.map(n => priceNight(room, n, P));
  return { total: nightly.reduce((a, n) => a + n.price, 0), nightly };
}
/** Cheapest stay per hotel: the room type with the lowest total for these nights. */
async function cheapestStays(hotelIds: string[], nights: string[]) {
  const out = new Map<string, number>();
  if (!hotelIds.length) return out;
  const [rooms, P] = await Promise.all([pool.query(`SELECT id, hotel_id, price FROM rooms WHERE hotel_id = ANY($1)`, [hotelIds]), loadPricing(hotelIds)]);
  for (const rm of rooms.rows) { const t = priceStay(rm, nights, P).total; if (!out.has(rm.hotel_id) || t < out.get(rm.hotel_id)!) out.set(rm.hotel_id, t); }
  return out;
}
// Sorting by the real price for the dates prices every matching hotel's stay; above this many hotels search sorts by
// base price instead (a real site would read a precomputed price index).
const PRICE_SORT_LIMIT = 5000;

/** Validates a SEASON / HOLIDAY / DISCOUNT rule body; returns the normalised fields or an error message. */
function parseRule(b: any, allowDiscount: boolean) {
  const kinds = allowDiscount ? ['SEASON', 'HOLIDAY', 'DISCOUNT'] : ['SEASON', 'HOLIDAY'];
  if (!kinds.includes(b.kind)) return { error: `kind must be ${kinds.join(', ')}` };
  const name = String(b.name || '').trim().slice(0, 80) || ({ SEASON: 'Season', HOLIDAY: 'Holiday', DISCOUNT: 'Discount' } as any)[b.kind];
  const start = b.startDate, end = b.endDate || b.startDate;
  if (!DATE_RE.test(String(start)) || !DATE_RE.test(String(end))) return { error: 'Pick a start and an end date' };
  if (end < start) return { error: 'The end date cannot be before the start date' };
  const type = b.kind === 'DISCOUNT' ? 'PERCENT' : b.adjustType, value = Number(b.adjustValue);
  if (b.kind === 'DISCOUNT' ? !(value >= 1 && value <= 90) : type === 'PERCENT' ? !(value >= -90 && value <= 500) : type === 'FIXED' ? !(value > 0 && value <= 10_000_000) : true)
    return { error: b.kind === 'DISCOUNT' ? 'A discount must be between 1 and 90%' : 'Use a percentage between -90 and 500, or a fixed price above 0' };
  return { kind: b.kind as string, name, start, end, type: type as string, value };
}
/** Validates 7 weekday percentages (Sun..Sat); null = inherit (hotel level only). */
function parseWeekdays(pct: any, allowNull: boolean) {
  if (!Array.isArray(pct) || pct.length !== 7) return { error: 'Send 7 values, Sunday to Saturday' };
  const out = pct.map((v: any) => v === null || v === '' ? null : Number(v));
  if (out.some((v: any) => v === null ? !allowNull : !(v >= -90 && v <= 500))) return { error: 'Each day must be between -90% and +500%' };
  return { pct: out as (number|null)[] };
}

// ---- Redis availability (one counter per room per night) ---------------
const nightKey = (roomId: string, night: string) => `availability:${roomId}:${night}`;

const NOT_LOADED = { statusCode: 409, message: 'Availability for these dates is not loaded in Redis' };

/** Reads availability for every night. Every night must already be in Redis; a missing key is an error. */
async function availabilityFor(roomId: string, nights: string[]) {
  const vals = await redis.mget(nights.map(n => nightKey(roomId, n)));
  if (vals.some(v => v === null)) throw NOT_LOADED;
  return Math.min(...vals.map(Number));
}

// All-or-nothing: only decrements when every night exists and still has stock.
// -2 = a night is not in Redis, -1 = a night is sold out.
const RESERVE_LUA = `
  for i=1,#KEYS do
    local v=redis.call('GET',KEYS[i])
    if not v then return -2 end
    if tonumber(v) <= 0 then return -1 end
  end
  local min=nil
  for i=1,#KEYS do
    local r=redis.call('DECR',KEYS[i])
    if min==nil or r<min then min=r end
  end
  return min
`;
// Gives nights back, never exceeding the room's total inventory. All-or-nothing: -2 if any night is not in Redis.
const RELEASE_LUA = `
  for i=1,#KEYS do
    if redis.call('EXISTS',KEYS[i])==0 then return -2 end
  end
  local min=nil
  for i=1,#KEYS do
    local r=math.min(tonumber(redis.call('GET',KEYS[i]))+1, tonumber(ARGV[1]))
    redis.call('SET',KEYS[i],r)
    if min==nil or r<min then min=r end
  end
  return min
`;

/** Releases the not-yet-past nights of a stay (past nights are no longer kept in Redis). Returns remaining, or null if nothing to release. */
async function releaseNights(roomId: string, totalRooms: number, checkIn: string, checkOut: string) {
  const today = todayStr();
  const keys = nightsOf(checkIn, checkOut).filter(n => n >= today).map(n => nightKey(roomId, n));
  if (!keys.length) return null;
  const remaining = Number(await redis.eval(RELEASE_LUA, keys.length, ...keys, String(totalRooms)));
  if (remaining === -2) throw NOT_LOADED;
  return remaining;
}

/** Loads a fresh room's availability (total_rooms per night) for the whole booking horizon. Never overwrites existing keys. */
async function seedRoomAvailability(rooms: { id: string; total_rooms: number }[]) {
  const today = todayStr();
  const pipe = redis.pipeline();
  for (const r of rooms)
    for (let i = 0; i < AVAILABILITY_DAYS; i++) pipe.set(nightKey(r.id, addDays(today, i)), String(r.total_rooms), 'NX');
  await pipe.exec();
}

/** Every key a room can have: yesterday (until the worker rolls it out) .. one night past the window. */
function roomKeys(roomId: string) {
  const first = addDays(todayStr(), -1);
  return Array.from({ length: AVAILABILITY_DAYS + 2 }, (_, i) => nightKey(roomId, addDays(first, i)));
}
async function deleteRoomAvailability(roomId: string) {
  await redis.unlink(...roomKeys(roomId));
}

/** Runs a pipeline and throws its first command error (ioredis reports those per command instead of rejecting). */
async function execOrThrow(pipe: ReturnType<typeof redis.pipeline>) {
  const res = await pipe.exec();
  const failed = res?.find(([err]) => err);
  if (failed) throw failed[0];
  return res!;
}
/** SETs key/value pairs in MSET chunks. */
async function msetAll(pairs: string[]) {
  const pipe = redis.pipeline();
  for (let i = 0; i < pairs.length; i += 2000) pipe.mset(...pairs.slice(i, i + 2000));
  await execOrThrow(pipe);
}
const isRedisOom = (e: any) => String(e?.message || e).startsWith('OOM');

// ---- Routes --------------------------------------------------------------
app.get('/api/health', async () => ({ ok: true, service: 'hotel-booking-lab-api' }));

// Learning project only: passwords are stored in plain text and listed on the login page so anyone can try every role.
app.get('/api/auth/demo-accounts', async () => {
  const counts = await pool.query(`SELECT role, count(*)::int AS n FROM users GROUP BY role`);
  // Up to 2000 per role so the capacity test (up to a million sellers) cannot blow up the login page.
  const r = await pool.query(`
    SELECT name, email, password, role, "isSample", "isLoadTest" FROM (
      SELECT name, email, password_hash AS password, role, is_sample AS "isSample", is_load_test AS "isLoadTest",
        row_number() OVER (PARTITION BY role ORDER BY is_load_test, created_at, length(email), email) AS rn
      FROM users) u
    WHERE rn <= 2000 ORDER BY CASE role WHEN 'ADMIN' THEN 0 WHEN 'SELLER' THEN 1 ELSE 2 END, rn`);
  return { counts: Object.fromEntries(counts.rows.map((x:any) => [x.role, x.n])), accounts: r.rows };
});

// Catalogue totals for the login page. Bookable room-nights = every room unit x every night of the Redis window,
// minus the nights held by active (confirmed or pending) bookings. Computed in PostgreSQL, the source of truth.
app.get('/api/stats', async () => {
  const first = todayStr(), end = addDays(first, AVAILABILITY_DAYS);
  const r = await pool.query(`SELECT
    (SELECT count(*)::int FROM hotels) AS hotels,
    (SELECT count(DISTINCT country)::int FROM hotels) AS countries,
    (SELECT count(DISTINCT (country, city))::int FROM hotels) AS cities,
    (SELECT count(*)::int FROM rooms) AS "roomTypes",
    (SELECT COALESCE(sum(total_rooms),0)::bigint FROM rooms) AS rooms,
    (SELECT COALESCE(sum(LEAST(check_out, $2::date) - GREATEST(check_in, $1::date)),0)::bigint FROM bookings
       WHERE status IN ('CONFIRMED','PENDING') AND check_out > $1::date AND check_in < $2::date) AS "bookedRoomNights"`, [first, end]);
  const x = r.rows[0], rooms = Number(x.rooms), booked = Number(x.bookedRoomNights);
  return { ...x, rooms, bookedRoomNights: booked, windowDays: AVAILABILITY_DAYS, firstNight: first, lastNight: addDays(end, -1),
    totalRoomNights: rooms * AVAILABILITY_DAYS, availableRoomNights: Math.max(0, rooms * AVAILABILITY_DAYS - booked) };
});

// Countries and cities that have hotels (sellers: only their own), for the search filters.
app.get('/api/locations', async (req: any) => {
  await tryAuth(req);
  const seller = req.userCtx?.role === 'SELLER';
  const r = await pool.query(`SELECT country, city, count(*)::int AS hotels FROM hotels ${seller ? 'WHERE seller_id=$1' : ''}
    GROUP BY country, city ORDER BY country, city`, seller ? [req.userCtx.id] : []);
  return r.rows;
});

app.post('/api/auth/login', async (req: any, reply) => {
  const { email, password } = req.body || {};
  const r = await pool.query(`SELECT id,email,name,role,password_hash FROM users WHERE email=$1`, [email]);
  if (!r.rows[0] || r.rows[0].password_hash !== password) return reply.code(401).send({ error: 'Invalid credentials' });
  const u = r.rows[0];
  return { token: app.jwt.sign({ id: u.id, role: u.role }), user: { id:u.id,email:u.email,name:u.name,role:u.role } };
});

// ?sort=price_asc|price_desc (default newest first). With dates, price = the cheapest room's total for the stay.
app.get('/api/hotels', async (req: any) => {
  await tryAuth(req);
  const { country, city } = req.query;
  const page = Math.max(1, Number(req.query.page) || 1), limit = Math.min(100, Math.max(1, Number(req.query.limit) || 12));
  const offset = (page-1) * limit;
  const byPrice = req.query.sort === 'price_asc' ? 1 : req.query.sort === 'price_desc' ? -1 : 0;
  const values:any[] = [];
  const conds: string[] = [];
  if (country) { values.push(country); conds.push(`h.country = $${values.length}`); }
  if (city) { values.push(city); conds.push(`h.city = $${values.length}`); }
  if (req.userCtx?.role === 'SELLER') { values.push(req.userCtx.id); conds.push(`h.seller_id = $${values.length}`); }
  const where = conds.length ? `WHERE ${conds.join(' AND ')}` : '';
  const total = (await pool.query(`SELECT count(*)::int AS n FROM hotels h ${where}`, values)).rows[0].n;
  const range = req.query.checkIn && req.query.checkOut ? parseRange(req.query.checkIn, req.query.checkOut) : null;
  const nights = range && 'nights' in range ? range.nights : null;
  const select = `SELECT h.id,h.name,h.address,h.city,h.country,h.description,
      COALESCE((SELECT MIN(r.price) FROM rooms r WHERE r.hotel_id=h.id),0) AS "startingPrice",
      (SELECT hp.url FROM hotel_photos hp WHERE hp.hotel_id=h.id LIMIT 1) AS thumbnail
    FROM hotels h`;
  let rows: any[], stays: Map<string, number> | null = null, priceSortApprox = false;
  if (byPrice && nights && total <= PRICE_SORT_LIMIT) {
    // Exact: price every matching hotel's stay, sort, then load just this page. Hotels without rooms go last.
    const ids: string[] = (await pool.query(`SELECT h.id FROM hotels h ${where}`, values)).rows.map((x:any) => x.id);
    stays = await cheapestStays(ids, nights);
    const key = (hid: string) => stays!.has(hid) ? byPrice * stays!.get(hid)! : Infinity;
    const pageIds = ids.sort((a, b) => key(a) - key(b) || a.localeCompare(b)).slice(offset, offset + limit);
    const found = (await pool.query(`${select} WHERE h.id = ANY($1)`, [pageIds])).rows;
    rows = pageIds.map(pid => found.find((h:any) => h.id === pid)).filter(Boolean);
  } else {
    priceSortApprox = !!(byPrice && nights);
    const order = byPrice ? `"startingPrice" ${byPrice > 0 ? 'ASC' : 'DESC'}, h.id` : 'h.created_at DESC, h.id';
    rows = (await pool.query(`${select} ${where} ORDER BY ${order} LIMIT $${values.length + 1} OFFSET $${values.length + 2}`, [...values, limit, offset])).rows;
  }
  // With dates: "from" = the cheapest room's stay (total, and average per night), and
  // soldOut = no room in the hotel has a free unit on every night (null if Redis has no data for those nights).
  if (nights && rows.length) {
    const pageStays = stays ?? await cheapestStays(rows.map((h:any) => h.id), nights);
    for (const h of rows) if (pageStays.has(h.id)) {
      h.startingTotal = pageStays.get(h.id); h.startingPrice = Math.round(h.startingTotal / nights.length); h.priceForDates = true;
    }
    const rooms = await pool.query(`SELECT id, hotel_id FROM rooms WHERE hotel_id = ANY($1)`, [rows.map((h:any) => h.id)]);
    const vals = rooms.rows.length ? await redis.mget(rooms.rows.flatMap((rm:any) => nights.map(n => nightKey(rm.id, n)))) : [];
    const free = new Map<string, number | null>();
    rooms.rows.forEach((rm:any, i:number) => {
      const v = vals.slice(i * nights.length, (i + 1) * nights.length);
      const roomFree = v.some(x => x === null) ? null : Math.min(...v.map(Number));
      const prev = free.has(rm.hotel_id) ? free.get(rm.hotel_id)! : 0;
      free.set(rm.hotel_id, prev === null || roomFree === null ? null : prev + Math.max(0, roomFree));
    });
    for (const h of rows) { const f = free.has(h.id) ? free.get(h.id)! : 0; h.availableRooms = f; h.soldOut = f === null ? null : f === 0; }
  }
  return { page, limit, total, priceSortApprox, items: rows };
});

// Optional ?checkIn=YYYY-MM-DD&checkOut=YYYY-MM-DD; defaults to tonight.
app.get('/api/hotels/:id', async (req:any, reply) => {
  const checkIn = req.query.checkIn || todayStr();
  const checkOut = req.query.checkOut || addDays(checkIn, 1);
  const range = parseRange(checkIn, checkOut);
  if ('error' in range) return reply.code(400).send({ error: range.error });
  await tryAuth(req);
  const h = await pool.query(`SELECT * FROM hotels WHERE id=$1`, [req.params.id]);
  if (!h.rows[0]) return reply.code(404).send({error:'Hotel not found'});
  if (req.userCtx?.role === 'SELLER' && h.rows[0].seller_id !== req.userCtx.id) return reply.code(403).send({error:'This hotel belongs to another seller'});
  const rooms = await pool.query(`SELECT * FROM rooms WHERE hotel_id=$1 ORDER BY price`, [req.params.id]);
  const photos = await pool.query(`SELECT * FROM hotel_photos WHERE hotel_id=$1`, [req.params.id]);
  const rules = await loadPricing([req.params.id]);
  const enriched = [];
  for (const room of rooms.rows) {
    const availableRooms = await availabilityFor(room.id, range.nights);
    const quote = priceStay(room, range.nights, rules);
    enriched.push({ ...room, availableRooms, nights: range.nights.length, nightly: quote.nightly, totalPrice: quote.total,
      avgNightly: Math.round(quote.total / range.nights.length) });
  }
  enriched.sort((a, b) => a.totalPrice - b.totalPrice);
  return { ...h.rows[0], checkIn, checkOut, nights: range.nights.length, rooms:enriched, photos:photos.rows };
});

/** Reserves the nights in Redis, then persists the booking + outbox events. Throws {statusCode:409} when sold out. */
async function createBooking(userId: string, r: any, checkIn: string, checkOut: string, nights: string[], status: 'PENDING'|'CONFIRMED' = 'PENDING', windowSeconds = PAYMENT_WINDOW_SECONDS) {
  // Priced before the Redis reserve, so a pricing failure never leaves a lock behind. The booking keeps this price.
  const quote = priceStay(r, nights, await loadPricing([r.hotel_id]));
  const keys = nights.map(n => nightKey(r.id, n));
  const t0 = performance.now();
  const remaining = Number(await redis.eval(RESERVE_LUA, keys.length, ...keys, String(r.total_rooms)));
  const redisMs = performance.now() - t0;
  if (remaining === -2) throw { ...NOT_LOADED, redisMs };
  if (remaining < 0) throw { statusCode: 409, message: 'No rooms available for the selected dates', redisMs };
  const totalPrice = quote.total;
  const t1 = performance.now();
  const client = await pool.connect();
  try {
    await client.query('BEGIN');
    const bookingId=id();
    // PENDING = room locked while the customer pays; the expiry worker releases it if they don't.
    const ins = await client.query(
      `INSERT INTO bookings(id,user_id,hotel_id,room_id,status,price,check_in,check_out,expires_at,paid_at,price_breakdown)
       VALUES($1,$2,$3,$4,$5,$6,$7,$8, CASE WHEN $5='PENDING' THEN now() + ($9 * interval '1 second') END, CASE WHEN $5='CONFIRMED' THEN now() END, $10)
       RETURNING expires_at AS "expiresAt"`,
      [bookingId,userId,r.hotel_id,r.id,status,totalPrice,checkIn,checkOut,windowSeconds,JSON.stringify(quote.nightly)]
    );
    const expiresAt = ins.rows[0].expiresAt;
    await addOutbox(client,'booking.created',bookingId,{bookingId,userId,hotelId:r.hotel_id,roomId:r.id,status,checkIn,checkOut,nights:nights.length,totalPrice,remaining,expiresAt});
    await addOutbox(client,'room.availability.changed',r.id,{roomId:r.id,checkIn,checkOut,remaining});
    await client.query('COMMIT');
    return { bookingId, status, checkIn, checkOut, nights: nights.length, totalPrice, priceBreakdown: quote.nightly, remaining, expiresAt, paymentWindowSeconds: status==='PENDING' ? windowSeconds : 0,
      timings: { redisMs, pgMs: performance.now() - t1 } };
  } catch(e) {
    await client.query('ROLLBACK');
    await releaseNights(r.id, r.total_rooms, checkIn, checkOut);
    throw e;
  } finally { client.release(); }
}

app.post('/api/bookings', async (req:any, reply) => {
  await auth(req, ['CUSTOMER']);
  const { roomId, checkIn, checkOut } = req.body || {};
  const range = parseRange(checkIn, checkOut);
  if ('error' in range) return reply.code(400).send({ error: range.error });
  const room = await pool.query(`SELECT * FROM rooms WHERE id=$1`, [roomId]);
  if (!room.rows[0]) return reply.code(404).send({error:'Room not found'});
  try {
    return await createBooking(req.userCtx!.id, room.rows[0], checkIn, checkOut, range.nights);
  } catch (e: any) {
    if (e?.statusCode === 409) return reply.code(409).send({ error: e.message });
    throw e;
  }
});

app.get('/api/bookings/me', async (req:any) => {
  await auth(req, ['CUSTOMER']);
  const r=await pool.query(`
    SELECT b.id,b.status,b.price,b.created_at,b.expires_at AS "expiresAt",b.paid_at AS "paidAt",
      GREATEST(0, CEIL(EXTRACT(EPOCH FROM (b.expires_at - now()))))::int AS "secondsLeft",
      to_char(b.check_in,'YYYY-MM-DD') AS "checkIn", to_char(b.check_out,'YYYY-MM-DD') AS "checkOut",
      (b.check_out - b.check_in) AS nights,
      h.id AS "hotelId", h.name AS "hotelName", h.city, r.name AS "roomName",
      round(b.price / GREATEST(1, b.check_out - b.check_in)) AS "nightlyPrice", b.price_breakdown AS "priceBreakdown"
    FROM bookings b JOIN hotels h ON h.id=b.hotel_id JOIN rooms r ON r.id=b.room_id
    WHERE b.user_id=$1 ORDER BY b.check_in DESC, b.created_at DESC`, [req.userCtx!.id]);
  const active = (b:any) => b.status === 'CONFIRMED' || b.status === 'PENDING';
  const confirmed = r.rows.filter(active);
  return r.rows.map((b:any) => ({
    ...b,
    // Other active stays sharing at least one night with this one (a "double booking").
    overlaps: !active(b) ? [] : confirmed
      .filter((o:any) => o.id !== b.id && o.checkIn < b.checkOut && b.checkIn < o.checkOut)
      .map((o:any) => ({ id:o.id, hotelName:o.hotelName, city:o.city, checkIn:o.checkIn, checkOut:o.checkOut })),
  }));
});

/** PENDING -> CONFIRMED inside the payment window. Throws {statusCode:404|409}. No real payment is processed. */
async function payBooking(bookingId: string, userId: string) {
  const client = await pool.connect();
  try {
    await client.query('BEGIN');
    const upd = await client.query(
      `UPDATE bookings SET status='CONFIRMED', paid_at=now()
       WHERE id=$1 AND user_id=$2 AND status='PENDING' AND expires_at > now() RETURNING *`, [bookingId, userId]);
    const row = upd.rows[0];
    if (!row) {
      await client.query('ROLLBACK');
      const b = await pool.query(`SELECT status, expires_at FROM bookings WHERE id=$1 AND user_id=$2`, [bookingId, userId]);
      if (!b.rows[0]) throw { statusCode: 404, message: 'Booking not found' };
      if (b.rows[0].status === 'PENDING') throw { statusCode: 409, message: 'Payment window has passed; the room was released' };
      throw { statusCode: 409, message: `Booking is ${b.rows[0].status.replace('_',' ').toLowerCase()}` };
    }
    await addOutbox(client,'booking.confirmed',row.id,{bookingId:row.id,userId:row.user_id,hotelId:row.hotel_id,roomId:row.room_id,paidAt:row.paid_at,totalPrice:Number(row.price)});
    await client.query('COMMIT');
    return { bookingId: row.id, status: 'CONFIRMED' as const, paidAt: row.paid_at };
  } catch(e) { await client.query('ROLLBACK').catch(()=>{}); throw e; } finally { client.release(); }
}

/** Marks a PENDING/CONFIRMED booking CANCELLED and releases its nights. `force` skips the owner and stay-started checks (admin). */
async function cancelBooking(bookingId: string, opts: { userId?: string; force?: boolean }) {
  const client = await pool.connect();
  try {
    await client.query('BEGIN');
    const b = await client.query(
      `SELECT b.*, r.total_rooms, to_char(b.check_in,'YYYY-MM-DD') AS ci, to_char(b.check_out,'YYYY-MM-DD') AS co
       FROM bookings b JOIN rooms r ON r.id=b.room_id WHERE b.id=$1 ${opts.userId ? 'AND b.user_id=$2' : ''} FOR UPDATE`,
      opts.userId ? [bookingId, opts.userId] : [bookingId]);
    const row = b.rows[0];
    if (!row) throw { statusCode: 404, message: 'Booking not found' };
    if (row.status !== 'CONFIRMED' && row.status !== 'PENDING') throw { statusCode: 409, message: `Booking is already ${row.status.replace('_',' ').toLowerCase()}` };
    if (!opts.force && row.ci <= todayStr()) throw { statusCode: 409, message: 'Stay has already started' };
    await client.query(`UPDATE bookings SET status='CANCELLED' WHERE id=$1`, [row.id]);
    const remaining = await releaseNights(row.room_id, row.total_rooms, row.ci, row.co);
    await addOutbox(client,'booking.cancelled',row.id,{bookingId:row.id,userId:row.user_id,hotelId:row.hotel_id,roomId:row.room_id,checkIn:row.ci,checkOut:row.co,remaining,forced:!!opts.force});
    await addOutbox(client,'room.availability.changed',row.room_id,{roomId:row.room_id,checkIn:row.ci,checkOut:row.co,remaining});
    await client.query('COMMIT');
    return { bookingId: row.id, status: 'CANCELLED' as const, remaining };
  } catch(e) { await client.query('ROLLBACK').catch(()=>{}); throw e; } finally { client.release(); }
}

app.post('/api/bookings/:id/pay', async (req:any, reply) => {
  await auth(req, ['CUSTOMER']);
  try { return await payBooking(req.params.id, req.userCtx!.id); }
  catch (e: any) { if (e?.statusCode) return reply.code(e.statusCode).send({ error: e.message }); throw e; }
});

app.post('/api/bookings/:id/cancel', async (req:any, reply) => {
  await auth(req, ['CUSTOMER']);
  try { return await cancelBooking(req.params.id, { userId: req.userCtx!.id }); }
  catch (e: any) { if (e?.statusCode) return reply.code(e.statusCode).send({ error: e.message }); throw e; }
});

// ---- Seller: only their own hotels -------------------------------------
app.get('/api/seller/hotels', async (req:any) => {
  await auth(req, ['SELLER']);
  const r = await pool.query(`
    SELECT h.id,h.name,h.address,h.city,h.country,h.description,
      (SELECT hp.url FROM hotel_photos hp WHERE hp.hotel_id=h.id LIMIT 1) AS thumbnail,
      (SELECT count(*)::int FROM rooms r WHERE r.hotel_id=h.id) AS "roomCount",
      (SELECT count(*)::int FROM bookings b WHERE b.hotel_id=h.id AND b.status IN ('CONFIRMED','PENDING') AND b.check_out > CURRENT_DATE) AS "upcomingBookings",
      COALESCE((SELECT min(price) FROM rooms r WHERE r.hotel_id=h.id),0) AS "startingPrice"
    FROM hotels h WHERE h.seller_id=$1 ORDER BY h.created_at DESC`, [req.userCtx!.id]);
  return r.rows;
});

// Stat tiles + per-day and per-hotel series for the next N days (default 7, max 31). Source: PostgreSQL bookings.
app.get('/api/seller/dashboard', async (req:any) => {
  await auth(req, ['SELLER']);
  const days = Math.min(31, Math.max(1, Number(req.query.days || 7)));
  const sid = req.userCtx!.id;
  const inv = await pool.query(`
    SELECT count(DISTINCT h.id)::int AS hotels, count(r.id)::int AS "roomTypes", COALESCE(sum(r.total_rooms),0)::int AS inventory
    FROM hotels h LEFT JOIN rooms r ON r.hotel_id=h.id WHERE h.seller_id=$1`, [sid]);
  const perDay = await pool.query(`
    WITH d AS (SELECT generate_series(CURRENT_DATE, CURRENT_DATE + ($2::int - 1), '1 day')::date AS day)
    SELECT to_char(d.day,'YYYY-MM-DD') AS day,
      COALESCE((SELECT count(*) FROM bookings b JOIN hotels h ON h.id=b.hotel_id
                WHERE h.seller_id=$1 AND b.status IN ('CONFIRMED','PENDING') AND b.check_in <= d.day AND b.check_out > d.day),0)::int AS booked,
      COALESCE((SELECT count(*) FROM bookings b JOIN hotels h ON h.id=b.hotel_id
                WHERE h.seller_id=$1 AND b.status='PENDING' AND b.check_in <= d.day AND b.check_out > d.day),0)::int AS locked,
      COALESCE((SELECT count(*) FROM bookings b JOIN hotels h ON h.id=b.hotel_id
                WHERE h.seller_id=$1 AND b.status='CONFIRMED' AND b.check_in = d.day),0)::int AS arrivals
    FROM d ORDER BY d.day`, [sid, days]);
  const perHotel = await pool.query(`
    SELECT h.id, h.name, h.city,
      COALESCE((SELECT sum(total_rooms) FROM rooms r WHERE r.hotel_id=h.id),0)::int AS inventory,
      COALESCE((SELECT sum(LEAST(b.check_out, CURRENT_DATE + $2::int) - GREATEST(b.check_in, CURRENT_DATE))
                FROM bookings b WHERE b.hotel_id=h.id AND b.status IN ('CONFIRMED','PENDING')
                AND b.check_in < CURRENT_DATE + $2::int AND b.check_out > CURRENT_DATE),0)::int AS "bookedRoomNights",
      COALESCE((SELECT count(*) FROM bookings b WHERE b.hotel_id=h.id AND b.status IN ('CONFIRMED','PENDING')
                AND b.check_in < CURRENT_DATE + $2::int AND b.check_out > CURRENT_DATE),0)::int AS bookings,
      COALESCE((SELECT sum(b.price) FROM bookings b WHERE b.hotel_id=h.id AND b.status='CONFIRMED'
                AND b.check_in >= CURRENT_DATE AND b.check_in < CURRENT_DATE + $2::int),0)::numeric AS revenue
    FROM hotels h WHERE h.seller_id=$1 ORDER BY h.name`, [sid, days]);
  const inventory = inv.rows[0].inventory;
  const capacity = inventory * days;
  const bookedRoomNights = perDay.rows.reduce((a:number,d:any)=>a+d.booked,0);
  const hotels = perHotel.rows.map((h:any) => {
    const cap = h.inventory * days;
    return { ...h, revenue: Number(h.revenue), capacityRoomNights: cap, freeRoomNights: cap - h.bookedRoomNights, occupancyPct: cap ? Math.round(100*h.bookedRoomNights/cap) : 0 };
  });
  return {
    days, from: perDay.rows[0]?.day, to: perDay.rows[perDay.rows.length-1]?.day,
    tiles: {
      hotels: inv.rows[0].hotels, roomTypes: inv.rows[0].roomTypes, inventory,
      bookings: hotels.reduce((a,h)=>a+h.bookings,0),
      bookedRoomNights, freeRoomNights: capacity - bookedRoomNights, capacityRoomNights: capacity,
      occupancyPct: capacity ? Math.round(100*bookedRoomNights/capacity) : 0,
      revenue: hotels.reduce((a,h)=>a+h.revenue,0),
      arrivalsToday: perDay.rows[0]?.arrivals ?? 0,
      lockedNow: perDay.rows.reduce((a:number,d:any)=>a+d.locked,0),
    },
    perDay: perDay.rows.map((d:any)=>({ ...d, free: Math.max(0, inventory - d.booked) })),
    hotels,
  };
});

app.get('/api/seller/hotels/:id/bookings', async (req:any, reply) => {
  await auth(req, ['SELLER']);
  const h = await pool.query(`SELECT id FROM hotels WHERE id=$1 AND seller_id=$2`, [req.params.id, req.userCtx!.id]);
  if (!h.rows[0]) return reply.code(404).send({error:'Hotel not found'});
  const r = await pool.query(`
    SELECT b.id,b.status,b.price,r.name AS "roomName",u.name AS "customerName",u.email AS "customerEmail",
      to_char(b.check_in,'YYYY-MM-DD') AS "checkIn", to_char(b.check_out,'YYYY-MM-DD') AS "checkOut", (b.check_out-b.check_in) AS nights
    FROM bookings b JOIN rooms r ON r.id=b.room_id JOIN users u ON u.id=b.user_id
    WHERE b.hotel_id=$1 ORDER BY b.check_in DESC, b.created_at DESC`, [req.params.id]);
  return r.rows;
});

// ---- Seller price management ---------------------------------------------
async function ownHotel(req: any, hotelId: string) {
  return (await pool.query(`SELECT id, name, country FROM hotels WHERE id=$1 AND seller_id=$2`, [hotelId, req.userCtx!.id])).rows[0];
}

// Base prices, weekday % (hotel and inherited country values), rules (hotel + inherited country) and the resulting
// price of every room for the next `days` nights (default 60).
app.get('/api/seller/hotels/:id/prices', async (req:any, reply) => {
  await auth(req, ['SELLER']);
  const hotel = await ownHotel(req, req.params.id);
  if (!hotel) return reply.code(404).send({ error: 'Hotel not found' });
  const from = DATE_RE.test(String(req.query.from)) ? String(req.query.from) : todayStr();
  const days = Math.min(120, Math.max(1, Number(req.query.days) || 60));
  const rooms = (await pool.query(`SELECT id, hotel_id, name, price, total_rooms FROM rooms WHERE hotel_id=$1 ORDER BY price, name`, [hotel.id])).rows;
  const P = await loadPricing([hotel.id]);
  const nights = Array.from({ length: days }, (_, i) => addDays(from, i));
  const roomName = new Map(rooms.map((r:any) => [r.id, r.name]));
  const order = { HOLIDAY: 0, SEASON: 1, DISCOUNT: 2 };
  return { hotel, from, days, rooms,
    countryPct: P.countryPct.get(hotel.country) ?? Array(7).fill(0), hotelPct: P.hotels.get(hotel.id)!.weekdayPct,
    rules: P.rules.filter(r => r.end_date >= todayStr())
      .sort((a, b) => order[a.kind] - order[b.kind] || a.start_date.localeCompare(b.start_date))
      .map(r => ({ ...r, source: ruleSource(r), roomName: r.room_id ? roomName.get(r.room_id) : null })),
    calendar: rooms.map((r:any) => ({ roomId: r.id, nightly: priceStay(r, nights, P).nightly })) };
});

app.patch('/api/seller/rooms/:id', async (req:any, reply) => {
  await auth(req, ['SELLER']);
  const price = Number(req.body?.price);
  if (!(price > 0 && price <= 10_000_000)) return reply.code(400).send({ error: 'Price must be between 1 and 10,000,000' });
  const r = await pool.query(`UPDATE rooms r SET price=$1 FROM hotels h WHERE r.id=$2 AND h.id=r.hotel_id AND h.seller_id=$3 RETURNING r.id`,
    [price, req.params.id, req.userCtx!.id]);
  if (!r.rows[0]) return reply.code(404).send({ error: 'Room not found' });
  return { ok: true };
});

// Hotel weekday %, Sunday..Saturday; null = use the country's value for that day.
app.put('/api/seller/hotels/:id/weekdays', async (req:any, reply) => {
  await auth(req, ['SELLER']);
  const hotel = await ownHotel(req, req.params.id);
  if (!hotel) return reply.code(404).send({ error: 'Hotel not found' });
  const w = parseWeekdays(req.body?.pct, true);
  if ('error' in w) return reply.code(400).send({ error: w.error });
  await pool.query(`UPDATE hotels SET weekday_pct=$1 WHERE id=$2`, [w.pct.every(v => v === null) ? null : w.pct, hotel.id]);
  return { ok: true };
});

app.post('/api/seller/hotels/:id/price-rules', async (req:any, reply) => {
  await auth(req, ['SELLER']);
  const hotel = await ownHotel(req, req.params.id);
  if (!hotel) return reply.code(404).send({ error: 'Hotel not found' });
  const b = req.body || {}, r = parseRule(b, true);
  if ('error' in r) return reply.code(400).send({ error: r.error });
  if (b.roomId && !(await pool.query(`SELECT 1 FROM rooms WHERE id=$1 AND hotel_id=$2`, [b.roomId, hotel.id])).rows[0])
    return reply.code(400).send({ error: 'That room is not in this hotel' });
  const ins = await pool.query(
    `INSERT INTO price_rules(id,hotel_id,room_id,kind,name,start_date,end_date,adjust_type,adjust_value) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9) RETURNING id`,
    [id(), hotel.id, b.roomId || null, r.kind, r.name, r.start, r.end, r.type, r.value]);
  return { ok: true, id: ins.rows[0].id };
});

app.delete('/api/seller/price-rules/:id', async (req:any, reply) => {
  await auth(req, ['SELLER']);
  // Country rules have no hotel_id, so a seller can never delete them.
  const r = await pool.query(`DELETE FROM price_rules pr USING hotels h WHERE pr.id=$1 AND h.id=pr.hotel_id AND h.seller_id=$2 RETURNING pr.id`,
    [req.params.id, req.userCtx!.id]);
  if (!r.rows[0]) return reply.code(404).send({ error: 'Price rule not found' });
  return { ok: true };
});

// ---- Admin: country pricing defaults ---------------------------------------
app.get('/api/admin/country-pricing', async (req:any) => {
  await auth(req, ['ADMIN']);
  const r = await pool.query(`
    SELECT c.country, cp.weekday_pct,
      (SELECT count(*)::int FROM hotels h WHERE h.country=c.country) AS hotels,
      (SELECT count(*)::int FROM hotels h WHERE h.country=c.country AND h.weekday_pct IS NOT NULL) AS "hotelsWithOwnWeekdays"
    FROM (SELECT DISTINCT country FROM hotels UNION SELECT country FROM country_pricing) c
    LEFT JOIN country_pricing cp ON cp.country=c.country ORDER BY c.country`);
  const rules = (await pool.query(`SELECT ${RULE_COLUMNS} FROM price_rules WHERE country IS NOT NULL AND end_date >= $1 ORDER BY start_date`, [todayStr()])).rows;
  return r.rows.map((c:any) => ({ country: c.country, hotels: c.hotels, hotelsWithOwnWeekdays: c.hotelsWithOwnWeekdays,
    weekdayPct: toPct(c.weekday_pct).map(x => x ?? 0), rules: rules.filter((x:any) => x.country === c.country) }));
});
app.put('/api/admin/country-pricing/:country/weekdays', async (req:any, reply) => {
  await auth(req, ['ADMIN']);
  const w = parseWeekdays(req.body?.pct, false);
  if ('error' in w) return reply.code(400).send({ error: w.error });
  await pool.query(`INSERT INTO country_pricing(country, weekday_pct) VALUES($1,$2) ON CONFLICT (country) DO UPDATE SET weekday_pct=EXCLUDED.weekday_pct`,
    [req.params.country, w.pct]);
  return { ok: true };
});
app.post('/api/admin/country-pricing/:country/rules', async (req:any, reply) => {
  await auth(req, ['ADMIN']);
  const r = parseRule(req.body || {}, false);
  if ('error' in r) return reply.code(400).send({ error: r.error });
  const ins = await pool.query(
    `INSERT INTO price_rules(id,country,kind,name,start_date,end_date,adjust_type,adjust_value) VALUES($1,$2,$3,$4,$5,$6,$7,$8) RETURNING id`,
    [id(), req.params.country, r.kind, r.name, r.start, r.end, r.type, r.value]);
  return { ok: true, id: ins.rows[0].id };
});
app.delete('/api/admin/price-rules/:id', async (req:any, reply) => {
  await auth(req, ['ADMIN']);
  const r = await pool.query(`DELETE FROM price_rules WHERE id=$1 AND country IS NOT NULL RETURNING id`, [req.params.id]);
  if (!r.rows[0]) return reply.code(404).send({ error: 'Country rule not found' });
  return { ok: true };
});

app.get('/api/admin/users', async (req:any,reply) => {
  await auth(req,['ADMIN']);
  const r=await pool.query(`SELECT id,email,name,role FROM users WHERE NOT is_load_test ORDER BY created_at DESC`);
  return r.rows;
});

app.post('/api/admin/impersonate/:userId', async (req:any,reply) => {
  await auth(req,['ADMIN']);
  const r=await pool.query(`SELECT id,email,name,role FROM users WHERE id=$1`,[req.params.userId]);
  if(!r.rows[0]) return reply.code(404).send({error:'User not found'});
  const u=r.rows[0];
  await pool.query(`INSERT INTO audit_logs(id,actor_user_id,action,target_user_id) VALUES($1,$2,'IMPERSONATION_STARTED',$3)`,
    [id(),req.userCtx!.id,u.id]);
  return { token: app.jwt.sign({id:u.id,role:u.role,impersonatedBy:req.userCtx!.id}), user:u };
});

// Example hotel rules so layered prices show right away: New Year's Eve at a fixed price per room type, and an
// "Autumn deal" discount on even-numbered sample hotels. Weekday % and seasons come from the country defaults.
// Only for sample hotels that have no rules yet.
async function ensureSamplePriceRules() {
  const t = todayStr(), y = Number(t.slice(0, 4));
  const sy = t > `${y}-01-15` ? y : y - 1; // the New Year that is coming next
  const hotels = (await pool.query(`SELECT id FROM hotels h WHERE is_sample AND NOT EXISTS (SELECT 1 FROM price_rules p WHERE p.hotel_id=h.id)`)).rows.map((h:any) => h.id);
  if (!hotels.length) return 0;
  await pool.query(`INSERT INTO price_rules(id,hotel_id,room_id,kind,name,start_date,end_date,adjust_type,adjust_value)
    SELECT gen_random_uuid(), r.hotel_id, r.id, 'HOLIDAY', 'New Year''s Eve', $2, $2, 'FIXED', round(r.price * 2.5 / 100) * 100
    FROM rooms r WHERE r.hotel_id = ANY($1)`, [hotels, `${sy}-12-31`]);
  await pool.query(`INSERT INTO price_rules(id,hotel_id,kind,name,start_date,end_date,adjust_type,adjust_value)
    SELECT gen_random_uuid(), h.id, 'DISCOUNT', 'Autumn deal', $2, $3, 'PERCENT', 10
    FROM hotels h WHERE h.id = ANY($1) AND (regexp_replace(h.name,'\D','','g'))::int % 2 = 0`, [hotels, `${sy}-10-01`, `${sy}-11-30`]);
  return hotels.length;
}

// Idempotent: re-running it ensures the sample users exist and only creates hotels when there are none yet.
app.post('/api/admin/sample-data', async (req:any) => {
  await auth(req,['ADMIN']);
  const client=await pool.connect();
  try {
    await client.query('BEGIN');
    const sampleUsers = [
      ['seller@example.com','seller123','Sample Seller','SELLER'],
      ['seller2@example.com','seller123','Sample Seller 2','SELLER'],
      ['customer@example.com','customer123','Sample Customer','CUSTOMER'],
      ['customer2@example.com','customer123','Sample Customer 2','CUSTOMER'],
    ];
    const ids: Record<string,string> = {};
    for (const [email,pw,name,role] of sampleUsers) {
      const r = await client.query(
        `INSERT INTO users(id,email,password_hash,name,role,is_sample) VALUES($1,$2,$3,$4,$5,true)
         ON CONFLICT (email) DO UPDATE SET is_sample=true RETURNING id`, [id(),email,pw,name,role]);
      ids[email] = r.rows[0].id;
    }
    const existing = await client.query(`SELECT count(*)::int AS n FROM hotels WHERE is_sample=true`);
    let hotelsCreated = 0;
    const newRooms: { id: string; total_rooms: number }[] = [];
    if (existing.rows[0].n > 0) {
      // Databases seeded before seller2 existed: hand even-numbered sample hotels to seller2.
      await client.query(`UPDATE hotels SET seller_id=$1 WHERE is_sample=true AND seller_id=$2 AND (regexp_replace(name,'\\D','','g'))::int % 2 = 0`,
        [ids['seller2@example.com'], ids['seller@example.com']]);
    } else {
      const cities=['Bangkok','Chiang Mai','Phuket','Pattaya','Hua Hin'];
      for(let i=1;i<=20;i++){
        const hid=id();
        await client.query(`INSERT INTO hotels(id,seller_id,name,address,city,description,is_sample) VALUES($1,$2,$3,$4,$5,$6,true)`,
          [hid,ids[i%2?'seller@example.com':'seller2@example.com'],`Sample Hotel ${i}`,`${i} Riverside Road`,cities[i%cities.length],'A sample hotel for learning the booking architecture.']);
        await client.query(`INSERT INTO hotel_photos(id,hotel_id,url) VALUES($1,$2,$3)`,
          [id(),hid,`https://picsum.photos/seed/hotel${i}/900/600`]);
        for(let j=1;j<=3;j++){
          const room = { id: id(), total_rooms: 3+j };
          await client.query(`INSERT INTO rooms(id,hotel_id,name,price,total_rooms,is_sample) VALUES($1,$2,$3,$4,$5,true)`,
            [room.id,hid,`Room Type ${j}`,1000+i*50+j*100,room.total_rooms]);
          newRooms.push(room);
        }
        hotelsCreated++;
      }
    }
    await client.query('COMMIT');
    // Every room's availability must be in Redis before it can be booked.
    await seedRoomAvailability(newRooms);
    await ensureSamplePriceRules();
    return {ok:true,
      message: hotelsCreated ? `Sample data generated (${hotelsCreated} hotels, 4 users)` : 'Sample users ensured; sample hotels already existed',
      login:{customer:'customer@example.com / customer123',customer2:'customer2@example.com / customer123',seller:'seller@example.com / seller123',seller2:'seller2@example.com / seller123'}};
  } catch(e) { await client.query('ROLLBACK'); throw e; } finally { client.release(); }
});

// Creates a few future bookings for ONE customer. Two of them overlap on purpose so the
// "double booking" notice in My bookings can be seen. Uses the same Redis reservation path as real bookings.
app.post('/api/admin/sample-bookings', async (req:any, reply) => {
  await auth(req,['ADMIN']);
  const { userId } = req.body || {};
  const u = await pool.query(`SELECT id,role,email FROM users WHERE id=$1`, [userId]);
  if (!u.rows[0]) return reply.code(404).send({ error: 'User not found' });
  if (u.rows[0].role !== 'CUSTOMER') return reply.code(400).send({ error: 'Sample bookings can only be created for CUSTOMER users' });
  const rooms = await pool.query(`SELECT r.*, h.name AS hotel_name FROM rooms r JOIN hotels h ON h.id=r.hotel_id ORDER BY random() LIMIT 5`);
  if (rooms.rows.length < 5) return reply.code(409).send({ error: 'Generate sample data first' });
  const t = todayStr();
  const plans = [
    { room: rooms.rows[0], checkIn: addDays(t, 2),  checkOut: addDays(t, 5) },
    { room: rooms.rows[1], checkIn: addDays(t, 4),  checkOut: addDays(t, 6) },   // overlaps the first stay
    { room: rooms.rows[2], checkIn: addDays(t, 10), checkOut: addDays(t, 12) },
    { room: rooms.rows[3], checkIn: t,              checkOut: addDays(t, 2) },   // "Staying now"
  ];
  // A completed stay from last week, inserted directly: past nights are not tracked in Redis.
  const past = rooms.rows[4];
  await pool.query(
    `INSERT INTO bookings(id,user_id,hotel_id,room_id,status,price,check_in,check_out,paid_at)
     VALUES($1,$2,$3,$4,'CONFIRMED',$5,$6,$7,now() - interval '10 days')`,
    [id(), userId, past.hotel_id, past.id, priceStay(past, nightsOf(addDays(t, -7), addDays(t, -4)), await loadPricing([past.hotel_id])).total, addDays(t, -7), addDays(t, -4)]);
  const created: any[] = [], skipped: any[] = [];
  for (const pl of plans) {
    const range = parseRange(pl.checkIn, pl.checkOut);
    if ('error' in range) continue;
    try {
      const b = await createBooking(userId, pl.room, pl.checkIn, pl.checkOut, range.nights, 'CONFIRMED');
      created.push({ ...b, hotel: pl.room.hotel_name, room: pl.room.name });
    } catch (e: any) {
      if (e?.statusCode === 409) skipped.push({ hotel: pl.room.hotel_name, room: pl.room.name, reason: e.message }); else throw e;
    }
  }
  await pool.query(`INSERT INTO audit_logs(id,actor_user_id,action,target_user_id,metadata) VALUES($1,$2,'SAMPLE_BOOKINGS_CREATED',$3,$4)`,
    [id(), req.userCtx!.id, userId, JSON.stringify({ created: created.length, skipped: skipped.length })]);
  created.push({ bookingId: null, status: 'CONFIRMED', checkIn: addDays(t, -7), checkOut: addDays(t, -4), hotel: past.hotel_name, room: past.name, completed: true });
  return { ok:true, message:`Created ${created.length} sample booking(s) for ${u.rows[0].email} (upcoming, overlapping, current and completed stays)${skipped.length?`, ${skipped.length} skipped (sold out)`:''}`, created, skipped };
});

// ---- Load simulation -------------------------------------------------------
// N throw-away customers book random rooms for the coming week at the same instant. A share pays at once,
// a share is "late" (lets the lock expire, then rebooks and pays), the rest abandon. Everything is timed so
// the report shows where the time goes (Redis lock, PostgreSQL write, outbox->Kafka lag, expiry worker).
const sleep = (ms: number) => new Promise(r => setTimeout(r, ms));
const timed = async <T,>(fn: () => Promise<T>) => {
  const t = performance.now();
  try { const value = await fn(); return { ok: true as const, ms: performance.now()-t, value }; }
  catch (e: any) { return { ok: false as const, ms: performance.now()-t, error: e }; }
};
function stats(ms: number[]) {
  if (!ms.length) return { count: 0, avgMs: 0, p50Ms: 0, p95Ms: 0, maxMs: 0 };
  const a = [...ms].sort((x,y)=>x-y); const q = (p:number) => a[Math.min(a.length-1, Math.floor(p*a.length))];
  return { count: a.length, avgMs: +(a.reduce((x,y)=>x+y,0)/a.length).toFixed(1), p50Ms: +q(0.5).toFixed(1), p95Ms: +q(0.95).toFixed(1), maxMs: +a[a.length-1].toFixed(1) };
}
const running = new Set<string>();

type SimParams = { mode: 'random-week'|'same-room-fallback'; customers: number; payRatio: number; lateRatio: number; windowSeconds: number; roomId?: string;
  country?: string; city?: string;              // random-week: only book hotels there
  fallbackScope?: 'any'|'country'|'city' };     // same-room-fallback: where the "other hotel" fallback may look
function simContext(runId: string) {
  const started = Date.now(); const log: string[] = [];
  const note = (m: string) => { log.push(`${((Date.now()-started)/1000).toFixed(1)}s ${m}`); app.log.info({ runId }, m); };
  const save = async (status: string, report: any) =>
    pool.query(`UPDATE simulation_runs SET status=$2, report=$3, finished_at=CASE WHEN $2 IN ('DONE','FAILED') THEN now() END WHERE id=$1`, [runId, status, JSON.stringify({ ...report, log })]);
  return { started, log, note, save };
}
async function simCreateCustomers(runId: string, n: number) {
  const short = runId.slice(0, 8);
  const values: any[] = []; const rowsSql: string[] = [];
  for (let i = 1; i <= n; i++) { values.push(id(), `sim-${short}-${i}@example.com`, 'sim123', `Sim ${short} #${i}`); rowsSql.push(`($${values.length-3},$${values.length-2},$${values.length-1},$${values.length},'CUSTOMER',true)`); }
  const users = await pool.query(`INSERT INTO users(id,email,password_hash,name,role,is_sample) VALUES ${rowsSql.join(',')} RETURNING id`, values);
  const customerIds: string[] = users.rows.map((u:any)=>u.id);
  await pool.query(`UPDATE simulation_runs SET customer_ids=$2 WHERE id=$1`, [runId, customerIds]);
  return customerIds;
}
/** Waits (max 30s) for this run's outbox events to reach Kafka and returns lag stats. */
async function simOutbox(started: number, note: (m: string) => void) {
  const drainStart = Date.now(); let pendingEvents = 1;
  while (pendingEvents > 0 && Date.now() - drainStart < 30000) {
    await sleep(500);
    pendingEvents = (await pool.query(`SELECT count(*)::int AS n FROM outbox_events WHERE created_at >= to_timestamp($1/1000.0) AND published_at IS NULL`, [started])).rows[0].n;
  }
  const drainMs = Date.now() - drainStart;
  note(`outbox drained in ${(drainMs/1000).toFixed(1)}s (${pendingEvents} events still unpublished)`);
  const lag = await pool.query(`
    SELECT count(*)::int AS events, count(published_at)::int AS published,
      COALESCE(avg(EXTRACT(EPOCH FROM (published_at-created_at))*1000),0)::float AS "avgMs",
      COALESCE(percentile_cont(0.95) WITHIN GROUP (ORDER BY EXTRACT(EPOCH FROM (published_at-created_at))*1000),0)::float AS "p95Ms",
      COALESCE(max(EXTRACT(EPOCH FROM (published_at-created_at))*1000),0)::float AS "maxMs"
    FROM outbox_events WHERE created_at >= to_timestamp($1/1000.0)`, [started]);
  return { ...lag.rows[0], avgMs: +lag.rows[0].avgMs.toFixed(0), p95Ms: +lag.rows[0].p95Ms.toFixed(0), maxMs: +lag.rows[0].maxMs.toFixed(0), drainMs, unpublished: pendingEvents };
}
async function simFinalStatuses(customerIds: string[]) {
  const final = await pool.query(`SELECT status, count(*)::int FROM bookings WHERE user_id = ANY($1) GROUP BY status`, [customerIds]);
  return Object.fromEntries(final.rows.map((r:any)=>[r.status, r.count]));
}

// Mode 2: everyone wants the SAME room for 7 nights. Fallback 1: another room type in the same hotel.
// Fallback 2: another hotel (up to 3 tried). Winners pay immediately. Shows how a crowd cascades through inventory.
async function runSameRoomSimulation(runId: string, params: SimParams) {
  const { started, log, note, save } = simContext(runId);
  try {
    const customerIds = await simCreateCustomers(runId, params.customers);
    note(`created ${customerIds.length} customers`);
    const rooms = (await pool.query(`SELECT r.*, h.name AS hotel_name, h.city, h.country FROM rooms r JOIN hotels h ON h.id=r.hotel_id ORDER BY h.name, r.price`)).rows;
    const target = rooms.find(r => r.id === params.roomId) || rooms[0];
    if (!target) throw new Error('No rooms exist. Generate sample data first.');
    const t = todayStr(), checkIn = t, checkOut = addDays(t, 7);
    const range = parseRange(checkIn, checkOut); if ('error' in range) throw new Error(range.error);
    const sameHotelOthers = rooms.filter(r => r.hotel_id === target.hotel_id && r.id !== target.id);
    const scope = params.fallbackScope || 'any';
    const inScope = (r: any) => scope === 'any' || (r.country === target.country && (scope === 'country' || r.city === target.city));
    const otherHotelIds = [...new Set(rooms.filter(r => r.hotel_id !== target.hotel_id && inScope(r)).map(r => r.hotel_id))];
    const shuffle = <T,>(a: T[]) => { const b = [...a]; for (let i = b.length-1; i > 0; i--) { const j = Math.floor(Math.random()*(i+1)); [b[i], b[j]] = [b[j], b[i]]; } return b; };
    const timings: Record<string, number[]> = { targetRoom: [], sameHotelOtherRoom: [], otherHotel: [], pay: [] };
    const tiers: Record<string, number> = { targetRoom: 0, sameHotelOtherRoom: 0, otherHotel: 0, noRoom: 0 };
    const attemptsPerTier: Record<string, number> = { targetRoom: 0, sameHotelOtherRoom: 0, otherHotel: 0 };
    const won: Record<string, number> = {}; let paid = 0, payFailed = 0, errors = 0;
    type Outcome = { tier: string; attempts: number; room?: string };
    const attempt = async (userId: string, room: any, tier: string) => {
      attemptsPerTier[tier]++;
      const r = await timed(() => createBooking(userId, room, checkIn, checkOut, range.nights, 'PENDING', params.windowSeconds));
      timings[tier].push(r.ms);
      if (!r.ok && r.error?.statusCode !== 409) { errors++; app.log.error(r.error); }
      return r.ok ? r.value : null;
    };
    const flow = async (userId: string): Promise<Outcome> => {
      let attempts = 0; let b = await attempt(userId, target, 'targetRoom'); attempts++;
      let tier = 'targetRoom', room = `${target.hotel_name} / ${target.name}`;
      if (!b) { tier = 'sameHotelOtherRoom'; for (const r of shuffle(sameHotelOthers)) { b = await attempt(userId, r, tier); attempts++; if (b) { room = `${r.hotel_name} / ${r.name}`; break; } } }
      if (!b) { tier = 'otherHotel'; outer: for (const hid of shuffle(otherHotelIds).slice(0, 3)) { for (const r of shuffle(rooms.filter(x => x.hotel_id === hid))) { b = await attempt(userId, r, tier); attempts++; if (b) { room = `${r.hotel_name} / ${r.name}`; break outer; } } } }
      if (!b) return { tier: 'noRoom', attempts };
      const p = await timed(() => payBooking(b.bookingId, userId)); timings.pay.push(p.ms); if (p.ok) paid++; else payFailed++;
      return { tier, attempts, room };
    };
    const burstStart = performance.now();
    const outcomes = await Promise.all(customerIds.map(flow));
    const burstMs = performance.now() - burstStart;
    for (const o of outcomes) { tiers[o.tier]++; if (o.room) won[o.room] = (won[o.room]||0) + 1; }
    const totalAttempts = outcomes.reduce((a,o)=>a+o.attempts,0);
    note(`cascade done in ${burstMs.toFixed(0)}ms: ${tiers.targetRoom} got the target room, ${tiers.sameHotelOtherRoom} another room in the hotel, ${tiers.otherHotel} another hotel, ${tiers.noRoom} nothing`);
    const outbox = await simOutbox(started, note);
    const ops: Record<string, any> = {
      'Target room attempt': stats(timings.targetRoom), 'Same-hotel other room attempt': stats(timings.sameHotelOtherRoom),
      'Other-hotel attempt': stats(timings.otherHotel), 'Pay request': stats(timings.pay),
    };
    const bottlenecks = [
      ...Object.entries(ops).filter(([,v]) => v.count).map(([name, v]) => ({ step: name, p95Ms: v.p95Ms, note: '' })),
      { step: 'Outbox -> Kafka publish lag (insert to publish)', p95Ms: outbox.p95Ms, note: `publisher polls every 1.5s, one Kafka send per event; ${outbox.events} events drained in ${(outbox.drainMs/1000).toFixed(1)}s` },
    ].sort((a,b)=>b.p95Ms-a.p95Ms);
    await save('DONE', {
      params, mode: params.mode, customers: customerIds.length, durationMs: Date.now()-started, burstMs: +burstMs.toFixed(0),
      throughputPerSec: +(totalAttempts / (burstMs/1000)).toFixed(1),
      target: { hotel: target.hotel_name, city: target.city, country: target.country, fallbackScope: scope, room: target.name, totalRooms: target.total_rooms, checkIn, checkOut, nights: 7,
        sameHotelOtherRooms: sameHotelOthers.length, sameHotelOtherCapacity: sameHotelOthers.reduce((a,r)=>a+r.total_rooms,0), otherHotels: otherHotelIds.length },
      tiers, attemptsPerTier, totalAttempts, avgAttemptsPerCustomer: +(totalAttempts / customerIds.length).toFixed(2),
      counts: { bookAttempts: totalAttempts, locked: customerIds.length - tiers.noRoom, soldOut: tiers.noRoom, paid, payFailed, errors, timedOut: 0, late: 0, abandoned: 0, rebooked: 0, rebookSoldOut: 0 },
      roomsWon: Object.entries(won).map(([room,count])=>({room,count})).sort((a:any,b:any)=>b.count-a.count),
      outcomes: [
        { outcome: `got the target room (${target.hotel_name} / ${target.name})`, count: tiers.targetRoom },
        { outcome: 'target sold out -> another room in the same hotel', count: tiers.sameHotelOtherRoom },
        { outcome: 'hotel sold out -> another hotel', count: tiers.otherHotel },
        { outcome: 'no room found (3 other hotels tried)', count: tiers.noRoom },
      ],
      finalStatuses: await simFinalStatuses(customerIds), timings: ops, outbox, bottlenecks, progressPct: 100,
    });
    note('done');
  } catch (e: any) { app.log.error(e); await save('FAILED', { error: e?.message || String(e), progressPct: 100 }); }
  finally { running.delete(runId); }
}

async function runSimulation(runId: string, params: SimParams) {
  const { started, log, note, save } = simContext(runId);
  try {
    const customerIds = await simCreateCustomers(runId, params.customers);
    note(`created ${customerIds.length} customers`);
    const rooms = (await pool.query(`SELECT r.*, h.name AS hotel_name FROM rooms r JOIN hotels h ON h.id=r.hotel_id
      WHERE ($1::text IS NULL OR h.country=$1) AND ($2::text IS NULL OR h.city=$2)`, [params.country ?? null, params.city ?? null])).rows;
    if (!rooms.length) throw new Error(params.country || params.city ? `No rooms in ${[params.city, params.country].filter(Boolean).join(', ')}.` : 'No rooms exist. Generate sample data first.');
    const t = todayStr();
    const pick = () => { const room = rooms[Math.floor(Math.random()*rooms.length)]; const start = Math.floor(Math.random()*7); const nights = 1 + Math.floor(Math.random()*Math.min(3, 7-start)); return { room, checkIn: addDays(t, start), checkOut: addDays(t, start+nights) }; };
    type Actor = { userId: string; plan: ReturnType<typeof pick>; behavior: 'pay'|'late'|'abandon'|'none'; bookingId?: string; expiresAt?: string; outcome: string; };
    const actors: Actor[] = customerIds.map(uid => ({ userId: uid, plan: pick(), behavior: 'none', outcome: '' }));
    const timings: Record<string, number[]> = { book: [], bookRedis: [], bookPg: [], bookRejected: [], pay: [], rebook: [], rebookPay: [], retryOtherRoom: [] };
    const counts: Record<string, number> = { bookAttempts: 0, locked: 0, soldOut: 0, retryOtherRoom: 0, retrySucceeded: 0, paid: 0, payFailed: 0, late: 0, timedOut: 0, rebookAttempts: 0, rebooked: 0, rebookSoldOut: 0, abandoned: 0, errors: 0 };
    const book = async (a: Actor, plan = a.plan, bucket = 'book') => {
      const range = parseRange(plan.checkIn, plan.checkOut); if ('error' in range) throw new Error(range.error);
      const r = await timed(() => createBooking(a.userId, plan.room, plan.checkIn, plan.checkOut, range.nights, 'PENDING', params.windowSeconds));
      timings[bucket].push(r.ms);
      if (r.ok) { a.bookingId = r.value.bookingId; a.expiresAt = r.value.expiresAt; if (bucket==='book') { timings.bookRedis.push(r.value.timings.redisMs); timings.bookPg.push(r.value.timings.pgMs); } return true; }
      if (r.error?.statusCode !== 409) { counts.errors++; app.log.error(r.error); } else if (bucket==='book') timings.bookRejected.push(r.ms);
      return false;
    };
    // 2. everyone books at once
    const burstStart = performance.now();
    const results = await Promise.all(actors.map(a => book(a)));
    const burstMs = performance.now() - burstStart;
    counts.bookAttempts = actors.length; counts.locked = results.filter(Boolean).length; counts.soldOut = actors.length - counts.locked;
    note(`burst: ${counts.locked} locked, ${counts.soldOut} sold out in ${burstMs.toFixed(0)}ms`);
    // 2b. sold-out customers try one other room
    const soldOut = actors.filter(a => !a.bookingId);
    await Promise.all(soldOut.map(async a => { counts.retryOtherRoom++; a.plan = pick(); if (await book(a, a.plan, 'retryOtherRoom')) counts.retrySucceeded++; else a.outcome = 'sold out twice'; }));
    // 3. behaviours
    const holders = actors.filter(a => a.bookingId); for (let i = holders.length-1; i > 0; i--) { const j = Math.floor(Math.random()*(i+1)); [holders[i], holders[j]] = [holders[j], holders[i]]; }
    const nPay = Math.round(holders.length * params.payRatio), nLate = Math.round(holders.length * params.lateRatio);
    holders.forEach((a, i) => a.behavior = i < nPay ? 'pay' : i < nPay + nLate ? 'late' : 'abandon');
    const payers = holders.filter(a => a.behavior==='pay');
    await Promise.all(payers.map(async a => { const r = await timed(() => payBooking(a.bookingId!, a.userId)); timings.pay.push(r.ms); if (r.ok) { counts.paid++; a.outcome = 'paid'; } else { counts.payFailed++; a.outcome = 'pay failed: ' + (r.error?.message||'error'); } }));
    note(`${counts.paid} paid immediately`);
    // 4. late + abandon: wait for the expiry worker
    const waiting = holders.filter(a => a.behavior !== 'pay'); counts.late = holders.filter(a => a.behavior==='late').length; counts.abandoned = holders.filter(a => a.behavior==='abandon').length;
    await save('RUNNING', { phase: `waiting ${params.windowSeconds}s for ${waiting.length} locks to expire`, counts, progressPct: 50 });
    const expiryLatency: number[] = []; const deadline = Date.now() + (params.windowSeconds + 15) * 1000;
    const pendingIds = new Set(waiting.map(a => a.bookingId!));
    while (pendingIds.size && Date.now() < deadline) {
      await sleep(500);
      const r = await pool.query(`SELECT id, status, expires_at FROM bookings WHERE id = ANY($1) AND status <> 'PENDING'`, [[...pendingIds]]);
      for (const row of r.rows) { pendingIds.delete(row.id); if (row.status==='PAYMENT_TIMEOUT') { counts.timedOut++; expiryLatency.push(Date.now() - new Date(row.expires_at).getTime()); } }
    }
    note(`${counts.timedOut} locks timed out (${pendingIds.size} still pending)`);
    // 5. late customers come back: rebook the same room/dates, then pay
    const late = holders.filter(a => a.behavior==='late');
    await Promise.all(late.map(async a => {
      counts.rebookAttempts++;
      if (await book(a, a.plan, 'rebook')) { counts.rebooked++; const r = await timed(() => payBooking(a.bookingId!, a.userId)); timings.rebookPay.push(r.ms); a.outcome = r.ok ? 'late, rebooked and paid' : 'late, rebooked, pay failed'; if (r.ok) counts.paid++; }
      else { counts.rebookSoldOut++; a.outcome = 'late, room gone on rebook'; }
    }));
    holders.filter(a => a.behavior==='abandon').forEach(a => a.outcome = 'abandoned (timed out)');
    note(`late customers: ${counts.rebooked} rebooked, ${counts.rebookSoldOut} lost the room`);
    const outbox = await simOutbox(started, note);
    // 7. bottleneck ranking: which step costs the most (p95)
    const ops: Record<string, any> = {
      'Redis lock (Lua reserve)': stats(timings.bookRedis), 'PostgreSQL booking write (tx + outbox rows)': stats(timings.bookPg),
      'Book request end-to-end': stats(timings.book), 'Rejected (sold out) request': stats(timings.bookRejected), 'Pay request': stats(timings.pay),
      'Retry on another room': stats(timings.retryOtherRoom), 'Rebook after timeout': stats(timings.rebook), 'Pay after rebook': stats(timings.rebookPay),
    };
    const expiry = stats(expiryLatency);
    const bottlenecks = [
      ...Object.entries(ops).filter(([,v]) => v.count).map(([name, v]) => ({ step: name, p95Ms: v.p95Ms, note: '' })),
      { step: 'Outbox -> Kafka publish lag (insert to publish)', p95Ms: outbox.p95Ms, note: `publisher polls every 1.5s, 50 rows per poll, one Kafka send per event; ${outbox.events} events drained in ${(outbox.drainMs/1000).toFixed(1)}s` },
      { step: 'Payment-timeout worker delay after expiry', p95Ms: expiry.p95Ms, note: 'worker polls every 2s (measured with 0.5s sampling)' },
    ].sort((a,b)=>b.p95Ms-a.p95Ms);
    const report = {
      params, mode: params.mode, customers: customerIds.length, durationMs: Date.now()-started, burstMs: +burstMs.toFixed(0),
      throughputPerSec: +(counts.bookAttempts / (burstMs/1000)).toFixed(1),
      counts, finalStatuses: await simFinalStatuses(customerIds),
      timings: ops, outbox, expiryWorker: expiry, bottlenecks,
      redisShareOfBookPct: timings.book.length ? Math.round(100 * timings.bookRedis.reduce((a,b)=>a+b,0) / timings.book.reduce((a,b)=>a+b,0)) : 0,
      pgShareOfBookPct: timings.book.length ? Math.round(100 * timings.bookPg.reduce((a,b)=>a+b,0) / timings.book.reduce((a,b)=>a+b,0)) : 0,
      outcomes: Object.entries(actors.reduce((m:Record<string,number>, a) => { const k = a.outcome || (a.bookingId ? 'holding lock' : 'no booking'); m[k]=(m[k]||0)+1; return m; }, {})).map(([outcome,count])=>({outcome,count})).sort((a:any,b:any)=>b.count-a.count),
      progressPct: 100,
    };
    await save('DONE', report); note('done');
  } catch (e: any) {
    app.log.error(e); await save('FAILED', { error: e?.message || String(e), progressPct: 100 });
  } finally { running.delete(runId); }
}

app.post('/api/admin/simulation', async (req:any, reply) => {
  await auth(req,['ADMIN']);
  if (running.size) return reply.code(409).send({ error: 'A simulation is already running' });
  const b = req.body || {};
  const params: SimParams = {
    mode: b.mode === 'same-room-fallback' ? 'same-room-fallback' : 'random-week',
    customers: Math.min(500, Math.max(1, Number(b.customers ?? 100))),
    payRatio: Math.min(1, Math.max(0, Number(b.payRatio ?? 0.5))),
    lateRatio: Math.min(1, Math.max(0, Number(b.lateRatio ?? 0.3))),
    windowSeconds: Math.min(60, Math.max(3, Number(b.windowSeconds ?? 5))),
    roomId: b.roomId || undefined,
    country: b.country || undefined, city: b.city || undefined,
    fallbackScope: ['country', 'city'].includes(b.fallbackScope) ? b.fallbackScope : 'any',
  };
  if (params.payRatio + params.lateRatio > 1) return reply.code(400).send({ error: 'payRatio + lateRatio must be <= 1' });
  if (params.mode === 'same-room-fallback') {
    if (!params.roomId) return reply.code(400).send({ error: 'roomId is required for the same-room simulation' });
    const r = await pool.query(`SELECT id FROM rooms WHERE id=$1`, [params.roomId]); if (!r.rows[0]) return reply.code(404).send({ error: 'Room not found' });
  }
  const runId = id();
  await pool.query(`INSERT INTO simulation_runs(id, status, params, report, created_by) VALUES($1,'RUNNING',$2,'{}',$3)`, [runId, JSON.stringify(params), req.userCtx!.id]);
  running.add(runId);
  (params.mode === 'same-room-fallback' ? runSameRoomSimulation : runSimulation)(runId, params); // fire and forget; poll GET /api/admin/simulation/:id
  return { runId, status: 'RUNNING', params };
});
app.get('/api/admin/simulation', async (req:any) => {
  await auth(req,['ADMIN']);
  const r = await pool.query(`SELECT id, status, params, created_at AS "createdAt", finished_at AS "finishedAt", cardinality(customer_ids) AS customers,
    (SELECT count(*)::int FROM bookings WHERE user_id = ANY(customer_ids) AND status IN ('CONFIRMED','PENDING')) AS "activeBookings"
    FROM simulation_runs ORDER BY created_at DESC LIMIT 10`);
  return r.rows;
});
app.get('/api/admin/simulation/:id', async (req:any, reply) => {
  await auth(req,['ADMIN']);
  const r = await pool.query(`SELECT id, status, params, report, created_at AS "createdAt", finished_at AS "finishedAt", cardinality(customer_ids) AS customers,
    (SELECT count(*)::int FROM bookings WHERE user_id = ANY(customer_ids) AND status IN ('CONFIRMED','PENDING')) AS "activeBookings"
    FROM simulation_runs WHERE id=$1`, [req.params.id]);
  if (!r.rows[0]) return reply.code(404).send({ error: 'Run not found' });
  return r.rows[0];
});
// Cancels every active booking made by this run's customers (admin force: stay-started rule does not apply).
app.post('/api/admin/simulation/:id/cancel-all', async (req:any, reply) => {
  await auth(req,['ADMIN']);
  const run = await pool.query(`SELECT customer_ids FROM simulation_runs WHERE id=$1`, [req.params.id]);
  if (!run.rows[0]) return reply.code(404).send({ error: 'Run not found' });
  const ids = (await pool.query(`SELECT id FROM bookings WHERE user_id = ANY($1) AND status IN ('CONFIRMED','PENDING')`, [run.rows[0].customer_ids || []])).rows.map((r:any)=>r.id);
  let cancelled = 0, failed = 0; const t0 = performance.now();
  for (const bid of ids) { try { await cancelBooking(bid, { force: true }); cancelled++; } catch (e:any) { failed++; if (!e?.statusCode) app.log.error(e); } }
  await pool.query(`INSERT INTO audit_logs(id,actor_user_id,action,metadata) VALUES($1,$2,'SIMULATION_CANCEL_ALL',$3)`, [id(), req.userCtx!.id, JSON.stringify({ runId: req.params.id, cancelled, failed })]);
  return { ok: true, cancelled, failed, durationMs: Math.round(performance.now()-t0), message: `Cancelled ${cancelled} booking(s) of this run's customers${failed?`, ${failed} failed`:''}` };
});

// Concurrency demo: every customer tries to book the same room for the same dates at the same instant.
// The Redis Lua reservation decides who gets a room; the rest get "No rooms available".
app.post('/api/admin/concurrent-booking', async (req:any, reply) => {
  await auth(req,['ADMIN']);
  const { roomId, checkIn, checkOut, confirm } = req.body || {};
  const range = parseRange(checkIn, checkOut);
  if ('error' in range) return reply.code(400).send({ error: range.error });
  const room = await pool.query(`SELECT r.*, h.name AS hotel_name FROM rooms r JOIN hotels h ON h.id=r.hotel_id WHERE r.id=$1`, [roomId]);
  if (!room.rows[0]) return reply.code(404).send({ error: 'Room not found' });
  const r = room.rows[0];
  const customers = await pool.query(`SELECT id,name,email FROM users WHERE role='CUSTOMER' ORDER BY created_at`);
  if (!customers.rows.length) return reply.code(409).send({ error: 'No customers exist yet' });
  const before = await availabilityFor(r.id, range.nights);
  const startedAt = Date.now();
  const settled = await Promise.allSettled(customers.rows.map((c:any) =>
    createBooking(c.id, r, checkIn, checkOut, range.nights, confirm ? 'CONFIRMED' : 'PENDING')));
  const durationMs = Date.now() - startedAt;
  const results = settled.map((x, i) => {
    const c = customers.rows[i];
    if (x.status === 'fulfilled') return { customer: c.name, email: c.email, ok: true, bookingId: x.value.bookingId, status: x.value.status, remaining: x.value.remaining, expiresAt: x.value.expiresAt };
    const e: any = x.reason;
    if (e?.statusCode !== 409) app.log.error(e);
    return { customer: c.name, email: c.email, ok: false, error: e?.statusCode === 409 ? e.message : 'Internal error' };
  });
  const after = await availabilityFor(r.id, range.nights);
  const won = results.filter(x => x.ok).length;
  await pool.query(`INSERT INTO audit_logs(id,actor_user_id,action,metadata) VALUES($1,$2,'CONCURRENT_BOOKING_DEMO',$3)`,
    [id(), req.userCtx!.id, JSON.stringify({ roomId, checkIn, checkOut, customers: customers.rows.length, won, before, after, confirm: !!confirm })]);
  return {
    ok: true,
    message: `${customers.rows.length} customers booked "${r.hotel_name} / ${r.name}" at once: ${won} got a room, ${results.length - won} were rejected. Availability ${before} -> ${after}.`,
    hotel: r.hotel_name, room: r.name, checkIn, checkOut, availabilityBefore: before, availabilityAfter: after, durationMs, results,
  };
});

app.delete('/api/admin/sample-data', async (req:any) => {
  await auth(req,['ADMIN']);
  await pool.query(`DELETE FROM bookings WHERE user_id IN (SELECT id FROM users WHERE is_sample=true) OR room_id IN (SELECT id FROM rooms WHERE is_sample=true)`);
  await pool.query(`DELETE FROM hotel_photos WHERE hotel_id IN (SELECT id FROM hotels WHERE is_sample=true)`);
  const rooms=await pool.query(`SELECT id FROM rooms WHERE is_sample=true`);
  for(const r of rooms.rows) await deleteRoomAvailability(r.id);
  await pool.query(`DELETE FROM rooms WHERE is_sample=true`);
  await pool.query(`DELETE FROM hotels WHERE is_sample=true`);
  await pool.query(`DELETE FROM users WHERE is_sample=true`);
  await pool.query(`DELETE FROM simulation_runs`);
  return {ok:true,message:'Sample data deleted (including simulation customers and runs)'};
});

async function outboxLoop() {
  while(true) {
    try {
      const r=await pool.query(`SELECT * FROM outbox_events WHERE published_at IS NULL ORDER BY id LIMIT 50`);
      for(const e of r.rows) {
        await producer.send({topic:e.topic,messages:[{key:e.event_key,value:JSON.stringify(e.payload)}]});
        await pool.query(`UPDATE outbox_events SET published_at=NOW() WHERE id=$1`,[e.id]);
      }
    } catch(err) { app.log.error(err); }
    await new Promise(r=>setTimeout(r,1500));
  }
}

// Releases rooms whose payment window passed: PENDING -> PAYMENT_TIMEOUT, nights back to Redis, events out.
async function expirePendingBookings() {
  const client = await pool.connect();
  try {
    await client.query('BEGIN');
    const r = await client.query(`
      WITH due AS (
        SELECT id FROM bookings WHERE status='PENDING' AND expires_at <= now() FOR UPDATE SKIP LOCKED LIMIT 100)
      UPDATE bookings b SET status='PAYMENT_TIMEOUT' FROM due, rooms r
      WHERE b.id=due.id AND r.id=b.room_id
      RETURNING b.id, b.user_id, b.hotel_id, b.room_id, r.total_rooms,
        to_char(b.check_in,'YYYY-MM-DD') AS ci, to_char(b.check_out,'YYYY-MM-DD') AS co`);
    for (const row of r.rows) {
      const remaining = await releaseNights(row.room_id, row.total_rooms, row.ci, row.co);
      await addOutbox(client,'booking.payment_timeout',row.id,{bookingId:row.id,userId:row.user_id,hotelId:row.hotel_id,roomId:row.room_id,checkIn:row.ci,checkOut:row.co,remaining});
      await addOutbox(client,'room.availability.changed',row.room_id,{roomId:row.room_id,checkIn:row.ci,checkOut:row.co,remaining});
      app.log.info({ bookingId: row.id, remaining }, 'booking payment timed out, room released');
    }
    await client.query('COMMIT');
  } catch (e) { await client.query('ROLLBACK'); throw e; } finally { client.release(); }
}
async function expiryLoop() {
  while (true) {
    try { await expirePendingBookings(); } catch (err) { app.log.error(err); }
    await new Promise(r => setTimeout(r, 2000));
  }
}

// Redis is a cache of PostgreSQL truth: write every room's availability for every night of the
// booking horizon (today .. today+AVAILABILITY_DAYS-1). Bookings fail if a night is missing, so all must be loaded.
// Run at startup (Redis may have restarted empty) and on demand from the admin panel.
async function rebuildAvailability() {
  const t0 = performance.now();
  const first = todayStr(), last = addDays(first, AVAILABILITY_DAYS - 1);
  const roomIds = new Set<string>();
  let roomNights = 0, oversold = 0, after = '00000000-0000-0000-0000-000000000000';
  // 200 rooms (73k room-nights) per query, so a large catalogue (Redis capacity test) never has to fit in memory at once.
  while (true) {
    const r = await pool.query(`
      WITH rs AS (SELECT id, total_rooms FROM rooms WHERE id > $3 ORDER BY id LIMIT 200)
      SELECT rs.id AS room_id, rs.total_rooms, to_char(d.night,'YYYY-MM-DD') AS night, count(b.id)::int AS booked
      FROM rs
      CROSS JOIN generate_series($1::date, $2::date, '1 day') AS d(night)
      LEFT JOIN bookings b ON b.room_id=rs.id AND b.status IN ('CONFIRMED','PENDING') AND b.check_in <= d.night AND b.check_out > d.night
      GROUP BY rs.id, rs.total_rooms, d.night`, [first, last, after]);
    if (!r.rows.length) break;
    const pairs: string[] = [];
    for (const row of r.rows) {
      roomIds.add(row.room_id);
      if (row.room_id > after) after = row.room_id;
      if (row.booked > row.total_rooms) oversold++;
      pairs.push(nightKey(row.room_id, row.night), String(Math.max(0, row.total_rooms - row.booked)));
    }
    // Plain SETs (no deletes) so a concurrent booking never sees one of its nights missing mid-rebuild.
    await msetAll(pairs);
    roomNights += r.rows.length;
  }
  // Stale keys: past nights and deleted rooms.
  let staleKeysDropped = 0;
  for await (const keys of redis.scanStream({ match: 'availability:*', count: 1000 }) as AsyncIterable<string[]>) {
    const stale = keys.filter(k => { const night = k.slice(-10); return night < first || night > last || !roomIds.has(k.slice('availability:'.length, -11)); });
    if (stale.length) { await redis.unlink(...stale); staleKeysDropped += stale.length; }
  }
  const summary = { roomNights, staleKeysDropped, oversoldRoomNights: oversold, ms: Math.round(performance.now() - t0) };
  app.log.info(summary, 'availability rebuilt from PostgreSQL');
  return summary;
}
app.post('/api/admin/rebuild-availability', async (req:any) => {
  await auth(req,['ADMIN']);
  const summary = await rebuildAvailability();
  return { ok: true, ...summary, message: `Redis availability rebuilt: ${summary.roomNights} room-nights set, ${summary.staleKeysDropped} stale keys dropped${summary.oversoldRoomNights?`, ${summary.oversoldRoomNights} oversold`:''}` };
});

// RAM used by the whole Redis instance (all keys, not just availability:*), from INFO memory.
async function redisMemory() {
  const info = Object.fromEntries((await redis.info('memory')).split('\r\n').filter(l => l.includes(':')).map(l => l.split(':') as [string, string]));
  return { usedBytes: Number(info.used_memory), peakBytes: Number(info.used_memory_peak), datasetBytes: Number(info.used_memory_dataset), maxBytes: Number(info.maxmemory) || 0 };
}

/** How many of these keys exist, via pipelined multi-key EXISTS. */
async function countExisting(keys: string[]) {
  const pipe = redis.pipeline();
  for (let i = 0; i < keys.length; i += 1000) pipe.exists(...keys.slice(i, i + 1000));
  return (await execOrThrow(pipe)).reduce((n, [, v]) => n + Number(v), 0);
}

// Availability grid (every room x every night of the window) with the value Redis holds for each cell. Filter by hotel
// and/or night; paged. Built from PostgreSQL rooms + computed keys instead of scanning Redis, so it stays fast with millions of keys.
app.get('/api/admin/redis-records', async (req:any) => {
  await auth(req,['ADMIN']);
  const { hotelId, night } = req.query;
  const page = Math.max(1, Number(req.query.page) || 1), limit = Math.min(500, Math.max(1, Number(req.query.limit) || 50));
  const first = todayStr(), last = addDays(first, AVAILABILITY_DAYS - 1);
  const allRooms = (await pool.query(`SELECT r.id, r.name, r.total_rooms, h.id AS hotel_id, h.name AS hotel_name FROM rooms r JOIN hotels h ON h.id=r.hotel_id ORDER BY h.name, r.name, r.id`)).rows;
  // Window edges: every room should have today and the last night, and nothing for yesterday or the night after the window.
  const ids = allRooms.map((r:any) => r.id);
  const [hasFirst, hasLast, beforeFirst, afterLast] = await Promise.all(
    [first, last, addDays(first, -1), addDays(last, 1)].map(n => countExisting(ids.map((i:string) => nightKey(i, n)))));
  const totalKeys = await redis.dbsize(); // every key in this Redis is an availability key
  const expectedKeys = allRooms.length * AVAILABILITY_DAYS;
  const problems = [
    totalKeys !== expectedKeys && `${(totalKeys - expectedKeys).toLocaleString('en')} keys vs expected`,
    hasFirst < ids.length && `${ids.length - hasFirst} rooms missing today`,
    hasLast < ids.length && `${ids.length - hasLast} rooms missing the last night`,
    beforeFirst && `${beforeFirst} keys for yesterday`,
    afterLast && `${afterLast} keys past the window`,
  ].filter(Boolean);
  const summary = {
    totalKeys, rooms: allRooms.length, windowDays: AVAILABILITY_DAYS, expectedKeys,
    firstNight: beforeFirst ? addDays(first, -1) : hasFirst ? first : null, lastNight: afterLast ? addDays(last, 1) : hasLast ? last : null,
    expectedFirstNight: first, expectedLastNight: last,
    windowOk: problems.length === 0, windowHint: problems.length ? problems.join(' · ') : 'every room has every night',
    memory: await redisMemory(),
  };
  const rooms = hotelId ? allRooms.filter((r:any) => r.hotel_id === hotelId) : allRooms;
  const nights = night ? [String(night)] : Array.from({ length: AVAILABILITY_DAYS }, (_, i) => addDays(first, i));
  const total = rooms.length * nights.length;
  // Row i = night i / rooms, room i % rooms (sorted by night, then hotel, then room).
  const slice = [];
  for (let i = (page - 1) * limit; i < Math.min(total, page * limit); i++) {
    const room = rooms[i % rooms.length], n = nights[Math.floor(i / rooms.length)];
    slice.push({ key: nightKey(room.id, n), night: n, room });
  }
  const vals = slice.length ? await redis.mget(slice.map(x => x.key)) : [];
  return { summary, page, limit, total,
    items: slice.map((x, i) => ({ key: x.key, night: x.night, available: vals[i] === null ? null : Number(vals[i]),
      roomId: x.room.id, roomName: x.room.name, hotelName: x.room.hotel_name, totalRooms: x.room.total_rooms })) };
});

// ---- Redis capacity test ---------------------------------------------------
// Creates sellers x hotels-per-seller hotels (3 room types each) in PostgreSQL and loads every room-night into Redis,
// batch by batch, until done, stopped, or Redis hits maxmemory (it then rejects writes with an OOM error).
// Rows are flagged is_load_test so they can be removed without touching sample or real data. One job at a time.
const LOAD_BATCH_HOTELS = 250; // 750 rooms, ~274k Redis keys per batch
const LOAD_LOCATIONS: [string, string][] = [
  ['Thailand', 'Bangkok'], ['Thailand', 'Chiang Mai'], ['Thailand', 'Phuket'], ['Thailand', 'Pattaya'], ['Thailand', 'Krabi'],
  ['Japan', 'Tokyo'], ['Japan', 'Osaka'], ['Japan', 'Kyoto'],
  ['Vietnam', 'Hanoi'], ['Vietnam', 'Ho Chi Minh City'], ['Vietnam', 'Da Nang'],
  ['Italy', 'Rome'], ['Italy', 'Milan'], ['Italy', 'Venice'],
  ['France', 'Paris'], ['France', 'Nice'], ['France', 'Lyon'],
  ['Israel', 'Tel Aviv'], ['Israel', 'Jerusalem'], ['Israel', 'Eilat'],
];
type LoadJob = {
  kind: 'generate' | 'remove'; status: 'RUNNING' | 'DONE' | 'REDIS_FULL' | 'STOPPED' | 'FAILED';
  sellers: number; hotelsPerSeller: number;
  planned: { sellers: number; hotels: number; rooms: number; keys: number };
  done: { sellers: number; hotels: number; rooms: number; keys: number };
  startedAt: number; finishedAt?: number; message: string; stop?: boolean;
};
let loadJob: LoadJob | null = null;

async function loadTestTotals() {
  const r = await pool.query(`SELECT
    (SELECT count(*)::int FROM users WHERE is_load_test) AS sellers,
    (SELECT count(*)::int FROM hotels WHERE is_load_test) AS hotels,
    (SELECT count(*)::int FROM rooms WHERE is_load_test) AS rooms,
    pg_database_size(current_database())::bigint AS "postgresBytes"`);
  return { ...r.rows[0], postgresBytes: Number(r.rows[0].postgresBytes) };
}

async function runLoadGenerate(job: LoadJob) {
  const tag = id().slice(0, 6); // keeps emails unique across runs
  const first = todayStr();
  const nights = Array.from({ length: AVAILABILITY_DAYS }, (_, i) => addDays(first, i));
  let seller = 0, hotelOfSeller = 0, sellerId = '';
  while (seller < job.sellers && !job.stop) {
    // Next batch of hotels, creating sellers as they come up.
    const users: any[] = [], hotels: any[] = [], rooms: any[] = [];
    while (hotels.length < LOAD_BATCH_HOTELS && seller < job.sellers) {
      if (hotelOfSeller === 0) { sellerId = id(); users.push({ id: sellerId, n: seller + 1 }); }
      const hid = id(), n = job.done.hotels + hotels.length + 1;
      hotels.push({ id: hid, seller: sellerId, name: `Load Hotel ${seller + 1}-${hotelOfSeller + 1}`, country: LOAD_LOCATIONS[n % LOAD_LOCATIONS.length][0], city: LOAD_LOCATIONS[n % LOAD_LOCATIONS.length][1], n });
      for (let j = 1; j <= 3; j++) rooms.push({ id: id(), hotel: hid, name: `Room Type ${j}`, price: 900 + (n % 40) * 50 + j * 100, total: 2 + j });
      if (++hotelOfSeller === job.hotelsPerSeller) { hotelOfSeller = 0; seller++; }
    }
    const client = await pool.connect();
    try {
      await client.query('BEGIN');
      if (users.length) await client.query(
        `INSERT INTO users(id,email,password_hash,name,role,is_load_test)
         SELECT u, 'load-' || $2 || '-' || n || '@load.test', 'load123', 'Load Seller ' || n, 'SELLER', true FROM unnest($1::uuid[], $3::int[]) AS t(u, n)`,
        [users.map(u => u.id), tag, users.map(u => u.n)]);
      await client.query(
        `INSERT INTO hotels(id,seller_id,name,address,city,country,description,is_load_test,created_at)
         SELECT h, s, name, n || ' Load Test Road', city, country, 'Generated by the Redis capacity test.', true, clock_timestamp() + n * interval '1 microsecond'
         FROM unnest($1::uuid[], $2::uuid[], $3::text[], $4::text[], $5::int[], $6::text[]) AS t(h, s, name, city, n, country)`,
        [hotels.map(h => h.id), hotels.map(h => h.seller), hotels.map(h => h.name), hotels.map(h => h.city), hotels.map(h => h.n), hotels.map(h => h.country)]);
      await client.query(
        `INSERT INTO hotel_photos(id,hotel_id,url) SELECT gen_random_uuid(), h, 'https://picsum.photos/seed/load' || n || '/900/600' FROM unnest($1::uuid[], $2::int[]) AS t(h, n)`,
        [hotels.map(h => h.id), hotels.map(h => h.n % 500)]);
      await client.query(
        `INSERT INTO rooms(id,hotel_id,name,price,total_rooms,is_load_test) SELECT * , true FROM unnest($1::uuid[], $2::uuid[], $3::text[], $4::numeric[], $5::int[])`,
        [rooms.map(r => r.id), rooms.map(r => r.hotel), rooms.map(r => r.name), rooms.map(r => r.price), rooms.map(r => r.total)]);
      await client.query('COMMIT');
    } catch (e) { await client.query('ROLLBACK'); throw e; } finally { client.release(); }
    const pairs: string[] = [];
    for (const r of rooms) for (const n of nights) pairs.push(nightKey(r.id, n), String(r.total));
    try {
      await msetAll(pairs);
    } catch (e) {
      if (!isRedisOom(e)) throw e;
      // Redis is full: undo this half-loaded batch so every remaining room is fully bookable.
      const keys = pairs.filter((_, i) => i % 2 === 0);
      for (let i = 0; i < keys.length; i += 5000) await redis.unlink(...keys.slice(i, i + 5000));
      await pool.query(`DELETE FROM hotels WHERE id = ANY($1)`, [hotels.map(h => h.id)]); // rooms + photos cascade
      await pool.query(`DELETE FROM users u WHERE u.id = ANY($1) AND NOT EXISTS (SELECT 1 FROM hotels h WHERE h.seller_id=u.id)`, [users.map(u => u.id)]);
      job.status = 'REDIS_FULL';
      job.message = `Redis is full (maxmemory reached) after ${job.done.keys.toLocaleString('en')} keys from this run. The last, partly loaded batch was rolled back.`;
      return;
    }
    job.done.sellers += users.length; job.done.hotels += hotels.length; job.done.rooms += rooms.length; job.done.keys += pairs.length / 2;
  }
  job.status = job.stop ? 'STOPPED' : 'DONE';
  job.message = `${job.stop ? 'Stopped' : 'Done'}: ${job.done.hotels.toLocaleString('en')} hotels, ${job.done.keys.toLocaleString('en')} Redis keys added.`;
}

async function runLoadRemove(job: LoadJob) {
  // Batches of rooms: delete their Redis keys (computed, no KEYS/SCAN), then the rows.
  while (!job.stop) {
    const rooms = (await pool.query(`SELECT id FROM rooms WHERE is_load_test LIMIT 1000`)).rows.map((r:any) => r.id as string);
    if (!rooms.length) break;
    const keys = rooms.flatMap(roomKeys);
    for (let i = 0; i < keys.length; i += 5000) await redis.unlink(...keys.slice(i, i + 5000));
    await pool.query(`DELETE FROM bookings WHERE room_id = ANY($1)`, [rooms]);
    await pool.query(`DELETE FROM rooms WHERE id = ANY($1)`, [rooms]);
    job.done.rooms += rooms.length; job.done.keys += rooms.length * AVAILABILITY_DAYS;
  }
  if (!job.stop) {
    await pool.query(`DELETE FROM bookings WHERE hotel_id IN (SELECT id FROM hotels WHERE is_load_test)`);
    job.done.hotels = (await pool.query(`DELETE FROM hotels WHERE is_load_test`)).rowCount || 0; // photos cascade
    job.done.sellers = (await pool.query(`DELETE FROM users WHERE is_load_test AND NOT EXISTS (SELECT 1 FROM hotels h WHERE h.seller_id=users.id)`)).rowCount || 0;
  }
  job.status = job.stop ? 'STOPPED' : 'DONE';
  job.message = `${job.stop ? 'Stopped' : 'Removed'}: ${job.done.rooms.toLocaleString('en')} rooms and their Redis keys.`;
}

function startLoadJob(job: LoadJob, run: (j: LoadJob) => Promise<void>) {
  loadJob = job;
  run(job).catch(e => { job.status = 'FAILED'; job.message = String(e?.message || e); app.log.error(e, 'load test failed'); })
    .finally(() => { job.finishedAt = Date.now(); });
}

app.get('/api/admin/load-test', async (req:any) => {
  await auth(req,['ADMIN']);
  const { stop, ...job } = loadJob || ({} as any);
  return { job: loadJob ? { ...job, elapsedMs: (loadJob.finishedAt || Date.now()) - loadJob.startedAt } : null,
    totals: await loadTestTotals(), redis: { keys: await redis.dbsize(), memory: await redisMemory() } };
});
app.post('/api/admin/load-test', async (req:any, reply) => {
  await auth(req,['ADMIN']);
  if (loadJob?.status === 'RUNNING') return reply.code(409).send({ error: 'A capacity test job is already running' });
  const sellers = Number(req.body?.sellers), hotelsPerSeller = Number(req.body?.hotelsPerSeller);
  if (![1000, 10000, 100000, 1000000].includes(sellers) || ![1, 10, 100].includes(hotelsPerSeller))
    return reply.code(400).send({ error: 'sellers must be 1000/10000/100000/1000000 and hotelsPerSeller 1/10/100' });
  const hotels = sellers * hotelsPerSeller;
  startLoadJob({ kind: 'generate', status: 'RUNNING', sellers, hotelsPerSeller, startedAt: Date.now(), message: 'Generating…',
    planned: { sellers, hotels, rooms: hotels * 3, keys: hotels * 3 * AVAILABILITY_DAYS }, done: { sellers: 0, hotels: 0, rooms: 0, keys: 0 } }, runLoadGenerate);
  return { ok: true };
});
app.post('/api/admin/load-test/stop', async (req:any) => {
  await auth(req,['ADMIN']);
  if (loadJob?.status === 'RUNNING') loadJob.stop = true;
  return { ok: true };
});
app.delete('/api/admin/load-test', async (req:any, reply) => {
  await auth(req,['ADMIN']);
  if (loadJob?.status === 'RUNNING') return reply.code(409).send({ error: 'Stop the running job first' });
  const t = await loadTestTotals();
  startLoadJob({ kind: 'remove', status: 'RUNNING', sellers: 0, hotelsPerSeller: 0, startedAt: Date.now(), message: 'Removing…',
    planned: { sellers: t.sellers, hotels: t.hotels, rooms: t.rooms, keys: t.rooms * AVAILABILITY_DAYS }, done: { sellers: 0, hotels: 0, rooms: 0, keys: 0 } }, runLoadRemove);
  return { ok: true };
});

// Runs of the availability worker (apps/availability-worker): nights added to / removed from Redis. Newest first.
app.get('/api/admin/availability-log', async (req:any) => {
  await auth(req,['ADMIN']);
  const limit = Math.min(500, Math.max(1, Number(req.query.limit) || 100));
  try {
    const r = await pool.query(`
      SELECT id, ran_at AS "ranAt", trigger, status, to_char(window_first,'YYYY-MM-DD') AS "windowFirst", to_char(window_last,'YYYY-MM-DD') AS "windowLast",
        added_keys AS "addedKeys", removed_keys AS "removedKeys", added_nights AS "addedNights", removed_nights AS "removedNights",
        duration_ms AS "durationMs", error
      FROM availability_window_log ORDER BY ran_at DESC, id DESC LIMIT $1`, [limit]);
    return r.rows;
  } catch (e: any) {
    if (e?.code === '42P01') return []; // table not created yet: the worker has never run
    throw e;
  }
});

// Idempotent schema upgrades for databases created before date-range bookings existed.
// Layered pricing schema: country defaults, hotel weekday %, and price_rules scoped to a country or a hotel.
async function migratePricing() {
  await pool.query(`CREATE TABLE IF NOT EXISTS country_pricing (country TEXT PRIMARY KEY, weekday_pct NUMERIC(6,2)[] NOT NULL DEFAULT '{0,0,0,0,0,0,0}')`);
  await pool.query(`ALTER TABLE hotels ADD COLUMN IF NOT EXISTS weekday_pct NUMERIC(6,2)[]`);
  await pool.query(`CREATE TABLE IF NOT EXISTS price_rules (
    id UUID PRIMARY KEY, hotel_id UUID REFERENCES hotels(id) ON DELETE CASCADE, country TEXT, room_id UUID REFERENCES rooms(id) ON DELETE CASCADE,
    kind TEXT NOT NULL CHECK (kind IN ('SEASON','HOLIDAY','DISCOUNT')), name TEXT NOT NULL, start_date DATE NOT NULL, end_date DATE NOT NULL,
    adjust_type TEXT NOT NULL CHECK (adjust_type IN ('PERCENT','FIXED')), adjust_value NUMERIC(12,2) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(), CHECK (end_date >= start_date),
    CONSTRAINT price_rules_scope_check CHECK ((country IS NULL) <> (hotel_id IS NULL) AND (room_id IS NULL OR hotel_id IS NOT NULL)))`);
  // Databases from the first pricing version: WEEKEND rules become hotel weekday %, sample weekend/season rules
  // give way to the country defaults, and rules may now belong to a country.
  const old = (await pool.query(`SELECT 1 FROM information_schema.columns WHERE table_name='price_rules' AND column_name='weekdays'`)).rows.length;
  if (old) {
    const weekend = (await pool.query(`SELECT pr.hotel_id, pr.weekdays, pr.adjust_value::float AS v FROM price_rules pr JOIN hotels h ON h.id=pr.hotel_id
      WHERE pr.kind='WEEKEND' AND pr.adjust_type='PERCENT' AND NOT h.is_sample ORDER BY pr.created_at`)).rows;
    for (const w of weekend) await pool.query(`UPDATE hotels SET weekday_pct = (SELECT array_agg(CASE WHEN i-1 = ANY($2::int[]) THEN $3::numeric
      ELSE (COALESCE(weekday_pct, array_fill(NULL::numeric, ARRAY[7])))[i] END ORDER BY i) FROM generate_series(1,7) i) WHERE id=$1`, [w.hotel_id, w.weekdays, w.v]);
    await pool.query(`DELETE FROM price_rules WHERE kind='WEEKEND'`);
    await pool.query(`DELETE FROM price_rules pr USING hotels h WHERE pr.hotel_id=h.id AND h.is_sample AND pr.kind='SEASON' AND pr.name='High season'`);
    await pool.query(`ALTER TABLE price_rules DROP COLUMN weekdays, ADD COLUMN IF NOT EXISTS country TEXT, ALTER COLUMN hotel_id DROP NOT NULL`);
    await pool.query(`ALTER TABLE price_rules DROP CONSTRAINT IF EXISTS price_rules_kind_check`);
    await pool.query(`ALTER TABLE price_rules ADD CONSTRAINT price_rules_kind_check CHECK (kind IN ('SEASON','HOLIDAY','DISCOUNT'))`);
    await pool.query(`ALTER TABLE price_rules ADD CONSTRAINT price_rules_scope_check CHECK ((country IS NULL) <> (hotel_id IS NULL) AND (room_id IS NULL OR hotel_id IS NOT NULL))`);
    app.log.info({ convertedWeekendRules: weekend.length }, 'pricing migrated to country + hotel layers');
  }
  await pool.query(`CREATE INDEX IF NOT EXISTS price_rules_hotel_idx ON price_rules(hotel_id)`);
  await pool.query(`CREATE INDEX IF NOT EXISTS price_rules_country_idx ON price_rules(country) WHERE country IS NOT NULL`);
  // Country defaults (Sunday..Saturday %), only where the admin has not set any yet.
  const defaults: [string, number[]][] = [
    ['Israel', [0, -15, 0, 0, 30, 40, 10]], ['Thailand', [0, 0, 0, -10, 0, 20, 30]], ['Japan', [0, 0, 0, 0, 0, 15, 25]],
    ['Vietnam', [0, 0, 0, 0, 0, 10, 20]], ['Italy', [0, 0, 0, 0, 0, 15, 20]], ['France', [0, 0, 0, 0, 0, 10, 20]]];
  for (const [c, pct] of defaults) await pool.query(`INSERT INTO country_pricing(country, weekday_pct) VALUES($1,$2) ON CONFLICT DO NOTHING`, [c, pct]);
  // Country seasons, only for countries without any country rule yet: Thai high season, Israeli summer.
  const t = todayStr(), y = Number(t.slice(0, 4));
  const winter = t > `${y}-01-15` ? y : y - 1, summer = t > `${y}-08-31` ? y + 1 : y;
  const seasons: [string, string, string, string, number][] = [
    ['Thailand', 'High season', `${winter}-12-15`, `${winter + 1}-01-15`, 40], ['Israel', 'Summer', `${summer}-07-01`, `${summer}-08-31`, 25]];
  for (const [c, name, start, end, v] of seasons) await pool.query(`INSERT INTO price_rules(id,country,kind,name,start_date,end_date,adjust_type,adjust_value)
    SELECT $1,$2,'SEASON',$3,$4,$5,'PERCENT',$6 WHERE NOT EXISTS (SELECT 1 FROM price_rules WHERE country=$2)`, [id(), c, name, start, end, v]);
}

async function migrate() {
  await pool.query(`ALTER TABLE bookings ADD COLUMN IF NOT EXISTS check_in DATE`);
  await pool.query(`ALTER TABLE bookings ADD COLUMN IF NOT EXISTS check_out DATE`);
  await pool.query(`UPDATE bookings SET check_in=created_at::date, check_out=created_at::date+1 WHERE check_in IS NULL`);
  await pool.query(`ALTER TABLE bookings ALTER COLUMN check_in SET NOT NULL, ALTER COLUMN check_out SET NOT NULL`);
  await pool.query(`CREATE INDEX IF NOT EXISTS bookings_room_dates_idx ON bookings(room_id, check_in, check_out)`);
  await pool.query(`ALTER TABLE bookings ADD COLUMN IF NOT EXISTS expires_at TIMESTAMPTZ`);
  await pool.query(`ALTER TABLE bookings ADD COLUMN IF NOT EXISTS paid_at TIMESTAMPTZ`);
  await pool.query(`ALTER TABLE bookings DROP CONSTRAINT IF EXISTS bookings_status_check`);
  await pool.query(`ALTER TABLE bookings ADD CONSTRAINT bookings_status_check CHECK (status IN ('PENDING','CONFIRMED','CANCELLED','EXPIRED','PAYMENT_TIMEOUT'))`);
  await pool.query(`CREATE INDEX IF NOT EXISTS bookings_pending_expiry_idx ON bookings(expires_at) WHERE status='PENDING'`);
  await pool.query(`ALTER TABLE outbox_events ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ NOT NULL DEFAULT now()`);
  await pool.query(`CREATE TABLE IF NOT EXISTS simulation_runs (
    id UUID PRIMARY KEY, status TEXT NOT NULL, params JSONB NOT NULL DEFAULT '{}'::jsonb, report JSONB NOT NULL DEFAULT '{}'::jsonb,
    customer_ids UUID[] NOT NULL DEFAULT '{}', created_by UUID, created_at TIMESTAMPTZ NOT NULL DEFAULT now(), finished_at TIMESTAMPTZ)`);
  for (const t of ['users', 'hotels', 'rooms']) await pool.query(`ALTER TABLE ${t} ADD COLUMN IF NOT EXISTS is_load_test BOOLEAN NOT NULL DEFAULT false`);
  await pool.query(`ALTER TABLE bookings ADD COLUMN IF NOT EXISTS price_breakdown JSONB`);
  // All hotels created before countries existed are in Thai cities.
  await pool.query(`ALTER TABLE hotels ADD COLUMN IF NOT EXISTS country TEXT NOT NULL DEFAULT 'Thailand'`);
  await migratePricing();
  await pool.query(`CREATE INDEX IF NOT EXISTS hotels_location_idx ON hotels(country, city)`);
  await pool.query(`CREATE INDEX IF NOT EXISTS hotels_created_idx ON hotels(created_at DESC, id)`);
  await pool.query(`CREATE INDEX IF NOT EXISTS hotel_photos_hotel_idx ON hotel_photos(hotel_id)`);
  await pool.query(`CREATE INDEX IF NOT EXISTS hotels_seller_idx ON hotels(seller_id)`);
  await pool.query(`CREATE INDEX IF NOT EXISTS rooms_load_test_idx ON rooms(id) WHERE is_load_test`);
  // Any run left RUNNING by a restart can never finish.
  await pool.query(`UPDATE simulation_runs SET status='FAILED', report = report || '{"error":"API restarted while running"}' WHERE status='RUNNING'`);
}

async function start() {
  await migrate();
  // A full Redis (maxmemory reached, e.g. by the capacity test) rejects writes; start anyway so the admin can clean up.
  try { await rebuildAvailability(); } catch (err) { app.log.error(err, 'startup availability rebuild failed'); }
  await producer.connect();
  await app.listen({port:Number(process.env.PORT||3010),host:'0.0.0.0'});
  outboxLoop();
  expiryLoop();
}
start();
