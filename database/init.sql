CREATE TABLE IF NOT EXISTS users (
  id UUID PRIMARY KEY,
  email TEXT UNIQUE NOT NULL,
  password_hash TEXT NOT NULL,
  name TEXT NOT NULL,
  role TEXT NOT NULL CHECK (role IN ('CUSTOMER','SELLER','ADMIN')),
  is_sample BOOLEAN NOT NULL DEFAULT FALSE,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS hotels (
  id UUID PRIMARY KEY,
  seller_id UUID NOT NULL REFERENCES users(id),
  name TEXT NOT NULL,
  address TEXT NOT NULL,
  city TEXT NOT NULL,
  description TEXT NOT NULL DEFAULT '',
  is_sample BOOLEAN NOT NULL DEFAULT FALSE,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS hotel_photos (
  id UUID PRIMARY KEY,
  hotel_id UUID NOT NULL REFERENCES hotels(id) ON DELETE CASCADE,
  url TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS rooms (
  id UUID PRIMARY KEY,
  hotel_id UUID NOT NULL REFERENCES hotels(id) ON DELETE CASCADE,
  name TEXT NOT NULL,
  price NUMERIC(12,2) NOT NULL,
  total_rooms INTEGER NOT NULL,
  is_sample BOOLEAN NOT NULL DEFAULT FALSE
);

-- Layered per-night pricing (see ARCHITECTURE.md): base price -> holiday or (season -> weekday %) -> discount.
-- Weekday % per country, Sunday..Saturday; hotels.weekday_pct (added by the API migration) overrides single days.
CREATE TABLE IF NOT EXISTS country_pricing (
  country TEXT PRIMARY KEY,
  weekday_pct NUMERIC(6,2)[] NOT NULL DEFAULT '{0,0,0,0,0,0,0}'
);

-- Seasons, holidays and discounts, for a whole country (admin) or one hotel / room type (seller).
CREATE TABLE IF NOT EXISTS price_rules (
  id UUID PRIMARY KEY,
  hotel_id UUID REFERENCES hotels(id) ON DELETE CASCADE,
  country TEXT,
  room_id UUID REFERENCES rooms(id) ON DELETE CASCADE,
  kind TEXT NOT NULL CHECK (kind IN ('SEASON','HOLIDAY','DISCOUNT')),
  name TEXT NOT NULL,
  start_date DATE NOT NULL,
  end_date DATE NOT NULL,
  adjust_type TEXT NOT NULL CHECK (adjust_type IN ('PERCENT','FIXED')),
  adjust_value NUMERIC(12,2) NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  CHECK (end_date >= start_date),
  CONSTRAINT price_rules_scope_check CHECK ((country IS NULL) <> (hotel_id IS NULL) AND (room_id IS NULL OR hotel_id IS NOT NULL))
);

CREATE TABLE IF NOT EXISTS bookings (
  id UUID PRIMARY KEY,
  user_id UUID NOT NULL REFERENCES users(id),
  hotel_id UUID NOT NULL REFERENCES hotels(id),
  room_id UUID NOT NULL REFERENCES rooms(id),
  status TEXT NOT NULL CHECK (status IN ('PENDING','CONFIRMED','CANCELLED','EXPIRED','PAYMENT_TIMEOUT')),
  price NUMERIC(12,2) NOT NULL,
  check_in DATE NOT NULL,
  check_out DATE NOT NULL,
  expires_at TIMESTAMPTZ,
  paid_at TIMESTAMPTZ,
  price_breakdown JSONB,                                       -- [{night, price, rule}] at booking time
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  CHECK (check_out > check_in)
);

CREATE TABLE IF NOT EXISTS audit_logs (
  id UUID PRIMARY KEY,
  actor_user_id UUID REFERENCES users(id),
  action TEXT NOT NULL,
  target_user_id UUID,
  metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS outbox_events (
  id UUID PRIMARY KEY,
  topic TEXT NOT NULL,
  event_key TEXT NOT NULL,
  payload JSONB NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  published_at TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS simulation_runs (
  id UUID PRIMARY KEY,
  status TEXT NOT NULL,
  params JSONB NOT NULL DEFAULT '{}'::jsonb,
  report JSONB NOT NULL DEFAULT '{}'::jsonb,
  customer_ids UUID[] NOT NULL DEFAULT '{}',
  created_by UUID,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  finished_at TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS hotels_city_idx ON hotels(city);
CREATE INDEX IF NOT EXISTS rooms_hotel_idx ON rooms(hotel_id);
CREATE INDEX IF NOT EXISTS price_rules_hotel_idx ON price_rules(hotel_id);
CREATE INDEX IF NOT EXISTS bookings_user_idx ON bookings(user_id);
CREATE INDEX IF NOT EXISTS bookings_room_dates_idx ON bookings(room_id, check_in, check_out);
CREATE INDEX IF NOT EXISTS bookings_pending_expiry_idx ON bookings(expires_at) WHERE status='PENDING';
CREATE INDEX IF NOT EXISTS outbox_unpublished_idx ON outbox_events(published_at);

INSERT INTO users(id,email,password_hash,name,role,is_sample) VALUES ('00000000-0000-0000-0000-000000000001','admin@example.com','admin123','Local Admin','ADMIN',false) ON CONFLICT (email) DO NOTHING;
