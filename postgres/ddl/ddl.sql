-- =========================================================
-- Hotel Analytics Platform - OLTP Schema (Postgres)
-- Single runnable DDL file
-- =========================================================

BEGIN;

-- -------------------------
-- Optional: clean re-run
-- -------------------------
-- Comment this section out if you don't want to drop tables on rerun.
DO $$
DECLARE
  r RECORD;
BEGIN
  FOR r IN (SELECT tablename FROM pg_tables WHERE schemaname = 'public') LOOP
    EXECUTE 'DROP TABLE IF EXISTS ' || quote_ident(r.tablename) || ' CASCADE';
  END LOOP;
END $$;

-- -------------------------
-- Enums (kept minimal)
-- -------------------------
DO $$ BEGIN
  CREATE TYPE booking_status AS ENUM ('CONFIRMED', 'CANCELLED', 'NO_SHOW');
EXCEPTION WHEN duplicate_object THEN NULL; END $$;

DO $$ BEGIN
  CREATE TYPE stay_status AS ENUM ('CHECKED_IN', 'CHECKED_OUT');
EXCEPTION WHEN duplicate_object THEN NULL; END $$;

DO $$ BEGIN
  CREATE TYPE invoice_status AS ENUM ('OPEN', 'PAID', 'VOID', 'REFUNDED');
EXCEPTION WHEN duplicate_object THEN NULL; END $$;

DO $$ BEGIN
  CREATE TYPE payment_status AS ENUM ('SUCCESS', 'FAILED', 'PENDING');
EXCEPTION WHEN duplicate_object THEN NULL; END $$;

DO $$ BEGIN
  CREATE TYPE discount_type AS ENUM ('PERCENT', 'FIXED');
EXCEPTION WHEN duplicate_object THEN NULL; END $$;

DO $$ BEGIN
  CREATE TYPE tax_type AS ENUM ('PERCENT', 'FIXED');
EXCEPTION WHEN duplicate_object THEN NULL; END $$;

DO $$ BEGIN
  CREATE TYPE room_status AS ENUM ('ACTIVE', 'INACTIVE', 'OUT_OF_SERVICE');
EXCEPTION WHEN duplicate_object THEN NULL; END $$;

DO $$ BEGIN
  CREATE TYPE availability_status AS ENUM ('AVAILABLE', 'OCCUPIED', 'BLOCKED');
EXCEPTION WHEN duplicate_object THEN NULL; END $$;

DO $$ BEGIN
  CREATE TYPE service_order_status AS ENUM ('ORDERED', 'DELIVERED', 'CANCELLED');
EXCEPTION WHEN duplicate_object THEN NULL; END $$;

DO $$ BEGIN
  CREATE TYPE task_status AS ENUM ('OPEN', 'IN_PROGRESS', 'DONE', 'CANCELLED');
EXCEPTION WHEN duplicate_object THEN NULL; END $$;

DO $$ BEGIN
  CREATE TYPE severity_level AS ENUM ('LOW', 'MEDIUM', 'HIGH', 'CRITICAL');
EXCEPTION WHEN duplicate_object THEN NULL; END $$;

DO $$ BEGIN
  CREATE TYPE contact_type AS ENUM ('EMAIL', 'PHONE');
EXCEPTION WHEN duplicate_object THEN NULL; END $$;

DO $$ BEGIN
  CREATE TYPE invoice_item_type AS ENUM ('ROOM', 'SERVICE', 'TAX', 'DISCOUNT', 'OTHER');
EXCEPTION WHEN duplicate_object THEN NULL; END $$;

-- =========================================================
-- 11) Reference / Normalization
-- =========================================================

CREATE TABLE IF NOT EXISTS address (
  address_id        BIGSERIAL PRIMARY KEY,
  street            TEXT,
  city              TEXT,
  state             TEXT,
  country           TEXT,
  postal_code       TEXT,
  created_at        TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS contact (
  contact_id        BIGSERIAL PRIMARY KEY,
  contact_type      contact_type NOT NULL,
  contact_value     TEXT NOT NULL,
  created_at        TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS identity_document (
  identity_document_id  BIGSERIAL PRIMARY KEY,
  document_type         TEXT NOT NULL,
  document_number_hash  TEXT NOT NULL,
  country               TEXT,
  expiry_date           DATE,
  created_at            TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE(document_number_hash)
);

CREATE TABLE IF NOT EXISTS channel (
  channel_id        BIGSERIAL PRIMARY KEY,
  channel_name      TEXT NOT NULL UNIQUE,
  created_at        TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- =========================================================
-- 1) Core Master Data
-- =========================================================

CREATE TABLE IF NOT EXISTS hotel (
  hotel_id          BIGSERIAL PRIMARY KEY,
  name              TEXT NOT NULL,
  brand             TEXT,
  address_id        BIGINT REFERENCES address(address_id),
  timezone          TEXT,
  created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at        TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS room_type (
  room_type_id      BIGSERIAL PRIMARY KEY,
  name              TEXT NOT NULL,
  max_occupancy     INT NOT NULL CHECK (max_occupancy > 0),
  base_rate         NUMERIC(12,2) NOT NULL CHECK (base_rate >= 0),
  created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at        TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS room (
  room_id           BIGSERIAL PRIMARY KEY,
  hotel_id          BIGINT NOT NULL REFERENCES hotel(hotel_id),
  room_type_id      BIGINT NOT NULL REFERENCES room_type(room_type_id),
  room_number       TEXT NOT NULL,
  floor             INT,
  status            room_status NOT NULL DEFAULT 'ACTIVE',
  created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE(hotel_id, room_number)
);

CREATE TABLE IF NOT EXISTS customer (
  customer_id       BIGSERIAL PRIMARY KEY,
  first_name        TEXT NOT NULL,
  last_name         TEXT NOT NULL,
  email             TEXT,
  phone             TEXT,
  is_member         BOOLEAN NOT NULL DEFAULT FALSE,
  -- optional normalization
  address_id        BIGINT REFERENCES address(address_id),
  contact_id        BIGINT REFERENCES contact(contact_id),
  created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at        TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS guest (
  guest_id              BIGSERIAL PRIMARY KEY,
  first_name            TEXT NOT NULL,
  last_name             TEXT NOT NULL,
  identity_document_id  BIGINT REFERENCES identity_document(identity_document_id),
  created_at            TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at            TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS employee (
  employee_id       BIGSERIAL PRIMARY KEY,
  hotel_id          BIGINT NOT NULL REFERENCES hotel(hotel_id),
  first_name        TEXT NOT NULL,
  last_name         TEXT NOT NULL,
  email             TEXT,
  role              TEXT NOT NULL, -- FRONT_DESK / HOUSEKEEPING / MAINTENANCE / MANAGER
  employment_status TEXT NOT NULL DEFAULT 'ACTIVE',
  hire_date         DATE,
  created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at        TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- =========================================================
-- 2) Booking & Stay Lifecycle
-- =========================================================

CREATE TABLE IF NOT EXISTS booking (
  booking_id        BIGSERIAL PRIMARY KEY,
  customer_id       BIGINT NOT NULL REFERENCES customer(customer_id),
  hotel_id          BIGINT NOT NULL REFERENCES hotel(hotel_id),
  channel_id        BIGINT REFERENCES channel(channel_id),
  booking_status    booking_status NOT NULL DEFAULT 'CONFIRMED',
  booking_channel   TEXT, -- keep for simplicity; can be replaced by channel_id
  checkin_date      DATE NOT NULL,
  checkout_date     DATE NOT NULL,
  created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
  CHECK (checkout_date > checkin_date)
);

CREATE TABLE IF NOT EXISTS booking_room (
  booking_room_id   BIGSERIAL PRIMARY KEY,
  booking_id        BIGINT NOT NULL REFERENCES booking(booking_id) ON DELETE CASCADE,
  room_id           BIGINT NOT NULL REFERENCES room(room_id),
  assigned_at       TIMESTAMPTZ,
  created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE(booking_id, room_id)
);

CREATE TABLE IF NOT EXISTS stay (
  stay_id           BIGSERIAL PRIMARY KEY,
  booking_id        BIGINT NOT NULL UNIQUE REFERENCES booking(booking_id) ON DELETE CASCADE,
  stay_status       stay_status NOT NULL DEFAULT 'CHECKED_IN',
  actual_checkin_at TIMESTAMPTZ,
  actual_checkout_at TIMESTAMPTZ,
  created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
  CHECK (actual_checkout_at IS NULL OR actual_checkin_at IS NULL OR actual_checkout_at >= actual_checkin_at)
);

CREATE TABLE IF NOT EXISTS stay_guest (
  stay_guest_id     BIGSERIAL PRIMARY KEY,
  stay_id           BIGINT NOT NULL REFERENCES stay(stay_id) ON DELETE CASCADE,
  guest_id          BIGINT NOT NULL REFERENCES guest(guest_id),
  created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE(stay_id, guest_id)
);

-- =========================================================
-- 3) Inventory & Availability
-- =========================================================

CREATE TABLE IF NOT EXISTS room_night (
  room_night_id     BIGSERIAL PRIMARY KEY,
  room_id           BIGINT NOT NULL REFERENCES room(room_id) ON DELETE CASCADE,
  night_date        DATE NOT NULL,
  availability      availability_status NOT NULL DEFAULT 'AVAILABLE',
  created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE(room_id, night_date)
);

CREATE TABLE IF NOT EXISTS room_block (
  room_block_id     BIGSERIAL PRIMARY KEY,
  room_id           BIGINT NOT NULL REFERENCES room(room_id) ON DELETE CASCADE,
  start_date        DATE NOT NULL,
  end_date          DATE NOT NULL,
  reason            TEXT,
  created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
  CHECK (end_date > start_date)
);

-- =========================================================
-- 4) Pricing, Rates & Discounts
-- =========================================================

CREATE TABLE IF NOT EXISTS rate_plan (
  rate_plan_id      BIGSERIAL PRIMARY KEY,
  hotel_id          BIGINT NOT NULL REFERENCES hotel(hotel_id) ON DELETE CASCADE,
  name              TEXT NOT NULL,
  refundable        BOOLEAN NOT NULL DEFAULT TRUE,
  created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE(hotel_id, name)
);

CREATE TABLE IF NOT EXISTS rate_calendar (
  rate_calendar_id  BIGSERIAL PRIMARY KEY,
  rate_plan_id      BIGINT NOT NULL REFERENCES rate_plan(rate_plan_id) ON DELETE CASCADE,
  cal_date          DATE NOT NULL,
  nightly_rate      NUMERIC(12,2) NOT NULL CHECK (nightly_rate >= 0),
  created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE(rate_plan_id, cal_date)
);

CREATE TABLE IF NOT EXISTS promotion (
  promotion_id      BIGSERIAL PRIMARY KEY,
  code              TEXT NOT NULL UNIQUE,
  discount_type     discount_type NOT NULL,
  discount_value    NUMERIC(12,2) NOT NULL CHECK (discount_value >= 0),
  start_date        DATE,
  end_date          DATE,
  created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
  CHECK (end_date IS NULL OR start_date IS NULL OR end_date >= start_date)
);

CREATE TABLE IF NOT EXISTS booking_discount (
  booking_discount_id BIGSERIAL PRIMARY KEY,
  booking_id          BIGINT NOT NULL REFERENCES booking(booking_id) ON DELETE CASCADE,
  promotion_id        BIGINT REFERENCES promotion(promotion_id),
  discount_amount     NUMERIC(12,2) NOT NULL CHECK (discount_amount >= 0),
  created_at          TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS tax_fee (
  tax_fee_id        BIGSERIAL PRIMARY KEY,
  hotel_id          BIGINT NOT NULL REFERENCES hotel(hotel_id) ON DELETE CASCADE,
  name              TEXT NOT NULL,
  tax_type          tax_type NOT NULL,
  tax_value         NUMERIC(12,2) NOT NULL CHECK (tax_value >= 0),
  created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE(hotel_id, name)
);

-- =========================================================
-- 5) Billing & Payments
-- =========================================================

CREATE TABLE IF NOT EXISTS invoice (
  invoice_id        BIGSERIAL PRIMARY KEY,
  booking_id        BIGINT NOT NULL REFERENCES booking(booking_id) ON DELETE CASCADE,
  invoice_status    invoice_status NOT NULL DEFAULT 'OPEN',
  issued_at         TIMESTAMPTZ,
  created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at        TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS invoice_line_item (
  invoice_line_item_id BIGSERIAL PRIMARY KEY,
  invoice_id           BIGINT NOT NULL REFERENCES invoice(invoice_id) ON DELETE CASCADE,
  item_type            invoice_item_type NOT NULL DEFAULT 'OTHER',
  description          TEXT,
  amount               NUMERIC(12,2) NOT NULL,
  created_at           TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS payment (
  payment_id        BIGSERIAL PRIMARY KEY,
  invoice_id        BIGINT NOT NULL REFERENCES invoice(invoice_id) ON DELETE CASCADE,
  payment_method    TEXT NOT NULL,
  payment_status    payment_status NOT NULL DEFAULT 'SUCCESS',
  amount            NUMERIC(12,2) NOT NULL CHECK (amount >= 0),
  paid_at           TIMESTAMPTZ,
  created_at        TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS refund (
  refund_id         BIGSERIAL PRIMARY KEY,
  payment_id        BIGINT NOT NULL REFERENCES payment(payment_id) ON DELETE CASCADE,
  refund_amount     NUMERIC(12,2) NOT NULL CHECK (refund_amount >= 0),
  refund_reason     TEXT,
  refunded_at       TIMESTAMPTZ,
  created_at        TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- =========================================================
-- 6) Services & Add-Ons
-- =========================================================

CREATE TABLE IF NOT EXISTS service_catalog (
  service_id        BIGSERIAL PRIMARY KEY,
  hotel_id          BIGINT NOT NULL REFERENCES hotel(hotel_id) ON DELETE CASCADE,
  name              TEXT NOT NULL,
  unit_price        NUMERIC(12,2) NOT NULL CHECK (unit_price >= 0),
  created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE(hotel_id, name)
);

CREATE TABLE IF NOT EXISTS service_order (
  service_order_id  BIGSERIAL PRIMARY KEY,
  booking_id        BIGINT NOT NULL REFERENCES booking(booking_id) ON DELETE CASCADE,
  order_status      service_order_status NOT NULL DEFAULT 'ORDERED',
  ordered_at        TIMESTAMPTZ,
  created_at        TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS service_order_item (
  service_order_item_id BIGSERIAL PRIMARY KEY,
  service_order_id      BIGINT NOT NULL REFERENCES service_order(service_order_id) ON DELETE CASCADE,
  service_id            BIGINT NOT NULL REFERENCES service_catalog(service_id),
  quantity              INT NOT NULL CHECK (quantity > 0),
  unit_price            NUMERIC(12,2) NOT NULL CHECK (unit_price >= 0),
  created_at            TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- =========================================================
-- 7) Policies, Cancellations & Exceptions
-- =========================================================

CREATE TABLE IF NOT EXISTS cancellation_policy (
  cancellation_policy_id BIGSERIAL PRIMARY KEY,
  hotel_id               BIGINT NOT NULL REFERENCES hotel(hotel_id) ON DELETE CASCADE,
  cutoff_hours           INT NOT NULL CHECK (cutoff_hours >= 0),
  penalty_percent        NUMERIC(5,2) NOT NULL CHECK (penalty_percent >= 0 AND penalty_percent <= 100),
  created_at             TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS booking_cancellation (
  booking_cancellation_id BIGSERIAL PRIMARY KEY,
  booking_id              BIGINT NOT NULL UNIQUE REFERENCES booking(booking_id) ON DELETE CASCADE,
  cancellation_reason     TEXT,
  penalty_amount          NUMERIC(12,2) NOT NULL DEFAULT 0 CHECK (penalty_amount >= 0),
  cancelled_at            TIMESTAMPTZ NOT NULL DEFAULT now(),
  created_at              TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS no_show (
  no_show_id       BIGSERIAL PRIMARY KEY,
  booking_id       BIGINT NOT NULL UNIQUE REFERENCES booking(booking_id) ON DELETE CASCADE,
  fee_charged      NUMERIC(12,2) NOT NULL DEFAULT 0 CHECK (fee_charged >= 0),
  recorded_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
  created_at       TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- =========================================================
-- 8) Feedback & Experience
-- =========================================================

CREATE TABLE IF NOT EXISTS review (
  review_id         BIGSERIAL PRIMARY KEY,
  booking_id        BIGINT NOT NULL UNIQUE REFERENCES booking(booking_id) ON DELETE CASCADE,
  overall_rating    INT NOT NULL CHECK (overall_rating >= 1 AND overall_rating <= 5),
  comment           TEXT,
  created_at        TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS review_category (
  review_category_id BIGSERIAL PRIMARY KEY,
  name               TEXT NOT NULL UNIQUE,
  created_at         TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS review_score (
  review_score_id     BIGSERIAL PRIMARY KEY,
  review_id           BIGINT NOT NULL REFERENCES review(review_id) ON DELETE CASCADE,
  review_category_id  BIGINT NOT NULL REFERENCES review_category(review_category_id),
  score               INT NOT NULL CHECK (score >= 1 AND score <= 5),
  created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE(review_id, review_category_id)
);

-- =========================================================
-- 9) Operations
-- =========================================================

CREATE TABLE IF NOT EXISTS check_in (
  check_in_id       BIGSERIAL PRIMARY KEY,
  stay_id           BIGINT NOT NULL UNIQUE REFERENCES stay(stay_id) ON DELETE CASCADE,
  employee_id       BIGINT REFERENCES employee(employee_id),
  checked_in_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
  created_at        TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS check_out (
  check_out_id      BIGSERIAL PRIMARY KEY,
  stay_id           BIGINT NOT NULL UNIQUE REFERENCES stay(stay_id) ON DELETE CASCADE,
  employee_id       BIGINT REFERENCES employee(employee_id),
  checked_out_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
  created_at        TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS housekeeping_task (
  housekeeping_task_id BIGSERIAL PRIMARY KEY,
  room_id              BIGINT NOT NULL REFERENCES room(room_id) ON DELETE CASCADE,
  assigned_employee_id BIGINT REFERENCES employee(employee_id),
  task_status          task_status NOT NULL DEFAULT 'OPEN',
  scheduled_for        DATE,
  completed_at         TIMESTAMPTZ,
  created_at           TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS maintenance_ticket (
  maintenance_ticket_id BIGSERIAL PRIMARY KEY,
  room_id               BIGINT NOT NULL REFERENCES room(room_id) ON DELETE CASCADE,
  assigned_employee_id  BIGINT REFERENCES employee(employee_id),
  issue_description     TEXT NOT NULL,
  severity              severity_level NOT NULL DEFAULT 'LOW',
  status                task_status NOT NULL DEFAULT 'OPEN',
  created_at            TIMESTAMPTZ NOT NULL DEFAULT now(),
  resolved_at           TIMESTAMPTZ
);

-- =========================================================
-- 10) Audit, Events & Trust
-- =========================================================

CREATE TABLE IF NOT EXISTS booking_event (
  booking_event_id   BIGSERIAL PRIMARY KEY,
  booking_id         BIGINT NOT NULL REFERENCES booking(booking_id) ON DELETE CASCADE,
  event_type         TEXT NOT NULL,
  event_timestamp    TIMESTAMPTZ NOT NULL DEFAULT now(),
  created_at         TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS audit_log (
  audit_log_id     BIGSERIAL PRIMARY KEY,
  entity_name      TEXT NOT NULL,
  entity_id        BIGINT NOT NULL,
  action_type      TEXT NOT NULL, -- INSERT/UPDATE/DELETE
  performed_by     TEXT,
  performed_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- =========================================================
-- Indexes (FK + common filters)
-- =========================================================

-- Core
CREATE INDEX IF NOT EXISTS idx_room_hotel_id ON room(hotel_id);
CREATE INDEX IF NOT EXISTS idx_room_room_type_id ON room(room_type_id);

-- Booking
CREATE INDEX IF NOT EXISTS idx_booking_customer_id ON booking(customer_id);
CREATE INDEX IF NOT EXISTS idx_booking_hotel_id ON booking(hotel_id);
CREATE INDEX IF NOT EXISTS idx_booking_dates ON booking(checkin_date, checkout_date);

CREATE INDEX IF NOT EXISTS idx_booking_room_booking_id ON booking_room(booking_id);
CREATE INDEX IF NOT EXISTS idx_booking_room_room_id ON booking_room(room_id);

-- Stay
CREATE INDEX IF NOT EXISTS idx_stay_booking_id ON stay(booking_id);
CREATE INDEX IF NOT EXISTS idx_stay_guest_stay_id ON stay_guest(stay_id);
CREATE INDEX IF NOT EXISTS idx_stay_guest_guest_id ON stay_guest(guest_id);

-- Inventory
CREATE INDEX IF NOT EXISTS idx_room_night_room_date ON room_night(room_id, night_date);
CREATE INDEX IF NOT EXISTS idx_room_block_room_dates ON room_block(room_id, start_date, end_date);

-- Pricing
CREATE INDEX IF NOT EXISTS idx_rate_plan_hotel_id ON rate_plan(hotel_id);
CREATE INDEX IF NOT EXISTS idx_rate_calendar_plan_date ON rate_calendar(rate_plan_id, cal_date);
CREATE INDEX IF NOT EXISTS idx_booking_discount_booking_id ON booking_discount(booking_id);

-- Billing
CREATE INDEX IF NOT EXISTS idx_invoice_booking_id ON invoice(booking_id);
CREATE INDEX IF NOT EXISTS idx_invoice_line_item_invoice_id ON invoice_line_item(invoice_id);
CREATE INDEX IF NOT EXISTS idx_payment_invoice_id ON payment(invoice_id);
CREATE INDEX IF NOT EXISTS idx_refund_payment_id ON refund(payment_id);

-- Services
CREATE INDEX IF NOT EXISTS idx_service_catalog_hotel_id ON service_catalog(hotel_id);
CREATE INDEX IF NOT EXISTS idx_service_order_booking_id ON service_order(booking_id);
CREATE INDEX IF NOT EXISTS idx_service_order_item_order_id ON service_order_item(service_order_id);

-- Ops
CREATE INDEX IF NOT EXISTS idx_check_in_employee_id ON check_in(employee_id);
CREATE INDEX IF NOT EXISTS idx_check_out_employee_id ON check_out(employee_id);
CREATE INDEX IF NOT EXISTS idx_housekeeping_room_id ON housekeeping_task(room_id);
CREATE INDEX IF NOT EXISTS idx_maintenance_room_id ON maintenance_ticket(room_id);

-- Audit
CREATE INDEX IF NOT EXISTS idx_booking_event_booking_id ON booking_event(booking_id);
CREATE INDEX IF NOT EXISTS idx_audit_log_entity ON audit_log(entity_name, entity_id);

COMMIT;
