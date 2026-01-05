\set ON_ERROR_STOP on

BEGIN;

-- =====================================================
-- seed_transactions.sql (rerunnable)
-- Strategy:
--   1) TRUNCATE everything (safe reruns)
--   2) Insert reference/master data using natural keys (no hardcoded IDs)
--   3) Insert transactional data
-- Notes:
--   - Enum literals are explicitly cast (fixes your tax_type error)
--   - Avoid SELECT * from VALUES where enums are involved
-- =====================================================

-- =====================================================
-- RESET (rerunnable)
-- =====================================================
TRUNCATE TABLE
  refund,
  payment,
  invoice_line_item,
  invoice,
  room_night,
  room_block,
  stay_guest,
  stay,
  booking_room,
  booking_discount,
  booking_cancellation,
  no_show,
  review_score,
  review,
  service_order_item,
  service_order,
  maintenance_ticket,
  housekeeping_task,
  check_out,
  check_in,
  booking_event,
  audit_log,
  booking,
  room,
  employee,
  guest,
  customer,
  service_catalog,
  tax_fee,
  promotion,
  rate_calendar,
  rate_plan,
  cancellation_policy,
  hotel,
  identity_document,
  contact,
  address,
  channel,
  review_category,
  room_type
RESTART IDENTITY CASCADE;

-- =====================================================
-- REFERENCE DATA
-- =====================================================

-- CHANNELS (unique: channel_name)
INSERT INTO channel (channel_name)
VALUES ('WEB'), ('MOBILE'), ('AGENT')
ON CONFLICT (channel_name) DO NOTHING;

-- REVIEW CATEGORIES (unique: name)
INSERT INTO review_category (name)
VALUES ('CLEANLINESS'), ('SERVICE'), ('VALUE'), ('LOCATION')
ON CONFLICT (name) DO NOTHING;

-- ADDRESSES (recommended unique: street,city,state,country,postal_code)
INSERT INTO address (street, city, state, country, postal_code)
VALUES
  ('123 Main St', 'Boston', 'MA', 'USA', '02110'),
  ('456 Ocean Ave', 'Miami', 'FL', 'USA', '33101')
ON CONFLICT (street, city, state, country, postal_code) DO NOTHING;

-- HOTELS (recommended unique: (name, brand))
WITH addr AS (
  SELECT address_id, street, city, state, country, postal_code
  FROM address
),
src AS (
  SELECT * FROM (VALUES
    ('Hotel Aurora', 'Aurora Group', '123 Main St', 'Boston', 'MA', 'USA', '02110', 'America/New_York'),
    ('Hotel Solace', 'Solace Group', '456 Ocean Ave', 'Miami',  'FL', 'USA', '33101', 'America/New_York')
  ) AS v(name, brand, street, city, state, country, postal_code, timezone)
)
INSERT INTO hotel (name, brand, address_id, timezone)
SELECT
  s.name,
  s.brand,
  a.address_id,
  s.timezone
FROM src s
JOIN addr a
  ON a.street = s.street
 AND a.city = s.city
 AND a.state = s.state
 AND a.country = s.country
 AND a.postal_code = s.postal_code
ON CONFLICT (name, brand) DO NOTHING;

-- ROOM TYPES (recommended unique: name)
INSERT INTO room_type (name, max_occupancy, base_rate)
VALUES
  ('Standard', 2, 120.00),
  ('Deluxe',   3, 180.00),
  ('Suite',    4, 280.00)
ON CONFLICT (name) DO UPDATE
SET max_occupancy = EXCLUDED.max_occupancy,
    base_rate     = EXCLUDED.base_rate;

-- ROOMS (unique: (hotel_id, room_number))
WITH rt AS (
  SELECT room_type_id, name FROM room_type
),
h AS (
  SELECT hotel_id, name FROM hotel WHERE name IN ('Hotel Aurora','Hotel Solace')
),
room_plan AS (
  SELECT
    h.hotel_id,
    (100 + gs)::text AS room_number,
    CASE WHEN gs <= 5 THEN 1 ELSE 2 END AS floor,
    CASE ((gs - 1) % 3)
      WHEN 0 THEN 'Standard'
      WHEN 1 THEN 'Deluxe'
      ELSE 'Suite'
    END AS room_type_name
  FROM h
  CROSS JOIN generate_series(1, 10) gs
)
INSERT INTO room (hotel_id, room_type_id, room_number, floor, status)
SELECT
  rp.hotel_id,
  rt.room_type_id,
  rp.room_number,
  rp.floor,
  'ACTIVE'::room_status
FROM room_plan rp
JOIN rt ON rt.name = rp.room_type_name
ON CONFLICT (hotel_id, room_number) DO UPDATE
SET room_type_id = EXCLUDED.room_type_id,
    floor        = EXCLUDED.floor,
    status       = EXCLUDED.status;

-- CUSTOMERS (recommended unique: email)
INSERT INTO customer (first_name, last_name, email, phone, is_member)
VALUES
  ('Alekya', 'Kastury', 'alekya@example.com', '555-0001', TRUE),
  ('John',   'Doe',     'john@example.com',   '555-0002', FALSE),
  ('Jane',   'Smith',   'jane@example.com',   '555-0003', TRUE),
  ('Rahul',  'Mehta',   'rahul@example.com',  '555-0004', FALSE),
  ('Priya',  'Iyer',    'priya@example.com',  '555-0005', TRUE),
  ('Sam',    'Lee',     'sam@example.com',    '555-0006', FALSE)
ON CONFLICT (email) DO UPDATE
SET first_name = EXCLUDED.first_name,
    last_name  = EXCLUDED.last_name,
    phone      = EXCLUDED.phone,
    is_member  = EXCLUDED.is_member;

-- EMPLOYEES (recommended unique: email)
WITH h AS (
  SELECT hotel_id, name FROM hotel WHERE name IN ('Hotel Aurora','Hotel Solace')
),
src AS (
  SELECT * FROM (VALUES
    ('Hotel Aurora','Mina','Patel','mina.patel@aurora.com','FRONT_DESK','ACTIVE', CURRENT_DATE - 300),
    ('Hotel Aurora','Luis','Garcia','luis.garcia@aurora.com','HOUSEKEEPING','ACTIVE', CURRENT_DATE - 200),
    ('Hotel Solace','Nina','Chen','nina.chen@solace.com','FRONT_DESK','ACTIVE', CURRENT_DATE - 250),
    ('Hotel Solace','Omar','Khan','omar.khan@solace.com','MAINTENANCE','ACTIVE', CURRENT_DATE - 180)
  ) AS v(hotel_name, first_name, last_name, email, role, employment_status, hire_date)
)
INSERT INTO employee (hotel_id, first_name, last_name, email, role, employment_status, hire_date)
SELECT
  h.hotel_id,
  s.first_name,
  s.last_name,
  s.email,
  s.role,
  s.employment_status,
  s.hire_date
FROM src s
JOIN h ON h.name = s.hotel_name
ON CONFLICT (email) DO UPDATE
SET hotel_id          = EXCLUDED.hotel_id,
    first_name        = EXCLUDED.first_name,
    last_name         = EXCLUDED.last_name,
    role              = EXCLUDED.role,
    employment_status = EXCLUDED.employment_status,
    hire_date         = EXCLUDED.hire_date;

-- RATE PLANS (unique: (hotel_id, name))
WITH h AS (
  SELECT hotel_id, name FROM hotel WHERE name IN ('Hotel Aurora','Hotel Solace')
),
src AS (
  SELECT * FROM (VALUES
    ('Hotel Aurora','BAR',              TRUE),
    ('Hotel Aurora','ADVANCE_PURCHASE', FALSE),
    ('Hotel Solace','BAR',             TRUE)
  ) AS v(hotel_name, name, refundable)
)
INSERT INTO rate_plan (hotel_id, name, refundable)
SELECT
  h.hotel_id,
  s.name,
  s.refundable
FROM src s
JOIN h ON h.name = s.hotel_name
ON CONFLICT (hotel_id, name) DO UPDATE
SET refundable = EXCLUDED.refundable;

-- RATE CALENDAR (unique: (rate_plan_id, cal_date))
INSERT INTO rate_calendar (rate_plan_id, cal_date, nightly_rate)
SELECT
  rp.rate_plan_id,
  (CURRENT_DATE + d)::date,
  CASE
    WHEN rp.name = 'BAR' THEN 150.00 + (d % 7) * 5
    ELSE 130.00 + (d % 7) * 4
  END
FROM rate_plan rp
CROSS JOIN generate_series(0, 29) d
ON CONFLICT (rate_plan_id, cal_date) DO UPDATE
SET nightly_rate = EXCLUDED.nightly_rate;

-- PROMOTIONS (unique: code) + enum cast
INSERT INTO promotion (code, discount_type, discount_value, start_date, end_date)
VALUES
  ('WELCOME10', 'PERCENT'::discount_type, 10.00, CURRENT_DATE - 30, CURRENT_DATE + 365),
  ('SAVE25',    'FIXED'::discount_type,   25.00, CURRENT_DATE - 30, CURRENT_DATE + 90)
ON CONFLICT (code) DO UPDATE
SET discount_type  = EXCLUDED.discount_type,
    discount_value = EXCLUDED.discount_value,
    start_date     = EXCLUDED.start_date,
    end_date       = EXCLUDED.end_date;

-- TAX/FEES (unique: (hotel_id, name)) + enum cast (THIS FIXES YOUR ERROR)
WITH h AS (
  SELECT hotel_id, name FROM hotel WHERE name IN ('Hotel Aurora','Hotel Solace')
),
src AS (
  SELECT * FROM (VALUES
    ('Hotel Aurora','CITY_TAX','PERCENT'::tax_type, 8.50),
    ('Hotel Solace','CITY_TAX','PERCENT'::tax_type, 9.25)
  ) AS v(hotel_name, name, tax_type, tax_value)
)
INSERT INTO tax_fee (hotel_id, name, tax_type, tax_value)
SELECT
  h.hotel_id,
  s.name,
  s.tax_type,
  s.tax_value
FROM src s
JOIN h ON h.name = s.hotel_name
ON CONFLICT (hotel_id, name) DO UPDATE
SET tax_type  = EXCLUDED.tax_type,
    tax_value = EXCLUDED.tax_value;

-- CANCELLATION POLICY (TRUNCATE makes this rerunnable; no unique in schema)
WITH h AS (SELECT hotel_id, name FROM hotel WHERE name IN ('Hotel Aurora','Hotel Solace'))
INSERT INTO cancellation_policy (hotel_id, cutoff_hours, penalty_percent)
SELECT
  h.hotel_id,
  CASE WHEN h.name = 'Hotel Aurora' THEN 24 ELSE 48 END,
  CASE WHEN h.name = 'Hotel Aurora' THEN 25.00 ELSE 30.00 END
FROM h;

-- =====================================================
-- TRANSACTIONAL DATA
-- =====================================================

-- BOOKINGS (cast booking_status enum explicitly)
WITH c  AS (SELECT customer_id, email FROM customer),
     h  AS (SELECT hotel_id, name FROM hotel),
     ch AS (SELECT channel_id, channel_name FROM channel),
src AS (
  SELECT * FROM (VALUES
    ('alekya@example.com','Hotel Aurora','WEB',    'CONFIRMED'::booking_status, CURRENT_DATE + 2,  CURRENT_DATE + 5,  'WEB'),
    ('john@example.com',  'Hotel Aurora','MOBILE', 'CONFIRMED'::booking_status, CURRENT_DATE + 7,  CURRENT_DATE + 10, 'MOBILE'),
    ('jane@example.com',  'Hotel Aurora','WEB',    'CANCELLED'::booking_status, CURRENT_DATE + 1,  CURRENT_DATE + 3,  'WEB'),
    ('rahul@example.com', 'Hotel Aurora','AGENT',  'NO_SHOW'::booking_status,   CURRENT_DATE - 2,  CURRENT_DATE,      'AGENT'),

    ('priya@example.com', 'Hotel Solace','WEB',    'CONFIRMED'::booking_status, CURRENT_DATE + 3,  CURRENT_DATE + 6,  'WEB'),
    ('sam@example.com',   'Hotel Solace','MOBILE', 'CONFIRMED'::booking_status, CURRENT_DATE + 10, CURRENT_DATE + 12, 'MOBILE'),
    ('alekya@example.com','Hotel Solace','WEB',    'CONFIRMED'::booking_status, CURRENT_DATE + 14, CURRENT_DATE + 16, 'WEB'),
    ('john@example.com',  'Hotel Solace','AGENT',  'CANCELLED'::booking_status, CURRENT_DATE + 5,  CURRENT_DATE + 8,  'AGENT'),

    ('jane@example.com',  'Hotel Aurora','MOBILE', 'CONFIRMED'::booking_status, CURRENT_DATE + 20, CURRENT_DATE + 23, 'MOBILE'),
    ('rahul@example.com', 'Hotel Solace','WEB',    'NO_SHOW'::booking_status,   CURRENT_DATE - 5,  CURRENT_DATE - 3,  'WEB'),
    ('priya@example.com', 'Hotel Aurora','AGENT',  'CONFIRMED'::booking_status, CURRENT_DATE + 25, CURRENT_DATE + 28, 'AGENT'),
    ('sam@example.com',   'Hotel Solace','MOBILE', 'CONFIRMED'::booking_status, CURRENT_DATE + 30, CURRENT_DATE + 33, 'MOBILE')
  ) AS v(customer_email, hotel_name, channel_name, booking_status, checkin_date, checkout_date, booking_channel)
)
INSERT INTO booking (customer_id, hotel_id, channel_id, booking_status, checkin_date, checkout_date, booking_channel)
SELECT
  c.customer_id,
  h.hotel_id,
  ch.channel_id,
  s.booking_status,
  s.checkin_date,
  s.checkout_date,
  s.booking_channel
FROM src s
JOIN c  ON c.email = s.customer_email
JOIN h  ON h.name  = s.hotel_name
JOIN ch ON ch.channel_name = s.channel_name;

-- BOOKING_ROOMS (deterministic assignment)
INSERT INTO booking_room (booking_id, room_id, assigned_at)
SELECT
  b.booking_id,
  r.room_id,
  now()
FROM (
  SELECT booking_id, hotel_id, row_number() OVER (ORDER BY booking_id) AS rn
  FROM booking
) b
JOIN (
  SELECT room_id, hotel_id, row_number() OVER (PARTITION BY hotel_id ORDER BY room_id) AS rn
  FROM room
) r
  ON r.hotel_id = b.hotel_id
 AND r.rn = ((b.rn - 1) % 10) + 1;

-- STAYS (confirmed only) + enum cast
INSERT INTO stay (booking_id, stay_status, actual_checkin_at, actual_checkout_at)
SELECT
  booking_id,
  'CHECKED_OUT'::stay_status,
  checkin_date::timestamp,
  checkout_date::timestamp
FROM booking
WHERE booking_status = 'CONFIRMED'::booking_status;

-- CHECK-IN / CHECK-OUT (use FRONT_DESK employee per hotel)
WITH fd AS (
  SELECT e.employee_id, e.hotel_id
  FROM employee e
  WHERE e.role = 'FRONT_DESK' AND e.employment_status = 'ACTIVE'
)
INSERT INTO check_in (stay_id, employee_id, checked_in_at)
SELECT
  s.stay_id,
  (SELECT employee_id FROM fd WHERE hotel_id = b.hotel_id LIMIT 1),
  s.actual_checkin_at
FROM stay s
JOIN booking b ON b.booking_id = s.booking_id;

WITH fd AS (
  SELECT e.employee_id, e.hotel_id
  FROM employee e
  WHERE e.role = 'FRONT_DESK' AND e.employment_status = 'ACTIVE'
)
INSERT INTO check_out (stay_id, employee_id, checked_out_at)
SELECT
  s.stay_id,
  (SELECT employee_id FROM fd WHERE hotel_id = b.hotel_id LIMIT 1),
  s.actual_checkout_at
FROM stay s
JOIN booking b ON b.booking_id = s.booking_id;

-- ROOM_NIGHT (occupied) + enum cast
INSERT INTO room_night (room_id, night_date, availability)
SELECT
  br.room_id,
  d::date,
  'OCCUPIED'::availability_status
FROM booking b
JOIN booking_room br ON br.booking_id = b.booking_id
JOIN generate_series(b.checkin_date, b.checkout_date - 1, interval '1 day') d
  ON b.booking_status = 'CONFIRMED'::booking_status;

-- CANCELLATIONS / NO-SHOWS
INSERT INTO booking_cancellation (booking_id, cancellation_reason, penalty_amount, cancelled_at)
SELECT booking_id, 'Change of plans', 0, now()
FROM booking
WHERE booking_status = 'CANCELLED'::booking_status;

INSERT INTO no_show (booking_id, fee_charged, recorded_at)
SELECT booking_id, 50.00, now()
FROM booking
WHERE booking_status = 'NO_SHOW'::booking_status;

-- DISCOUNTS (tie promo by code)
WITH promo AS (SELECT promotion_id, code FROM promotion)
INSERT INTO booking_discount (booking_id, promotion_id, discount_amount)
SELECT *
FROM (
  SELECT 1 AS booking_id, (SELECT promotion_id FROM promo WHERE code='WELCOME10') AS promotion_id, 25.00
  UNION ALL
  SELECT 2, (SELECT promotion_id FROM promo WHERE code='SAVE25'), 25.00
) v(booking_id, promotion_id, discount_amount);

-- INVOICE (enum cast)
INSERT INTO invoice (booking_id, invoice_status, issued_at)
SELECT
  booking_id,
  'PAID'::invoice_status,
  now()
FROM booking
WHERE booking_status = 'CONFIRMED'::booking_status;

-- INVOICE LINE ITEMS: ROOM
INSERT INTO invoice_line_item (invoice_id, item_type, description, amount)
SELECT
  i.invoice_id,
  'ROOM'::invoice_item_type,
  'Room charges',
  (b.checkout_date - b.checkin_date) * rt.base_rate
FROM invoice i
JOIN booking b       ON b.booking_id = i.booking_id
JOIN booking_room br ON br.booking_id = b.booking_id
JOIN room r          ON r.room_id = br.room_id
JOIN room_type rt    ON rt.room_type_id = r.room_type_id;

-- INVOICE LINE ITEMS: TAX (on ROOM only)
INSERT INTO invoice_line_item (invoice_id, item_type, description, amount)
SELECT
  i.invoice_id,
  'TAX'::invoice_item_type,
  tf.name,
  ROUND((SUM(li.amount) * (tf.tax_value / 100.0))::numeric, 2)
FROM invoice i
JOIN booking b ON b.booking_id = i.booking_id
JOIN tax_fee tf ON tf.hotel_id = b.hotel_id AND tf.name = 'CITY_TAX'
JOIN invoice_line_item li ON li.invoice_id = i.invoice_id AND li.item_type = 'ROOM'::invoice_item_type
GROUP BY i.invoice_id, tf.name, tf.tax_value;

-- INVOICE LINE ITEMS: DISCOUNT
INSERT INTO invoice_line_item (invoice_id, item_type, description, amount)
SELECT
  i.invoice_id,
  'DISCOUNT'::invoice_item_type,
  'Promo discount',
  -bd.discount_amount
FROM invoice i
JOIN booking_discount bd ON bd.booking_id = i.booking_id;

-- PAYMENT (enum cast)
INSERT INTO payment (invoice_id, payment_method, payment_status, amount, paid_at)
SELECT
  i.invoice_id,
  'CREDIT_CARD',
  'SUCCESS'::payment_status,
  (SELECT COALESCE(SUM(amount),0) FROM invoice_line_item li WHERE li.invoice_id = i.invoice_id),
  now()
FROM invoice i;

-- REFUND (example for booking_id=1)
INSERT INTO refund (payment_id, refund_amount, refund_reason, refunded_at)
SELECT
  p.payment_id,
  30.00,
  'Goodwill refund',
  now()
FROM payment p
JOIN invoice i ON i.invoice_id = p.invoice_id
WHERE i.booking_id = 1;

COMMIT;
