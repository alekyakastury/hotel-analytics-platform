BEGIN;

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
INSERT INTO channel (channel_name)
VALUES ('WEB'), ('MOBILE'), ('AGENT');

INSERT INTO review_category (name)
VALUES ('CLEANLINESS'), ('SERVICE'), ('VALUE'), ('LOCATION');

INSERT INTO address (street, city, state, country, postal_code)
VALUES
  ('123 Main St', 'Boston', 'MA', 'USA', '02110'),
  ('456 Ocean Ave', 'Miami', 'FL', 'USA', '33101');

-- =====================================================
-- HOTELS
-- =====================================================
INSERT INTO hotel (name, brand, address_id, timezone)
VALUES
  ('Hotel Aurora', 'Aurora Group', 1, 'America/New_York'),
  ('Hotel Solace', 'Solace Group', 2, 'America/New_York');

-- =====================================================
-- ROOM TYPES (insert fresh; no ON CONFLICT needed)
-- =====================================================
INSERT INTO room_type (name, max_occupancy, base_rate)
VALUES
  ('Standard', 2, 120.00),
  ('Deluxe',   3, 180.00),
  ('Suite',    4, 280.00);

-- =====================================================
-- ROOMS (10 per hotel) using lookup by room_type name
-- =====================================================
WITH rt AS (
  SELECT room_type_id, name FROM room_type
),
room_plan AS (
  SELECT
    h.hotel_id,
    (100 + gs)::TEXT AS room_number,
    CASE WHEN gs <= 5 THEN 1 ELSE 2 END AS floor,
    CASE ((gs - 1) % 3)
      WHEN 0 THEN 'Standard'
      WHEN 1 THEN 'Deluxe'
      ELSE 'Suite'
    END AS room_type_name
  FROM hotel h
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
JOIN rt ON rt.name = rp.room_type_name;

-- =====================================================
-- CUSTOMERS
-- =====================================================
INSERT INTO customer (first_name, last_name, email, phone, is_member)
VALUES
  ('Alekya', 'Kastury', 'alekya@example.com', '555-0001', TRUE),
  ('John',   'Doe',     'john@example.com',   '555-0002', FALSE),
  ('Jane',   'Smith',   'jane@example.com',   '555-0003', TRUE),
  ('Rahul',  'Mehta',   'rahul@example.com',  '555-0004', FALSE),
  ('Priya',  'Iyer',    'priya@example.com',  '555-0005', TRUE),
  ('Sam',    'Lee',     'sam@example.com',    '555-0006', FALSE);

-- =====================================================
-- EMPLOYEES
-- =====================================================
INSERT INTO employee (hotel_id, first_name, last_name, email, role, employment_status, hire_date)
VALUES
  (1, 'Mina', 'Patel', 'mina.patel@aurora.com', 'FRONT_DESK', 'ACTIVE', CURRENT_DATE - 300),
  (1, 'Luis', 'Garcia','luis.garcia@aurora.com','HOUSEKEEPING','ACTIVE', CURRENT_DATE - 200),
  (2, 'Nina', 'Chen',  'nina.chen@solace.com',  'FRONT_DESK', 'ACTIVE', CURRENT_DATE - 250),
  (2, 'Omar', 'Khan',  'omar.khan@solace.com',  'MAINTENANCE','ACTIVE', CURRENT_DATE - 180);

-- =====================================================
-- RATE PLANS + CALENDAR
-- =====================================================
INSERT INTO rate_plan (hotel_id, name, refundable)
VALUES
  (1, 'BAR', TRUE),
  (1, 'ADVANCE_PURCHASE', FALSE),
  (2, 'BAR', TRUE);

INSERT INTO rate_calendar (rate_plan_id, cal_date, nightly_rate)
SELECT
  rp.rate_plan_id,
  (CURRENT_DATE + d)::date,
  CASE
    WHEN rp.name = 'BAR' THEN 150.00 + (d % 7) * 5
    ELSE 130.00 + (d % 7) * 4
  END
FROM rate_plan rp
CROSS JOIN generate_series(0, 29) d;

INSERT INTO promotion (code, discount_type, discount_value, start_date, end_date)
VALUES
  ('WELCOME10', 'PERCENT', 10.00, CURRENT_DATE - 30, CURRENT_DATE + 365),
  ('SAVE25',    'FIXED',   25.00, CURRENT_DATE - 30, CURRENT_DATE + 90);

INSERT INTO tax_fee (hotel_id, name, tax_type, tax_value)
VALUES
  (1, 'CITY_TAX', 'PERCENT', 8.50),
  (2, 'CITY_TAX', 'PERCENT', 9.25);

-- =====================================================
-- BOOKINGS
-- =====================================================
INSERT INTO booking (customer_id, hotel_id, channel_id, booking_status, checkin_date, checkout_date, booking_channel)
VALUES
  (1, 1, 1, 'CONFIRMED', CURRENT_DATE + 2,  CURRENT_DATE + 5,  'WEB'),
  (2, 1, 2, 'CONFIRMED', CURRENT_DATE + 7,  CURRENT_DATE + 10, 'MOBILE'),
  (3, 1, 1, 'CANCELLED', CURRENT_DATE + 1,  CURRENT_DATE + 3,  'WEB'),
  (4, 1, 3, 'NO_SHOW',   CURRENT_DATE - 2,  CURRENT_DATE,      'AGENT'),

  (5, 2, 1, 'CONFIRMED', CURRENT_DATE + 3,  CURRENT_DATE + 6,  'WEB'),
  (6, 2, 2, 'CONFIRMED', CURRENT_DATE + 10, CURRENT_DATE + 12, 'MOBILE'),
  (1, 2, 1, 'CONFIRMED', CURRENT_DATE + 14, CURRENT_DATE + 16, 'WEB'),
  (2, 2, 3, 'CANCELLED', CURRENT_DATE + 5,  CURRENT_DATE + 8,  'AGENT'),

  (3, 1, 2, 'CONFIRMED', CURRENT_DATE + 20, CURRENT_DATE + 23, 'MOBILE'),
  (4, 2, 1, 'NO_SHOW',   CURRENT_DATE - 5,  CURRENT_DATE - 3,  'WEB'),
  (5, 1, 3, 'CONFIRMED', CURRENT_DATE + 25, CURRENT_DATE + 28, 'AGENT'),
  (6, 2, 2, 'CONFIRMED', CURRENT_DATE + 30, CURRENT_DATE + 33, 'MOBILE');

-- =====================================================
-- BOOKING_ROOMS
-- =====================================================
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

-- =====================================================
-- STAYS + CHECKIN/OUT (confirmed only)
-- =====================================================
INSERT INTO stay (booking_id, stay_status, actual_checkin_at, actual_checkout_at)
SELECT booking_id, 'CHECKED_OUT', checkin_date::timestamp, checkout_date::timestamp
FROM booking
WHERE booking_status = 'CONFIRMED';

INSERT INTO check_in (stay_id, employee_id, checked_in_at)
SELECT s.stay_id,
       CASE WHEN b.hotel_id = 1 THEN 1 ELSE 3 END,
       s.actual_checkin_at
FROM stay s
JOIN booking b ON b.booking_id = s.booking_id;

INSERT INTO check_out (stay_id, employee_id, checked_out_at)
SELECT s.stay_id,
       CASE WHEN b.hotel_id = 1 THEN 1 ELSE 3 END,
       s.actual_checkout_at
FROM stay s
JOIN booking b ON b.booking_id = s.booking_id;

-- =====================================================
-- ROOM_NIGHT (occupied)
-- =====================================================
INSERT INTO room_night (room_id, night_date, availability)
SELECT br.room_id, d::date, 'OCCUPIED'
FROM booking b
JOIN booking_room br ON br.booking_id = b.booking_id
JOIN generate_series(b.checkin_date, b.checkout_date - 1, interval '1 day') d
  ON b.booking_status = 'CONFIRMED';

-- =====================================================
-- EXCEPTIONS
-- =====================================================
INSERT INTO booking_cancellation (booking_id, cancellation_reason, penalty_amount, cancelled_at)
SELECT booking_id, 'Change of plans', 0, now()
FROM booking
WHERE booking_status = 'CANCELLED';

INSERT INTO no_show (booking_id, fee_charged, recorded_at)
SELECT booking_id, 50.00, now()
FROM booking
WHERE booking_status = 'NO_SHOW';

-- =====================================================
-- DISCOUNTS (two bookings)
-- =====================================================
INSERT INTO booking_discount (booking_id, promotion_id, discount_amount)
VALUES (1, 1, 25.00),
       (2, 2, 25.00);

-- =====================================================
-- INVOICE + LINE ITEMS + PAYMENT + REFUND
-- =====================================================
INSERT INTO invoice (booking_id, invoice_status, issued_at)
SELECT booking_id, 'PAID', now()
FROM booking
WHERE booking_status = 'CONFIRMED';

INSERT INTO invoice_line_item (invoice_id, item_type, description, amount)
SELECT i.invoice_id, 'ROOM', 'Room charges',
       (b.checkout_date - b.checkin_date) * rt.base_rate
FROM invoice i
JOIN booking b       ON b.booking_id = i.booking_id
JOIN booking_room br ON br.booking_id = b.booking_id
JOIN room r          ON r.room_id = br.room_id
JOIN room_type rt    ON rt.room_type_id = r.room_type_id;

INSERT INTO invoice_line_item (invoice_id, item_type, description, amount)
SELECT i.invoice_id, 'TAX', tf.name,
       ROUND((SUM(li.amount) * (tf.tax_value / 100.0))::numeric, 2)
FROM invoice i
JOIN booking b ON b.booking_id = i.booking_id
JOIN tax_fee tf ON tf.hotel_id = b.hotel_id AND tf.name = 'CITY_TAX'
JOIN invoice_line_item li ON li.invoice_id = i.invoice_id AND li.item_type = 'ROOM'
GROUP BY i.invoice_id, tf.name, tf.tax_value;

INSERT INTO invoice_line_item (invoice_id, item_type, description, amount)
SELECT i.invoice_id, 'DISCOUNT', 'Promo discount', -bd.discount_amount
FROM invoice i
JOIN booking_discount bd ON bd.booking_id = i.booking_id;

INSERT INTO payment (invoice_id, payment_method, payment_status, amount, paid_at)
SELECT i.invoice_id, 'CREDIT_CARD', 'SUCCESS',
       (SELECT COALESCE(SUM(amount),0) FROM invoice_line_item li WHERE li.invoice_id = i.invoice_id),
       now()
FROM invoice i;

INSERT INTO refund (payment_id, refund_amount, refund_reason, refunded_at)
SELECT p.payment_id, 30.00, 'Goodwill refund', now()
FROM payment p
JOIN invoice i ON i.invoice_id = p.invoice_id
WHERE i.booking_id = 1;

COMMIT;
