/*
  MSSQL Airline Dataset Seeder (ffengine_test)
  - Synthetic 1-year airline demand + booking + pricing dataset
  - Target size: 4.5 - 5.0 GB (default 4.8 GB)
  - Idempotent policy: TRUNCATE + refill
*/

SET NOCOUNT ON;
SET XACT_ABORT ON;
SET ANSI_NULLS ON;
SET QUOTED_IDENTIFIER ON;

IF DB_ID(N'ffengine_test') IS NULL
BEGIN
    THROW 50001, 'Database ffengine_test does not exist.', 1;
END;

USE ffengine_test;

IF SCHEMA_ID(N'ffengine') IS NULL
BEGIN
    EXEC(N'CREATE SCHEMA ffengine AUTHORIZATION dbo;');
END;

DECLARE @target_size_gb DECIMAL(10,2) = 4.80;
DECLARE @min_size_gb DECIMAL(10,2) = 4.50;
DECLARE @max_size_gb DECIMAL(10,2) = 5.00;
DECLARE @batch_rows INT = 100000;
DECLARE @start_date DATE = DATEADD(DAY, -365, CAST(SYSUTCDATETIME() AS DATE));
DECLARE @end_date DATE = DATEADD(DAY, -1, CAST(SYSUTCDATETIME() AS DATE));

IF @target_size_gb < @min_size_gb OR @target_size_gb > @max_size_gb
BEGIN
    THROW 50002, '@target_size_gb must be within 4.50 and 5.00.', 1;
END;

IF @batch_rows < 10000
BEGIN
    THROW 50003, '@batch_rows must be >= 10000.', 1;
END;

PRINT 'Preparing schema...';

IF OBJECT_ID(N'ffengine.ffe_customers', N'U') IS NULL
BEGIN
    CREATE TABLE ffengine.ffe_customers
    (
        customer_id BIGINT IDENTITY(1,1) NOT NULL PRIMARY KEY,
        customer_ref VARCHAR(24) NOT NULL UNIQUE,
        first_name NVARCHAR(60) NOT NULL,
        last_name NVARCHAR(60) NOT NULL,
        email VARCHAR(180) NOT NULL,
        phone VARCHAR(24) NOT NULL,
        birth_date DATE NOT NULL,
        gender CHAR(1) NOT NULL,
        country_code CHAR(2) NOT NULL,
        city NVARCHAR(80) NOT NULL,
        loyalty_tier VARCHAR(20) NOT NULL,
        marketing_opt_in BIT NOT NULL,
        registered_at DATETIME2(0) NOT NULL,
        last_seen_at DATETIME2(0) NOT NULL,
        created_at DATETIME2(0) NOT NULL CONSTRAINT DF_ffe_customers_created_at DEFAULT SYSUTCDATETIME()
    );
END;

IF OBJECT_ID(N'ffengine.ffe_airports', N'U') IS NULL
BEGIN
    CREATE TABLE ffengine.ffe_airports
    (
        airport_id INT IDENTITY(1,1) NOT NULL PRIMARY KEY,
        iata_code CHAR(3) NOT NULL UNIQUE,
        airport_name NVARCHAR(140) NOT NULL,
        city NVARCHAR(80) NOT NULL,
        country_code CHAR(2) NOT NULL,
        timezone_offset_min SMALLINT NOT NULL
    );
END;

IF OBJECT_ID(N'ffengine.ffe_routes', N'U') IS NULL
BEGIN
    CREATE TABLE ffengine.ffe_routes
    (
        route_id INT IDENTITY(1,1) NOT NULL PRIMARY KEY,
        origin_airport_id INT NOT NULL,
        destination_airport_id INT NOT NULL,
        distance_km INT NOT NULL,
        is_international BIT NOT NULL,
        route_code AS (
            CONVERT(VARCHAR(3), origin_airport_id) + '-' + CONVERT(VARCHAR(3), destination_airport_id)
        ) PERSISTED
    );
END;

IF OBJECT_ID(N'ffengine.ffe_fare_classes', N'U') IS NULL
BEGIN
    CREATE TABLE ffengine.ffe_fare_classes
    (
        fare_class_id TINYINT IDENTITY(1,1) NOT NULL PRIMARY KEY,
        fare_class_code VARCHAR(16) NOT NULL UNIQUE,
        cabin_class VARCHAR(16) NOT NULL,
        base_multiplier DECIMAL(8,4) NOT NULL,
        changeable BIT NOT NULL,
        refundable BIT NOT NULL
    );
END;

IF OBJECT_ID(N'ffengine.ffe_flights', N'U') IS NULL
BEGIN
    CREATE TABLE ffengine.ffe_flights
    (
        flight_id BIGINT IDENTITY(1,1) NOT NULL PRIMARY KEY,
        flight_no VARCHAR(16) NOT NULL,
        route_id INT NOT NULL,
        scheduled_departure DATETIME2(0) NOT NULL,
        scheduled_arrival DATETIME2(0) NOT NULL,
        actual_departure DATETIME2(0) NULL,
        actual_arrival DATETIME2(0) NULL,
        aircraft_code VARCHAR(12) NOT NULL,
        seat_capacity SMALLINT NOT NULL,
        flight_status VARCHAR(20) NOT NULL,
        created_at DATETIME2(0) NOT NULL CONSTRAINT DF_ffe_flights_created_at DEFAULT SYSUTCDATETIME()
    );
END;

IF OBJECT_ID(N'ffengine.ffe_flight_requests', N'U') IS NULL
BEGIN
    CREATE TABLE ffengine.ffe_flight_requests
    (
        request_id BIGINT IDENTITY(1,1) NOT NULL PRIMARY KEY,
        customer_id BIGINT NOT NULL,
        route_id INT NOT NULL,
        request_ts DATETIME2(0) NOT NULL,
        travel_date DATE NOT NULL,
        return_date DATE NULL,
        trip_type VARCHAR(16) NOT NULL,
        adult_count TINYINT NOT NULL,
        child_count TINYINT NOT NULL,
        infant_count TINYINT NOT NULL,
        fare_class_id TINYINT NOT NULL,
        channel VARCHAR(16) NOT NULL,
        device_type VARCHAR(16) NOT NULL,
        locale VARCHAR(10) NOT NULL,
        request_status VARCHAR(20) NOT NULL,
        session_id CHAR(36) NOT NULL,
        is_weekend_request BIT NOT NULL,
        search_payload NVARCHAR(1200) NOT NULL,
        created_at DATETIME2(0) NOT NULL CONSTRAINT DF_ffe_flight_requests_created_at DEFAULT SYSUTCDATETIME()
    );
END;

IF OBJECT_ID(N'ffengine.ffe_bookings', N'U') IS NULL
BEGIN
    CREATE TABLE ffengine.ffe_bookings
    (
        booking_id BIGINT IDENTITY(1,1) NOT NULL PRIMARY KEY,
        request_id BIGINT NOT NULL,
        customer_id BIGINT NOT NULL,
        flight_id BIGINT NOT NULL,
        pnr_code CHAR(10) NOT NULL,
        booking_status VARCHAR(20) NOT NULL,
        booking_ts DATETIME2(0) NOT NULL,
        travel_date DATE NOT NULL,
        passenger_count TINYINT NOT NULL,
        channel VARCHAR(16) NOT NULL,
        created_at DATETIME2(0) NOT NULL CONSTRAINT DF_ffe_bookings_created_at DEFAULT SYSUTCDATETIME()
    );
END;

IF OBJECT_ID(N'ffengine.ffe_bookings', N'U') IS NOT NULL
BEGIN
    DECLARE @pnr_uq_name SYSNAME;

    SELECT TOP (1) @pnr_uq_name = kc.name
    FROM sys.key_constraints kc
    JOIN sys.index_columns ic
        ON ic.object_id = kc.parent_object_id
       AND ic.index_id = kc.unique_index_id
    JOIN sys.columns c
        ON c.object_id = ic.object_id
       AND c.column_id = ic.column_id
    WHERE kc.parent_object_id = OBJECT_ID(N'ffengine.ffe_bookings')
      AND kc.type = 'UQ'
      AND c.name = 'pnr_code';

    IF @pnr_uq_name IS NOT NULL
        EXEC(N'ALTER TABLE ffengine.ffe_bookings DROP CONSTRAINT [' + @pnr_uq_name + N']');

    IF COL_LENGTH(N'ffengine.ffe_bookings', N'pnr_code') <> 10
        ALTER TABLE ffengine.ffe_bookings ALTER COLUMN pnr_code CHAR(10) NOT NULL;

    IF NOT EXISTS (
        SELECT 1
        FROM sys.key_constraints
        WHERE parent_object_id = OBJECT_ID(N'ffengine.ffe_bookings')
          AND type = 'UQ'
          AND name = N'UQ_ffe_bookings_pnr_code'
    )
        ALTER TABLE ffengine.ffe_bookings
            ADD CONSTRAINT UQ_ffe_bookings_pnr_code UNIQUE (pnr_code);
END;

IF OBJECT_ID(N'ffengine.ffe_ticket_prices', N'U') IS NULL
BEGIN
    CREATE TABLE ffengine.ffe_ticket_prices
    (
        price_id BIGINT IDENTITY(1,1) NOT NULL PRIMARY KEY,
        booking_id BIGINT NOT NULL,
        fare_class_id TINYINT NOT NULL,
        currency_code CHAR(3) NOT NULL,
        base_fare DECIMAL(12,2) NOT NULL,
        tax_amount DECIMAL(12,2) NOT NULL,
        fee_amount DECIMAL(12,2) NOT NULL,
        discount_amount DECIMAL(12,2) NOT NULL,
        total_amount AS (base_fare + tax_amount + fee_amount - discount_amount) PERSISTED,
        priced_at DATETIME2(0) NOT NULL
    );
END;

PRINT 'Dropping and re-creating FK constraints for truncate...';

IF OBJECT_ID(N'ffengine.FK_ffe_routes_origin_airport', N'F') IS NOT NULL
    ALTER TABLE ffengine.ffe_routes DROP CONSTRAINT FK_ffe_routes_origin_airport;
IF OBJECT_ID(N'ffengine.FK_ffe_routes_destination_airport', N'F') IS NOT NULL
    ALTER TABLE ffengine.ffe_routes DROP CONSTRAINT FK_ffe_routes_destination_airport;
IF OBJECT_ID(N'ffengine.FK_ffe_flights_route', N'F') IS NOT NULL
    ALTER TABLE ffengine.ffe_flights DROP CONSTRAINT FK_ffe_flights_route;
IF OBJECT_ID(N'ffengine.FK_ffe_requests_customer', N'F') IS NOT NULL
    ALTER TABLE ffengine.ffe_flight_requests DROP CONSTRAINT FK_ffe_requests_customer;
IF OBJECT_ID(N'ffengine.FK_ffe_requests_route', N'F') IS NOT NULL
    ALTER TABLE ffengine.ffe_flight_requests DROP CONSTRAINT FK_ffe_requests_route;
IF OBJECT_ID(N'ffengine.FK_ffe_requests_fare', N'F') IS NOT NULL
    ALTER TABLE ffengine.ffe_flight_requests DROP CONSTRAINT FK_ffe_requests_fare;
IF OBJECT_ID(N'ffengine.FK_ffe_bookings_request', N'F') IS NOT NULL
    ALTER TABLE ffengine.ffe_bookings DROP CONSTRAINT FK_ffe_bookings_request;
IF OBJECT_ID(N'ffengine.FK_ffe_bookings_customer', N'F') IS NOT NULL
    ALTER TABLE ffengine.ffe_bookings DROP CONSTRAINT FK_ffe_bookings_customer;
IF OBJECT_ID(N'ffengine.FK_ffe_bookings_flight', N'F') IS NOT NULL
    ALTER TABLE ffengine.ffe_bookings DROP CONSTRAINT FK_ffe_bookings_flight;
IF OBJECT_ID(N'ffengine.FK_ffe_prices_booking', N'F') IS NOT NULL
    ALTER TABLE ffengine.ffe_ticket_prices DROP CONSTRAINT FK_ffe_prices_booking;
IF OBJECT_ID(N'ffengine.FK_ffe_prices_fare', N'F') IS NOT NULL
    ALTER TABLE ffengine.ffe_ticket_prices DROP CONSTRAINT FK_ffe_prices_fare;

TRUNCATE TABLE ffengine.ffe_ticket_prices;
TRUNCATE TABLE ffengine.ffe_bookings;
TRUNCATE TABLE ffengine.ffe_flight_requests;
TRUNCATE TABLE ffengine.ffe_flights;
TRUNCATE TABLE ffengine.ffe_routes;
TRUNCATE TABLE ffengine.ffe_fare_classes;
TRUNCATE TABLE ffengine.ffe_airports;
TRUNCATE TABLE ffengine.ffe_customers;

ALTER TABLE ffengine.ffe_routes
ADD CONSTRAINT FK_ffe_routes_origin_airport
    FOREIGN KEY (origin_airport_id) REFERENCES ffengine.ffe_airports(airport_id);
ALTER TABLE ffengine.ffe_routes
ADD CONSTRAINT FK_ffe_routes_destination_airport
    FOREIGN KEY (destination_airport_id) REFERENCES ffengine.ffe_airports(airport_id);
ALTER TABLE ffengine.ffe_flights
ADD CONSTRAINT FK_ffe_flights_route
    FOREIGN KEY (route_id) REFERENCES ffengine.ffe_routes(route_id);
ALTER TABLE ffengine.ffe_flight_requests
ADD CONSTRAINT FK_ffe_requests_customer
    FOREIGN KEY (customer_id) REFERENCES ffengine.ffe_customers(customer_id);
ALTER TABLE ffengine.ffe_flight_requests
ADD CONSTRAINT FK_ffe_requests_route
    FOREIGN KEY (route_id) REFERENCES ffengine.ffe_routes(route_id);
ALTER TABLE ffengine.ffe_flight_requests
ADD CONSTRAINT FK_ffe_requests_fare
    FOREIGN KEY (fare_class_id) REFERENCES ffengine.ffe_fare_classes(fare_class_id);
ALTER TABLE ffengine.ffe_bookings
ADD CONSTRAINT FK_ffe_bookings_request
    FOREIGN KEY (request_id) REFERENCES ffengine.ffe_flight_requests(request_id);
ALTER TABLE ffengine.ffe_bookings
ADD CONSTRAINT FK_ffe_bookings_customer
    FOREIGN KEY (customer_id) REFERENCES ffengine.ffe_customers(customer_id);
ALTER TABLE ffengine.ffe_bookings
ADD CONSTRAINT FK_ffe_bookings_flight
    FOREIGN KEY (flight_id) REFERENCES ffengine.ffe_flights(flight_id);
ALTER TABLE ffengine.ffe_ticket_prices
ADD CONSTRAINT FK_ffe_prices_booking
    FOREIGN KEY (booking_id) REFERENCES ffengine.ffe_bookings(booking_id);
ALTER TABLE ffengine.ffe_ticket_prices
ADD CONSTRAINT FK_ffe_prices_fare
    FOREIGN KEY (fare_class_id) REFERENCES ffengine.ffe_fare_classes(fare_class_id);

PRINT 'Seeding dimensions...';

INSERT INTO ffengine.ffe_airports (iata_code, airport_name, city, country_code, timezone_offset_min)
VALUES
('IST','Istanbul Airport','Istanbul','TR',180),
('ESB','Esenboga Airport','Ankara','TR',180),
('ADB','Adnan Menderes Airport','Izmir','TR',180),
('AYT','Antalya Airport','Antalya','TR',180),
('SAW','Sabiha Gokcen Airport','Istanbul','TR',180),
('LHR','Heathrow Airport','London','GB',0),
('CDG','Charles de Gaulle Airport','Paris','FR',60),
('FRA','Frankfurt Airport','Frankfurt','DE',60),
('AMS','Schiphol Airport','Amsterdam','NL',60),
('MAD','Madrid Barajas Airport','Madrid','ES',60),
('FCO','Fiumicino Airport','Rome','IT',60),
('JFK','John F. Kennedy Airport','New York','US',-300),
('EWR','Newark Liberty Airport','Newark','US',-300),
('YYZ','Toronto Pearson Airport','Toronto','CA',-300),
('DXB','Dubai International Airport','Dubai','AE',240),
('DOH','Hamad International Airport','Doha','QA',180),
('RUH','King Khalid Airport','Riyadh','SA',180),
('CAI','Cairo International Airport','Cairo','EG',120),
('ATH','Athens International Airport','Athens','GR',120),
('BEG','Nikola Tesla Airport','Belgrade','RS',60),
('VIE','Vienna International Airport','Vienna','AT',60),
('ZRH','Zurich Airport','Zurich','CH',60),
('BRU','Brussels Airport','Brussels','BE',60),
('CPH','Copenhagen Airport','Copenhagen','DK',60),
('ARN','Arlanda Airport','Stockholm','SE',60),
('HEL','Helsinki Airport','Helsinki','FI',120),
('BUD','Budapest Airport','Budapest','HU',60),
('WAW','Chopin Airport','Warsaw','PL',60),
('PRG','Vaclav Havel Airport','Prague','CZ',60),
('DUB','Dublin Airport','Dublin','IE',0);

INSERT INTO ffengine.ffe_fare_classes
    (fare_class_code, cabin_class, base_multiplier, changeable, refundable)
VALUES
    ('ECO_BASIC','economy',1.0000,0,0),
    ('ECO_FLEX','economy',1.1800,1,0),
    ('BUSI_STD','business',2.4000,1,1),
    ('BUSI_FLEX','business',2.9000,1,1);

;WITH airport_pairs AS
(
    SELECT
        a.airport_id AS origin_airport_id,
        b.airport_id AS destination_airport_id,
        ROW_NUMBER() OVER (ORDER BY ABS(CHECKSUM(CONCAT(a.iata_code, '-', b.iata_code)))) AS rn
    FROM ffengine.ffe_airports a
    CROSS JOIN ffengine.ffe_airports b
    WHERE a.airport_id <> b.airport_id
)
INSERT INTO ffengine.ffe_routes (origin_airport_id, destination_airport_id, distance_km, is_international)
SELECT TOP (240)
    p.origin_airport_id,
    p.destination_airport_id,
    250 + ABS(CHECKSUM(CONCAT('km', p.rn))) % 4200 AS distance_km,
    CASE
        WHEN a.country_code = b.country_code THEN 0
        ELSE 1
    END AS is_international
FROM airport_pairs p
JOIN ffengine.ffe_airports a ON p.origin_airport_id = a.airport_id
JOIN ffengine.ffe_airports b ON p.destination_airport_id = b.airport_id
ORDER BY p.rn;

PRINT 'Seeding customers...';

DECLARE @customer_seed_rows INT = 500000;
DECLARE @today DATE = CAST(SYSUTCDATETIME() AS DATE);

;WITH n AS
(
    SELECT TOP (@customer_seed_rows) ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS rn
    FROM sys.all_objects a
    CROSS JOIN sys.all_objects b
)
INSERT INTO ffengine.ffe_customers
(
    customer_ref, first_name, last_name, email, phone, birth_date, gender,
    country_code, city, loyalty_tier, marketing_opt_in, registered_at, last_seen_at
)
SELECT
    CONCAT('C', RIGHT(CONCAT('00000000', n.rn), 8)),
    CASE n.rn % 20
        WHEN 0 THEN N'Alex' WHEN 1 THEN N'Maria' WHEN 2 THEN N'Ahmet' WHEN 3 THEN N'Elif'
        WHEN 4 THEN N'John' WHEN 5 THEN N'Sarah' WHEN 6 THEN N'Mehmet' WHEN 7 THEN N'Zeynep'
        WHEN 8 THEN N'Omar' WHEN 9 THEN N'Lina' WHEN 10 THEN N'Ethan' WHEN 11 THEN N'Emma'
        WHEN 12 THEN N'Noah' WHEN 13 THEN N'Sofia' WHEN 14 THEN N'Kerem' WHEN 15 THEN N'Mina'
        WHEN 16 THEN N'Can' WHEN 17 THEN N'Ipek' WHEN 18 THEN N'Daniel' ELSE N'Laura'
    END,
    CASE n.rn % 20
        WHEN 0 THEN N'Yilmaz' WHEN 1 THEN N'Demir' WHEN 2 THEN N'Kaya' WHEN 3 THEN N'Koc'
        WHEN 4 THEN N'Smith' WHEN 5 THEN N'Johnson' WHEN 6 THEN N'Brown' WHEN 7 THEN N'Wilson'
        WHEN 8 THEN N'Taylor' WHEN 9 THEN N'Martin' WHEN 10 THEN N'White' WHEN 11 THEN N'King'
        WHEN 12 THEN N'Lopez' WHEN 13 THEN N'Hill' WHEN 14 THEN N'Scott' WHEN 15 THEN N'Green'
        WHEN 16 THEN N'Baker' WHEN 17 THEN N'Adams' WHEN 18 THEN N'Young' ELSE N'Clark'
    END,
    LOWER(CONCAT('c', n.rn, '@synthetic-mail.ffengine.local')),
    CONCAT('+90', RIGHT(CONCAT('0000000000', ABS(CHECKSUM(CONCAT('p', n.rn))) % 10000000000), 10)),
    DATEADD(DAY, -1 * (7000 + ABS(CHECKSUM(CONCAT('b', n.rn))) % 12000), @today),
    CASE WHEN n.rn % 2 = 0 THEN 'F' ELSE 'M' END,
    CASE n.rn % 10
        WHEN 0 THEN 'TR' WHEN 1 THEN 'DE' WHEN 2 THEN 'GB' WHEN 3 THEN 'FR' WHEN 4 THEN 'NL'
        WHEN 5 THEN 'AE' WHEN 6 THEN 'US' WHEN 7 THEN 'QA' WHEN 8 THEN 'IT' ELSE 'ES'
    END,
    CASE n.rn % 12
        WHEN 0 THEN N'Istanbul' WHEN 1 THEN N'Ankara' WHEN 2 THEN N'Izmir' WHEN 3 THEN N'Antalya'
        WHEN 4 THEN N'London' WHEN 5 THEN N'Paris' WHEN 6 THEN N'Dubai' WHEN 7 THEN N'New York'
        WHEN 8 THEN N'Rome' WHEN 9 THEN N'Madrid' WHEN 10 THEN N'Amsterdam' ELSE N'Doha'
    END,
    CASE
        WHEN n.rn % 100 < 5 THEN 'platinum'
        WHEN n.rn % 100 < 20 THEN 'gold'
        WHEN n.rn % 100 < 55 THEN 'silver'
        ELSE 'classic'
    END,
    CASE WHEN n.rn % 100 < 62 THEN 1 ELSE 0 END,
    DATEADD(DAY, -1 * (ABS(CHECKSUM(CONCAT('r', n.rn))) % 3650), @today),
    DATEADD(DAY, -1 * (ABS(CHECKSUM(CONCAT('l', n.rn))) % 365), @today)
FROM n;

PRINT 'Seeding flights (1 year)...';

DECLARE @day_count INT = DATEDIFF(DAY, @start_date, @end_date) + 1;

;WITH d AS
(
    SELECT TOP (@day_count) ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) - 1 AS day_offset
    FROM sys.all_objects
),
f AS
(
    SELECT 1 AS slot UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4
)
INSERT INTO ffengine.ffe_flights
(
    flight_no,
    route_id,
    scheduled_departure,
    scheduled_arrival,
    actual_departure,
    actual_arrival,
    aircraft_code,
    seat_capacity,
    flight_status
)
SELECT
    CONCAT('FF', RIGHT(CONCAT('0000', r.route_id), 4), f.slot),
    r.route_id,
    DATEADD(
        MINUTE,
        ((f.slot - 1) * 300) + (ABS(CHECKSUM(CONCAT('dep', r.route_id, d.day_offset, f.slot))) % 60),
        DATEADD(DAY, d.day_offset, CAST(@start_date AS DATETIME2(0)))
    ) AS scheduled_departure,
    DATEADD(
        MINUTE,
        ((f.slot - 1) * 300) + 60 + (r.distance_km / 7),
        DATEADD(DAY, d.day_offset, CAST(@start_date AS DATETIME2(0)))
    ) AS scheduled_arrival,
    NULL,
    NULL,
    CASE ABS(CHECKSUM(CONCAT('ac', r.route_id, f.slot))) % 4
        WHEN 0 THEN 'A320'
        WHEN 1 THEN 'B737'
        WHEN 2 THEN 'A321'
        ELSE 'B738'
    END,
    160 + (ABS(CHECKSUM(CONCAT('sc', r.route_id, d.day_offset, f.slot))) % 61),
    CASE
        WHEN ABS(CHECKSUM(CONCAT('st', r.route_id, d.day_offset, f.slot))) % 100 < 88 THEN 'completed'
        WHEN ABS(CHECKSUM(CONCAT('st', r.route_id, d.day_offset, f.slot))) % 100 < 96 THEN 'delayed'
        ELSE 'cancelled'
    END
FROM ffengine.ffe_routes r
CROSS JOIN d
CROSS JOIN f
WHERE f.slot <= CASE WHEN r.is_international = 1 THEN 3 ELSE 4 END;

-- Fill actual times after initial insert
UPDATE f
SET
    actual_departure =
        CASE
            WHEN f.flight_status = 'cancelled' THEN NULL
            ELSE DATEADD(MINUTE, ABS(CHECKSUM(CONCAT('ad', f.flight_id))) % 65, f.scheduled_departure)
        END,
    actual_arrival =
        CASE
            WHEN f.flight_status = 'cancelled' THEN NULL
            ELSE DATEADD(MINUTE, ABS(CHECKSUM(CONCAT('aa', f.flight_id))) % 95, f.scheduled_arrival)
        END
FROM ffengine.ffe_flights f;

PRINT 'Generating high-volume flight requests + bookings + pricing...';

DECLARE @customer_count BIGINT = (SELECT COUNT(*) FROM ffengine.ffe_customers);
DECLARE @route_count INT = (SELECT COUNT(*) FROM ffengine.ffe_routes);
DECLARE @fare_count INT = (SELECT COUNT(*) FROM ffengine.ffe_fare_classes);
DECLARE @flight_count BIGINT = (SELECT COUNT(*) FROM ffengine.ffe_flights);

DECLARE @summer_start DATE = DATEFROMPARTS(YEAR(@end_date), 6, 1);
DECLARE @summer_end DATE = DATEFROMPARTS(YEAR(@end_date), 8, 31);
IF @summer_start < @start_date
BEGIN
    SET @summer_start = DATEADD(YEAR, 1, @summer_start);
    SET @summer_end = DATEADD(YEAR, 1, @summer_end);
END;

DECLARE @summer_days INT = DATEDIFF(DAY, @summer_start, @summer_end) + 1;
IF @summer_days < 1 SET @summer_days = 92;

DECLARE @overall_days INT = DATEDIFF(DAY, @start_date, @end_date) + 1;
DECLARE @current_size_gb DECIMAL(18,3) = 0.0;
DECLARE @loop_guard INT = 0;
DECLARE @batch_seed INT;
DECLARE @req_min BIGINT;
DECLARE @req_max BIGINT;
DECLARE @new_request_rows BIGINT;
DECLARE @trim_batch_rows INT = 40000;

WHILE 1 = 1
BEGIN
    SET @loop_guard += 1;
    SET @batch_seed = ABS(CHECKSUM(NEWID()));
    SET @req_min = ISNULL((SELECT MAX(request_id) FROM ffengine.ffe_flight_requests), 0) + 1;

    ;WITH n AS
    (
        SELECT TOP (@batch_rows) ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS rn
        FROM sys.all_objects a
        CROSS JOIN sys.all_objects b
    ),
    req_seed AS
    (
        SELECT
            n.rn,
            ((ABS(CHECKSUM(CONCAT(@batch_seed, '-c-', n.rn))) % @customer_count) + 1) AS customer_id,
            ((ABS(CHECKSUM(CONCAT(@batch_seed, '-r-', n.rn))) % @route_count) + 1) AS route_id,
            ((ABS(CHECKSUM(CONCAT(@batch_seed, '-f-', n.rn))) % @fare_count) + 1) AS fare_class_id,
            ABS(CHECKSUM(CONCAT(@batch_seed, '-w-', n.rn))) % 100 AS weight_bucket,
            ABS(CHECKSUM(CONCAT(@batch_seed, '-t-', n.rn))) % 1440 AS minute_of_day
        FROM n
    ),
    req_dates AS
    (
        SELECT
            s.*,
            CASE
                WHEN s.weight_bucket < 35 THEN
                    DATEADD(DAY, ABS(CHECKSUM(CONCAT(@batch_seed, '-sd-', s.rn))) % @summer_days, @summer_start)
                ELSE
                    DATEADD(DAY, ABS(CHECKSUM(CONCAT(@batch_seed, '-od-', s.rn))) % @overall_days, @start_date)
            END AS request_date
        FROM req_seed s
    )
    INSERT INTO ffengine.ffe_flight_requests
    (
        customer_id,
        route_id,
        request_ts,
        travel_date,
        return_date,
        trip_type,
        adult_count,
        child_count,
        infant_count,
        fare_class_id,
        channel,
        device_type,
        locale,
        request_status,
        session_id,
        is_weekend_request,
        search_payload
    )
    SELECT
        d.customer_id,
        d.route_id,
        DATEADD(MINUTE, d.minute_of_day, CAST(d.request_date AS DATETIME2(0))),
        DATEADD(DAY, 1 + (ABS(CHECKSUM(CONCAT('td', @batch_seed, d.rn))) % 90), d.request_date),
        CASE
            WHEN ABS(CHECKSUM(CONCAT('rt', @batch_seed, d.rn))) % 100 < 46 THEN
                DATEADD(DAY, 2 + (ABS(CHECKSUM(CONCAT('rd', @batch_seed, d.rn))) % 21),
                    DATEADD(DAY, 1 + (ABS(CHECKSUM(CONCAT('td2', @batch_seed, d.rn))) % 90), d.request_date))
            ELSE NULL
        END AS return_date,
        CASE
            WHEN ABS(CHECKSUM(CONCAT('tt', @batch_seed, d.rn))) % 100 < 46 THEN 'round_trip'
            ELSE 'one_way'
        END AS trip_type,
        1 + (ABS(CHECKSUM(CONCAT('ac', @batch_seed, d.rn))) % 4),
        ABS(CHECKSUM(CONCAT('cc', @batch_seed, d.rn))) % 3,
        CASE WHEN ABS(CHECKSUM(CONCAT('ic', @batch_seed, d.rn))) % 100 < 14 THEN 1 ELSE 0 END,
        d.fare_class_id,
        CASE ABS(CHECKSUM(CONCAT('ch', @batch_seed, d.rn))) % 3
            WHEN 0 THEN 'web'
            WHEN 1 THEN 'mobile'
            ELSE 'agent'
        END AS channel,
        CASE ABS(CHECKSUM(CONCAT('dv', @batch_seed, d.rn))) % 4
            WHEN 0 THEN 'desktop'
            WHEN 1 THEN 'android'
            WHEN 2 THEN 'ios'
            ELSE 'tablet'
        END AS device_type,
        CASE ABS(CHECKSUM(CONCAT('lc', @batch_seed, d.rn))) % 5
            WHEN 0 THEN 'tr-TR'
            WHEN 1 THEN 'en-GB'
            WHEN 2 THEN 'en-US'
            WHEN 3 THEN 'de-DE'
            ELSE 'fr-FR'
        END AS locale,
        CASE
            WHEN ABS(CHECKSUM(CONCAT('rs', @batch_seed, d.rn))) % 100 < 70 THEN 'searched'
            WHEN ABS(CHECKSUM(CONCAT('rs', @batch_seed, d.rn))) % 100 < 94 THEN 'quoted'
            ELSE 'abandoned'
        END AS request_status,
        LOWER(CONVERT(CHAR(36), NEWID())),
        CASE WHEN DATEPART(WEEKDAY, d.request_date) IN (1, 7) THEN 1 ELSE 0 END AS is_weekend_request,
        CONCAT(
            N'{"source":"ffengine_web","utm":"summer_campaign","promo":"',
            RIGHT(CONCAT('000', ABS(CHECKSUM(CONCAT('pm', @batch_seed, d.rn))) % 1000), 3),
            N'","search_hash":"',
            RIGHT(CONCAT('000000', ABS(CHECKSUM(CONCAT('sh', @batch_seed, d.rn))) % 1000000), 6),
            N'","notes":"', REPLICATE(N'x', 700), N'"}'
        ) AS search_payload
    FROM req_dates d
    WHERE d.request_date BETWEEN @start_date AND @end_date;

    SET @req_max = ISNULL((SELECT MAX(request_id) FROM ffengine.ffe_flight_requests), 0);
    SET @new_request_rows = @req_max - @req_min + 1;
    IF @new_request_rows < 1 SET @new_request_rows = 0;

    INSERT INTO ffengine.ffe_bookings
    (
        request_id, customer_id, flight_id, pnr_code, booking_status, booking_ts, travel_date, passenger_count, channel
    )
    SELECT
        r.request_id,
        r.customer_id,
        1 + (ABS(CHECKSUM(CONCAT('fl', r.request_id))) % @flight_count) AS flight_id,
        CONCAT('P', RIGHT(CONCAT('000000000', r.request_id), 9)),
        CASE
            WHEN ABS(CHECKSUM(CONCAT('bs', r.request_id))) % 100 < 73 THEN 'ticketed'
            WHEN ABS(CHECKSUM(CONCAT('bs', r.request_id))) % 100 < 89 THEN 'cancelled'
            ELSE 'no_show'
        END AS booking_status,
        DATEADD(MINUTE, ABS(CHECKSUM(CONCAT('bt', r.request_id))) % 180, r.request_ts),
        r.travel_date,
        r.adult_count + r.child_count + r.infant_count,
        r.channel
    FROM ffengine.ffe_flight_requests r
    WHERE r.request_id BETWEEN @req_min AND @req_max
      AND ABS(CHECKSUM(CONCAT('conv', r.request_id))) % 100 < 41;

    INSERT INTO ffengine.ffe_ticket_prices
    (
        booking_id, fare_class_id, currency_code, base_fare, tax_amount, fee_amount, discount_amount, priced_at
    )
    SELECT
        b.booking_id,
        r.fare_class_id,
        CASE
            WHEN c.country_code IN ('TR', 'DE', 'FR', 'NL', 'IT', 'ES') THEN 'EUR'
            WHEN c.country_code IN ('US', 'CA') THEN 'USD'
            ELSE 'USD'
        END AS currency_code,
        CAST((60 + (rt.distance_km * 0.18) + (fc.base_multiplier * 45) +
             ((ABS(CHECKSUM(CONCAT('sx', b.booking_id))) % 40) * 1.7)) AS DECIMAL(12,2)) AS base_fare,
        CAST((12 + (rt.distance_km * 0.04)) AS DECIMAL(12,2)) AS tax_amount,
        CAST((8 + (ABS(CHECKSUM(CONCAT('fx', b.booking_id))) % 18)) AS DECIMAL(12,2)) AS fee_amount,
        CAST((CASE WHEN b.booking_status = 'cancelled' THEN 25 ELSE ABS(CHECKSUM(CONCAT('dx', b.booking_id))) % 22 END) AS DECIMAL(12,2)) AS discount_amount,
        DATEADD(MINUTE, 1 + (ABS(CHECKSUM(CONCAT('px', b.booking_id))) % 60), b.booking_ts)
    FROM ffengine.ffe_bookings b
    JOIN ffengine.ffe_flight_requests r ON r.request_id = b.request_id
    JOIN ffengine.ffe_routes rt ON rt.route_id = r.route_id
    JOIN ffengine.ffe_customers c ON c.customer_id = b.customer_id
    JOIN ffengine.ffe_fare_classes fc ON fc.fare_class_id = r.fare_class_id
    WHERE b.request_id BETWEEN @req_min AND @req_max;

    SELECT
        @current_size_gb = CAST(SUM(ps.reserved_page_count) * 8.0 / 1024 / 1024 AS DECIMAL(18,3))
    FROM sys.dm_db_partition_stats ps
    JOIN sys.tables t ON t.object_id = ps.object_id
    WHERE t.name LIKE 'ffe[_]%';

    PRINT CONCAT(
        'Batch ', @loop_guard,
        ' | requests=', @new_request_rows,
        ' | size_gb=', @current_size_gb
    );

    IF @current_size_gb >= @target_size_gb
        BREAK;
    IF @current_size_gb >= @max_size_gb
        BREAK;
    IF @loop_guard >= 200
        BREAK;
END;

-- Hard cap enforcement: if last batch crossed 5.0 GB, trim newest request chains.
WHILE @current_size_gb > @max_size_gb
BEGIN
    ;WITH tail_req AS
    (
        SELECT TOP (@trim_batch_rows) request_id
        FROM ffengine.ffe_flight_requests
        ORDER BY request_id DESC
    )
    DELETE p
    FROM ffengine.ffe_ticket_prices p
    JOIN ffengine.ffe_bookings b ON b.booking_id = p.booking_id
    JOIN tail_req t ON t.request_id = b.request_id;

    ;WITH tail_req AS
    (
        SELECT TOP (@trim_batch_rows) request_id
        FROM ffengine.ffe_flight_requests
        ORDER BY request_id DESC
    )
    DELETE b
    FROM ffengine.ffe_bookings b
    JOIN tail_req t ON t.request_id = b.request_id;

    ;WITH tail_req AS
    (
        SELECT TOP (@trim_batch_rows) request_id
        FROM ffengine.ffe_flight_requests
        ORDER BY request_id DESC
    )
    DELETE r
    FROM ffengine.ffe_flight_requests r
    JOIN tail_req t ON t.request_id = r.request_id;

    SELECT
        @current_size_gb = CAST(SUM(ps.reserved_page_count) * 8.0 / 1024 / 1024 AS DECIMAL(18,3))
    FROM sys.dm_db_partition_stats ps
    JOIN sys.tables t ON t.object_id = ps.object_id
    WHERE t.name LIKE 'ffe[_]%';

    PRINT CONCAT('Trim applied | size_gb=', @current_size_gb);

    IF NOT EXISTS (SELECT 1 FROM ffengine.ffe_flight_requests)
        BREAK;
END;

IF EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'ffengine.ffe_flight_requests') AND name = N'IX_ffe_requests_request_ts')
    DROP INDEX IX_ffe_requests_request_ts ON ffengine.ffe_flight_requests;
CREATE INDEX IX_ffe_requests_request_ts ON ffengine.ffe_flight_requests(request_ts);

IF EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'ffengine.ffe_flight_requests') AND name = N'IX_ffe_requests_travel_date')
    DROP INDEX IX_ffe_requests_travel_date ON ffengine.ffe_flight_requests;
CREATE INDEX IX_ffe_requests_travel_date ON ffengine.ffe_flight_requests(travel_date);

IF EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'ffengine.ffe_bookings') AND name = N'IX_ffe_bookings_booking_ts')
    DROP INDEX IX_ffe_bookings_booking_ts ON ffengine.ffe_bookings;
CREATE INDEX IX_ffe_bookings_booking_ts ON ffengine.ffe_bookings(booking_ts);

IF EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'ffengine.ffe_ticket_prices') AND name = N'IX_ffe_prices_currency')
    DROP INDEX IX_ffe_prices_currency ON ffengine.ffe_ticket_prices;
CREATE INDEX IX_ffe_prices_currency ON ffengine.ffe_ticket_prices(currency_code);

PRINT 'Final size summary:';

EXEC sp_spaceused N'ffengine.ffe_customers';
EXEC sp_spaceused N'ffengine.ffe_flight_requests';
EXEC sp_spaceused N'ffengine.ffe_bookings';
EXEC sp_spaceused N'ffengine.ffe_ticket_prices';

SELECT
    CAST(SUM(ps.reserved_page_count) * 8.0 / 1024 / 1024 AS DECIMAL(18,3)) AS total_ffe_size_gb
FROM sys.dm_db_partition_stats ps
JOIN sys.tables t ON t.object_id = ps.object_id
WHERE t.name LIKE 'ffe[_]%';

SELECT
    CAST(request_ts AS DATE) AS request_day,
    COUNT(*) AS request_count
FROM ffengine.ffe_flight_requests
GROUP BY CAST(request_ts AS DATE)
ORDER BY request_day;

PRINT 'Seed completed.';

