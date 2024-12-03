CREATE DATABASE hospitality_analytics;
USE hospitality_analytics;
CREATE TABLE dim_date (
    date DATE,                -- Represents dates in May, June, and July
    mmm_yy VARCHAR(7),        -- Date in the format of mmm yy (e.g., "May 23")
    week_no INT,              -- Unique week number for each date
    day_type VARCHAR(10)      -- Indicates whether the day is "Weekend" or "Weekday"
);
ALTER TABLE dim_date MODIFY COLUMN week_no VARCHAR(10);

CREATE TABLE dim_hotels (
    property_id INT PRIMARY KEY,    -- Unique ID for each hotel
    property_name VARCHAR(255),     -- Name of the hotel/property
    category VARCHAR(50),           -- Hotel category (e.g., "Luxury", "Business")
    city VARCHAR(100)               -- City where the hotel is located
);

CREATE TABLE dim_rooms (
    room_id VARCHAR(10) PRIMARY KEY, -- Type of room (e.g., "RT1", "RT2", etc.)
    room_class VARCHAR(50)           -- Class of room (e.g., "Standard", "Elite", "Premium", "Presidential")
);

CREATE TABLE fact_aggregated_bookings (
    property_id INT,                    -- Foreign key referencing dim_hotels
    check_in_date DATE,                 -- Check-in date of customers
    room_category VARCHAR(10),          -- Type of room (e.g., "RT1", "RT2")
    successful_bookings INT,            -- Number of successful bookings for that room type on that date
    capacity INT,                       -- Maximum room capacity for that room type on that date
    FOREIGN KEY (property_id) REFERENCES dim_hotels(property_id),
    FOREIGN KEY (room_category) REFERENCES dim_rooms(room_id)
);

CREATE TABLE fact_bookings (
    booking_id INT PRIMARY KEY,                  -- Unique Booking ID for each customer
    property_id INT,                             -- Foreign key referencing dim_hotels
    booking_date DATE,                           -- Date when the customer booked the room
    check_in_date DATE,                          -- Check-in date
    check_out_date DATE,                         -- Check-out date
    no_guests INT,                               -- Number of guests who stayed in the room
    room_category VARCHAR(10),                   -- Type of room booked (e.g., "RT1")
    booking_platform VARCHAR(50),                -- Booking platform used by the customer
    ratings_given INT,                           -- Ratings given by the customer
    booking_status VARCHAR(50),                  -- Booking status (e.g., "Cancelled", "Checked Out", "No show")
    revenue_generated DECIMAL(10, 2),            -- Revenue generated from the booking
    revenue_realized DECIMAL(10, 2),             -- Final revenue after adjustments based on booking status
    FOREIGN KEY (property_id) REFERENCES dim_hotels(property_id),
    FOREIGN KEY (room_category) REFERENCES dim_rooms(room_id)
);
ALTER TABLE fact_bookings MODIFY COLUMN booking_id VARCHAR(50);
ALTER TABLE fact_bookings MODIFY COLUMN ratings_given INT NULL;
ALTER TABLE fact_bookings MODIFY COLUMN ratings_given VARCHAR(5);

-- paths 
LOAD DATA INFILE 'C:\\ProgramData\\MySQL\\MySQL Server 8.0\\Uploads\\dim_date.csv'
INTO TABLE dim_date
FIELDS TERMINATED BY ',' ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

LOAD DATA INFILE 'C:\\ProgramData\\MySQL\\MySQL Server 8.0\\Uploads\\dim_hotels.csv'
INTO TABLE dim_hotels
FIELDS TERMINATED BY ',' ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

LOAD DATA INFILE 'C:\\ProgramData\\MySQL\\MySQL Server 8.0\\Uploads\\dim_rooms.csv'
INTO TABLE dim_rooms
FIELDS TERMINATED BY ',' ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

LOAD DATA INFILE 'C:\\ProgramData\\MySQL\\MySQL Server 8.0\\Uploads\\fact_aggregated_bookings.csv'
INTO TABLE fact_aggregated_bookings
FIELDS TERMINATED BY ',' ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

LOAD DATA INFILE 'C:\\ProgramData\\MySQL\\MySQL Server 8.0\\Uploads\\fact_bookings.csv'
INTO TABLE fact_bookings
FIELDS TERMINATED BY ',' ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

-- TOTAL REVENUE
SELECT SUM(revenue_realized) AS total_revenue
FROM fact_bookings;

-- Occupancy Rate
SELECT 
    SUM(successful_bookings) / SUM(capacity) * 100 AS occupancy_rate
FROM fact_aggregated_bookings;

-- Cancellation Rate
SELECT 
    COUNT(*) AS total_bookings,
    SUM(CASE WHEN booking_status = 'Cancelled' THEN 1 ELSE 0 END) AS cancellations,
    (SUM(CASE WHEN booking_status = 'Cancelled' THEN 1 ELSE 0 END) / COUNT(*)) * 100 AS cancellation_rate
FROM fact_bookings;

-- Total Bookings
SELECT COUNT(*) AS total_bookings
FROM fact_bookings;

-- Utilized Capacity
SELECT 
    SUM(successful_bookings) AS utilized_rooms,
    SUM(capacity) AS total_capacity,
    (SUM(successful_bookings) / SUM(capacity)) * 100 AS utilized_capacity_percentage
FROM fact_aggregated_bookings;

-- Trend Analysis (Booking and Revenue Trends Over Time)
SELECT 
    DATE_FORMAT(booking_date, '%Y-%m') AS month,
    COUNT(*) AS total_bookings,
    SUM(revenue_realized) AS total_revenue
FROM fact_bookings
GROUP BY month
ORDER BY month;

-- Weekday & Weekend Revenue and Bookings
SELECT 
    d.day_type,
    COUNT(b.booking_id) AS total_bookings,
    SUM(b.revenue_realized) AS total_revenue
FROM fact_bookings b
JOIN dim_date d ON b.check_in_date = d.date
GROUP BY d.day_type;

-- Revenue by State & Hotel
SELECT 
    h.city,
    h.property_id,
    SUM(b.revenue_realized) AS total_revenue
FROM fact_bookings b
JOIN dim_hotels h ON b.property_id = h.property_id
GROUP BY h.city, h.property_id;

-- Class-Wise Revenue
SELECT 
    room_category,
    SUM(revenue_realized) AS class_revenue
FROM fact_bookings
GROUP BY room_category;

-- Booking Status Analysis (Checked-out, Cancelled, No-show)
SELECT 
    booking_status,
    COUNT(*) AS total_count
FROM fact_bookings
GROUP BY booking_status;

-- Weekly Key Trend (Revenue, Total Bookings, Occupancy)
CREATE TEMPORARY TABLE temp_aggregated_bookings AS
SELECT 
    d.week_no,
    COUNT(b.booking_id) AS total_bookings,
    SUM(b.revenue_realized) AS total_revenue
FROM fact_bookings b
JOIN dim_date d ON b.check_in_date = d.date
GROUP BY d.week_no;

CREATE TEMPORARY TABLE temp_aggregated_capacity AS
SELECT 
    d.week_no,
    SUM(a.successful_bookings) AS total_successful_bookings,
    SUM(a.capacity) AS total_capacity
FROM fact_aggregated_bookings a
JOIN dim_date d ON a.check_in_date = d.date
GROUP BY d.week_no;

SELECT 
    ab.week_no,
    ab.total_bookings,
    ab.total_revenue,
    (ac.total_successful_bookings / ac.total_capacity) * 100 AS occupancy_rate
FROM temp_aggregated_bookings ab
JOIN temp_aggregated_capacity ac ON ab.week_no = ac.week_no;

SET SESSION net_read_timeout = 120;
SET SESSION net_write_timeout = 120;


























