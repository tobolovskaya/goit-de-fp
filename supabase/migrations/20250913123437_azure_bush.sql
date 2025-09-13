-- SQL script to create the output table for enriched athlete statistics
-- Run this script in your MySQL database before starting the streaming pipeline

USE olympic_dataset;

-- Create table for storing enriched athlete statistics
CREATE TABLE IF NOT EXISTS enriched_athlete_stats (
    id INT AUTO_INCREMENT PRIMARY KEY,
    sport VARCHAR(100) NOT NULL,
    medal_status VARCHAR(20) NOT NULL,
    sex VARCHAR(10) NOT NULL,
    country_noc VARCHAR(10) NOT NULL,
    avg_height DECIMAL(5,2) NOT NULL,
    avg_weight DECIMAL(5,2) NOT NULL,
    timestamp TIMESTAMP NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_sport (sport),
    INDEX idx_medal_status (medal_status),
    INDEX idx_country (country_noc),
    INDEX idx_timestamp (timestamp)
);

-- Create index for better query performance
CREATE INDEX idx_sport_medal_country ON enriched_athlete_stats (sport, medal_status, country_noc);

-- Show table structure
DESCRIBE enriched_athlete_stats;