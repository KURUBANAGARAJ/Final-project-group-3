-- Create the data warehouse database if it doesn't exist
CREATE DATABASE IF NOT EXISTS datawarehouse;

-- Switch to the newly created data warehouse database
USE datawarehouse;

-- Create dimension tables
CREATE TABLE IF NOT EXISTS DimLocation (
    location_id INT AUTO_INCREMENT PRIMARY KEY,
    location_description VARCHAR(255) NOT NULL,
    address VARCHAR(255) NOT NULL
);

CREATE TABLE IF NOT EXISTS DimCrimeType (
    crime_type_id INT AUTO_INCREMENT PRIMARY KEY,
    iucr VARCHAR(255) NOT NULL,
    primary_type VARCHAR(255) NOT NULL,
    description VARCHAR(255) NOT NULL
);

-- Create fact table
CREATE TABLE IF NOT EXISTS FactCrime (
    id INT AUTO_INCREMENT PRIMARY KEY,
    date DATE NOT NULL,
    location_id INT NOT NULL,
    crime_type_id INT NOT NULL,
    arrest BOOLEAN NOT NULL,
    student_count_total INT NOT NULL,
    student_count_low_income INT NOT NULL,
    student_count_special_ed INT NOT NULL,
    student_count_english_learners INT NOT NULL,
    student_count_black INT NOT NULL,
    student_count_hispanic INT NOT NULL,
    student_count_white INT NOT NULL,
    student_count_asian INT NOT NULL,
    student_count_native_american INT NOT NULL,
    student_count_other_ethnicity INT NOT NULL,
    student_count_asian_pacific_islander INT NOT NULL,
    student_count_multi INT NOT NULL,
    student_count_hawaiian_pacific_islander INT NOT NULL,
    student_count_ethnicity_not_available INT NOT NULL,
    college_enrollment_rate_mean FLOAT NOT NULL,
    graduation_rate_mean FLOAT NOT NULL,
    overall_rating INT NOT NULL,
    FOREIGN KEY (location_id) REFERENCES DimLocation(location_id),
    FOREIGN KEY (crime_type_id) REFERENCES DimCrimeType(crime_type_id)
);

-- Create stored procedure to populate dimension and fact tables
DELIMITER //

CREATE PROCEDURE PopulateDimensionFactTables()
BEGIN
    -- Insert distinct locations into DimLocation
    INSERT INTO DimLocation (location_description, address)
    SELECT DISTINCT location_description, address FROM aggregated_database1.aggregated_data;

    -- Insert distinct crime types into DimCrimeType
    INSERT INTO DimCrimeType (iucr, primary_type, description)
    SELECT DISTINCT iucr, primary_type, description FROM aggregated_database1.aggregated_data;

    -- Insert data into FactCrime with foreign key references
    INSERT INTO FactCrime (date, location_id, crime_type_id, arrest, student_count_total, student_count_low_income, student_count_special_ed, student_count_english_learners, student_count_black, student_count_hispanic, student_count_white, student_count_asian, student_count_native_american, student_count_other_ethnicity, student_count_asian_pacific_islander, student_count_multi, student_count_hawaiian_pacific_islander, student_count_ethnicity_not_available, college_enrollment_rate_mean, graduation_rate_mean, overall_rating)
    SELECT 
        date, 
        (SELECT location_id FROM DimLocation WHERE location_description = aggregated_data.location_description AND address = aggregated_data.address),
        (SELECT crime_type_id FROM DimCrimeType WHERE iucr = aggregated_data.iucr AND primary_type = aggregated_data.primary_type AND description = aggregated_data.description),
        arrest,
        student_count_total,
        student_count_low_income,
        student_count_special_ed,
        student_count_english_learners,
        student_count_black,
        student_count_hispanic,
        student_count_white,
        student_count_asian,
        student_count_native_american,
        student_count_other_ethnicity,
        student_count_asian_pacific_islander,
        student_count_multi,
        student_count_hawaiian_pacific_islander,
        student_count_ethnicity_not_available,
        college_enrollment_rate_mean,
        graduation_rate_mean,
        overall_rating
    FROM aggregated_database1.aggregated_data;
END //

DELIMITER ;

