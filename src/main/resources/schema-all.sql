CREATE SCHEMA stable;
CREATE TABLE stable.horses (
  horse_id NUMERIC(3) PRIMARY KEY,
  horse_name VARCHAR(255),
  breed VARCHAR(255),
  age NUMERIC(3),
    color VARCHAR(255),
  coat_pattern VARCHAR(255),
  training_level VARCHAR(255)
);