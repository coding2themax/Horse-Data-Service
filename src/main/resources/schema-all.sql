CREATE SCHEMA stable;
CREATE TABLE stable.horses (
  horse_id NUMERIC(3) PRIMARY KEY,
    horse_status_id NUMERIC(3) ,
  horse_name VARCHAR(255),
  breed VARCHAR(255),
  age NUMERIC(3),
  color VARCHAR(255),
  coat_pattern VARCHAR(255),
  training_level VARCHAR(255),
  CONSTRAINT fk_horse_status_id FOREIGN KEY (horse_status_id) REFERENCES stable.horse_status(horse_status_id)
);
CREATE TABLE stable.horse_status (
  horse_status_id NUMERIC(3) PRIMARY KEY,
  horse_status VARCHAR(255),
  last_updated TIMESTAMP
);

INSERT INTO stable.horse_status (horse_status_id, horse_status, last_updated) VALUES (1, 'Active', CURRENT_TIMESTAMP);
INSERT INTO stable.horse_status (horse_status_id, horse_status, last_updated) VALUES (2, 'Inactive', CURRENT_TIMESTAMP);

INSERT INTO stable.horses (horse_id, horse_status_id, horse_name, breed, age, color, coat_pattern, training_level) VALUES (1, 1, 'Horse1', 'Thoroughbred', 5, 'Brown', 'Solid', 'Advanced');
INSERT INTO stable.horses (horse_id, horse_status_id, horse_name, breed, age, color, coat_pattern, training_level) VALUES (2, 1, 'Horse2', 'Quarter Horse', 8, 'Chestnut', 'Spotted', 'Intermediate');
INSERT INTO stable.horses (horse_id, horse_status_id, horse_name, breed, age, color, coat_pattern, training_level) VALUES (3, 2, 'Horse3', 'Arabian', 10, 'Gray', 'Pinto', 'Beginner');