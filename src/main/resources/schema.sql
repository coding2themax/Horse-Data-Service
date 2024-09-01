CREATE SCHEMA  IF NOT EXISTS stable;


CREATE TABLE stable.horse_status (
  horse_status_id NUMERIC(3) PRIMARY KEY,
  horse_status VARCHAR(255)
);

CREATE TABLE IF NOT EXISTS stable.horses (
  horse_id NUMERIC(3) NOT NULL PRIMARY KEY,
  horse_status_id NUMERIC(3) NOT NULL,
  horse_name VARCHAR(255),
  breed VARCHAR(255),
  age NUMERIC(3),
  color VARCHAR(255),
  coat_pattern VARCHAR(255),
  training_level VARCHAR(255),
  CONSTRAINT fk_horse_status_id FOREIGN KEY (horse_status_id) REFERENCES stable.horse_status(horse_status_id)
);

CREATE TABLE stable.horse_goods (
  goods_id NUMERIC(3) PRIMARY KEY,
  goods_name VARCHAR(255),
  good_description VARCHAR(255)

);

INSERT INTO stable.horse_status (horse_status_id, horse_status) VALUES (1, 'Active');
INSERT INTO stable.horse_status (horse_status_id, horse_status) VALUES (2, 'Inactive');

INSERT INTO stable.horses (horse_id, horse_status_id, horse_name, breed, age, color, coat_pattern, training_level) VALUES (1, 1, 'Horse1', 'Thoroughbred', 5, 'Brown', 'Solid', 'Advanced');
INSERT INTO stable.horses (horse_id, horse_status_id, horse_name, breed, age, color, coat_pattern, training_level) VALUES (2, 1, 'Horse2', 'Quarter Horse', 8, 'Chestnut', 'Spotted', 'Intermediate');
INSERT INTO stable.horses (horse_id, horse_status_id, horse_name, breed, age, color, coat_pattern, training_level) VALUES (3, 2, 'Horse3', 'Arabian', 10, 'Gray', 'Pinto', 'Beginner');