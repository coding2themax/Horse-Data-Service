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

INSERT INTO stable.horses (horse_id, horse_name, breed, age, color, coat_pattern, training_level) VALUES (1, 'Horse1', 'Thoroughbred', 5, 'Brown', 'Solid', 'Advanced'), (2, 'Horse2', 'Quarter Horse', 8, 'Chestnut', 'Spotted', 'Intermediate'), (3, 'Horse3', 'Arabian', 4, 'Gray', 'Pinto', 'Beginner'); 