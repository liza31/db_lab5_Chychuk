-- Setup auxiliary `missiles_test` table with test data for the `missiles` table population

-- -- Drop table if already exists

DROP TABLE IF EXISTS missiles_test;

-- -- Create table from scratch

CREATE TABLE missiles_test
(
    missile_id      INT         GENERATED ALWAYS AS IDENTITY,

    model_name      VARCHAR     NOT NULL,


    PRIMARY KEY (missile_id)
);

-- -- Populate table with test data

INSERT INTO
    missiles_test (model_name)
VALUES
    ('Shahed-136/131'),
    ('X-101/X-555'),
    ('Kalibr'),
    ('X-22'),
    ('X-59'),
    ('X-47 Kinzhal');


-- Execute insertion of test data from `missiles_test` table into `missiles` table in loop

DO $$
    DECLARE
        missile_model_name missiles_test.model_name%TYPE;
    BEGIN
        FOR missile_model_name IN SELECT model_name FROM missiles_test
            LOOP
                IF NOT exists(SELECT 1 FROM missiles WHERE missiles.model_name = missile_model_name) THEN
                    INSERT INTO missiles (model_name) VALUES (missile_model_name);
                END IF;
            END LOOP;
    END;
$$;


-- Drop auxiliary `missiles_test` table

DROP TABLE missiles_test;
