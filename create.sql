-- Primary entities related tables

-- -- Attack entity table

CREATE TABLE attacks
(
    attack_id           INT         GENERATED ALWAYS AS IDENTITY,

    start_datetime      TIMESTAMP   NOT NULL,
    end_datetime        TIMESTAMP   NOT NULL,

    info_source         VARCHAR     NOT NULL,


    PRIMARY KEY (attack_id)
);

-- -- Attack group entity table & connections

CREATE TABLE attack_groups
(
    group_id            INT         GENERATED ALWAYS AS IDENTITY,

    attack_id           INT         NOT NULL,

    units_launched      INT         NULL,
    units_destroyed     INT         NULL,


    PRIMARY KEY (group_id),

    FOREIGN KEY (attack_id) REFERENCES attacks (attack_id)
);


-- Secondary entities related tables

-- -- Potential target entity related tables

CREATE TABLE potential_targets
(
    target_id           INT         GENERATED ALWAYS AS IDENTITY,

    general_name        VARCHAR     NOT NULL,


    PRIMARY KEY (target_id)
);

CREATE TABLE group_targets
(
    group_id            INT,
    target_id           INT,


    PRIMARY KEY (group_id, target_id),

    FOREIGN KEY (group_id) REFERENCES attack_groups (group_id),
    FOREIGN KEY (target_id) REFERENCES potential_targets (target_id)
);

-- -- Launch place entity related tables

CREATE TABLE launch_places
(
    place_id            INT         GENERATED ALWAYS AS IDENTITY,

    general_name        VARCHAR     NOT NULL,


    PRIMARY KEY (place_id)
);

CREATE TABLE group_launch_places
(
    group_id            INT,
    place_id            INT,


    PRIMARY KEY (group_id, place_id),

    FOREIGN KEY (group_id) REFERENCES attack_groups (group_id),
    FOREIGN KEY (place_id) REFERENCES launch_places (place_id)
);

-- -- Missile entity related tables

CREATE TABLE missiles
(
    missile_id          INT         GENERATED ALWAYS AS IDENTITY,

    model_name          VARCHAR     NOT NULL,


    PRIMARY KEY (missile_id)
);

CREATE TABLE group_missiles
(
    group_id           INT,
    missile_id          INT,


    PRIMARY KEY (group_id, missile_id),

    FOREIGN KEY (group_id) REFERENCES attack_groups (group_id),
    FOREIGN KEY (missile_id) REFERENCES missiles (missile_id)
);
