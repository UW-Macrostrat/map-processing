DROP TABLE IF EXISTS gmus.geounits_macrounits_types;
CREATE TABLE gmus.geounits_macrounits_types (
  id serial PRIMARY KEY NOT NULL,
  type character varying(200)
);

INSERT INTO gmus.geounits_macrounits_types (id, type) VALUES 
(1, 'mbr_name-unit_name'), 
(2, 'fm_name-unit_name'),
(3, 'gp_name-unit_name'), 
(4, 'sgp_name-unit_name'),

(5, 'mbr_name-strat_unit'), 
(6, 'fm_name-strat_unit'),
(7, 'gp_name-strat_unit'), 
(8, 'sgp_name-strat_unit'), 

(9, 'mbr_name-unitdesc'),
(10, 'fm_name-unitdesc'),
(11, 'gp_name-unitdesc'),
(12, 'sgp_name-unitdesc'),

(13, 'mbr_name-unit_com'),
(14, 'fm_name-unit_com'),
(15, 'gp_name-unit_com'),
(16, 'sgp_name-unit_com'),

(17, 'ns-mbr_name-unit_name'), 
(18, 'ns-fm_name-unit_name'),
(19, 'ns-gp_name-unit_name'), 
(20, 'ns-sgp_name-unit_name'), 

(21, 'ns-mbr_name-strat_unit'), 
(22, 'ns-fm_name-strat_unit'),
(23, 'ns-gp_name-strat_unit'), 
(24, 'ns-sgp_name-strat_unit'), 

(25, 'ns-mbr_name-unitdesc'),
(26, 'ns-fm_name-unitdesc'),
(27, 'ns-gp_name-unitdesc'),
(28, 'ns-sgp_name-unitdesc'),

(29, 'ns-mbr_name-unit_com'),
(30, 'ns-fm_name-unit_com'),
(31, 'ns-gp_name-unit_com'),
(32, 'ns-sgp_name-unit_com'),
(88, 'misses'),
(99, 'manual-matches');

DROP TABLE IF EXISTS gmus.geounits_macrounits;

CREATE TABLE gmus.geounits_macrounits (
  id serial PRIMARY KEY NOT NULL,
  geologic_unit_gid integer NOT NULL,
  unit_id integer NOT NULL,
  strat_name_id integer NOT NULL,
  unit_link character varying(50),
  type integer NOT NULL,
  verified Boolean DEFAULT FALSE,
  match Boolean DEFAULT null,
  age_diff numeric DEFAULT null
);


