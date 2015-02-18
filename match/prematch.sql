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

(5, 'mbr_name-unitdesc'),
(6, 'fm_name-unitdesc'),
(7, 'gp_name-unitdesc'),
(8, 'sgp_name-unitdesc'),

(9, 'mbr_name-unit_com'),
(10, 'fm_name-unit_com'),
(11, 'gp_name-unit_com'),
(12, 'sgp_name-unit_com'),

(13, 'ns-mbr_name-unit_name'), 
(14, 'ns-fm_name-unit_name'),
(15, 'ns-gp_name-unit_name'), 
(16, 'ns-sgp_name-unit_name'), 

(17, 'ns-mbr_name-unitdesc'),
(18, 'ns-fm_name-unitdesc'),
(19, 'ns-gp_name-unitdesc'),
(20, 'ns-sgp_name-unitdesc'),

(21, 'ns-mbr_name-unit_com'),
(22, 'ns-fm_name-unit_com'),
(23, 'ns-gp_name-unit_com'),
(24, 'ns-sgp_name-unit_com');

DROP TABLE IF EXISTS gmus.geounits_macrounits_redo;

CREATE TABLE gmus.geounits_macrounits_redo (
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


