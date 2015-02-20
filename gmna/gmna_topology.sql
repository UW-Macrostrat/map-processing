ALTER TABLE gmna.geologic_units ADD COLUMN geom_clean geometry;
Update gmna.geologic_units SET geom_clean = st_snaptogrid(st_buffer(st_makevalid(geom),0),0.00001);

SELECT CreateTopology('gmna_topo', find_srid('gmna', 'geologic_units', 'geom'));
SELECT AddTopoGeometryColumn('gmna_topo', 'gmna', 'geologic_units', 'topogeom', 'MULTIPOLYGON');
UPDATE gmna.geologic_units SET topogeom = toTopoGeom(geom_clean, 'gmna_topo', 1);

ALTER TABLE gmna.geologic_units ADD COLUMN geom_topo_clean geometry;
UPDATE gmna.geologic_units SET geom_topo_clean = topogeom::geometry;

