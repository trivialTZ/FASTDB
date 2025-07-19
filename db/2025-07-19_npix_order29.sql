CREATE EXTENSION IF NOT EXISTS pg_healpix;

ALTER TABLE diaobject ADD COLUMN npix_order29 BIGINT;

UPDATE diaobject SET npix_order29 = healpix_ang2ipix_nest(536870912, ra, dec);

CREATE INDEX CONCURRENTLY idx_diaobject_npix29 ON diaobject USING btree(npix_order29);