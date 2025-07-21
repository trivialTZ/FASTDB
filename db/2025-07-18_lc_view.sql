-- View joining diaobject with all matching diasource and diaforcedsource rows
-- Produces one diaobject row with JSONB arrays of diasource and diaforcedsource structs
CREATE OR REPLACE VIEW diaobject_with_all_sources AS
SELECT
  d.*,
  COALESCE(ds.diasources, '[]')     AS diasources,
  COALESCE(fs.forced_sources, '[]') AS forced_sources
FROM diaobject d
LEFT JOIN (
  SELECT diaobjectid, diaobject_procver,
         jsonb_agg(to_jsonb(s) ORDER BY s.midpointmjdtai) AS diasources
  FROM diasource
  GROUP BY diaobjectid, diaobject_procver
) ds USING (diaobjectid, processing_version)
LEFT JOIN (
  SELECT diaobjectid, diaobject_procver,
         jsonb_agg(to_jsonb(fs) ORDER BY fs.midpointmjdtai) AS forced_sources
  FROM diaforcedsource
  GROUP BY diaobjectid, diaobject_procver
) fs USING (diaobjectid, processing_version);
