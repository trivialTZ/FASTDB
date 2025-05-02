--- no alais description should match any description in processing_version
--- the database structure does not enforce this, so the code that creates
--- aliases should
CREATE TABLE processing_version_alias(
  id integer,
  description text PRIMARY KEY
);
CREATE INDEX idx_procveral_id ON processing_version_alias(id);
ALTER TABLE processing_version_alias ADD CONSTRAINT fk_procveral_procver
  FOREIGN KEY (id) REFERENCES processing_version(id) ON DELETE RESTRICT;


CREATE TABLE spectruminfo(
  specinfo_id UUID NOT NULL PRIMARY KEY DEFAULT gen_random_uuid(),
  root_diaobject_id UUID NOT NULL,
  facility text,
  inserted_at timestamp with time zone,
  mjd real,
  z real,
  classid int
);
CREATE INDEX idx_spectruminfo_root_diaobject_id ON spectruminfo(root_diaobject_id);
CREATE INDEX idx_spectruminfo_classid ON spectruminfo(classid);
ALTER TABLE spectruminfo ADD CONSTRAINT fk_spectruminfo_root_diaobject
  FOREIGN KEY (root_diaobject_id) REFERENCES root_diaobject(id) ON DELETE RESTRICT;

  
CREATE TABLE wantedspectra(
  wantspec_id text NOT NULL PRIMARY KEY,
  root_diaobject_id UUID NOT NULL,
  wanttime timestamp with time zone,
  user_id UUID NOT NULL,
  requester text,
  priority smallint
);
CREATE INDEX ix_wantedspectra_root_diaobject_id ON wantedspectra(root_diaobject_id);
CREATE INDEX ix_wantedspectra_user_id ON wantedspectra(user_id);
CREATE INDEX ix_wantedspectra_wanttime ON wantedspectra(wanttime);
ALTER TABLE wantedspectra ADD CONSTRAINT fk_wantedspectra_root_diaobject
  FOREIGN KEY (root_diaobject_id) REFERENCES root_diaobject(id) ON DELETE RESTRICT;
ALTER TABLE wantedspectra ADD CONSTRAINT fk_wantedspectra_user
  FOREIGN KEY (user_id) REFERENCES authuser(id) ON DELETE RESTRICT;


CREATE TABLE plannedspectra(
  plannedspec_id UUID NOT NULL PRIMARY KEY DEFAULT gen_random_uuid(),
  root_diaobject_id UUID NOT NULL,
  facility text,
  created_at timestamp with time zone,
  plantime timestamp with time zone,
  comment text
);
CREATE INDEX ix_plannedspectra_root_diaobject_id ON plannedspectra(root_diaobject_id);
CREATE INDEX ix_plannedspectra_created_at ON plannedspectra(created_at);
CREATE INDEX ix_plannedspectra_plantime ON plannedspectra(plantime);
ALTER TABLE plannedspectra ADD CONSTRAINT fk_plannedspectra_root_diaobject
  FOREIGN KEY (root_diaobject_id) REFERENCES root_diaobject(id) ON DELETE RESTRICT;

