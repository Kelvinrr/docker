INSERT into spatial_ref_sys (srid, auth_name, auth_srid, proj4text, srtext) values ( 930100, 'iau2000', 30100,
	 '+proj=longlat +a=1737400 +b=1737400 +no_defs ', 'GEOGCS["Moon 2000",DATUM["D_Moon_2000",SPHEROID["Moon_2000_IAU_IAG",1737400.0,0.0]],PRIMEM["Greenwich",0],UNIT["Decimal_Degree",0.0174532925199433]]');

DROP TABLE spectra;
DROP TABLE datatype;
DROP TABLE filelookups;
DROP TABLE filemetadata;

CREATE TABLE filemetadata (
	  file_id SERIAL primary key,
	  filepath VARCHAR (256),
	  product_id VARCHAR (128),
	  keywords JSON
);

CREATE TABLE filelookups (
	  file_id INTEGER REFERENCES filemetadata (file_id),
	  observation_id SMALLINT,
	  incidence DOUBLE PRECISION,
	  emission DOUBLE PRECISION,
	  location GEOMETRY(POINT, 30100),
	  PRIMARY KEY (file_id, observation_id)
);

-- SRID 30100 is for the Moon
-- SELECT AddGeometryColumn ('public','filelookups','location',30100,'POINT',2, false);

CREATE TABLE datatype (
	  type_id SERIAL PRIMARY KEY,
	  type VARCHAR (16)
);

CREATE TABLE spectra (
	  spectra_id BIGSERIAL PRIMARY KEY,
	  file_id INTEGER,
	  observation_id SMALLINT,
	  type_id INTEGER REFERENCES datatype (type_id),
	  spectra DOUBLE PRECISION[],
	  FOREIGN KEY (file_id, observation_id) REFERENCES filelookups (file_id, observation_id)
);
