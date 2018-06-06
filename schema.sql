INSERT into spatial_ref_sys (srid, auth_name, auth_srid, proj4text, srtext) values ( 930100, 'iau2000', 30100,
	 '+proj=longlat +a=1737400 +b=1737400 +no_defs ', 'GEOGCS["Moon 2000",DATUM["D_Moon_2000",SPHEROID["Moon_2000_IAU_IAG",1737400.0,0.0]],PRIMEM["Greenwich",0],UNIT["Decimal_Degree",0.0174532925199433]]');

DROP TABLE spectra;
DROP TABLE datatype;
DROP TABLE filelookups CASCADE;
DROP TABLE filemetadata CASCADE;
DROP TABLE ref1;
DROP TABLE ref2;

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
	  PRIMARY KEY (file_id, observation_id)
);

-- SRID 930100 is for the Moon
SELECT AddGeometryColumn ('public', 'filelookups', 'location', 930100, 'POINT', 2, false);

-- type should be enum
CREATE TABLE ref1 (
	  file_id INTEGER,
		observation_id SMALLINT,
	  spectra_id BIGSERIAL,
	  spectra DOUBLE PRECISION[],
		FOREIGN KEY (file_id, observation_id) REFERENCES filelookups (file_id, observation_id)
);

-- type should be enum
CREATE TABLE ref2 (
	  file_id INTEGER,
		observation_id SMALLINT,
	  spectra_id BIGSERIAL,
	  spectra DOUBLE PRECISION[],
		FOREIGN KEY (file_id, observation_id) REFERENCES filelookups (file_id, observation_id)
);

-- shard everything together
SELECT create_distributed_table('filemetadata', 'file_id');
SELECT create_distributed_table('filelookups', 'file_id');
SELECT create_distributed_table('ref1', 'file_id');
SELECT create_distributed_table('ref2', 'file_id');
