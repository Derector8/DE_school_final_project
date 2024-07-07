-- STAGING LAYER
-- Create staging chembl_id_lookup table
CREATE TABLE IF NOT EXISTS rbukharev.staging_chembl_id_lookup (
    chembl_id VARCHAR(20),
    entity_type VARCHAR(50),
    last_active INT,
    resource_url TEXT,
    status VARCHAR(10)
);

-- Create staging molecule table
CREATE TABLE IF NOT EXISTS rbukharev.staging_molecule (
    pref_name VARCHAR(255),
    molecule_chembl_id VARCHAR(20),
    max_phase NUMERIC(2,1),
    therapeutic_flag BOOLEAN,
    dosed_ingredient BOOLEAN,
    structure_type VARCHAR(10),
    chebi_par_id BIGINT,
    molecule_type VARCHAR(30),
    first_approval INT,
    oral BOOLEAN,
    parenteral BOOLEAN,
    topical BOOLEAN,
    black_box_warning SMALLINT,
    natural_product SMALLINT,
    first_in_class SMALLINT,
    chirality SMALLINT,
    prodrug SMALLINT,
    inorganic_flag SMALLINT,
    usan_year INT,
    availability_type SMALLINT,
    usan_stem VARCHAR(50),
    polymer_flag SMALLINT,
    usan_substem VARCHAR(50),
    usan_stem_definition VARCHAR(1000),
    indication_class VARCHAR(1000),
    withdrawn_flag BOOLEAN,
    chemical_probe SMALLINT,
    orphan SMALLINT,
    molecule_structures JSON,
    molecule_properties JSON
);

--LOGIC LAYER
--Create chembl_id_lookup table
CREATE TABLE IF NOT EXISTS rbukharev.chembl_id_lookup (
    chembl_id VARCHAR(20) NOT NULL PRIMARY KEY,
    entity_type VARCHAR(50),
    last_active INT,
    resource_url TEXT,
    status VARCHAR(10)
);

--Create molecules_dictionary table
CREATE TABLE IF NOT EXISTS rbukharev.molecules_dictionary (
    chembl_id VARCHAR(20) NOT NULL PRIMARY KEY,
    pref_name VARCHAR(255),
    max_phase NUMERIC(2,1),
    therapeutic_flag BOOLEAN,
    dosed_ingredient BOOLEAN,
    structure_type VARCHAR(10),
    chebi_par_id BIGINT,
    molecule_type VARCHAR(30),
    first_approval INT,
    oral BOOLEAN,
    parenteral BOOLEAN,
    topical BOOLEAN,
    black_box_warning SMALLINT,
    natural_product SMALLINT,
    first_in_class SMALLINT,
    chirality SMALLINT,
    prodrug SMALLINT,
    inorganic_flag SMALLINT,
    usan_year INT,
    availability_type SMALLINT,
    usan_stem VARCHAR(50),
    polymer_flag SMALLINT,
    usan_substem VARCHAR(50),
    usan_stem_definition VARCHAR(1000),
    indication_class VARCHAR(1000),
    withdrawn_flag BOOLEAN,
    chemical_probe SMALLINT,
    orphan SMALLINT
);

--Create compound_properties table
CREATE TABLE IF NOT EXISTS rbukharev.compound_properties (
    chembl_id VARCHAR(20) NOT NULL PRIMARY KEY,
    mw_freebase numeric(9,2),
    alogp numeric(9,2),
    hba int,
    hbd int,
    psa numeric(9,2),
	rt int,
	ro3_pass varchar(3),
	num_ro5_violations smallint,
	cx_most_apka numeric(9,2),
	cx_most_bpka numeric(9,2),
	cx_logp numeric(9,2),
	cx_logdcx_logd numeric(9,2),
	molecular_species varchar(50),
	full_mwt numeric(9,2),
	aromatic_rings int,
	heavy_atoms int,
	qed_weighted numeric(3,2),
	mw_monoisotopic numeric(11,4),
	full_molformula varchar(100),
	hba_lipinski int,
	hbd_lipinski int,
	num_lipinski_ro5_violations smallint,
	np_likeness_score numeric(3,2)
);

--Create compound_structures table
CREATE TABLE IF NOT EXISTS rbukharev.compound_structures (
    chembl_id VARCHAR(20) NOT NULL PRIMARY KEY,
    molfile TEXT,
    standard_inchi VARCHAR(4000),
    standard_inchi_key VARCHAR(27),
    canonical_smiles VARCHAR(4000)
);

--Create molecules_fingerprints table
CREATE TABLE IF NOT EXISTS rbukharev.molecules_fingerprints (
    chembl_id VARCHAR(20) NOT NULL PRIMARY KEY,
    fps TEXT
);

--DIMENSION/FACT LAYER
--Create fact_top10_molecule_similarities table
CREATE TABLE IF NOT EXISTS rbukharev.fact_top10_molecule_similarities (
    source_chembl_id VARCHAR(20) NOT NULL,
    target_chembl_id VARCHAR(20) NOT NULL,
    tanimoto_similarity_score FLOAT
);

--Create dim_top10_molecule_properties table
CREATE TABLE IF NOT EXISTS rbukharev.dim_top10_molecule_properties (
    chembl_id VARCHAR(20) NOT NULL PRIMARY KEY,
    molecule_type VARCHAR(30),
    mw_freebase numeric(9,2),
    alogp numeric(9,2),
    psa numeric(9,2),
    cx_logp numeric(9,2),
    molecular_species varchar(50),
    full_mwt numeric(9,2),
    aromatic_rings int,
    heavy_atoms int
);