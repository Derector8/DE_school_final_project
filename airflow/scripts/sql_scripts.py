"""
Script, containing sql queries for creating and ingesting tables 
"""

# Create staging tables
create_staging_molecule_table = \
    """
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
"""

create_staging_chembl_id_lookup_table = \
    """
CREATE TABLE IF NOT EXISTS rbukharev.staging_chembl_id_lookup (
    chembl_id VARCHAR(20),
    entity_type VARCHAR(50),
    last_active INT,
    resource_url TEXT,
    status VARCHAR(10)
);
"""

# Create logic layer tables
create_chembl_id_lookup_table = \
    """
CREATE TABLE IF NOT EXISTS rbukharev.chembl_id_lookup (
    chembl_id VARCHAR(20) NOT NULL PRIMARY KEY,
    entity_type VARCHAR(50),
    last_active INT,
    resource_url TEXT,
    status VARCHAR(10)
);
"""

create_molecules_dict_table = \
    """
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

"""
create_compound_properties_table = \
    """
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
"""
create_compound_structures_table = \
    """
CREATE TABLE IF NOT EXISTS rbukharev.compound_structures (
    chembl_id VARCHAR(20) NOT NULL PRIMARY KEY,
    molfile TEXT,
    standard_inchi VARCHAR(4000),
    standard_inchi_key VARCHAR(27),
    canonical_smiles VARCHAR(4000)
);
"""
create_molecules_fingerprints_table = \
    """
CREATE TABLE IF NOT EXISTS rbukharev.molecules_fingerprints (
    chembl_id VARCHAR(20) NOT NULL PRIMARY KEY,
    fps TEXT
);
"""

# Scripts for data ingestion from staging tables

ingest_chembl_id_lookup_sql = \
    """
INSERT INTO rbukharev.chembl_id_lookup
SELECT 
chembl_id, 
entity_type, 
last_active, 
resource_url, 
status
FROM rbukharev.staging_chembl_id_lookup
WHERE chembl_id is not Null;
"""

ingest_molecules_dict_sql = \
    """
INSERT INTO rbukharev.molecules_dictionary
SELECT 
molecule_chembl_id as chembl_id, 
pref_name, 
max_phase, therapeutic_flag, 
dosed_ingredient, 
structure_type, 
chebi_par_id, 
molecule_type, 
first_approval, 
oral, 
parenteral, 
topical, 
black_box_warning, 
natural_product, 
first_in_class, 
chirality, 
prodrug, 
inorganic_flag, 
usan_year, 
availability_type, 
usan_stem, 
polymer_flag, 
usan_substem, 
usan_stem_definition, 
indication_class, 
withdrawn_flag, 
chemical_probe, 
orphan
FROM rbukharev.staging_molecule
WHERE molecule_chembl_id is not NULL;
"""

ingest_compound_properties_sql = \
    """
INSERT INTO rbukharev.compound_properties
SELECT 
	js.molecule_chembl_id as chembl_id,
	(js.molecule_properties ->> 'mw_freebase')::numeric(9,2) as mw_freebase,
	(js.molecule_properties ->> 'alogp')::numeric(9,2) as alogp,
	(js.molecule_properties ->> 'hba')::int as hba,
	(js.molecule_properties ->> 'hbd')::int as hbd,
	(js.molecule_properties ->> 'psa')::numeric(9,2) as psa,
	(js.molecule_properties ->> 'rtb')::int as rtb,
	(js.molecule_properties ->> 'ro3_pass')::varchar(3) as ro3_pass,
	(js.molecule_properties ->> 'num_ro5_violations')::smallint as num_ro5_violations,
	(js.molecule_properties ->> 'cx_most_apka')::numeric(9,2) as cx_most_apka,
	(js.molecule_properties ->> 'cx_most_bpka')::numeric(9,2) as cx_most_bpka,
	(js.molecule_properties ->> 'cx_logp')::numeric(9,2) as cx_logp,
	(js.molecule_properties ->> 'cx_logdcx_logd')::numeric(9,2) as cx_logdcx_logd,
	(js.molecule_properties ->> 'molecular_species')::varchar(50) as molecular_species,
	(js.molecule_properties ->> 'full_mwt')::numeric(9,2) as full_mwt,
	(js.molecule_properties ->> 'aromatic_rings')::int as aromatic_rings,
	(js.molecule_properties ->> 'heavy_atoms')::int as heavy_atoms,
	(js.molecule_properties ->> 'qed_weighted')::numeric(3,2) as qed_weighted,
	(js.molecule_properties ->> 'mw_monoisotopic')::numeric(11,4) as mw_monoisotopic,
	(js.molecule_properties ->> 'full_molformula')::varchar(100) as full_molformula,
	(js.molecule_properties ->> 'hba_lipinski')::int as hba_lipinski,
	(js.molecule_properties ->> 'hbd_lipinski')::int as hbd_lipinski,
	(js.molecule_properties ->> 'num_lipinski_ro5_violations')::smallint as num_lipinski_ro5_violations,
	(js.molecule_properties ->> 'np_likeness_score')::numeric(3,2) as np_likeness_score
FROM (
    SELECT 
    molecule_chembl_id,
    molecule_properties::jsonb 
    FROM rbukharev.staging_molecule) AS js;
"""

ingest_compound_structures_sql = \
    """
INSERT INTO rbukharev.compound_structures
SELECT 
	js.molecule_chembl_id as chembl_id,
	(js.molecule_structures ->> 'molfile')::text as molfile,
	(js.molecule_structures ->> 'standard_inchi')::varchar(4000) as standard_inchi,
	(js.molecule_structures ->> 'standard_inchi_key')::varchar(27) as standard_inchi_key,
	(js.molecule_structures ->> 'canonical_smiles')::varchar(4000) as canonical_smiles,
FROM (
    SELECT 
    molecule_chembl_id, 
    molecule_structures::jsonb  
    FROM rbukharev.staging_molecule) AS js;
"""

#Script for cleaning compound structures with Null smiles

clean_compound_structures_sql = \
    """
DELETE FROM rbukharev.compound_structures
WHERE canonical_smiles is Null;
"""

# Scripts for creating dimentional and fact tables

create_fact_molecule_similarities_table = \
    """
CREATE TABLE IF NOT EXISTS rbukharev.fact_top10_molecule_similarities (
    source_chembl_id VARCHAR(20) NOT NULL,
    target_chembl_id VARCHAR(20) NOT NULL,
    Tanimoto_similarity_score FLOAT
);
"""

create_dim_molecule_properties_table = \
    """
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
"""

#Script for ingestion dimentional table with data, refered to fact table

ingest_dim_molecule_properties_sql = \
    """
INSERT INTO rbukharev.dim_top10_molecule_properties
SELECT 
cp.chembl_id,
md.molecule_type,
cp.mw_freebase,
cp.alogp,
cp.psa,
cp.cx_logp,
cp.molecular_species,
cp.full_mwt,
cp.aromatic_rings,
cp.heavy_atoms  
FROM rbukharev.compound_properties cp
JOIN rbukharev.molecules_dictionary md on cp.chembl_id = md.chembl_id
WHERE (cp.chembl_id in (SELECT distinct(source_chembl_id) FROM rbukharev.fact_top10_molecule_similarities))
    OR
    (cp.chembl_id in (SELECT distinct(target_chembl_id) FROM rbukharev.fact_top10_molecule_similarities))
    ;
"""
