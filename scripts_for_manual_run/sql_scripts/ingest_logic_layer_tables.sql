--ingest_chembl_id_lookup
INSERT INTO rbukharev.chembl_id_lookup
SELECT
    chembl_id,
    entity_type,
    last_active,
    resource_url,
    status
FROM rbukharev.staging_chembl_id_lookup
WHERE chembl_id is not Null
;
--ingest_molecules_dict
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
WHERE molecule_chembl_id is not NULL
;
--ingest_compound_properties
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
--ingest_compound_structures
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
--clean compound structures table
DELETE FROM rbukharev.compound_structures
WHERE canonical_smiles is Null;

