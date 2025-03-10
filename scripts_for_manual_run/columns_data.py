"""
File contains d_type dicts with column types and cols lists with column names.
"""
from sqlalchemy import types

# ------------------------------D_TYPE DICTS------------------------------

raw_molecule_dtype_dict = {
    "pref_name": types.VARCHAR(length=255),
    "molecule_chembl_id": types.VARCHAR(length=20),
    "max_phase": types.Numeric(precision=2, scale=1),
    "therapeutic_flag": types.BOOLEAN(),
    "dosed_ingredient": types.BOOLEAN(),
    "structure_type": types.VARCHAR(length=10),
    "chebi_par_id": types.BIGINT(),
    "molecule_type": types.VARCHAR(length=30),
    "first_approval": types.INTEGER(),
    "oral": types.BOOLEAN(),
    "parenteral": types.BOOLEAN(),
    "topical": types.BOOLEAN(),
    "black_box_warning": types.SMALLINT(),
    "natural_product": types.SMALLINT(),
    "first_in_class": types.SMALLINT(),
    "chirality": types.SMALLINT(),
    "prodrug": types.SMALLINT(),
    "inorganic_flag": types.SMALLINT(),
    "usan_year": types.INTEGER(),
    "availability_type": types.SMALLINT(),
    "usan_stem": types.VARCHAR(length=50),
    "polymer_flag": types.SMALLINT(),
    "usan_substem": types.VARCHAR(length=50),
    "usan_stem_definition": types.VARCHAR(length=1000),
    "indication_class": types.VARCHAR(length=1000),
    "withdrawn_flag": types.BOOLEAN(),
    "chemical_probe": types.SMALLINT(),
    "orphan": types.SMALLINT(),
    "molecule_structures": types.JSON(),
    "molecule_properties": types.JSON(),
    }

raw_chembl_id_lookup_dtype_dict = {
    "chembl_id": types.VARCHAR(length=20),
    "entity_type": types.VARCHAR(length=50),
    "last_active": types.INTEGER(),
    "resource_url": types.TEXT(),
    "status": types.VARCHAR(length=10),
    }

compound_structures_dtype_dict = {
    "chembl_id": types.VARCHAR(length=20),
    "molfile": types.TEXT(),
    "standard_inchi": types.VARCHAR(length=4000),
    "standard_inchi_key": types.VARCHAR(length=27),
    "canonical_smiles": types.VARCHAR(length=4000),
    }

compound_properties_dtype_dict = {
    "chembl_id": types.VARCHAR(length=20),
    "molecule_properties.mw_freebase": types.Numeric(precision=9, scale=2),
    "molecule_properties.alogp": types.Numeric(precision=9, scale=2),
    "molecule_properties.hba": types.INTEGER(),
    "molecule_properties.hbd": types.INTEGER(),
    "molecule_properties.psa": types.Numeric(precision=9, scale=2),
    "molecule_properties.rtb": types.INTEGER(),
    "molecule_properties.ro3_pass": types.VARCHAR(length=3),
    "molecule_properties.num_ro5_violations": types.SMALLINT(),
    "molecule_properties.cx_most_apka": types.Numeric(precision=9, scale=2),
    "molecule_properties.cx_most_bpka": types.Numeric(precision=9, scale=2),
    "molecule_properties.cx_logp": types.Numeric(precision=9, scale=2),
    "molecule_properties.cx_logd": types.Numeric(precision=9, scale=2),
    "molecule_properties.molecular_species": types.VARCHAR(length=50),
    "molecule_properties.full_mwt": types.Numeric(precision=9, scale=2),
    "molecule_properties.aromatic_rings": types.INTEGER(),
    "molecule_properties.heavy_atoms": types.INTEGER(),
    "molecule_properties.qed_weighted": types.Numeric(precision=3, scale=2),
    "molecule_properties.mw_monoisotopic": types.Numeric(precision=11, scale=4),
    "molecule_properties.full_molformula": types.VARCHAR(length=100),
    "molecule_properties.hba_lipinski": types.INTEGER(),
    "molecule_properties.hbd_lipinski": types.INTEGER(),
    "molecule_properties.num_lipinski_ro5_violations": types.SMALLINT(),
    "molecule_properties.np_likeness_score": types.Numeric(precision=3, scale=2),
    }

fps_dtype_dict = {
    "chembl_id": types.VARCHAR(length=20),
    "fps": types.TEXT(),
    }

fact_sim_dtype_dict = {
    "target_chembl_id": types.VARCHAR(length=20),
    "source_chembl_id": types.VARCHAR(length=20),
    "tanimoto_similarity_score": types.FLOAT(),
    "has_duplicates_of_last_largest_score": types.BOOLEAN(),
    }

# ------------------------------COLUMN LISTS------------------------------
raw_chembl_id_lookup_cols = [
    "chembl_id",
    "entity_type",
    "last_active",
    "resource_url",
    "status",
]

raw_molecule_cols = [
    "pref_name",
    "molecule_chembl_id",
    "max_phase",
    "therapeutic_flag",
    "dosed_ingredient",
    "structure_type",
    "chebi_par_id",
    "molecule_type",
    "first_approval",
    "oral",
    "parenteral",
    "topical",
    "black_box_warning",
    "natural_product",
    "first_in_class",
    "chirality",
    "prodrug",
    "inorganic_flag",
    "usan_year",
    "availability_type",
    "usan_stem",
    "polymer_flag",
    "usan_substem",
    "usan_stem_definition",
    "indication_class",
    "withdrawn_flag",
    "chemical_probe",
    "orphan",
    "molecule_structures",
    "molecule_properties",
]

molecule_dictionary_cols = [
    "chembl_id",
    "pref_name",
    "max_phase",
    "therapeutic_flag",
    "dosed_ingredient",
    "structure_type",
    "chebi_par_id",
    "molecule_type",
    "first_approval",
    "oral",
    "parenteral",
    "topical",
    "black_box_warning",
    "natural_product",
    "first_in_class",
    "chirality",
    "prodrug",
    "inorganic_flag",
    "usan_year",
    "availability_type",
    "usan_stem",
    "polymer_flag",
    "usan_substem",
    "usan_stem_definition",
    "indication_class",
    "withdrawn_flag",
    "chemical_probe",
    "orphan",
]

compound_properties_cols = [
    "chembl_id",
    "mw_freebase",
    "alogp",
    "hba",
    "hbd",
    "psa",
    "rtb",
    "ro3_pass",
    "num_ro5_violations",
    "cx_most_apka",
    "cx_most_bpka",
    "cx_logp",
    "cx_logd",
    "molecular_species",
    "full_mwt",
    "aromatic_rings",
    "heavy_atoms",
    "qed_weighted",
    "mw_monoisotopic",
    "full_molformula",
    "hba_lipinski",
    "hbd_lipinski",
    "num_lipinski_ro5_violations",
    "np_likeness_score",
]

compound_structures_cols = [
    "chembl_id",
    "molfile",
    "standard_inchi",
    "standard_inchi_key",
    "canonical_smiles",
]
