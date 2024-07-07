--Ingest dimentional top 10 molecule properties table
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