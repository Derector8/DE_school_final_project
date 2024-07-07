"""
Script, containing sql queries for creating views, based on datamart tables
"""
#   7.a
avg_sim_per_source_molecule_view = \
    """
CREATE OR REPLACE VIEW rbukharev.avg_sim_per_source_molecule AS
SELECT 
    distinct (source_chembl_id) as source_chembl_id ,
    avg(tanimoto_similarity_score) over (partition by source_chembl_id) as average_similarity
FROM rbukharev.fact_top10_molecule_similarities ;
"""
#   7.b
avg_deviation_of_alogp_view = \
    """
CREATE OR REPLACE VIEW rbukharev.avg_deviation_of_alogp AS
SELECT
    distinct(source_chembl_id),
    avg(abs(dtmp_s.alogp - dtmp_t.alogp)) over (partition by source_chembl_id) as average_deviation_of_alogp
FROM rbukharev.fact_top10_molecule_similarities fms
JOIN rbukharev.dim_top10_molecule_properties dtmp_s on fms.source_chembl_id = dtmp_s.chembl_id 
JOIN rbukharev.dim_top10_molecule_properties dtmp_t on fms.target_chembl_id = dtmp_t.chembl_id ;
"""
#   8.a
pivot_view = \
    """
CREATE OR REPLACE VIEW rbukharev.pivot_10_source_molecules AS
SELECT 
    target_chembl_id,
    max(CASE WHEN source_chembl_id = 'CHEMBL543422' THEN tanimoto_similarity_score end) as CHEMBL543422,
    max(CASE WHEN source_chembl_id = 'CHEMBL1190790' THEN tanimoto_similarity_score end) as CHEMBL1190790,
    max(CASE WHEN source_chembl_id = 'CHEMBL6347' THEN tanimoto_similarity_score end) as CHEMBL6347,
    max(CASE WHEN source_chembl_id = 'CHEMBL134219' THEN tanimoto_similarity_score end) as CHEMBL134219,
    max(CASE WHEN source_chembl_id = 'CHEMBL135689' THEN tanimoto_similarity_score end) as CHEMBL135689,
    max(CASE WHEN source_chembl_id = 'CHEMBL541592' THEN tanimoto_similarity_score end) as CHEMBL541592,
    max(CASE WHEN source_chembl_id = 'CHEMBL6290' THEN tanimoto_similarity_score end) as CHEMBL6290,
    max(CASE WHEN source_chembl_id = 'CHEMBL6358' THEN tanimoto_similarity_score end) as CHEMBL6358,
    max(CASE WHEN source_chembl_id = 'CHEMBL268768' THEN tanimoto_similarity_score end) as CHEMBL268768,
    max(CASE WHEN source_chembl_id = 'CHEMBL540325' THEN tanimoto_similarity_score end) as CHEMBL540325
FROM rbukharev.fact_top10_molecule_similarities
WHERE target_chembl_id = 'CHEMBL6351'
GROUP BY target_chembl_id
;
"""
#   8.b
next_and_second_most_sim_view = \
    """
CREATE OR REPLACE VIEW rbukharev.next_and_second_most_sim AS
SELECT 
    source_chembl_id,
    target_chembl_id,
    tanimoto_similarity_score,
    lead(target_chembl_id) over (partition by target_chembl_id order by tanimoto_similarity_score desc) as next_most_similar_target_molecule,
    nth_value(target_chembl_id, 2) over (partition by source_chembl_id order by tanimoto_similarity_score desc rows between unbounded preceding and unbounded following) as second_most_similar_target_molecule
FROM rbukharev.fact_top10_molecule_similarities 
;
"""
#   8.c
avg_similarity_score_total_view = \
    """
CREATE OR REPLACE VIEW rbukharev.avg_similarity_score_total AS
SELECT
    case when grouping(ftms.source_chembl_id, dtmp.aromatic_rings, dtmp.heavy_atoms) = 7
        then 'TOTAL' else ftms.source_chembl_id end as source_chembl_id,
    case when grouping(ftms.source_chembl_id, dtmp.aromatic_rings, dtmp.heavy_atoms) = 7
        then 'TOTAL' else cast(dtmp.aromatic_rings as varchar) end as aromatic_rings,
    case when grouping(ftms.source_chembl_id, dtmp.aromatic_rings, dtmp.heavy_atoms) = 7
        then 'TOTAL' else cast(dtmp.heavy_atoms as varchar) end as heavy_atoms,
    avg(ftms.tanimoto_similarity_score) as avg_tanimoto_similarity_score
FROM rbukharev.fact_top10_molecule_similarities ftms
JOIN rbukharev.dim_top10_molecule_properties dtmp on ftms.source_chembl_id = dtmp.chembl_id 
GROUP BY grouping sets((ftms.source_chembl_id), (dtmp.aromatic_rings, dtmp.heavy_atoms), (dtmp.heavy_atoms), ())
;
"""
