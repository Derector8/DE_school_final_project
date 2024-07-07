# DE_school_final_project
This project contains scripts for ingesting molecules from ChemBL API and searching top-10 most similar molecules for input set of molecules in s3 folder.


## DWH(Data Warehouse) structure:
#### Dwh consists of 3 layers:
###### Staging layer:
1) staging_chembl_id_lookup
2) staging_molecule
This tables are ingested directly from ChemBL API 

###### Logic layer:
1)  chembl_id_lookup
2)  molecules_dictionary
3)  compound_properties
4)  compound_structures
This tables are ingested from staging layer with some data cleaning and setting primary keys.

###### Datamart layer:
1) dim_top10_molecule_properties
2) fact_top10_molecule_similarities

-Fact table is ingested after similarity calculations, based on logic_layer tables and contains computed similarity between target molecules(input molecules) and top-10 source molecules from all ChemBL molecules. Also it contains flag to show, if there are more molecules with the same similarity scores, as in top-10, but they are not included.
-Dim table ingested with only refered to fact table molecule properties.

###### Views based on Datamart:
1) avg_sim_per_source_molecule - Shows average similarity score per source molecule.
2) avg_deviation_of_alogp - Shows average deviation of alogp of similar molecule from the source molecule.
3) pivot_10_source_molecules - In first column shows target_molecule_chembl_id, 10 other columns - source_molecule_chembl_id's and in each cell is their similarity score.
4) next_and_second_most_sim - Shows all source and target molecules chembl id's, next most similar target molecule(compared to target_molecule of curret row), second most similar target molecule chembl_id to the current row source molecule.
5) avg_similarity_score_total - Shows Average similarity score per: 1. source molecule 2. source molecule's aromatic rings and heavy atoms 3.source molecule's heavy atoms 4. whole dataset(for the whole dataset aggregation nulls replaced by "TOTAL")

## Usage:
You can run this application in 2 ways - Manually or via Docker Airflow.

#### For Manual run:
Open scripts_for_manual_run folder
!!! Install all requirements from requirements.txt !!!
Run steps:
1) Open credentials.py and fill in your credentials(sometimes renew AWS credentials, if they are expired)
2) In sql_scripts folder run create_tables.sql
3) Run database_ingestion.py(to load ChemBL molecules and chembl_id_lookup data into postgres)
4) In sql_scripts folder run ingest_logic_layer_tables.sql
5) Run fingerprints_compute.py (to compute fingerprints for all molecules and save to s3 bucket and postgres table) 
6) Run similarity_compute.py (to compute all similarities for input file's molecules and save to s3 bucket, and compute top 10 similarities and save to postgres fact table)
7) In sql_scripts folder run ingest_dim_top10_table.sql
8) In sql_scripts folder run create_views.sql

That's all - go to your Postgres client and check tables and views!
If you want to calculate similarities for new data appeared in s3 bucket in future - just run similarity_compute.py(don't forget to provide new file_name template string as function argument at the bottom of script), truncate dim_top10_molecule_properties and launch ingest_dim_top10_table.sql(In sql_scripts folder)

#### For Docker Airflow run:
1) Open airflow folder
2) Open credentials.py(in scripts_folder) and fill in your credentials(sometimes renew AWS credentials in airflow, if they are expired)
3) Run docker-compose up in airflow folder
4) Trigger 3 dags one after another:
  a. ingest_database_dag - it will create tables for staging and logic layer and ingest this tables.
  b. compute_fps_and_sim_dag - it will compute fingerprints for all ChemBL molecules and save them to s3 bucket and molecules_fingerprints table in postgres. Then compute similarities for molecules in input_files from s3 folder, save full similarity_scores to s3 bucket and top-10 similarities to postgres fact table.
  c. create_views_dag - it will create views, based on the datamart.
5) If you want to receive notifications on success and failure dag runs - open airflow.cfg in config folder and fill in your smtp credentials. And set your email in dags scripts in default_args email parameter.

###### Examples:
All pictures with DWH tables structure and views results examples are located in examples folder.




