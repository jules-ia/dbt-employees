{{ config(
    materialized='view',
    tags=['employees', 'bronze'],
    post_hook="{{ set_column_tags({
        'identite_nom': 'DATAMESH.SQLMESH.VARCHAR_MASKING = \\'employees\\'',
        'identite_nom_de_naissance': 'DATAMESH.SQLMESH.VARCHAR_MASKING = \\'employees\\'',
        'identite_prenom': 'DATAMESH.SQLMESH.VARCHAR_MASKING = \\'employees\\'',
        'identite_n_de_securite_sociale': 'DATAMESH.SQLMESH.VARCHAR_MASKING = \\'employees\\'',
        'identite_commune_de_naiss': 'DATAMESH.SQLMESH.VARCHAR_MASKING = \\'employees\\'',
    }) }}"
) }}

SELECT
    identite_matricule,
    identite_item,
    identite_nom,
    identite_nom_de_naissance,
    identite_prenom,
    identite_civilite_code,
    identite_civilite_listes_jds_libelle,
    identite_sexe,
    identite_sexe_listes_jds_libelle,
    identite_situation_familiale,
    identite_situation_familiale_listes_jds_libelle,
    identite_n_de_securite_sociale,
    identite_date_de_naissance,
    identite_nationalite,
    identite_nationalite_pays_insee_libelle,
    identite_commune_de_naiss,
    identite_code_insee_commune,
    identite_dept_de_naiss,
    identite_dept_de_naiss_listes_jds_libelle,
    identite_pays_de_naiss_pays_insee_code_pays,
    identite_pays_de_naiss_pays_insee_libelle,
    identite_matricule_local,
    'teamsrh' AS source,
    updated_at,
    ingested_at
FROM {{ source('datalake_staging', 's1ide') }}
