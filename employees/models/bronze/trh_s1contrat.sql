{{ config(
    materialized='view',
    tags=['employees', 'bronze']
) }}

SELECT
    contrat_matricule,
    contrat_date_debut,
    contrat_date_fin,
    contrat_contrat,
    contrat_date_anciennete_groupe,
    contrat_date_anciennete_societe,
    contrat_date_anciennete_reconst,
    contrat_date_debut_preavis,
    contrat_date_fin_preavis,
    contrat_date_sortie_physique,
    contrat_date_de_solde,
    contrat_societe,
    contrat_motif_de_changement,
    contrat_motif_de_changement_listes_jds_libelle,
    contrat_motif_entree,
    contrat_motif_entree_motifs_d_entree_libelle,
    contrat_motif_sortie_solde_tout_compte,
    contrat_motif_sortie_solde_tout_compte_motifs_de_sortie_libelle,
    contrat_regl,
    'teamsrh' AS source,
    updated_at,
    ingested_at
FROM {{ source('datalake_staging', 's1contrat') }}
