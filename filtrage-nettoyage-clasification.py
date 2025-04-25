import csv
import mysql.connector
from mysql.connector import Error
from collections import defaultdict

# Configuration de la connexion à la base de données
config = {
    'user': 'hassanpaziaud',
    'password': '3fr3i',
    'host': 'localhost',
    'database': 'formation_apprentissage',
    'raise_on_warnings': True
}

# Chemin vers le fichier CSV
csv_file_path = r'C:\Users\pazia\OneDrive - Efrei\projetBDD\pythonprogramme\apprentissage_-_effectifs_detailles_2010-2011.csv'

def create_connection():
    """Crée une connexion à la base de données MySQL"""
    try:
        connection = mysql.connector.connect(**config)
        return connection
    except Error as e:
        print(f"Erreur de connexion à MySQL: {e}")
        return None

def clean_data(value):
    """Nettoie les données en supprimant les espaces superflus et en gérant les valeurs vides"""
    if value is None or str(value).strip() == '':
        return None
    return str(value).strip()

def insert_data_in_batches(cursor, table, columns, data, batch_size=1000):
    """Insère les données par lots pour optimiser les performances"""
    for i in range(0, len(data), batch_size):
        batch = data[i:i + batch_size]
        placeholders = ', '.join(['%s'] * len(columns))
        query = f"INSERT IGNORE INTO {table} ({', '.join(columns)}) VALUES ({placeholders})"
        cursor.executemany(query, batch)

def process_csv_and_insert_data():
    connection = create_connection()
    if connection is None:
        return

    try:
        cursor = connection.cursor()

        # Dictionnaires pour stocker les données uniques et éviter les doublons
        specialites = set()
        organismes = set()
        etablissements = set()
        diplomes = set()
        sites_formation = set()
        entreprises = set()
        apprenants = set()
        contrats = set()

        # Structures pour stocker les données avant insertion
        specialites_data = []
        organismes_data = []
        etablissements_data = []
        diplomes_data = []
        sites_formation_data = []
        entreprises_data = []
        apprenants_data = []
        contrats_data = []

        # Compteur pour les diplômes sans code
        diplome_counter = 1

        # Lire le fichier CSV avec le bon délimiteur
        with open(csv_file_path, mode='r', encoding='utf-8') as csvfile:
            reader = csv.DictReader(csvfile, delimiter=';')
            
            for row in reader:
                # Nettoyer toutes les valeurs du row
                cleaned_row = {k: clean_data(v) for k, v in row.items()}

                # Correction des noms de colonnes qui diffèrent
                cleaned_row['numero_section'] = cleaned_row.get('num_section') or cleaned_row.get('numero_section')
                cleaned_row['code_postal_site'] = cleaned_row.get('code_postal_site') or cleaned_row.get('code_postal_site')
                cleaned_row['code_postal_jeune'] = cleaned_row.get('code_postal_jeune') or cleaned_row.get('code_postal_jeune')

                # Gestion spéciale du diplôme - générer un ID si null
                diplome_value = cleaned_row['diplome']
                if diplome_value is None:
                    diplome_value = f"GEN_{diplome_counter}"
                    diplome_counter += 1

                # Traitement SPECIALITE
                specialite_key = cleaned_row['code_groupe_specialite']
                if specialite_key not in specialites:
                    specialites.add(specialite_key)
                    specialites_data.append((
                        cleaned_row['code_groupe_specialite'],
                        cleaned_row['libelle_specialite'],
                        cleaned_row['libelle_specialite_com']
                    ))

                # Traitement ORGANISME_GESTIONNAIRE
                organisme_key = cleaned_row['id_og']
                if organisme_key not in organismes:
                    organismes.add(organisme_key)
                    organismes_data.append((
                        cleaned_row['id_og'],
                        cleaned_row['libelle_og']
                    ))

                # Traitement ETABLISSEMENT
                etablissement_key = cleaned_row['id_etab']
                if etablissement_key not in etablissements:
                    etablissements.add(etablissement_key)
                    etablissements_data.append((
                        cleaned_row['id_etab'],
                        cleaned_row['nom_complet_cfa'],
                        cleaned_row['id_og']
                    ))

                # Traitement DIPLOME
                diplome_key = diplome_value
                if diplome_key not in diplomes:
                    diplomes.add(diplome_key)
                    diplomes_data.append((
                        diplome_value,
                        cleaned_row['libelle_diplome'],
                        cleaned_row['code_niveau'],
                        cleaned_row['type_diplome'],
                        cleaned_row['code_groupe_specialite'],
                        int(cleaned_row['duree_formation_mois']) if cleaned_row['duree_formation_mois'] else None
                    ))

                # Traitement SITE_FORMATION
                site_formation_key = cleaned_row['id_siteformation']
                if site_formation_key not in sites_formation:
                    sites_formation.add(site_formation_key)
                    sites_formation_data.append((
                        cleaned_row['id_siteformation'],
                        cleaned_row['code_uai_site'],
                        cleaned_row['libelle_lien_cfa'],
                        cleaned_row['nom_site_formation'],
                        cleaned_row['adresse1_site'],
                        cleaned_row['adresse2_site'],
                        cleaned_row['adresse3_site'],
                        cleaned_row['code_commune_site_insee'],
                        cleaned_row['code_postal_site'],
                        cleaned_row['libelle_ville_site'],
                        cleaned_row['id_etab']
                    ))

                # Traitement ENTREPRISE
                entreprise_key = f"{cleaned_row['code_naf_entreprise']}_{cleaned_row['code_insee_entreprise']}"
                if entreprise_key not in entreprises:
                    entreprises.add(entreprise_key)
                    entreprises_data.append((
                        entreprise_key,  # Utilisation de la clé composite comme ID
                        cleaned_row['code_naf_entreprise'],
                        cleaned_row['depart_entreprise'],
                        cleaned_row['code_insee_entreprise'],
                        cleaned_row['code_uai_etab_annee_prec']
                    ))

                # Traitement APPRENANT
                apprenant_key = cleaned_row['num_section']  # Utilisation du numéro de section comme ID
                if apprenant_key not in apprenants:
                    apprenants.add(apprenant_key)
                    apprenants_data.append((
                        apprenant_key,
                        cleaned_row['num_section'],
                        int(cleaned_row['age_jeune_decembre']) if cleaned_row['age_jeune_decembre'] else None,
                        cleaned_row['handicap_oui_non_vide'],
                        cleaned_row['code_sexe'],
                        cleaned_row['sexe'],
                        cleaned_row['code_qualite'],
                        cleaned_row['libelle_qualite'],
                        cleaned_row['code_nationalite'],
                        cleaned_row['libelle_nationalite'],
                        cleaned_row['code_pcs'],
                        cleaned_row['libelle_pcs_parent'],
                        cleaned_row['code_statut_jeune'],
                        cleaned_row['libelle_statut_jeune'],
                        cleaned_row['code_origine_prec_cfa'],
                        cleaned_row['libelle_origine_prec_cfa'],
                        cleaned_row['code_origine_annee_prec'],
                        cleaned_row['libelle_origine_annee_prec'],
                        cleaned_row['code_depart_jeune_insee'],
                        cleaned_row['code_commune_jeune_insee'],
                        cleaned_row['code_postal_jeune'],
                        cleaned_row['libelle_ville_jeune']
                    ))

                # Traitement CONTRAT
                contrat_key = f"{cleaned_row['annee_scolaire']}_{diplome_value}_{apprenant_key}"
                if contrat_key not in contrats:
                    contrats.add(contrat_key)
                    contrats_data.append((
                        contrat_key,
                        cleaned_row['annee_scolaire'],
                        cleaned_row['annee_formation'],
                        cleaned_row['num_section'],
                        diplome_value,
                        cleaned_row['id_siteformation'],
                        cleaned_row['id_etab'],
                        cleaned_row['id_og'],
                        cleaned_row['code_groupe_specialite'],
                        cleaned_row['code_naf_entreprise'],
                        cleaned_row['code_insee_entreprise'],
                        apprenant_key
                    ))

        # Insertion des données dans les tables dans le bon ordre
        print("Insertion des données...")

        # SPECIALITE
        if specialites_data:
            insert_data_in_batches(cursor, 'SPECIALITE', 
                                ['code_groupe_specialite', 'libelle_specialite', 'libelle_specialite_com'], 
                                specialites_data)
            print(f"{len(specialites_data)} spécialités insérées")

        # ORGANISME_GESTIONNAIRE
        if organismes_data:
            insert_data_in_batches(cursor, 'ORGANISME_GESTIONNAIRE', 
                                ['id_og', 'libelle_og'], 
                                organismes_data)
            print(f"{len(organismes_data)} organismes gestionnaires insérés")

        # ETABLISSEMENT
        if etablissements_data:
            insert_data_in_batches(cursor, 'ETABLISSEMENT', 
                                ['id_etab', 'nom_complet_cfa', 'id_og'], 
                                etablissements_data)
            print(f"{len(etablissements_data)} établissements insérés")

        # DIPLOME
        if diplomes_data:
            insert_data_in_batches(cursor, 'DIPLOME', 
                                ['diplome', 'libelle_diplome', 'code_niveau', 'type_diplome', 'code_groupe_specialite', 'duree_formation_mois'], 
                                diplomes_data)
            print(f"{len(diplomes_data)} diplômes insérés")

        # SITE_FORMATION
        if sites_formation_data:
            insert_data_in_batches(cursor, 'SITE_FORMATION', 
                                ['id_siteformation', 'code_uai_site', 'libelle_lien_cfa', 'nom_site_formation', 
                                 'adresse1_site', 'adresse2_site', 'adresse3_site', 'code_commune_site_insee', 
                                 'code_postal_site', 'libelle_ville_site', 'id_etab'], 
                                sites_formation_data)
            print(f"{len(sites_formation_data)} sites de formation insérés")

        # ENTREPRISE
        if entreprises_data:
            insert_data_in_batches(cursor, 'ENTREPRISE', 
                                ['id_entreprise', 'code_naf_entreprise', 'depart_entreprise', 'code_insee_entreprise', 'code_uai_etab_annee_prec'], 
                                entreprises_data)
            print(f"{len(entreprises_data)} entreprises insérées")

        # APPRENANT
        if apprenants_data:
            insert_data_in_batches(cursor, 'APPRENANT', 
                                ['id_apprenant', 'numero_section', 'age_jeune_decembre', 'handicap_oui_non_vide', 
                                 'code_sexe', 'sexe', 'code_qualite', 'libelle_qualite', 'code_nationalite', 
                                 'libelle_nationalite', 'code_pcs', 'libelle_pcs_parent', 'code_statut_jeune', 
                                 'libelle_statut_jeune', 'code_origine_prec_cfa', 'libelle_origine_prec_cfa', 
                                 'code_origine_annee_prec', 'libelle_origine_annee_prec', 'code_depart_jeune_insee', 
                                 'code_commune_jeune_insee', 'code_postal_jeune', 'libelle_ville_jeune'], 
                                apprenants_data)
            print(f"{len(apprenants_data)} apprenants insérés")

        # CONTRAT
        if contrats_data:
            insert_data_in_batches(cursor, 'CONTRAT', 
                                ['id_contrat', 'annee_scolaire', 'annee_formation', 'numero_section', 
                                 'diplome', 'id_siteformation', 'id_etab', 'id_og', 'code_groupe_specialite', 
                                 'code_naf_entreprise', 'code_insee_entreprise', 'id_apprenant'], 
                                contrats_data)
            print(f"{len(contrats_data)} contrats insérés")

        # Valider les changements
        connection.commit()
        print("Toutes les données ont été insérées avec succès!")

    except Error as e:
        print(f"Erreur lors de l'insertion des données: {e}")
        connection.rollback()
    except Exception as e:
        print(f"Erreur inattendue: {e}")
        connection.rollback()
    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()
            print("Connexion MySQL fermée")

# Exécution du script
if __name__ == "__main__":
    process_csv_and_insert_data()