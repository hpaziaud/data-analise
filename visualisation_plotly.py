import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
import mysql.connector
from mysql.connector import Error
import matplotlib.pyplot as plt
from plotly.subplots import make_subplots

# Configuration de la connexion à la base de données
config = {
    'user': 'hassanpaziaud',
    'password': '3fr3i',
    'host': 'localhost',
    'database': 'formation_apprentissage',
    'raise_on_warnings': True
}

def create_connection():
    """Crée une connexion à la base de données MySQL"""
    try:
        connection = mysql.connector.connect(**config)
        return connection
    except Error as e:
        print(f"Erreur de connexion à MySQL: {e}")
        return None

def fetch_data(query):
    """Récupère les données depuis la base de données"""
    connection = create_connection()
    if connection is None:
        return None
    
    try:
        cursor = connection.cursor(dictionary=True)
        cursor.execute(query)
        result = cursor.fetchall()
        return pd.DataFrame(result)
    except Error as e:
        print(f"Erreur lors de l'exécution de la requête: {e}")
        return None
    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()

def plot_age_distribution():
    """Visualisation de la distribution des âges des apprenants"""
    query = """
    SELECT age_jeune_decembre as age, COUNT(*) as count 
    FROM APPRENANT 
    WHERE age_jeune_decembre IS NOT NULL
    GROUP BY age_jeune_decembre
    ORDER BY age
    """
    df = fetch_data(query)
    
    if df is not None:
        fig = px.histogram(df, x='age', y='count', 
                         title="Distribution des âges des apprenants",
                         labels={'age': 'Âge', 'count': "Nombre d'apprenants"},
                         nbins=20)
        fig.update_layout(bargap=0.1)
        fig.show()

def plot_gender_distribution():
    """Répartition par sexe des apprenants"""
    query = """
    SELECT sexe, COUNT(*) as count 
    FROM APPRENANT 
    WHERE sexe IS NOT NULL
    GROUP BY sexe
    """
    df = fetch_data(query)
    
    if df is not None:
        fig = px.pie(df, values='count', names='sexe',
                    title="Répartition par sexe des apprenants",
                    hole=0.3)
        fig.update_traces(textposition='inside', textinfo='percent+label')
        fig.show()

def plot_top_specialties(n=10):
    """Top n des spécialités les plus populaires"""
    query = f"""
    SELECT s.libelle_specialite, COUNT(c.id_contrat) as count
    FROM CONTRAT c
    JOIN SPECIALITE s ON c.code_groupe_specialite = s.code_groupe_specialite
    GROUP BY s.libelle_specialite
    ORDER BY count DESC
    LIMIT {n}
    """
    df = fetch_data(query)
    
    if df is not None:
        fig = px.bar(df, x='libelle_specialite', y='count',
                    title=f"Top {n} des spécialités les plus populaires",
                    labels={'libelle_specialite': 'Spécialité', 'count': "Nombre de contrats"},
                    color='count')
        fig.update_layout(xaxis={'categoryorder':'total descending'})
        fig.show()

def plot_contracts_by_department():
    """Nombre de contrats par département"""
    query = """
    SELECT e.depart_entreprise as departement, COUNT(*) as count
    FROM CONTRAT c
    JOIN ENTREPRISE e ON c.code_insee_entreprise = e.code_insee_entreprise
    WHERE e.depart_entreprise IS NOT NULL
    GROUP BY e.depart_entreprise
    """
    df = fetch_data(query)
    
    if df is not None:
        fig = px.choropleth(df, 
                          locations="departement", 
                          locationmode="FR-department",
                          color="count",
                          scope="europe",
                          title="Nombre de contrats par département en France",
                          color_continuous_scale="Blues",
                          hover_name="departement",
                          hover_data={"count": True, "departement": False})
        fig.update_geos(fitbounds="locations", visible=False)
        fig.update_layout(margin={"r":0,"t":30,"l":0,"b":0})
        fig.show()

def plot_duration_by_specialty():
    """Durée moyenne de formation par spécialité (top 10)"""
    query = """
    SELECT s.libelle_specialite, AVG(d.duree_formation_mois) as duree_moyenne
    FROM DIPLOME d
    JOIN SPECIALITE s ON d.code_groupe_specialite = s.code_groupe_specialite
    WHERE d.duree_formation_mois IS NOT NULL
    GROUP BY s.libelle_specialite
    ORDER BY duree_moyenne DESC
    LIMIT 10
    """
    df = fetch_data(query)
    
    if df is not None:
        fig = px.bar(df, x='libelle_specialite', y='duree_moyenne',
                    title="Durée moyenne de formation par spécialité (top 10)",
                    labels={'libelle_specialite': 'Spécialité', 'duree_moyenne': 'Durée moyenne (mois)'},
                    color='duree_moyenne')
        fig.update_layout(xaxis={'categoryorder':'total descending'})
        fig.show()

def plot_handicap_distribution():
    """Répartition des apprenants en situation de handicap"""
    query = """
    SELECT 
        CASE 
            WHEN handicap_oui_non_vide IS NULL THEN 'Non renseigné'
            WHEN handicap_oui_non_vide = '' THEN 'Non renseigné'
            ELSE handicap_oui_non_vide
        END as handicap,
        COUNT(*) as count
    FROM APPRENANT
    GROUP BY handicap
    """
    df = fetch_data(query)
    
    if df is not None:
        fig = px.pie(df, values='count', names='handicap',
                    title="Répartition des apprenants en situation de handicap",
                    hole=0.3)
        fig.update_traces(textposition='inside', textinfo='percent+label')
        fig.show()

def plot_contracts_by_year():
    """Évolution du nombre de contrats par année scolaire"""
    query = """
    SELECT annee_scolaire, COUNT(*) as count
    FROM CONTRAT
    GROUP BY annee_scolaire
    ORDER BY annee_scolaire
    """
    df = fetch_data(query)
    
    if df is not None:
        fig = px.line(df, x='annee_scolaire', y='count',
                     title="Évolution du nombre de contrats par année scolaire",
                     labels={'annee_scolaire': 'Année scolaire', 'count': 'Nombre de contrats'})
        fig.update_traces(mode='lines+markers')
        fig.show()

def create_dashboard():
    """Crée un tableau de bord avec plusieurs visualisations"""
    fig = make_subplots(rows=2, cols=2,
                       subplot_titles=("Distribution des âges", "Répartition par sexe",
                                      "Top 10 des spécialités", "Évolution des contrats"))
    
    # Graphique 1: Distribution des âges
    age_query = """
    SELECT age_jeune_decembre as age, COUNT(*) as count 
    FROM APPRENANT 
    WHERE age_jeune_decembre IS NOT NULL
    GROUP BY age_jeune_decembre
    ORDER BY age
    """
    age_df = fetch_data(age_query)
    fig.add_trace(go.Histogram(x=age_df['age'], y=age_df['count'], name="Âges"), row=1, col=1)
    
    # Graphique 2: Répartition par sexe
    gender_query = """
    SELECT sexe, COUNT(*) as count 
    FROM APPRENANT 
    WHERE sexe IS NOT NULL
    GROUP BY sexe
    """
    gender_df = fetch_data(gender_query)
    fig.add_trace(go.Pie(labels=gender_df['sexe'], values=gender_df['count'], name="Sexe"), row=1, col=2)
    
    # Graphique 3: Top 10 spécialités
    spec_query = """
    SELECT s.libelle_specialite, COUNT(c.id_contrat) as count
    FROM CONTRAT c
    JOIN SPECIALITE s ON c.code_groupe_specialite = s.code_groupe_specialite
    GROUP BY s.libelle_specialite
    ORDER BY count DESC
    LIMIT 10
    """
    spec_df = fetch_data(spec_query)
    fig.add_trace(go.Bar(x=spec_df['libelle_specialite'], y=spec_df['count'], name="Spécialités"), row=2, col=1)
    
    # Graphique 4: Évolution des contrats
    year_query = """
    SELECT annee_scolaire, COUNT(*) as count
    FROM CONTRAT
    GROUP BY annee_scolaire
    ORDER BY annee_scolaire
    """
    year_df = fetch_data(year_query)
    fig.add_trace(go.Scatter(x=year_df['annee_scolaire'], y=year_df['count'], 
                   mode='lines+markers', name="Contrats"), row=2, col=2)
    
    fig.update_layout(height=800, width=1000, title_text="Tableau de bord des formations en apprentissage")
    fig.show()

def main():
    """Fonction principale pour exécuter les visualisations"""
    print("Génération des visualisations...")
    
    # Visualisations individuelles
    plot_age_distribution()
    plot_gender_distribution()
    plot_top_specialties()
    plot_contracts_by_department()
    plot_duration_by_specialty()
    plot_handicap_distribution()
    plot_contracts_by_year()
    
    # Tableau de bord complet
    create_dashboard()
    
    print("Visualisations terminées!")

if __name__ == "__main__":
    main()