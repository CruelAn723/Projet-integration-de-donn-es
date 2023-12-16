import requests
import mysql.connector
from concurrent.futures import ThreadPoolExecutor

# Connection à la base de données
conn = mysql.connector.connect(
    host='localhost',
    user='root',
    password='123123123',
    database='commune_de_france'
)
cursor = conn.cursor()

batch_size = 95  # Nombre de code INSEE a recuperer pour chaque batch

# On sélectionne les lignes dont la population est nulle 
cursor.execute("SELECT Code_commune_INSEE FROM communes WHERE Population IS NULL")
rows = cursor.fetchall()

# On initialise les parties de l'URL début et fin 
baseURL = "https://geo.api.gouv.fr/communes/"
endURL = "?fields=population&format=json"

# Initialise des listes vides pour stocker les réponses d'API et les données populations
api_responses = []
population_data = []

# On divise les lignes en batch et on construit les requêtes vers l'API
for i in range(0, len(rows), batch_size):
    current_batch = rows[i:i + batch_size]

    if not current_batch:
        continue  # on ignore les batch vides

    # On construit les URLs de requêtes avec les code_INSEE
    api_urls = [f"{baseURL}{str(row[0]).zfill(5)}{endURL}" for row in current_batch]

    print(f"\nProcessing batch {i//batch_size + 1}...")
    print("API URLs:", api_urls)

    # On envoie des requêtes d'API en parallèle grâce à ThreadPoolExecutor
    try:
        with ThreadPoolExecutor() as executor:
            responses = list(executor.map(requests.get, api_urls))
    except Exception as e:
        print(f"Error in API request: {e}")
        # On log les erreurs et les exceptions
        continue  # En cas d'erreur sur le batch on igonre et on passe au suivant

    # On extrait les données de population de la réponse de l'API
    for j, response in enumerate(responses):
        api_responses.append(response)

        try:
            data = response.json()
            population = data.get('population', None)
            population_data.append(population)
            print(f"Population for {current_batch[j][0]}: {population}")
        except Exception as e:
            print(f"Error parsing response: {e}")
            # On log les données et les erreurs si nécessaire 
            continue  # On ignore le reste du traitement en cas d'erreur

        # On met à jour la base de données pour chaque ligne
        code_INSEE = str(current_batch[j][0]).zfill(5)
        try:
            cursor.execute("UPDATE communes SET Population = %s WHERE Code_commune_INSEE = %s", (population, code_INSEE))
            print(f"Updated population for {code_INSEE}: {population}")
        except Exception as e:
            print(f"Error updating database: {e}")
            # On log les données et les erreurs si nécessaire 

    # On commit les modifications après le traitement de chaque lot
    conn.commit()

# On ferme la connexion
conn.close()
