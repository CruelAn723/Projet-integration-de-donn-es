import requests
import mysql.connector
from concurrent.futures import ThreadPoolExecutor
import configparser


config = configparser.ConfigParser()
config.read('Config.ini')

db_config = config['database']

# Connection à la base de données
conn = mysql.connector.connect(
    host=db_config['host'],
    user=db_config['user'],
    password=db_config['password'],
    database=db_config['database']
)

cursor = conn.cursor()

batch_size = 95  # Number of code_INSEEs to fetch in each batch

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
        continue  # Skip empty batches

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
        continue  # Skip the rest of the processing for this batch if there's an error

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
            continue  # Skip the rest of the processing for this item if there's an error

        # On met à jour la base de données pour chaque ligne
        code_INSEE = str(current_batch[j][0]).zfill(5)
        try:
            update_query = "UPDATE communes SET Population = %s WHERE Code_commune_INSEE = %s"
            cursor.execute(update_query, (population, code_INSEE))
            print(f"Updated population for {code_INSEE}: {population}")
        except mysql.connector.Error as e:
            print(f"Error updating database: {e}")

    # On commit les modifications après le traitement de chaque lot
    conn.commit()

# On ferme la connexion
conn.close()
