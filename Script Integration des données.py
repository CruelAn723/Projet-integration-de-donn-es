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

batch_size = 50  # Number of code_INSEEs to fetch in each batch

# On selecctionne les lignes dont la population est nulle 
cursor.execute("SELECT Code_commune_INSEE FROM communes WHERE Population IS NULL")
rows = cursor.fetchall()

# On initialise les Parties de l'URL debut et fin 
baseURL = "https://geo.api.gouv.fr/communes/"
endURL = "?fields=population&format=json"

# Initialise des listes vides pour stocker les reponses d'api et les données populations
api_responses = []
population_data = []

# on divise les lignes en batch et On construits les requetes vers l'api
for i in range(0, len(rows), batch_size):
    current_batch = rows[i:i + batch_size]

    # on construit les URLs de requetes avec les code_INSEE
    api_urls = [f"{baseURL}{str(row[0]).zfill(5)}{endURL}" for row in current_batch]

    print(f"\nProcessing batch {i//batch_size + 1}...")
    print("API URLs:", api_urls)

    # On envoie des requettes d'Api en parralle grace à ThreadPoolExecutor
    try:
        with ThreadPoolExecutor() as executor:
            responses = list(executor.map(requests.get, api_urls))
    except Exception as e:
        print(f"Error in API request: {e}")
        # on Log les erreurs et les  exceptions

    # On extrait les données de population de la reponse de l'API
    for j, response in enumerate(responses):
        api_responses.append(response)

        try:
            data = response.json()
            population = data.get('population', None)
            population_data.append(population)
            print(f"Population for {current_batch[j][0]}: {population}")
        except Exception as e:
            print(f"Error parsing response: {e}")
            # on Log les données et les erreurs si necessaire 

# On mets a jour la base de données 
for i, row in enumerate(current_batch):
    code_INSEE = str(row[0]).zfill(5)
    population = population_data[i]

    try:
        cursor.execute("UPDATE communes SET Population = %s WHERE Code_commune_INSEE = %s", (population, code_INSEE))
        print(f"Updated population for {code_INSEE}: {population}")
    except Exception as e:
        print(f"Error updating database: {e}")
        # on Log les données et les erreurs si necessaire 


# on Commit les modifications
conn.commit()

# on ferme la connection
conn.close()
