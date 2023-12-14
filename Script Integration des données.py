import requests
import mysql.connector

# Connection à la base de données
conn = mysql.connector.connect(
    host='localhost',
    user='root',
    password='123123123',
    database='commune_de_france'
)
cursor = conn.cursor()

# On selecctionne les ligne dont la population est nulle 
cursor.execute("SELECT Code_commune_INSEE FROM communes WHERE Population IS NULL")
rows = cursor.fetchall()

# On initialise les Partie de l'URL debut et 
baseURL = "https://geo.api.gouv.fr/communes/"
endURL = "?fields=population&format=json"

# On parcours chaque ligne en recuperant le code INSEE 
for row in rows:
    code_INSEE = str(row[0]).zfill(5)  # On comble de Zero si neccessaire 
    api_url = f"{baseURL}{code_INSEE}{endURL}"

    # On construit la requette API
    response = requests.get(api_url)
    
    # On Affiche la reponse pour avoir de la visibilité
    print(f"API Response for {code_INSEE}: {response.text}")

    data = response.json()

    # On Affiche le contenu de la reponse
    print(f"Retrieved Data for {code_INSEE}: {data}")

    # On recupere les données de population
    population = data.get('population', None)

    # On mets a jour les inforation de opulation dans la base de données
    cursor.execute("UPDATE communes SET Population = %s WHERE Code_commune_INSEE = %s", (population, code_INSEE))

# On commit
conn.commit()

# On ferme la connection a la base de données
conn.close()
