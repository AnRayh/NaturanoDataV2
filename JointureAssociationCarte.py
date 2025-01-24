import pandas as pd
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from google.oauth2.service_account import Credentials

# Authentification Google Sheets
def authenticate_google_sheets():
    """
    Authentifie le script avec l'API Google Sheets en utilisant un fichier de clé de service.
    """
    creds = Credentials.from_service_account_file(
        'chemin/vers/votre/clé/service.json', 
        scopes=["https://www.googleapis.com/auth/spreadsheets"]
    )
    service = build('sheets', 'v4', credentials=creds)
    return service

# Extraction de l'ID du classeur depuis une URL
def extract_sheet_id(spreadsheet_url):
    """
    Extrait l'ID du classeur depuis une URL Google Sheets.
    """
    return spreadsheet_url.split("/d/")[1].split("/")[0]

# Lecture des données d'une feuille Google Sheets
def read_sheet(service, spreadsheet_id, range_name):
    """
    Lit les données d'une feuille Google Sheets et les retourne sous forme de DataFrame.
    """
    sheet = service.spreadsheets()
    result = sheet.values().get(spreadsheetId=spreadsheet_id, range=range_name).execute()
    values = result.get('values', [])
    
    if not values:
        return pd.DataFrame()  # Retourne un DataFrame vide si aucune donnée n'est trouvée

    # La première ligne contient les en-têtes
    headers = values[0]
    data = values[1:]
    return pd.DataFrame(data, columns=headers)

# Écriture des données dans une feuille Google Sheets
def write_sheet(service, spreadsheet_id, range_name, dataframe):
    """
    Écrit les données d'un DataFrame dans une feuille Google Sheets.
    """
    sheet = service.spreadsheets()
    data = [dataframe.columns.tolist()] + dataframe.values.tolist()
    body = {
        'values': data
    }
    sheet.values().update(
        spreadsheetId=spreadsheet_id,
        range=range_name,
        valueInputOption="RAW",
        body=body
    ).execute()

# Copier les données de liste_cartes vers Cartes
def copy_liste_cartes_to_cartes(service, spreadsheet_id_source, spreadsheet_id_destination):
    """
    Copie les données de la feuille 'liste_cartes' dans le classeur source
    vers la feuille 'Cartes' dans le classeur destination.
    """
    # Lire les données de la feuille 'liste_cartes'
    liste_cartes_df = read_sheet(service, spreadsheet_id_source, "liste_cartes!A:G")

    if liste_cartes_df.empty:
        print("La feuille 'liste_cartes' est vide. Aucune donnée à copier.")
        return

    # Écrire les données dans la feuille 'Cartes' du classeur destination
    write_sheet(service, spreadsheet_id_destination, "Cartes!A:G", liste_cartes_df)
    print("Les données de 'liste_cartes' ont été copiées avec succès dans 'Cartes'.")

# Fonction principale
def main():
    try:
        # Authentification et création du service
        service = authenticate_google_sheets()

        # URL et extraction de l'ID du classeur source
        spreadsheet_url_source = ""
        spreadsheet_id_source = extract_sheet_id(spreadsheet_url_source)

        # URL et extraction de l'ID du classeur destination (où on écrit les données)
        spreadsheet_url_destination = ""
        spreadsheet_id_destination = extract_sheet_id(spreadsheet_url_destination)

        # Copier les données de 'liste_cartes' vers 'Cartes'
        copy_liste_cartes_to_cartes(service, spreadsheet_id_source, spreadsheet_id_destination)

    except HttpError as err:
        print(f"Une erreur s'est produite : {err}")
    except Exception as e:
        print(f"Erreur inattendue : {e}")

# Point d'entrée principal
if __name__ == "__main__":
    main()
