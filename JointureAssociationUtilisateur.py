import pandas as pd
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from google.oauth2.service_account import Credentials

# Authentification Google Sheets
def authenticate_google_sheets():
    creds = Credentials.from_service_account_file(
        'chemin/vers/votre/clé/service.json', 
        scopes=["https://www.googleapis.com/auth/spreadsheets"]
    )
    service = build('sheets', 'v4', credentials=creds)
    return service

# Extraction de l'ID du classeur depuis une URL
def extract_sheet_id(spreadsheet_url):
    return spreadsheet_url.split("/d/")[1].split("/")[0]

# Lecture des données d'une feuille Google Sheets
def read_sheet(service, spreadsheet_id, range_name):
    sheet = service.spreadsheets()
    result = sheet.values().get(spreadsheetId=spreadsheet_id, range=range_name).execute()
    values = result.get('values', [])
    if not values:
        return pd.DataFrame()
    headers = values[0]
    data = values[1:]
    return pd.DataFrame(data, columns=headers)

# Écriture des données dans une feuille Google Sheets
def write_sheet(service, spreadsheet_id, range_name, dataframe):
    sheet = service.spreadsheets()
    data = [dataframe.columns.tolist()] + dataframe.values.tolist()
    body = {'values': data}
    sheet.values().update(
        spreadsheetId=spreadsheet_id,
        range=range_name,
        valueInputOption="RAW",
        body=body
    ).execute()

# Génération du tableau "Utilisateur"
def generate_utilisateur_table(kdata_df, liste_cartes_df, liste_kiosque_df):
    # Joindre les données de kdata avec liste_cartes sur card_UID
    utilisateur_data = kdata_df.merge(liste_cartes_df[['card_UID', 'noms']], on='card_UID', how='left')
    
    # Joindre les données avec liste_kiosque sur deviceID
    utilisateur_data = utilisateur_data.merge(liste_kiosque_df[['deviceID', 'kiosque', 'adresse']], on='deviceID', how='left')
    
    # Sélectionner les colonnes nécessaires
    utilisateur_data = utilisateur_data[['card_UID', 'date', 'noms', 'kiosque', 'deviceID', 'adresse', 'Montant', 'Volume']]
    
    # Trier par date
    utilisateur_data['date'] = pd.to_datetime(utilisateur_data['date'], errors='coerce')
    utilisateur_data = utilisateur_data.sort_values(by='date', ascending=False)
    
    return utilisateur_data

# Fonction principale
def main():
    try:
        # Authentification et création du service
        service = authenticate_google_sheets()

        # URL et extraction de l'ID des classeurs source et destination
        spreadsheet_url_source = ""
        spreadsheet_id_source = extract_sheet_id(spreadsheet_url_source)

        spreadsheet_url_destination = ""
        spreadsheet_id_destination = extract_sheet_id(spreadsheet_url_destination)

        # Lecture des feuilles source
        kdata_df = read_sheet(service, spreadsheet_id_source, "kdata!A:H")
        liste_cartes_df = read_sheet(service, spreadsheet_id_source, "liste_cartes!A:G")
        liste_kiosque_df = read_sheet(service, spreadsheet_id_source, "liste kiosque!A:C")

        if kdata_df.empty or liste_cartes_df.empty or liste_kiosque_df.empty:
            print("Une ou plusieurs feuilles sont vides. Vérifiez les données.")
            return

        # Générer le tableau "Utilisateur"
        utilisateur_df = generate_utilisateur_table(kdata_df, liste_cartes_df, liste_kiosque_df)

        # Écrire les données dans la feuille "Utilisateur" du classeur destination
        write_sheet(service, spreadsheet_id_destination, "Utilisateur!A:H", utilisateur_df)
        print("Le tableau 'Utilisateur' a été généré avec succès.")

    except HttpError as err:
        print(f"Une erreur s'est produite : {err}")
    except Exception as e:
        print(f"Erreur inattendue : {e}")

# Point d'entrée principal
if __name__ == "__main__":
    main()
