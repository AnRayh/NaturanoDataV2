import pandas as pd
from googleapiclient.errors import HttpError
from googleapiclient.discovery import build
from google.oauth2.service_account import Credentials

#### Mbola miandry kely fa manahirana
# Fonction pour authentifier Google Sheets
def authenticate_google_sheets():
    creds = Credentials.from_service_account_file(
        'path/to/your/credentials.json',
        scopes=["https://www.googleapis.com/auth/spreadsheets"]
    )
    service = build('sheets', 'v4', credentials=creds)
    return service

def extract_sheet_id(url):
    """Extrait l'ID du classeur depuis son URL."""
    try:
        return url.split('/d/')[1].split('/')[0]
    except IndexError:
        raise ValueError("URL invalide. Assurez-vous qu'il s'agit d'une URL Google Sheets valide.")
    
# Fonction pour lire une feuille Google Sheets dans un DataFrame
def read_sheet(service, spreadsheet_id, range_name):
    sheet = service.spreadsheets()
    result = sheet.values().get(spreadsheetId=spreadsheet_id, range=range_name).execute()
    values = result.get('values', [])
    if not values:
        return pd.DataFrame()  # Retourner un DataFrame vide si la feuille est vide
    return pd.DataFrame(values[1:], columns=values[0])  # En-têtes de colonne dans la première ligne

# Fonction pour écrire un DataFrame dans une feuille Google Sheets
def write_sheet(service, spreadsheet_id, range_name, dataframe):
    sheet = service.spreadsheets()
    body = {
        'values': [dataframe.columns.tolist()] + dataframe.values.tolist()
    }
    sheet.values().update(
        spreadsheetId=spreadsheet_id,
        range=range_name,
        valueInputOption='RAW',
        body=body
    ).execute()

def generate_systeme_table_from_kdata(kdata_df):
    """
    Génère le tableau 'Système' directement à partir des données de 'kdata'.
    """
    # Vérifier si les colonnes nécessaires existent dans kdata
    required_columns = ['deviceID', 'Date', 'Localisation', 'EtatSim800L', 'EtatRFID', 'EtatRTC', 
                        'EtatLCD', 'EtatWire', 'EtatDebimetre']
    for col in required_columns:
        if col not in kdata_df.columns:
            raise ValueError(f"La colonne {col} est manquante dans les données de 'kdata'.")

    # Sélectionner les colonnes nécessaires
    systeme_data = kdata_df[required_columns].copy()

    # Convertir les dates pour tri
    systeme_data['Date'] = pd.to_datetime(systeme_data['Date'], errors='coerce')

    # Évaluer l'état global
    def evaluate_status(row):
        components = ['EtatSim800L', 'EtatRFID', 'EtatRTC', 'EtatLCD', 'EtatWire', 'EtatDebimetre']
        if all(row[comp] == 'OK' for comp in components):
            return 'Fonctionnel'
        return 'Défaut'

    systeme_data['État Global'] = systeme_data.apply(evaluate_status, axis=1)

    # Ajouter des commentaires
    def generate_comment(row):
        components = ['EtatSim800L', 'EtatRFID', 'EtatRTC', 'EtatLCD', 'EtatWire', 'EtatDebimetre']
        issues = [comp for comp in components if row[comp] == 'NON']
        return ", ".join(issues) if issues else "Aucun problème détecté"

    systeme_data['Commentaires'] = systeme_data.apply(generate_comment, axis=1)

    # Trier par date
    systeme_data = systeme_data.sort_values(by='Date', ascending=False)

    return systeme_data

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

        # Lecture de la feuille 'kdata' du classeur source
        kdata_df = read_sheet(service, spreadsheet_id_source, "kdata!A:J")

        if kdata_df.empty:
            print("La feuille 'kdata' est vide. Vérifiez les données.")
            return

        # Génération de la table 'Système'
        systeme_df = generate_systeme_table_from_kdata(kdata_df)

        # Écriture des données dans la feuille "Système" du classeur destination
        write_sheet(service, spreadsheet_id_destination, "Système!A:K", systeme_df)

    except HttpError as err:
        print(f"Une erreur s'est produite : {err}")
    except Exception as e:
        print(f"Erreur inattendue : {e}")

if __name__ == "__main__":
    main()
