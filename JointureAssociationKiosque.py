import os
import pandas as pd
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

# Scopes nécessaires
SCOPES = ['https://www.googleapis.com/auth/spreadsheets']

def authenticate_google_sheets(token_file="token.json", credentials_file="credentials.json"):
    """Authentifie et retourne un service Google Sheets."""
    creds = None
    if os.path.exists(token_file):
        creds = Credentials.from_authorized_user_file(token_file, SCOPES)
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(credentials_file, SCOPES)
            creds = flow.run_local_server(port=3000)
        with open(token_file, "w") as token:
            token.write(creds.to_json())
    return build("sheets", "v4", credentials=creds)

def extract_sheet_id(url):
    """Extrait l'ID du classeur depuis son URL."""
    try:
        return url.split('/d/')[1].split('/')[0]
    except IndexError:
        raise ValueError("URL invalide. Assurez-vous qu'il s'agit d'une URL Google Sheets valide.")

def read_sheet(service, spreadsheet_id, range_name):
    """Lit les données d'une feuille Google Sheets et retourne un DataFrame."""
    result = service.spreadsheets().values().get(spreadsheetId=spreadsheet_id, range=range_name).execute()
    values = result.get("values", [])
    if not values:
        print(f"Aucune donnée trouvée dans la plage : {range_name}")
        return pd.DataFrame()
    return pd.DataFrame(values[1:], columns=values[0])

def write_sheet(service, spreadsheet_id, range_name, data_frame):
    """Écrit les données d'un DataFrame dans une feuille Google Sheets."""
    body = {'values': [data_frame.columns.tolist()] + data_frame.values.tolist()}
    service.spreadsheets().values().update(spreadsheetId=spreadsheet_id, range=range_name, valueInputOption="RAW", body=body).execute()
    print(f"Données écrites dans la plage : {range_name}")

def generate_kiosque_table(kdata_df, kiosque_df):
    """Génère le tableau 'Kiosque' avec les données nécessaires."""
    
    # Joindre les données sur deviceID
    kiosque_data = kdata_df.merge(kiosque_df[['Numéro', 'Localisation', 'Fonctionnalité']], left_on='deviceID', right_on='Numéro', how='left')
    
    # Sélectionner et calculer les colonnes nécessaires
    kiosque_data = kiosque_data[['Date', 'deviceID',  'Localisation', 'Fonctionnalité', 'Duree de Fonctionnement', 'Batterie Voltage']]
    
    # Trier par Date
    kiosque_data['Date'] = pd.to_datetime(kiosque_data['Date'], errors='coerce')
    kiosque_data = kiosque_data.sort_values(by='Date', ascending=False)
    
    # Ajouter Type de Kiosque et Statut (les valeurs doivent être fournies ou définies par l'utilisateur)
    kiosque_data['Type de Kiosque'] = 'Vente'  # Par exemple, ou une autre logique
    kiosque_data['Statut'] = 'Fonctionnel'  # Par exemple, ou une autre logique

    return kiosque_data

def main():
    try:
        # Authentification et création du service
        service = authenticate_google_sheets()

        # URL et extraction de l'ID du classeur source
        spreadsheet_url_source = "https://docs.google.com/spreadsheets/d/1CX5ZU04Rb6vdVB91H5lrW2IebO3cWkkR8QCDaZST7w/edit"
        spreadsheet_id_source = extract_sheet_id(spreadsheet_url_source)

        # URL et extraction de l'ID du classeur destination
        spreadsheet_url_destination = "https://docs.google.com/spreadsheets/d/DESTINATION_SPREADSHEET_ID/edit"
        spreadsheet_id_destination = extract_sheet_id(spreadsheet_url_destination)

        # Lecture des feuilles du classeur source
        kiosque_df = read_sheet(service, spreadsheet_id_source, "liste kiosque!A:E")
        kdata_df = read_sheet(service, spreadsheet_id_source, "kdata!A:G")

        if kiosque_df.empty or kdata_df.empty:
            print("Une ou plusieurs feuilles sont vides. Vérifiez les données.")
            return

        # Création de la table 'système'
        system_df = generate_kiosque_table(kiosque_df, kdata_df)

        # Écriture des données dans la feuille "système" du classeur destination
        write_sheet(service, spreadsheet_id_destination, "système!A:F", system_df)

    except HttpError as err:
        print(f"Une erreur s'est produite : {err}")
    except Exception as e:
        print(f"Erreur inattendue : {e}")

if __name__ == "__main__":
    main()