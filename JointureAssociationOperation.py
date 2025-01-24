import os
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
import pandas as pd

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
    service.spreadsheets().values().update(
        spreadsheetId=spreadsheet_id,
        range=range_name,
        valueInputOption="RAW",
        body=body
    ).execute()
    print(f"Données écrites dans la plage : {range_name}")

def create_operations_table(kdata_df, kiosque_df, cartes_df):
    """Crée la table 'Operations' en associant les données des trois tables."""
    if kdata_df.empty or kiosque_df.empty or cartes_df.empty:
        raise ValueError("Une ou plusieurs feuilles sont vides.")
    
    # Jointure avec 'liste kiosque' pour obtenir 'Localisation'
    kdata_enriched = kdata_df.merge(kiosque_df[["deviceID", "Localisation"]], on="deviceID", how="left")

    # Jointure avec 'liste cartes' pour obtenir 'Nom Utilisateur' et autres détails
    operations = kdata_enriched.merge(
        cartes_df[["card_UID", "Noms", "Adresse"]],
        on="card_UID",
        how="left"
    )

    # Réorganiser les colonnes dans l'ordre souhaité
    operations = operations.rename(columns={
        "Noms": "Nom Utilisateur",
        "Localisation": "Localisation"
    })
    operations = operations[[
        "date","deviceID", "Localisation", "Nom Utilisateur", 
        "card_UID", "Montant", "dureeDis"
    ]]

    # Trier par date décroissante
    operations = operations.sort_values(by="date", ascending=False)

    return operations

def main():
    try:
        # Authentification et création du service
        service = authenticate_google_sheets()

        # URL et extraction de l'ID du classeur source
        spreadsheet_url_source = "https://docs.google.com/spreadsheets/d/1CX5ZU04Rb6vdVB91H5lrW2IebO3cWkkR8QCDaZST7w/edit"
        spreadsheet_id_source = extract_sheet_id(spreadsheet_url_source)

        # URL et extraction de l'ID du classeur destination (où on écrit les données)
        spreadsheet_url_destination = ""
        spreadsheet_id_destination = extract_sheet_id(spreadsheet_url_destination)

        # Lecture des feuilles du classeur source
        kdata_df = read_sheet(service, spreadsheet_id_source, "kdata!A:G")
        kiosque_df = read_sheet(service, spreadsheet_id_source, "liste kiosque!A:E")
        cartes_df = read_sheet(service, spreadsheet_id_source, "liste_cartes!A:G")

        if kdata_df.empty or kiosque_df.empty or cartes_df.empty:
            print("Une ou plusieurs feuilles sont vides. Vérifiez les données.")
            return

        # Création de la table 'Operations'
        operations_df = create_operations_table(kdata_df, kiosque_df, cartes_df)

        # Écriture des données dans la feuille "Operations" du classeur destination
        write_sheet(service, spreadsheet_id_destination, "Operations!A:I", operations_df)

    except HttpError as err:
        print(f"Une erreur s'est produite : {err}")
    except Exception as e:
        print(f"Erreur inattendue : {e}")

if __name__ == "__main__":
    main()