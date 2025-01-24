import os.path
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
import pandas as pd

# Scopes nécessaires
SCOPES = ['https://www.googleapis.com/auth/spreadsheets']

# Fonction utilitaire pour extraire l'ID du classeur depuis une URL
def extract_sheet_id(url):
    """Extrait l'ID du classeur depuis son URL"""
    try:
        return url.split('/d/')[1].split('/')[0]
    except IndexError:
        raise ValueError("URL invalide. Assurez-vous qu'il s'agit d'une URL Google Sheets valide.")

# Traitement des colonnes
def NumeroTel(deviceID):
    return pd.Series([
        number.replace("'", "").strip().replace(" ", "-")
        if " " in number else
        f"{number[:3]}-{number[3:5]}-{number[5:8]}-{number[8:]}" if len(number) == 10
        else number
        for number in deviceID
    ])

def FormatKiosque(kiosque, to_upper=True):
    return pd.Series([name.upper() if to_upper else name.lower() for name in kiosque])

def process_google_sheets(source_url, destination_url=None):
    creds = None
    if os.path.exists("token.json"):
        creds = Credentials.from_authorized_user_file("token.json", SCOPES)
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                "credentials.json", SCOPES
            )
            creds = flow.run_local_server(port=3000)
        with open("token.json", "w") as token:
            token.write(creds.to_json())

    try:
        service = build("sheets", "v4", credentials=creds)

        # Extraction des IDs des classeurs
        source_id = extract_sheet_id(source_url)
        destination_id = None
        if destination_url:
            destination_id = extract_sheet_id(destination_url)

        # Lecture des données existantes
        SAMPLE_RANGE_NAME = "Liste kiosque!A:E"
        result = service.spreadsheets().values().get(
            spreadsheetId=source_id, range=SAMPLE_RANGE_NAME
        ).execute()
        values = result.get("values", [])

        if not values:
            print("Aucune donnée trouvée dans le fichier source.")
            return

        # Transformation en DataFrame
        df = pd.DataFrame(values[1:], columns=values[0])

        # Application des transformations
        df['Numéro'] = NumeroTel(df['Numéro'])
        df['Kiosque'] = FormatKiosque(df['Kiosque'], to_upper=True)

        # Vérification si un fichier destination est fourni
        if destination_id:
            # Ajout des données transformées dans le fichier destination existant
            body = {
                'values': [df.columns.tolist()] + df.values.tolist()
            }
            service.spreadsheets().values().update(
                spreadsheetId=destination_id,
                range="Listes_Kiosques!A:E",
                valueInputOption="RAW",
                body=body
            ).execute()
        else:
            # Création d'un nouveau fichier Google Sheets
            NEW_SPREADSHEET_TITLE = "DataBase I"
            spreadsheet = service.spreadsheets().create(body={
                'properties': {'title': NEW_SPREADSHEET_TITLE}
            }).execute()
            new_spreadsheet_id = spreadsheet['spreadsheetId']

            # Ajout des données transformées dans le nouveau fichier
            body = {
                'values': [df.columns.tolist()] + df.values.tolist()
            }
            service.spreadsheets().values().update(
                spreadsheetId=new_spreadsheet_id,
                range="Listes_Kiosques!A:E",
                valueInputOption="RAW",
                body=body
            ).execute()

            

    except HttpError as err:
        print(f"Une erreur s'est produite : {err}")

if __name__ == "__main__":
    source_url = ""
    destination_url = ""
    process_google_sheets(source_url, destination_url)
