import os.path
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
import pandas as pd
from datetime import datetime
import numpy as np

# Scopes nécessaires
SCOPES = ['https://www.googleapis.com/auth/spreadsheets']

# Fonctions utilitaires
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

def VoltageBatt(Voltage):
    return pd.Series([
        f"{float(value):.2f}" if value.replace(".", "", 1).isdigit() else np.nan
        for value in Voltage
    ])

def DateHeure(Date):
    return pd.Series([
        datetime.strptime(date_str, "%d/%m/%Y %H:%M:%S").isoformat()
        if pd.notna(date_str) else np.nan
        for date_str in Date
    ])

def ValidityFormat(validite):
    return validite.replace("undefined", " ")

def TempDistribution(temp):
    temp_in_sec = temp // 1000
    hours, remainder = divmod(temp_in_sec, 3600)
    minutes, seconds = divmod(remainder, 60)
    return f"{hours:02}:{minutes:02}:{seconds:02}"

def main():
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

        # URL des classeurs source et destination
        source_url = "https://docs.google.com/spreadsheets/d/1CX5ZU04Rb6vdVB91H5lrW2IebO3cWkkR8QCDaZST7w/edit"
        destination_url = ""

        # Extraction des IDs des classeurs
        source_id = extract_sheet_id(source_url)
        destination_id = extract_sheet_id(destination_url)

        # Lecture des données depuis le fichier source
        SAMPLE_RANGE_NAME = "kdata!A:G"
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
        df['deviceID'] = NumeroTel(df['deviceID'])
        df['batt_voltage'] = VoltageBatt(df['batt_voltage'])
        df['date'] = DateHeure(df['date'])
        df['unix_validity'] = df['unix_validity'].apply(ValidityFormat)
        df['dureeDis'] = df['dureeDis'].astype(int).apply(TempDistribution)

        # Écriture dans le fichier destination
        body = {
            'values': [df.columns.tolist()] + df.values.tolist()
        }
        service.spreadsheets().values().update(
            spreadsheetId=destination_id,
            range="Kiosque_Data!A:G",
            valueInputOption="RAW",
            body=body
        ).execute()

        print(f"Les données ont été écrites dans le fichier destination : {destination_url}")

    except HttpError as err:
        print(f"Une erreur s'est produite : {err}")

if __name__ == "__main__":
    main()
