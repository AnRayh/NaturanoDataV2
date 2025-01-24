import os.path
import pandas as pd
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

# Scopes nécessaires
SCOPES = ['https://www.googleapis.com/auth/spreadsheets']

def extract_sheet_id(sheet_url):
    """
    Extrait l'ID d'un classeur Google Sheets depuis son URL.
    """
    if "docs.google.com/spreadsheets/d/" in sheet_url:
        return sheet_url.split("/d/")[1].split("/")[0]
    raise ValueError("URL du classeur invalide.")

def traiter_kiosque(data):
    # Convertir les données en DataFrame
    df = pd.DataFrame(data)

    # Séparer la colonne 'Numéro carte' en 'Compteur' et 'ID'
    df[['Compteur', 'ID']] = df['Numéro carte'].str.split(expand=True)

def tel(phone):
    if phone and isinstance(phone, str) and phone.strip():
        phone = phone.replace(" ", "").strip()
        if len(phone) == 10:  # Format attendu
            return f"{phone[:3]}-{phone[3:5]}-{phone[5:8]}-{phone[8:]}"
    return phone

def main():
    # Entrée des URLs et noms des feuilles
    source_url = ""
    source_sheet_name = "liste_cartes"
    destination_url = ""
    destination_sheet_name = ""

    # Extraction des IDs des classeurs
    source_id = extract_sheet_id(source_url)
    destination_id = extract_sheet_id(destination_url)

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

        # Lecture des données depuis la feuille source
        range_source = f"{source_sheet_name}!A:G"
        result = service.spreadsheets().values().get(
            spreadsheetId=source_id, range=range_source
        ).execute()
        values = result.get("values", [])

        if not values:
            print("Aucune donnée trouvée dans la feuille source.")
            return

        # Transformation des données en DataFrame
        df = pd.DataFrame(values[1:], columns=values[0])

        # Application des transformations
        df[['Compteur', 'ID']] = df['Numéro carte'].str.split(expand=True)
        df['Téléphone'] = df['Téléphone'].apply(lambda x: tel(x))

        # Ajout des données transformées dans la feuille destination
        body = {
            'values': [df.columns.tolist()] + df.values.tolist()
        }
        range_destination = f"{destination_sheet_name}!A:H"
        service.spreadsheets().values().update(
            spreadsheetId=destination_id,
            range=range_destination,
            valueInputOption="RAW",
            body=body
        ).execute()

       
    except HttpError as err:
        print(f"Une erreur s'est produite : {err}")


if __name__ == "__main__":
    main()