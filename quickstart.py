import os.path
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

# SCOPES corrigés
SCOPES = ['https://www.googleapis.com/auth/spreadsheets',
          'https://www.googleapis.com/auth/drive']

SAMPLE_SPREADSHEET_ID = ""
SAMPLE_RANGE_NAME = ""
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
        sheet = service.spreadsheets()

        # Assurez-vous que cette plage existe dans votre feuille
        SAMPLE_RANGE_NAME = "Test1!A1:C10"  # Adaptez la plage
        result = sheet.values().get(
            spreadsheetId=SAMPLE_SPREADSHEET_ID, range=SAMPLE_RANGE_NAME
        ).execute()
        values = result.get("values", [])

        if not values:
            print("Aucune donnée trouvée dans la plage spécifiée.")
            return

        print("Données récupérées :")
        for row in values:
            print(row)

    except HttpError as err:
        print(f"Une erreur s'est produite : {err}")

if __name__ == "__main__":
    main()
