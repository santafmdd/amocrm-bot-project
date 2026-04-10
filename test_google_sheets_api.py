import os.path

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

# Для первого теста берем чтение и запись в таблицу
SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

# ВСТАВЬ СЮДА ID таблицы
SPREADSHEET_ID = "1snOH42aIRUtxS3AU9PJPHSrk1vDyFQdiIJNJRhasxX0"

# Тестовый диапазон
READ_RANGE = "analytics_writer_test!E1:J15"
WRITE_RANGE = "analytics_writer_test!J25"


def main():
    creds = None

    if os.path.exists("token.json"):
        creds = Credentials.from_authorized_user_file("token.json", SCOPES)

    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file("credentials.json", SCOPES)
            creds = flow.run_local_server(port=0)

        with open("token.json", "w", encoding="utf-8") as token:
            token.write(creds.to_json())

    try:
        service = build("sheets", "v4", credentials=creds)

        sheet = service.spreadsheets()

        print("=== ЧТЕНИЕ ===")
        result = sheet.values().get(
            spreadsheetId=SPREADSHEET_ID,
            range=READ_RANGE
        ).execute()

        values = result.get("values", [])
        if not values:
            print("Диапазон пустой или не прочитан.")
        else:
            for row in values:
                print(row)

        print("\n=== ЗАПИСЬ ===")
        body = {
            "values": [["api test ok"]]
        }

        write_result = sheet.values().update(
            spreadsheetId=SPREADSHEET_ID,
            range=WRITE_RANGE,
            valueInputOption="RAW",
            body=body
        ).execute()

        print("Запись выполнена.")
        print(write_result)

    except HttpError as err:
        print(f"Ошибка Google API: {err}")


if __name__ == "__main__":
    main()