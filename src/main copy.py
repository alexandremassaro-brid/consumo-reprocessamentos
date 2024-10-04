from office365.runtime.auth.authentication_context import AuthenticationContext
# from office365.runtime.auth.user_credential import UserCredential
from office365.sharepoint.client_context import ClientContext
from office365.sharepoint.files.file import File
import pandas as pd
import io
import os
import numpy as np
from dotenv import load_dotenv

load_dotenv()

def main():
  # print('Main Function')
  # username = os.getenv('USERNAME')
  # password = os.getenv('PASSWORD')
  # relative_url = os.getenv('RELATIVE_URL')
  # site_url = os.getenv('SITE_URL')

  # sp_ctx = get_sp_connection(site_url, username, password)

  # if sp_ctx is not None:
  #   print('Connection successful')
  # else:
  #   print('Connection failed')
  #   exit()

  
  # Get a list of files in the Sharepoint directory
  # files = get_files_urls(sp_ctx, relative_url)

  print("📂 Getting files from xlsx directory...")
  files = os.listdir("files/xlsx")
  print("✔ Got files")

  c = 0
  falha = []
  sucesso = []

  # Loop through files
  print("📂 Start reading files...")
  for f in files :
    if (os.path.isdir(f)):
      continue

    print("------------------------------------------------------------")

    c += 1
    print("📂 Checking integrity of file #" + str(c) + ": " + f)
    file_path = os.path.join("files/xlsx", f)
    print("📂 Checking file path: " + file_path)
    if not os.path.isfile(file_path) or f.split(".")[-1] != "xlsx":
      print("❌ This is not a valid file... " + f.split(".")[-1])
      continue

    print("✔ This is valid file: " + file_path)
    # print('File #' + str(f["id"]) + ': ' + f["name"])

    # df = download_xlsx_data(sp_ctx, f["serverRelativeUrl"])
    df : pd.DataFrame

    try:
      print("📂 Reading file " + f + " into data frame...")
      df = pd.read_excel(file_path, sheet_name='BASE', header= 0, usecols="A:AC")
    except Exception as error:
      # os.replace(file_path, os.path.join("files/xlsx/danificados", f))
      print("❌ Error reading file: " + f)
      print("❌ " + str(error))
      falha.append(f)
      continue

    print("🔱 Filtering out rows where Sum_DIFERENÇA VOL = 0 or empty")
    
    if df is None:
      print('❌ No data to filter')
      falha.append(f)
      continue

    # Filter data - We only need data where 'Sum_DIFERENÇA VOL' is not 0
    filtered_df = df[df['Sum_DIFERENÇA VOL'].dropna() != 0.0]

    # Check if there is data left to save
    if (filtered_df['Sum_DIFERENÇA VOL'].empty): 
      print('❌ No data to save')
      falha.append(f)
      continue

    print("✔ " + str(filtered_df['Sum_DIFERENÇA VOL'].count()) + " rows remain after filter.")

    csv_file_name = f.split('.')[0] + "_filtered.csv"
    print("💾 Saving filtered data frame to file " + csv_file_name)
    csv_file_path = os.path.join("files/csv", csv_file_name)
    # Save data to a csv
    try:
      # save_to_csv(filtered_df, str(f["id"]) + '_filtered.csv')
      # save_to_csv(filtered_df, csv_file_path)
      print('💾 Saving data to csv')
      _ = filtered_df.to_csv(csv_file_path, header=True, index=False, encoding='utf-8')
      sucesso.append[f]
      print("✔ File size: " + os.path.getsize(csv_file_path))
      print("✔ File saved successfully!")
    except Exception as error:
      # print('Error saving file: ' + f["name"])
      print('❌ Error saving file: ' + csv_file_name)
      print("❌ " + str(error))
    
    print("🗑 Moving file: " + f)
    try:
      os.replace(file_path, os.path.join("files/xlsx/carregados", f))
    except IsADirectoryError as error:
      print("❌ Error moving file: " + f)
      print("❌ Source is a file but destination is a directory.")
      print("❌ " + str(error))
    except PermissionError as error:
      print("❌ Error moving file: " + f)
      print("❌ Operation not permitted.")
      print("❌ " + str(error))
    except OSError as error:
      print("❌ Error moving file: " + f)
      print("❌ " + str(error))
    except Exception as error:
      print("❌ Unknown error moving file: " + f)
      print("❌ " + str(error))

  print("✔ Sucessos: ")
  for f in sucesso:
    print("- " + sucesso)

  print("❌ Falhas: ")
  for f in falha:
    print("- " + falha)
  # Fazer upload para S3

  # Mandar instrução de copy para inserir dados no Redshift

# Conectar ao SharePoint
def get_sp_connection(site_url, username, password):
  ctx_auth = AuthenticationContext(site_url)
  if ctx_auth.acquire_token_for_user(username, password):
    print("✔ Authentication successful")
    ctx = ClientContext(site_url, ctx_auth)
    return ctx

  return None

# Get a list of files inside the Sharepoint directory
def get_files_urls(ctx, relative_url):
  libraryRoot = ctx.web.get_folder_by_server_relative_path(relative_url)
  files = libraryRoot.files
  ctx.load(files)
  ctx.execute_query()

  files_urls = []
  for f in files:
    files_urls.append(
        {
            "id": len(files_urls),
            "name": f.name,
            "serverRelativeUrl": f.serverRelativeUrl
        }
    )

  return files_urls

# Fazer Download dos xlsx e carregar em um DataFrame Pandas
# Download xlsx data to Pandas DataFrame
def download_xlsx_data(ctx, file_url):
  
  response = File.open_binary(ctx, file_url)

  print('📥 Starting download...')
  # save data to BytesIO stream
  bytes_file_obj = io.BytesIO()
  bytes_file_obj.write(response.content)
  bytes_file_obj.seek(0)  # set file object to start

  print('📝 Adding data to data list')
  # load Excel file from BytesIO stream

  try:
    df = pd.read_excel(bytes_file_obj, sheet_name='BASE', header= 0, usecols="A:AC")
  except Exception as e:
    print('❌ Error trying to read file: ' + file_url)
    print(f"❌ Exception: {e}") # Print the exception to help diagnose the problem
    return

  return df

# Filtrar dados onde a diferenca de volume seja diferente de 0

# Salvar dados em um arquivo csv ou gz
# Save to csv
def save_to_csv(df, filename):
  print('💾 Saving data to csv')
  df.to_csv(filename)
  print("✔ File size: " + os.path.getsize(filename))

main()
