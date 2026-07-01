from dotenv import load_dotenv
from src.class_file import CSV, IBGE_API
load_dotenv()

def main():
 print("Iniciando o processo de ingestão de dados")
 csv=CSV()
 csv.Extract()
 csv.Load()
 ibge=IBGE_API()
 ibge.Extract()
 ibge.Load()

if __name__ == "__main__":
    main()
