from dotenv import load_dotenv
from src.class_file import API_wheather,CSV,IBGE_API
load_dotenv()

def main():
    print("Inicio do proceso EL da pipeline na camada RAW")
    try:
        dados_csv=CSV()
        dados_csv.Extract()
        dados_csv.Load()
        dados_ibge=IBGE_API()
        dados_ibge.Extract()
        dados_ibge.Load()
        
    except Exception as e:
        print("erro na pipeline de extração e load")
        raise e

if __name__ == "__main__":
    main()
