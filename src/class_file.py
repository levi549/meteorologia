from typing import List
import json
from supabase import create_client,Client
import os
import requests
import csv
from abc import ABC, abstractmethod


class Datasource(ABC):
    def __init__(self):
        self.BD_conection: Client= create_client(
            os.getenv("SUPABASE_URL"),
            os.getenv("SUPABASE_KEY"))

    @abstractmethod
    def Extract(self):
        pass
    @abstractmethod
    def Load(self):
        pass


class API_wheather(Datasource):
    def __init__(self,Resposta=None):
        super().__init__()
        self.url_api=os.getenv("Weather_API_URL")
        self.api_key=os.getenv("Weather_API_KEY")
        self.Resposta=Resposta or []
        


    def Extract(self):  
        try:   
            cidades=  self.BD_conection.table('cidade').select('nome').execute()
            if not cidades.data or len(cidades.data) == 0:
                raise ValueError("Erro ao extrair info do BD")
            lista_cidades=[cidade['nome']  for cidade in cidades.data]
            for cidade in lista_cidades:
                response=requests.get(f'{self.url_api}?q={cidade}&appid={self.api_key}&units=metric&lang=pt_br')
                if response.status_code==200:
                    response=response.json()
                    self.Resposta.append(response)
                    print(f"Extração da cidade{cidade} realizado com sucesso")
                else:
                    raise ValueError(f"erro ao extrair cidade:{cidade}")

        except Exception as e:
            print(f"Ocorreu um erro ao fazer a extração da API Weather.{e}")
            raise e 
    def Load(self):
        try:
            if not self.Resposta:
                raise ValueError("Não existe dados para serem carregados no BD")
            response=self.BD_conection.table("raw_wheather_api").insert({
                "dados_json":self.Resposta
                }).execute()
         
            print("Sucesso ao carregar dados da api_wheather no BD")

        except Exception as e:
            print("erro ao carregar os dados da API_wheather no BD")
            raise e

 
class CSV(Datasource):
    def __init__(self,csv_file=None):
        super().__init__()
        self.csv_file=csv_file or []
        self.path=None
        


    def Extract(self):
        try:
            self.path = os.path.dirname(__file__)

            self.path = os.path.join(
                self.path,
                "..",
                "data",
                "data_Historic.csv"
            )
            self.path = os.path.abspath(self.path)
            with open(self.path,'r',encoding='utf-8') as file:
                self.csv_file=csv.DictReader(file)
                self.csv_file=list(self.csv_file)
                print("sucesso ao ler arquivo csv")
        except Exception as e:
            print("erro ao ler aqrquivo csv")
            raise e  
    def Load(self):
        try:
            if not self.csv_file:
                raise ValueError("Não há nenhum arquivo para fazer upload")
            response=self.BD_conection.table("raw_csv").upsert(self.csv_file,on_conflict="city_id,dt").execute()
            print("sucesso ao carregar csv file no BD")
        except Exception as e:
            print("erro ao carregar arquivo cvs no BD")
            raise e


class IBGE_API(Datasource):

    def __init__(self,population=None):
        super().__init__()
        self.api_ibge=os.environ.get("IBGE_API")
        self.sidra_api=os.environ.get("SIDRA_API")
        self.population=population or []
    def Extract(self):
        try:
            cidades=self.BD_conection.table("cidade").select("nome").execute()
            if not cidades.data or len(cidades.data) == 0:
                raise ValueError("Erro ao extrair info do BD")
            lista_cidades=[cidade['nome']  for cidade in cidades.data]
            response=requests.get(self.api_ibge)
            if response.status_code != 200:
                raise ValueError("erro ao buscar id das cidades")
            response=response.json()
            cidades_id=[]
            cidades_ibge={cidade['nome']:cidade['id'] for cidade in response}
            for cidade in lista_cidades:
                if cidade in cidades_ibge:
                    cidades_id.append(cidades_ibge[cidade])
            for ids in cidades_id:
                response=requests.get(f'{self.sidra_api}[{ids}]')
                if response.status_code!=200:
                    raise ValueError("erro ao fazer extraçã da populaçã no sidra_api")
                response=response.json()
                self.population.append(response)
        except Exception as e:
            print(f"Ocorreu um erro ao fazer a extração dos dados do IBGE no extract.{e}")
            raise e
    def Load(self):
        try:

            if not self.population:
                raise ValueError("Nao existe dados do ibge para fazer o load da população")
            response=self.BD_conection.table("raw_ibge").upsert({
                "dados_json_ibge":self.population},
                 on_conflict="dados_json_ibge"
                ).execute()
            if not response.data:
             print("Aviso: O banco não retornou nenhum dado.")
            print(f"Sucesso ao carregar dados ibge")

        except Exception as e:
            print("erro ao carregar dados do ibge no load")
            raise 