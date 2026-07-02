from contextlib import contextmanager
from datetime import datetime
class log:
    def __init__(self):
        self.BD_conection: Client = create_client(
            os.getenv("SUPABASE_URL"),
            os.getenv("SUPABASE_KEY")
        )
        self.ultimo_id_processados = 0 
    @contextmanager
    def log_job(self, nome_job, pipeline_id):
        data_inicio = datetime.now().isoformat()

        response = self.BD_conection.table("log_job").insert({
            "pipeline_id": pipeline_id,
            "nome_job": nome_job,
            "data_inicio": data_inicio,
            "status": "RUNNING"
        }).execute()
        if not response.data:
            raise ValueError("Erro ao criar log do job no BD")
        id_log = response.data[0]['id']
        
        try:
            yield self
            
            data_fim = datetime.now().isoformat()
            self.BD_conection.table("log_job").update({
                "data_fim": data_fim,
                "status": "SUCCESS",
                "ultimo_id_processados": self.ultimo_id_processados
            }).eq("id", id_log).execute()
            
        except Exception as e:
            data_fim = datetime.now().isoformat()
            self.BD_conection.table("log_job").update({
                "data_fim": data_fim,
                "status": "FAILED",
                "error": str(e)
            }).eq("id", id_log).execute()
            raise e


class Log_pipeline:
    def __init__(self):
        self.BD_conection: Client = create_client(
            os.getenv("SUPABASE_URL"),
            os.getenv("SUPABASE_KEY")
        )

    def log_inicio(self, nome_pipeline, airflow_id):
        data_inicio = datetime.now().isoformat()
   
        self.BD_conection.table('log_pipeline').insert({
            "id": airflow_id, 
            "nome_pipeline": nome_pipeline,
            "data_inicio": data_inicio,
            "status": "RUNNING"
        }).execute()

    def log_sucesso(self, context):
        data_fim = datetime.now().isoformat()
        airflow_id = context.get('dag_run').run_id
        self.BD_conection.table("log_pipeline").update({
            "data_fim": data_fim,
            "status": "SUCCESS"
        }).eq("id", airflow_id).execute()

    def log_erro(self, context):
        data_fim = datetime.now().isoformat()
        airflow_id = context.get('dag_run').run_id
        error = context.get('exception')
        
        self.BD_conection.table("log_pipeline").update({
            "data_fim": data_fim,
            "status": "FAILED",
            "error": str(error)
        }).eq("id", airflow_id).execute()