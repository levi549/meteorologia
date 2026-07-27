from pyspark.ml.clustering import KMeans,KMeansModel
from pyspark.ml.classification import RandomForestClassifier,RandomForestClassificationModel
from abc import ABC, abstractmethod
import mlflow
import mlflow.pyspark.ml

class ML(ABC):
    def __init__(self):
        self.Modelo=None
        self.Modelo_treinado=None
    @abstractmethod
    def treino(self, data):
        pass
    @abstractmethod
    def predict(self, data):
      pass
    @abstractmethod
    def save_model(self, path):
       pass
    @abstractmethod
    def load_model(self, path):
        pass
class ML_kmeans(ML):
    def __init__(self):
        super().__init__()
        self.Modelo=KMeans(featuresCol="features",predictionCol="prediction", k=3, seed=0)
        mlflow.set_experiment("Kmeans")
        mlflow.pysaprk.ml.autolog()
    def treino(self, data):
        with mlflow.start_run():
            try:
                self.Modelo_treinado=self.Modelo.fit(data)
                print("Treinamento do modelo KMeans concluído com sucesso.")
            except Exception as e:
                print(f"Ocorreu um erro durante o treinamento do modelo KMeans: {e}")
                raise e
    def predict(self, data):
         with mlflow.start_run():
            try:
                return self.Modelo_treinado.transform(data)
            except Exception as e:
                print(f"Ocorreu um erro durante a previsão com o modelo KMeans: {e}")
                raise e
    def save_model(self, path):
        try:
            self.Modelo_treinado.write().overwrite().save(path)
            print(f"Modelo KMeans salvo com sucesso em: {path}")
        except Exception as e:
            print(f"Ocorreu um erro ao salvar o modelo KMeans: {e}")
            raise e
    def load_model(self, path):
        try:
            self.Modelo_treinado=KMeansModel.load(path)
            print(f"Modelo KMeans carregado com sucesso de: {path}")
        except Exception as e:
            print(f"Ocorreu um erro ao carregar o modelo KMeans: {e}")
            raise e
  
class ML_RandomForest(ML):
    def __init__(self, predictionCol, labelCol):
        super().__init__()
        self.Modelo=RandomForestClassifier(
        featuresCol="features",
        labelCol=labelCol,
        predictionCol=predictionCol,
        numTrees=100,    
        seed=0,
        maxDepth=5,
        maxBins=10)

class ML_nivel_de_alerta(ML_RandomForest):
    def __init__(self):
        super().__init__(predictionCol="prediction_alerta", labelCol="nivel_de_alerta")
        mlflow.set_experiment("ML_nivel_de_alerta")
        mlflow.pysaprk.ml.autolog()

    def treino(self, data):
         with mlflow.start_run():
            try:
                self.Modelo_treinado=self.Modelo.fit(data)
            
                print("Treinamento do modelo de classificação supervisionada de nivel de alerta concluído com sucesso.")
            except Exception as e:
                print(f"Ocorreu um erro durante o treinamento do modelo de classificação supervisionada de nivel de alerta: {e}")
                raise e

    def predict(self, data):
         with mlflow.start_run():
            try:
                return self.Modelo_treinado.transform(data)
            except Exception as e:
                print(f"Ocorreu um erro durante a previsão com o modelo de classificação supervisionada de nivel de alerta: {e}")
                raise e
    def save_model(self, path):
        try:
            self.Modelo_treinado.write().overwrite().save(path)
            print(f"Modelo de classificação supervisionada de nivel de alerta salvo com sucesso em: {path}")
        except Exception as e:
            print(f"Ocorreu um erro ao salvar o modelo de classificação supervisionada de nivel de alerta: {e}")
            raise e
    def load_model(self, path):
        try:
            self.Modelo_treinado=RandomForestClassificationModel.load(path)
            print(f"Modelo de classificação supervisionada de nivel de alerta carregado com sucesso de: {path}")
        except Exception as e:
            print(f"Ocorreu um erro ao carregar o modelo de classificação supervisionada de nivel de alerta: {e}")
            raise e

class Ml_anomaly(ML_RandomForest):
    def __init__(self):
        super().__init__(predictionCol="prediction_anomaly", labelCol="anomaly_name")
        mlflow.pysaprk.ml.autolog()
    def treino(self, data):
         with mlflow.start_run():
            try:
                self.Modelo_treinado=self.Modelo.fit(data)
                print("Treinamento do modelo de classificação de anomalias concluído com sucesso.")
            except Exception as e:
                print(f"Ocorreu um erro durante o treinamento do modelo de classificação de anomalias: {e}")
                raise e

    def predict(self, data):
         with mlflow.start_run():
            try:
                return self.Modelo_treinado.transform(data)
            except Exception as e:
                print(f"Ocorreu um erro durante a previsão com o modelo de classificação de anomalias: {e}")
                raise e

    def save_model(self, path):
        try:
            self.Modelo_treinado.write().overwrite().save(path)
            print(f"Modelo de classificação de anomalias salvo com sucesso em: {path}")
        except Exception as e:
            print(f"Ocorreu um erro ao salvar o modelo de classificação de anomalias: {e}")
            raise e
    def load_model(self, path):
        try:
            self.Modelo_treinado=RandomForestClassificationModel.load(path)
            print(f"Modelo de classificação de anomalias carregado com sucesso de: {path}")
        except Exception as e:
            print(f"Ocorreu um erro ao carregar o modelo de classificação de anomalias: {e}")
            raise e