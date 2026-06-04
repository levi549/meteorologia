from pyspark.ml.clustering import KMeans
from pyspark.ml.classification import RandomForestClassifier
from abc import ABC, abstractmethod

class ML(ABC):
    def __init__(self,predictionCol,labelCol):
        self.Modelo=RandomForestClassifier(
        featuresCol="features",
        labelCol=labelCol,
        predictionCol=predictionCol,
        numTrees=100,    
        seed=0,
        maxDepth=5,
        maxBins=10)
    @abstractmethod
    def treino(self, data):
        pass
    @abstractmethod
    def predict(self, data):
      pass



class ML_kmeans():
    def __init__(self):
        self.kmeans = KMeans(featuresCol="features",predictionCol="prediction", k=3, seed=0)
    def treino(self, data):
        try:
            self.kmeans_treinado=self.kmeans.fit(data)
            print("Treinamento do modelo KMeans concluído com sucesso.")
        except Exception as e:
            print(f"Ocorreu um erro durante o treinamento do modelo KMeans: {e}")
            raise e
    def predict(self, data):
        try:
         return self.kmeans_treinado.transform(data)
        except Exception as e:
            print(f"Ocorreu um erro durante a previsão com o modelo KMeans: {e}")
            raise e




class ML_nivel_de_alerta(ML):
    def __init__(self):
        super().__init__(predictionCol="prediction_alerta", labelCol="Nivel_de_alerta")


    def treino(self, data):
        try:
            self.Modelo_treinado=self.Modelo.fit(data)
            self.resultado=self.Modelo_treinado.transform(data)
            print("Treinamento do modelo de classificação supervisionada de nivel de alerta concluído com sucesso.")
        except Exception as e:
            print(f"Ocorreu um erro durante o treinamento do modelo de classificação supervisionada de nivel de alerta: {e}")
            raise e

    def predict(self, data):
        try:
            return self.Modelo_treinado.transform(data)
        except Exception as e:
            print(f"Ocorreu um erro durante a previsão com o modelo de classificação supervisionada de nivel de alerta: {e}")
            raise e


class Ml_anomaly(ML):
    def __init__(self):
        super().__init__(predictionCol="prediction_anomaly", labelCol="anomaly_name")

    def treino(self, data):
        try:
            self.Modelo_treinado=self.Modelo.fit(data)
            self.resultado=self.Modelo_treinado.transform(data)
            print("Treinamento do modelo de classificação de anomalias concluído com sucesso.")
        except Exception as e:
            print(f"Ocorreu um erro durante o treinamento do modelo de classificação de anomalias: {e}")
            raise e

    def predict(self, data):
        try:
            return self.Modelo_treinado.transform(data)
        except Exception as e:
            print(f"Ocorreu um erro durante a previsão com o modelo de classificação de anomalias: {e}")
            raise e