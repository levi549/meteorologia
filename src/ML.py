from pyspark.ml.clustering import KMeans
from pyasrk.ml.classification import RandomForestClassifier
class ML_kmeans:
    def __init__(self):
        self.kmeans = KMeans(featuresCol="features",predictionCol="prediction", k=3, seed=0)

    def treino(self, data):
        try:
            self.modelo=self.kmeans.fit(data)
            self.resultado=self.modelo.transform(data)
            print("Treinamento do modelo KMeans concluído com sucesso.")
        except Exception as e:
            print(f"Ocorreu um erro durante o treinamento do modelo KMeans: {e}")
            raise e


class ML_supervisionado:
    def __init__(self):
        self.Modelo=RandomForestClassifier(featuresCol="features",
        labelCol="anomaly_name",
        predictionCol="prediction",
        numTrees=100,
        seed=0,
        maxDepth=5,
        maxBins=10)

    def treino(self, data):
        try:
            self.Modelo=self.Modelo.fit(data)
            self.resultado=self.Modelo.transform(data)
            print("Treinamento do modelo de classificação supervisionada concluído com sucesso.")
        except Exception as e:
            print(f"Ocorreu um erro durante o treinamento do modelo de classificação supervisionada: {e}")
            raise e

        def predict(self, data):
            try:
                return self.Modelo.transform(data)
            except Exception as e:
                print(f"Ocorreu um erro durante a previsão com o modelo de classificação supervisionada: {e}")
                raise e