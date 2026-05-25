from pyspark.ml.clustering import KMeans
class ML:
    def __init__(self):
        self.kmeans = KMeans(featuresCol="features", k=3, seed=0)

    def treino(self, data):
        try:
            self.modelo=self.kmeans.fit(data)
            self.resultado=self.modelo.transform(data)
            print("Treinamento do modelo KMeans concluído com sucesso.")
        except Exception as e:
            print(f"Ocorreu um erro durante o treinamento do modelo KMeans: {e}")
            raise e