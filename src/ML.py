from sklearn.cluster import KMeans
import matplotlib.pyplot as plt
class ML:
    def __init__(self):
        self.kmeans = KMeans(n_clusters=3, random_state=0)

    def treino(self, data):
        self.kmeans.fit(data)