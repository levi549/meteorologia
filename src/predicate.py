from datetime import datetime
class Predicate:
    def __init__(self, Vmin, Vmax,Vmin2, Vmax2, num_partitions):
        self.Vmin = Vmin
        self.Vmax = Vmax
        self.Vmin2 = Vmin2
        self.Vmax2 = Vmax2
        self.num_partitions = num_partitions

    def gerar_predicate(self):
        lista = []
        if self.Vmin != self.Vmax and self.Vmin !=datetime.fromisoformat('1970-01-01 00:00:00+00'):
            intervalo=(self.Vmax-self.Vmin)/self.num_partitions
            x=self.Vmin
            lista.append(f"ingested_at >= '{x.strftime('%Y-%m-%d %H:%M:%S')}' and ingested_at <= '{(x + intervalo).strftime('%Y-%m-%d %H:%M:%S')}'")
            for i in range(self.num_partitions-1):
                x += intervalo
                lista.append(f"ingested_at > '{x.strftime('%Y-%m-%d %H:%M:%S')}' and ingested_at <= '{(x + intervalo).strftime('%Y-%m-%d %H:%M:%S')}'")
            print(f"Predicates gerados: {lista}")
        else:
            x = self.Vmin2
            intervalo=(self.Vmax2-self.Vmin2)/self.num_partitions
            lista.append(f"dt >= '{x.strftime('%Y-%m-%d %H:%M:%S')}' and dt <= '{(x + intervalo).strftime('%Y-%m-%d %H:%M:%S')}'")
            for i in range(self.num_partitions-1):
                x += intervalo
                lista.append(f"dt > '{x.strftime('%Y-%m-%d %H:%M:%S')}' and dt <= '{(x + intervalo).strftime('%Y-%m-%d %H:%M:%S')}'")
        return lista 