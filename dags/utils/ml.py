"""Machine Learning module."""
# pylint: disable=invalid-name
# pylint: disable=import-error

import os
import pandas as pd

from sklearn.preprocessing import StandardScaler
from sklearn.ensemble import IsolationForest
from airflow.models import Variable

# pylint: disable=invalid-name
PATH_LOCAL = Variable.get("local_path")
OUTLIERS_FRACTION = float(Variable.get("outliers_fraction"))


def anomaly(logical_year):
    """Function to search for anomalies."""
    dfs = []
    file_name = f"{logical_year}_03_agg.parquet"
    file_path = os.path.join(PATH_LOCAL, file_name)

    if os.path.exists(file_path):

        df = pd.read_parquet(file_path)

        for origin in df["origin"].unique():
            item = df[df["origin"] == origin]
            item.drop(["origin"], axis=1, inplace=True)
            item.set_index("fl_date", inplace=True)

            scaler = StandardScaler()
            np_scaled = scaler.fit_transform(item.values.reshape(-1, 1))
            data = pd.DataFrame(np_scaled)

            # train isolation forest
            model = IsolationForest(contamination=OUTLIERS_FRACTION)
            model.fit(data)

            item["anomaly"] = model.predict(data)
            item["origin"] = origin

            item.reset_index(inplace=True)
            item = item[["origin", "fl_date", "mean_dep_delay", "anomaly"]]

            dfs.append(item)

        df = pd.concat(dfs, axis=0)
        df = df.reset_index()
        return df

    return None


if __name__ == "__main__":
    pass
