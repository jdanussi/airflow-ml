"""Machine Learning module."""
# pylint: disable=invalid-name
# pylint: disable=import-error

import os
import warnings

import pandas as pd
import numpy as np

from sklearn.preprocessing import StandardScaler
from sklearn.ensemble import IsolationForest
from airflow.models import Variable

from statsmodels.tsa.arima.model import ARIMA
import statsmodels.tsa.api as tsa
from statsmodels.tools.sm_exceptions import ConvergenceWarning

from joblib import Parallel, delayed

warnings.simplefilter("ignore", ConvergenceWarning)


PATH_LOCAL = Variable.get("local_path")
OUTLIERS_FRACTION = float(Variable.get("outliers_fraction"))


def anomaly(logical_year):
    """Function to search for anomalies."""

    file_name = f"{logical_year}_03_agg.parquet"
    file_path = os.path.join(PATH_LOCAL, file_name)

    if os.path.exists(file_path):
        df = pd.read_parquet(file_path)
        df_anomalies_if = anomaly_insolation_forest(df)
        df_anomalies_arima = anomaly_arima(df_anomalies_if)
        return df_anomalies_arima

    return None


def anomaly_insolation_forest(df):
    """Find anomalies with Insolation Forest model."""
    dfs = []

    for origin in df["origin"].unique():
        item = df[df["origin"] == origin].copy()
        item.drop(["origin"], axis=1, inplace=True)
        item.set_index("fl_date", inplace=True)

        scaler = StandardScaler()
        np_scaled = scaler.fit_transform(item.values.reshape(-1, 1))
        data = pd.DataFrame(np_scaled)

        # train isolation forest
        model = IsolationForest(contamination=OUTLIERS_FRACTION)
        model.fit(data)

        item["anomaly_if"] = model.predict(data)
        item.replace({"anomaly_if": 1}, 0, inplace=True)
        item.replace({"anomaly_if": -1}, 1, inplace=True)

        item["origin"] = origin

        item.reset_index(inplace=True)
        item = item[["origin", "fl_date", "mean_dep_delay", "anomaly_if"]]

        dfs.append(item)

    df = pd.concat(dfs, axis=0)
    df = df.reset_index()
    return df


def get_order_sets(n, n_per_set) -> list:
    """Create the order_sets used to find the best ARMA model."""
    n_sets = list(range(n))
    order_sets = [n_sets[i : i + n_per_set] for i in range(0, n, n_per_set)]
    return order_sets


def find_aic_for_model(data, p, n, q, model):
    """Finc model AIC."""
    try:
        if p == 0 and q == 1:
            # since p=0 and q=1 is already calculated
            return None, (p, n, q)
        ts_results = model(data, order=(p, n, q)).fit()
        curr_aic = ts_results.aic
        return curr_aic, (p, n, q)
    except Exception as e:  # pylint: disable=broad-except
        print(f"Exception occurred: {e}")
        return None, (p, n, q)


MAX_P, MAX_Q = int(Variable.get("max_p")), int(Variable.get("max_q"))


def find_best_order_for_model(data, model):
    """Find the best model: min AIC."""
    p_ar, q_ma = MAX_P, MAX_Q
    final_results = []
    ts_results = model(data, order=(0, 0, 1)).fit()
    min_aic = ts_results.aic
    final_results.append((min_aic, (0, 0, 1)))
    # example if q_ma is 6
    # order_sets would be [[0, 1, 2, 3, 4], [5]]
    order_sets = get_order_sets(q_ma, 5)
    for p in range(0, p_ar):
        for order_set in order_sets:
            # fit the model and find the aic
            # results = (find_aic_for_model(data, p, q, model, 0) for q in order_set)
            results = Parallel(n_jobs=len(order_set), prefer="threads")(
                delayed(find_aic_for_model)(data, p, 0, q, model) for q in order_set
            )

            final_results.extend(results)
    results_df = pd.DataFrame(final_results, columns=["aic", "order"])
    min_df = results_df[results_df["aic"] == results_df["aic"].min()]
    min_aic = min_df["aic"].iloc[0]
    min_order = min_df["order"].iloc[0]
    return min_aic, min_order, results_df


def find_anomalies(squared_errors):
    """Funtion to find anomalies and thresholds."""
    threshold = np.mean(squared_errors) + np.std(squared_errors)
    predictions = (squared_errors >= threshold).astype(int)
    return predictions, threshold


def anomaly_arima(df):
    """Find anomalies with ARMA model."""
    dfs = []

    try:
        for origin in df["origin"].unique():
            item = df[df["origin"] == origin].copy()

            if item.shape[0] == 1:
                continue  # continue here

            item.drop(["origin"], axis=1, inplace=True)
            item.set_index("fl_date", inplace=True)
            item.index = pd.to_datetime(item.index)

            print("----------------------------------------")
            print(f"origin: {origin}")
            print(f"cantidad de registros: {len(item)}")

            # We know the freq in this case so we set it
            item = item.asfreq("d")

            # Last Observation Carried Forward
            item["mean_dep_delay"] = item["mean_dep_delay"].ffill()

            print(f"cantidad de registros luego del ffill: {len(item)}")

            min_aic = find_best_order_for_model(item["mean_dep_delay"], ARIMA)[0]
            min_order = find_best_order_for_model(item["mean_dep_delay"], ARIMA)[1]
            print(f"min_aic: {min_aic}")
            print(f"min_order: {min_order}")
            print("----------------------------------------")

            arma = tsa.ARIMA(item["mean_dep_delay"], order=min_order)
            arma_fit = arma.fit()
            squared_errors = arma_fit.resid**2
            predictions = find_anomalies(squared_errors)[0]

            item["anomaly_arima"] = predictions
            item["origin"] = origin

            item.reset_index(inplace=True)
            item = item[
                ["origin", "fl_date", "mean_dep_delay", "anomaly_if", "anomaly_arima"]
            ]

            dfs.append(item)

        df = pd.concat(dfs, axis=0)
        df = df.reset_index()
        df["fl_date"] = df["fl_date"].dt.strftime("%Y-%m-%d")

        # replace NaN values in anomaly_if column with 0
        df["anomaly_if"] = df["anomaly_if"].fillna(0)

        return df
    except Exception as e:  # pylint: disable=broad-except
        print(f"Exception occurred: {e}")
        return None


if __name__ == "__main__":
    pass
