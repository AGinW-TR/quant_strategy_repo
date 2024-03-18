import pandas as pd
from catboost import CatBoostRegressor
from sklearn.metrics import mean_squared_error, mean_absolute_error
from utilities.util import load_config, today, save_file, log, drop_columns
from data_preparation import feature_engineer
from math import sqrt
import time
import warnings

warnings.filterwarnings("ignore")


class ModelTrainer:
    def __init__(self):
        self.config = load_config()
        self.model = None

    def load_data(self, filepath):
        log(f"Loading data from {filepath}")
        return pd.read_csv(filepath, parse_dates=["Date"], index_col=["Date"])

    def processed_data(self, data):
        log("Processing data")
        log('selecting in-scope tickers')
        selected_tickers = pd.read_csv('data/market_data/selected_tickers.csv')['Ticker'].tolist()
        data = data.query('Ticker in @selected_tickers')

        # data = feature_engineer.calculate_moving_averages(data)
        # data = feature_engineer.calculate_rsi(data)
        # data = feature_engineer.calculate_n_day_return(data)

        data = feature_engineer.get_lag(data,
                                        columns=self.config['features_to_lag'])
        data = feature_engineer.binarlizer(data,
                                           categorecal_features=self.config['categorical_features'])

        data.dropna(inplace=True)
        self.data = data
        return data

    def temporal_data_split(self, data, cutoff_date, targets):
        log("Splitting data into train and test sets")
        print(f"\tUsing {cutoff_date} as the cutoff date")

        train = data.loc[data.index < cutoff_date]
        test = data.loc[data.index >= cutoff_date]

        X_train = drop_columns(train, self.config['drop_features'])
        y_train = train[targets]
        X_test = drop_columns(test, self.config['drop_features'])
        y_test = test[targets]

        print('\tcolumns using:', ', '.join(X_train.columns))
        print('\ttargets:', targets)

        save_file(X_train, f'data/experiment/{today()}/X_train.pkl')
        save_file(X_test, f'data/experiment/{today()}/X_test.pkl')
        save_file(y_train, f'data/experiment/{today()}/y_train.pkl')
        save_file(y_test, f'data/experiment/{today()}/y_test.pkl')

        self.X_train = X_train
        self.y_train = y_train
        self.X_test = X_test
        self.y_test = y_test

    def train(self):
        log("Training model")
        X_train = self.X_train
        y_train = self.y_train
        X_test = self.X_test
        y_test = self.y_test

        hps = {
                'iterations': 2000,
                'learning_rate': 0.03,
                'depth': 6,
                'l2_leaf_reg': 3,
                'min_data_in_leaf': 20,
                'loss_function': 'RMSE',
                'early_stopping_rounds': 100
        }

        if len(self.config['targets']) > 1:
            hps['loss_function'] = 'MultiRMSE'

        self.model = CatBoostRegressor(**hps)
            
        self.model.fit(X_train,
                       y_train,
                       eval_set=(X_test, y_test),
                       verbose=100)

        self.evaluate_predictions(X_train, y_train, 'Train')
        self.evaluate_predictions(X_test, y_test, 'Test')

    def evaluate_predictions(self, X, y_true, data_name):
        y_pred = self.model.predict(X)

        rmse = sqrt(mean_squared_error(y_true, y_pred))
        mae = mean_absolute_error(y_true, y_pred)
        print(f'{data_name}:')
        print(f"\tRMSE: {rmse}")
        print(f"\tMAE: {mae}")

    def predict_and_save_all(self):
        time.sleep(1)  # sleep for 1s ensure output format is correct

        log("Making predictions for both train and test sets")
        y_train_pred = self.model.predict(self.X_train)
        y_test_pred = self.model.predict(self.X_test)

        # Initialize prediction columns with None for all rows
        pred_columns = [f'Predictions_{x}' for x in self.config['targets']]
        for col in pred_columns:
            self.data[col] = None

        # Assign predictions to the appropriate segments
        # based on the cutoff date
        # Ensure your y_train_pred and y_test_pred are aligned with
        # the data's indexing and have the correct shape

        self.data.reset_index(inplace=True)

        # Filter and assign training predictions
        train_indices = self.data[self.data['Date'] < self.config['cutoff_date']].index
        for i, pred_col in enumerate(pred_columns):
            if len(self.config['targets']) > 1:
                self.data.loc[train_indices, pred_col] = y_train_pred[:, i]
            else:
                self.data.loc[train_indices, pred_col] = y_train_pred


        # Filter and assign testing predictions
        test_indices = self.data[self.data['Date'] >= self.config['cutoff_date']].index
        for i, pred_col in enumerate(pred_columns):
            if len(self.config['targets']) > 1:
                self.data.loc[test_indices, pred_col] = y_test_pred[:, i]
            else:
                self.data.loc[test_indices, pred_col] = y_test_pred

        # Save to CSV
        log("Saving data with predictions")
        save_file(self.data,
                  f'data/experiment/{today()}/data_with_predictions.pkl')

    def run(self):
        filepath = "data/processed_data/data_clean.csv"
        cutoff_date = self.config["cutoff_date"]

        data = self.load_data(filepath)
        data = self.processed_data(data)
        self.temporal_data_split(data, cutoff_date,
                                 targets=self.config['targets'])
        self.train()
        self.predict_and_save_all()


if __name__ == "__main__":
    trainer = ModelTrainer()
    trainer.run()


