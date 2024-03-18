import pandas as pd
from sklearn.metrics import (mean_squared_error, mean_absolute_error, r2_score,
                             mean_absolute_percentage_error)
from utilities.util import load_file, log, today, load_config
from math import sqrt


def sellable_and_profitable_analysis(data, target):
    df = data.copy()
    df['target_minue_pred'] = df[target] - df['Predictions_' + target]
    df['same_sign'] = (df[target] * df['Predictions_' + target] > 0).astype(int)

    df['same_sign_and_pred_positive'] = (df['same_sign']) & (df['Predictions_' + target] > 0)
    df['same_sign_and_pred_positive_sellable'] = (df['same_sign_and_pred_positive']) & (df['target_minue_pred'] > 0)

    same_sign_and_pred_positive_sellable_pct = df['same_sign_and_pred_positive_sellable'].mean() * 100
    return same_sign_and_pred_positive_sellable_pct

def threshold_analysis(data, target, thresholds):
    res = []
    for threshold in thresholds:
        data_filtered = data.query(f'Predictions_{target} > {threshold}')
        correct_sign_pct = get_correct_sign(data_filtered, target)

        df_correct_sign = get_correct_sign(data_filtered, target, return_df=True)

        same_sign_and_pred_positive_sellable_pct = sellable_and_profitable_analysis(data_filtered, target)

        res.append([threshold,
                    len(df_correct_sign),
                    correct_sign_pct,
                    same_sign_and_pred_positive_sellable_pct,
                    ])

    df_threshold_analysis = pd.DataFrame(res, columns=['Threshold', 'n_correct_sign', 'Correct sign pct', 'same_sign_and_pred_positive_sellable_pct'])    
    display(df_threshold_analysis)
    return df_threshold_analysis


def get_correct_sign(data, target, return_df=False):
    if return_df:
        return data[data[target] * data[f'Predictions_{target}'] > 0]
    else:
        return (data[target] * data[f'Predictions_{target}'] > 0).mean() * 100


def eval_dataset(data, targets, res, data_name):
    for target in targets:
        y_true = data[target]
        y_pred = data[f'Predictions_{target}']

        rmse = sqrt(mean_squared_error(y_true, y_pred))
        mae = mean_absolute_error(y_true, y_pred)
        mape = mean_absolute_percentage_error(y_true, y_pred)
        r2 = r2_score(y_true, y_pred)
        res.append((target, data_name, rmse, mae, mape, r2))

    safe_max_price_pct = (data['Predictions_period_max_price_pct'] < data['period_max_price_pct']).mean() * 100
    safe_min_price_pct = (data['Predictions_period_min_price_pct'] < data['period_min_price_pct'] * 0.9).mean() * 100

    correct_sign_max = get_correct_sign(data, 'period_max_price_pct')

    print('on data:', data_name)
    print(f"\tCorrect max price sign pct: {correct_sign_max:.04}%")
    print(f"\tSafe max price pct: {safe_max_price_pct:.04}%")
    print(f"\tSafe min price pct: {safe_min_price_pct:.04}%")

    threshold_analysis(data, 'period_max_price_pct', range(0, 100, 10))

    if data_name == 'Test':
        print('\n\ncomments:')
        print('  - Safe max price pct is the percentage of samples whose predicted max price are lower than the actual max price.')
        print('  - Safe min price pct is the percentage of samples whose predicted min price are lower than the actual max price.')
        print('  - Safe max price pct ensure that the predicted high can be reached (so that we can sell)')
        print('  - Safe min price pct ensure that the predicted low can NOT be reached (so that we won\'t lose money)')
    return res


def evaluate_predictions(data, config):
    """
    Evaluate the predictions using various metrics.
    """
    targets = config['targets']
    cutoff_date = config['cutoff_date']

    res = []
    res = eval_dataset(data.query('Date < @cutoff_date'), targets, res, data_name='Train')
    res = eval_dataset(data.query('Date >= @cutoff_date'), targets, res, data_name='Test')

    df_res = pd.DataFrame(res, columns=['Target', 'Dataset', 'RMSE', 'MAE', 'MAPE', 'R2'])
    display(df_res)


def main():
    config = load_config()
    predictions_file = f"data/experiment/20240317/data_with_predictions.pkl"
    # predictions_file = f"data/experiment/{today()}/data_with_predictions.pkl"

    data = load_file(predictions_file)
    evaluate_predictions(data, config)


if __name__ == "__main__":
    main()

