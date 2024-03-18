import os 
import logging
import yaml
import datetime
import json
import pickle
import pandas as pd 

def log(message, level='info'):
    """
    Log a message at the specified level.

    Parameters:
    - message: The message to log.
    - level: The logging level ('info', 'warning', 'error').
    """
    if isinstance(message, list):
        message = ' '.join(map(str, message))
        
    logger = logging.getLogger()
    if not logger.handlers:
        logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    if level == 'info':
        logger.info(message)
    elif level == 'warning':
        logger.warning(message)
    elif level == 'error':
        logger.error(message)


def load_config(config_path='config.yaml'):
    """
    Load configuration settings from a YAML file.
    
    Parameters:
    - config_path: Path to the configuration YAML file.
    
    Returns:
    - A dictionary with configuration settings.
    """
    with open(config_path, 'r') as file:
        config = yaml.safe_load(file)
    return config

def load_file(file_name):
    file_type = os.path.splitext(file_name)[-1]
    if file_type == '.csv':
        return pd.read_csv(file_name)
    elif file_type == '.json':
        with open(file_name, 'r') as f:
            data = json.load(f)  
            return data
    elif file_type == '.pkl':
        with open(file_name, 'rb') as f:  
            return pickle.load(f)
    else:
        raise TypeError(f'file type {file_type} not recognized.')
    

def save_file(data, file_path, index=True):
    dir_name = os.path.dirname(file_path)
    os.makedirs(dir_name, exist_ok=True)

    file_type = os.path.splitext(file_path)[-1]
    if file_type == '.csv':
        data.to_csv(file_path, index=index)
    elif file_type == '.json':
        with open(file_path, 'w') as f:
            json.dump(data, f)
    elif file_type == '.pkl':
        with open(file_path, 'wb') as f:  
            pickle.dump(data, f)
    else:
        raise TypeError(f'file type {file_type} not recognized.')
    

def drop_columns(data, columns):
    """
    Drop columns from a DataFrame.
    
    Parameters:
    - data: DataFrame to drop columns from.
    - columns: List of column names to drop.
    
    Returns:
    - DataFrame with specified columns removed.
    """
    for col in columns:
        if col not in data.columns:
            log(f'Column {col} not found in DataFrame')
            columns.remove(col)
    return data.drop(columns=columns)

def now():
    return datetime.datetime.now().strftime('%Y%m%d_%H%M%S')

def today():
    return datetime.datetime.now().strftime('%Y%m%d')