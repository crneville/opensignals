# %%
import argparse
import os
import pandas as pd
from pathlib import Path

from halo import Halo

import sys
sys.path.insert(0, os.path.join(os.getcwd(), 'src'))

from opensignals.data.yahoo import Yahoo
from opensignals.features import RSI, SMA

script_dir = os.path.abspath(os.path.dirname(os.path.realpath(__file__)))

spinner = Halo(text='', spinner='dots')

def main(output_dir=None, friday_date=None, recreate=False):
    if friday_date is not None:
        friday_date = pd.Timestamp(friday_date).to_pydatetime()

    db_dir = Path('db')

    yahoo = Yahoo()
    yahoo.download_data(db_dir, recreate=recreate)

    features_generators = [
        RSI(num_days=5, interval=14, variable='adj_close'),
        RSI(num_days=5, interval=21, variable='adj_close'),
        SMA(num_days=5, interval=14, variable='adj_close'),
        SMA(num_days=5, interval=21, variable='adj_close'),
    ]

    spinner.start('Generating features')

    train, test, live, feature_names = yahoo.get_data(db_dir,
                                                      features_generators=features_generators,
                                                      feature_prefix='feature',
                                                      last_friday=friday_date)

    training_data_output_path = 'example_training_data_yahoo.csv'
    tournament_data_output_path = 'tournament_data_yahoo.csv'

    if output_dir is not None:
        os.makedirs(output_dir, exist_ok=True)
        training_data_output_path = f'{output_dir}/example_training_data_yahoo.csv'
        tournament_data_output_path = f'{output_dir}/example_tournament_data_yahoo.csv'

    train['friday_date'] = pd.to_datetime(train['friday_date'], format='%Y%m%d')
    train.to_csv(training_data_output_path)
    
    tournament_data = pd.concat([test, live])
    tournament_data['friday_date'] = pd.to_datetime(tournament_data['friday_date'], format='%Y%m%d')
    tournament_data.to_csv(tournament_data_output_path)

    spinner.succeed()

# %%
# NOTE: https://github.com/microsoft/vscode-jupyter/issues/1837
is_vscode = 'VSCODE_CWD' in os.environ.keys()
if is_vscode: sys.argv = ['']

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Signals example data pipeline')
    parser.add_argument('--output_dir', default=os.path.join(script_dir, 'data'))
    parser.add_argument('--recreate', action='store_true')
    parser.add_argument('--friday-date', default=None)

    args = parser.parse_args()
    if args.friday_date is None or len(args.friday_date) == 0:
        args.friday_date = os.environ['FRIDAY_DATE'] if 'FRIDAY_DATE' in os.environ else None
    if args.friday_date is not None:
        print(f'Open Signals Overriding Friday Date to {args.friday_date}')

    main(args.output_dir, friday_date=args.friday_date, recreate=args.recreate)
# %%