import datetime
import logging
from ..Clarifier.clarifier_metrics import SPA, recommendations, save_files
from ..utils.azure_client import azure_blob

import pandas as pd
from ..rdo.wtp1_parameters import metrics 

import azure.functions as func


def main(mytimer: func.TimerRequest) -> None:
    utc_timestamp = datetime.datetime.utcnow().replace(
        tzinfo=datetime.timezone.utc).isoformat()

    if mytimer.past_due:
        logging.info('The timer is past due!')

    logging.info('Python timer trigger function ran at %s', utc_timestamp)


    conn_str = 'DefaultEndpointsProtocol=https;AccountName=strdoenvpdhdev;AccountKey=WPW5NHX2VJtSt/uAcB8f5l2zt5bjaYnTuv7YV6Mcmdl7X139bt8AAWYV28YoxY4sCQyPevsFBWdWkFg1aWfXWw==;EndpointSuffix=core.windows.net'    
    azure_client = azure_blob(container='rdo-environmental', connection_string=conn_str)
    clar_config = azure_client.load_file(blob='Config_Files/clarifier_config.yaml', save_as='clar_config.yaml', file_type='yaml')
    rdo_config = azure_client.load_file(blob='Config_Files/rdo_config.yaml', save_as='rdo_config.yaml', file_type='yaml')

    # load data 
    azure_client = azure_blob(container=rdo_config['wtp1']['wtp1_storage']['container'], connection_string=rdo_config['wtp1']['wtp1_storage']['conn_str'])
    df = azure_client.load_file(blob='Raw_Data/PI/pi_rdo_env_last_5days.parquet', save_as=rdo_config['intermed_folder']+'/5days.parquet', file_type='parquet')

    df_sample = azure_client.load_file(blob='User_Data/wtp_sample_data.json', save_as=rdo_config['intermed_folder']+'/wtp_sample_data.json', file_type='json').T
    df_sample.index = pd.to_datetime(df_sample.index)

    # load RDO metrics
    rdo_metrics = metrics(df=df, df_sample=df_sample, pi_tags=rdo_config['wtp1']['wtp1_pi_tags'], conversions=clar_config['conversions'], constants=rdo_config['wtp1']['wtp1_constants']).get_metrics()

    # create state point analysis diagram 
    spa = SPA(metrics=rdo_metrics, constants=rdo_config['wtp1']['wtp1_constants'], conversions=clar_config['conversions'], env=rdo_config['enviro_type'])

    # capture state point analysis data and updated rdo metrics
    spa_dat, rdo_metrics = spa.get_sp_data(return_results=True)

    # capture recommendations to display on dashboard 
    recs = recommendations(metrics=rdo_metrics, constants=rdo_config['wtp1']['wtp1_constants'], conversions=clar_config['conversions'], recs=dict(), sp_dat=spa_dat)
    dashboard_recs = recs.get_recommendations()

    # plot state point analysis 
    spa.plot_spa(plot_config=rdo_config['wtp1']['wtp1_spa_plot_config'], intermed_folder=rdo_config['intermed_folder'], storage=rdo_config['wtp1']['wtp1_storage'], recs=dashboard_recs)

    save_files(storage=rdo_config['wtp1']['wtp1_storage'], \
        metrics=rdo_metrics, recs=dashboard_recs, \
            sp_dat=spa_dat, env=rdo_config['enviro_type'], \
                intermed_folder=rdo_config['intermed_folder']).save()
                