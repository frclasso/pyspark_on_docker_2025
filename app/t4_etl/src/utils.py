__author__ = "Fabio Classo"

import os
import argparse
import simplejson
import simplejson.errors


def extract_source_data(saprk, sources):
    """
    Loop through each source in the source list and create a dictionary of
    dataset name and its dataframe.

    :param sources: List of sources
    :param spark: spark session
    :return: dictionary of key:dataset_name and value:dataframe
    """
    
    df_dict = {}

    for source in sources:
        df_dict[source["name"]] = read_source_data(saprk, source["storeType"], source["storeConfig"])
    return df_dict

def read_source_data(spark, source_type, source_config):
    """
    For a source type and source config, it reads the data and
    returns it in the fomr of spark dataframe
    :param spark: spark session
    :param source_type: string signifying the source type
    :param source_config: dictionary with all the necessary information about a source data
    :return: spark dataframe
    """
    if source_type == "object_store" and source_config["format"].lower() == "csv":
        return spark.read.csv(source_config["path"], header=True, inferSchema=True)


def save_to_sink(df, config, env="prod"):
    """
    Given a spark dataframe, env and its sink configurations, the function dumps the data to sink
    :param env: dev, stage, pre-prod, prod
    :param df: spark dataframe
    :param config: sink config
    :return: NA
    """
    try:
        sink_config = config["storeConfig"]
        if config["storeType"] == "object_store":
            df.write.format(sink_config["format"]).mode(sink_config["mode"]).save(sink_config["path"].format(env=env))
    except Exception as e:
        raise Exception(f"Error while saving data to sink: {e}")


def read_job_config(config_file_name):
    """
    Given the config file name, it reads the config file and returns it in the form of JSON dict
    :return: config in the form of dictionary
    """
    config_path = os.path.join(os.path.dirname(__file__), config_file_name)
    with open(config_file_name) as config_file:
        try:
            return simplejson.load(config_file)
        except simplejson.errors.JSONDecodeError as error:
            raise simplejson.errors.JSONDecodeError(f"Issue with job config: {config_file_name}.json , Error: {error}")
        
def parse_known_cmd_args():
    """
    Parse Cmd line args
    Return Example: Namespace(config_file_name=job_config.json)
    :return: ArgumentParser Namespace
    """
    parser = argparse.ArgumentParser()
    parser.add_argument("--config_file_name", help="Specify config file name", action="store")
    parser.add_argument("--env", help="env, dev, pre-prod, prod", action="store")
    return parser.parse_known_args()[0]


