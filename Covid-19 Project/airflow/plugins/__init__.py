from __future__ import division, absolute_import, print_function

from airflow.plugins_manager import AirflowPlugin

import operators
import helpers

# Defining the plugin class
class UdacityPlugin(AirflowPlugin):
    name = "udacity_plugin"
    operators = [
        operators.S3ToRedshiftOperator,
        operators.DataQualityOperator,
        operators.CreateOrDeleteOperator,
        operators.LoadTableOperator,
        operators.KaggleToLocal,
        operators.ExportToS3
    ]
    helpers = [
        helpers.SqlQueries
    ]
