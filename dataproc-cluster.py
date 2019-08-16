from google.cloud import dataproc_v1
from google.cloud import monitoring_v3
import time
from googleapiclient import discovery
from oauth2client.client import GoogleCredentials

credentials = GoogleCredentials.get_application_default()

service = discovery.build('compute', 'v1', credentials=credentials)


client = dataproc_v1.ClusterControllerClient()

project_id = 'searce-playground'
zone = 'us-central1-a'
region = 'global'


def dataproc_clusters():

    global list_clusters
    list_clusters = []

    for cluster in client.list_clusters(project_id, region):
        list_clusters.append(cluster.cluster_name)

    return list_clusters


print(dataproc_clusters())


def hdfs_capacity(project_id):

    client = monitoring_v3.MetricServiceClient()
    project_name = client.project_path(project_id)
    series = monitoring_v3.types.TimeSeries()
    series.metric.type = 'dataproc.googleapis.com/cluster/hdfs/storage_capacity'
    series.resource.type = 'cloud_dataproc_cluster'
    # series.resource.labels['tunnel_name'] = 'cluster_name'
    series.resource.labels['zone'] = 'region'

    interval = monitoring_v3.types.TimeInterval()
    now = time.time()
    interval.end_time.seconds = int(now)
    interval.end_time.nanos = int(
            (now - interval.end_time.seconds) * 10**9)
    interval.start_time.seconds = int(now - 3600 * 24 * 14)  # 14 days
    interval.start_time.nanos = interval.end_time.nanos

    complete_aggregation = monitoring_v3.types.Aggregation()
    complete_aggregation.alignment_period.seconds = int(3600 * 24 * 1)
    complete_aggregation.per_series_aligner = (
        monitoring_v3.enums.Aggregation.Aligner.ALIGN_MEAN)

    aggregation = monitoring_v3.types.Aggregation()
    aggregation.alignment_period.seconds = int(3600 * 24 * 1)
    aggregation.per_series_aligner = (
        monitoring_v3.enums.Aggregation.Aligner.ALIGN_MEAN)
    aggregation.cross_series_reducer = (
        monitoring_v3.enums.Aggregation.Reducer.REDUCE_SUM)

    dataproc_clusters()
    x = dataproc_clusters()

    for i in x:
        result = client.list_time_series(project_name, 'metric.type=\"dataproc.googleapis.com/cluster/hdfs/storage_capacity\" resource.type=\"cloud_dataproc_cluster\" resource.label.cluster_name = "'+i+'"',interval,monitoring_v3.enums.ListTimeSeriesRequest.TimeSeriesView.FULL,aggregation)
        tmp_val1 = list(result)
        print 'Total hdfs storage capacity in GB =', int(tmp_val1[0].points[0].value.double_value)

        results = client.list_time_series(project_name, 'metric.type =\"dataproc.googleapis.com/cluster/hdfs/storage_utilization\" resource.type=\"cloud_dataproc_cluster\" resource.label.cluster_name = "'+i+'"',interval,monitoring_v3.enums.ListTimeSeriesRequest.TimeSeriesView.FULL,aggregation)
        tmp_val2 = list(results)
        print 'Total hdfs storage storage_utilization in GB =', int(tmp_val2[0].points[0].value.double_value)

    # results = client.list_time_series(project_name, 'metric.type = "dataproc.googleapis.com/cluster/hdfs/storage_capacity" resource.type = "cloud_dataproc_cluster" AND metric.label.cluster_name = "cluster-1"',interval,monitoring_v3.enums.ListTimeSeriesRequest.TimeSeriesView.FULL,aggregation)
    # for j in results:
    # print(j)


hdfs_capacity(project_id)