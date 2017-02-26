'''
Created on Feb 25, 2017

@author: ctsbala
'''

import luigi
import boto3
import time
from luigi.contrib import redshift
from datetime import date


class RedshiftClusterTarget(luigi.Target):
    
    def __init__(self,name):
        self.clustername=name
    
    def exists(self):
        response = client.describe_clusters(
                    ClusterIdentifier=self.cluster_name
                )
        
        if any(i['ClusterIdentifier']==self.cluster_name for i in response['Clusters']):
                # cluster is not available. create new cluster
                return True

        return False

class CreateOrRestoreCluster(luigi.Task):
    
    cluster_name=luigi.Parameter()
    dbname=luigi.Parameter()
    
    def run(self):
        
        client = boto3.client('redshift')
        
        response = client.describe_clusters(
                    ClusterIdentifier=self.cluster_name
                )
        
        if not any(i['ClusterIdentifier']==self.cluster_name for i in response['Clusters']):
            response = client.create_cluster(
                            DBName=self.dbname,
                            ClusterIdentifier=self.cluster_name,
                            ClusterType='multi-node',
                            NodeType='dc1.large',
                            MasterUsername='master_user_name',
                            MasterUserPassword='master_password',
                            ClusterSecurityGroups=[
                                'default',
                            ],
                            AvailabilityZone='us-east-1a',
                            PreferredMaintenanceWindow='tue:08:30-tue:09:00',
                            ClusterParameterGroupName='default.redshift-1.0',
                            AutomatedSnapshotRetentionPeriod=10,
                            Port=5439,
                            ClusterVersion='1.0',
                            AllowVersionUpgrade=True,
                            NumberOfNodes=2,
                            PubliclyAccessible=True,
                            Encrypted=True
                        )
            
            waiter = client.get_waiter('cluster_available')
            
            waiter.wait(
                            ClusterIdentifier=self.cluster_name
                        )

    def output(self):
        RedshiftClusterTarget(self.cluster_name)

class copyFromS3toTable(luigi.contrib.redshift.S3CopyToTable):
    
    s3path=luigi.Parameter()
    tablename=luigi.Parameter()
    
    def requires(self):
        CreateOrRestoreCluster(cluster_name="redshift_cluster_name",dbname="redshift_database_name")
        
    def redshift_credentials(self):
        return {
            'host' : 'AWS_REDSHIFT_HOST',
            'port' : '5439',
            'database' : 'redshift_database_name',
            'username' : 'master_user_name',
            'password' : 'master_password',
            'aws_access_key_id' : 'AWS_ACCESSKEY',
            'aws_secret_access_key' : 'AWS_SECRET_ACCESS_KEY'
        }
    
    @property
    def aws_access_key_id(self):
        return self.redshift_credentials()['aws_access_key_id']

    @property
    def aws_secret_access_key(self):
        return self.redshift_credentials()['aws_secret_access_key']

    @property
    def database(self):
        return self.redshift_credentials()['database']

    @property
    def user(self):
        return self.redshift_credentials()['username']

    @property
    def password(self):
        return self.redshift_credentials()['password']

    @property
    def host(self):
        return self.redshift_credentials()['host'] + ':' + self.redshift_credentials()['port']
    
    def s3_load_path(self):
        #return self.redshift_credentials()['s3_load_path']
        return self.s3path
    
    @property
    def copy_options(self):
        return "gzip region 'us-west-2'"
        #return '' 
    
    @property
    def table(self):
        #return self.redshift_credentials()['tablename']
        return self.tablename
    
    @property
    def do_truncate_table(self):
        """
        Return True if table should be truncated before copying new data in.
        """
        return True
    

class snapShotAndShutDown(luigi.Task):
    #date.today().strftime("%Y-%m-%d")
    
    snapshotdate=luigi.Parameter()
    
    def requires(self):
        
         tables_to_insert=[('s3://awssampledbuswest2/ssbgz/dwdate','dwdate')
                          ('s3://awssampledbuswest2/ssbgz/part','part'),
                          ('s3://awssampledbuswest2/ssbgz/lineorder','lineorder'),
                          ('s3://awssampledbuswest2/ssbgz/customer','customer'),
                          ('s3://awssampledbuswest2/ssbgz/supplier','supplier')
                         ]
         return [copyFromS3toTable(s3path=path,tablename=table) for path,table in tables_to_insert]
        
    def run(self):
        
        client = boto3.client('redshift')
               
        response = client.create_cluster_snapshot(
                        SnapshotIdentifier='refshift-cluster-snapshot-'+self.snapshotdate,
                        ClusterIdentifier='redshift_cluster_name',
                        Tags=[
                            {
                                'Key': 'refshift-cluster-snapshot-'+self.snapshotdate,
                                'Value': 'success'
                                },
                              ]
        )
        
        waiter = client.get_waiter('snapshot_available')
        
        waiter.wait(
                SnapshotIdentifier='refshift-cluster-snapshot-'+self.snapshotdate
            )

        time.sleep(60)
        response = client.delete_cluster(
            ClusterIdentifier='redshift_cluster_name',
            SkipFinalClusterSnapshot=True
            #,FinalClusterSnapshotIdentifier='testdata20170223'
        )
        
        waiter = client.get_waiter('cluster_deleted')
        
        waiter.wait(
                    ClusterIdentifier='redshift_cluster_name'
                )

        
    def output(self):
        pass

if __name__ == "__main__":
    d=date.today().strftime("%Y-%m-%d")
    luigi.run(main_task_cls=snapShotAndShutDown,cmdline_args=["--snapshotdate={0}".format(d)])        
