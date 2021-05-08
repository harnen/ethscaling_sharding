import boto3
import pandas as pd

def get_ec2_status(response):
    av_zones, instance_ids, state_names= [], [], []
    for res in response['Reservations']:
        for ins in res['Instances']:
            av_zones.append(ins['Placement']['AvailabilityZone'])
            instance_ids.append(ins['InstanceId'])
            state_names.append(ins['State']['Name'])
    return pd.DataFrame({
        'InstanceId': instance_ids,
        'Availibility Zone': av_zones,
        'State': state_names
    })


ec2 = boto3.client('ec2')

response = ec2.describe_instances()

#print(response)

data_ec2 = get_ec2_status(response)
