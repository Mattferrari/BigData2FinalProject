import boto3

buckets = [
    "aaveaave09",
    "bitcoinbtc09",
    "cardanoada09",
    "dogecoindoge09",
    "ethereumeth09",
    "polkadotdot09",
    "ripplexrp09",
    "shibainushb09",
    "solanasol09",
    "stellarxlm09"
]

bucket_dict = {
    "aaveaave09": "AAVEUSD_",
    "bitcoinbtc09": "BTCUSD_",
    "cardanoada09": "ADAUSD_",
    "dogecoindoge09": "DOGEUSD_",
    "ethereumeth09": "ETHUSD_",
    "polkadotdot09": "DOTUSD_",
    "ripplexrp09": "XRPUSD_",
    "shibainushb09": "SHIBUSD_",
    "solanasol09": "SOLUSD_",
    "stellarxlm09": "XLMUSD_"
}


client = boto3.client('glue', region_name='eu-south-2')

# Create database 
try:
    response = client.create_database(
        DatabaseInput={
            'Name': 'crypto_bros',
            'Description': 'This database is crypto currencies data',
        }
    )
    print("Successfully created database")
except Exception as e:
    print("error in creating database",e)

s3_targets = []

for bucket in buckets:
    
    csv_name = bucket_dict[bucket]
    # Create Glue Crawler 
    for year in range(1,6):
        dictionary = {}
        dictionary["Path"] = f's3://{bucket}/202{year}/'
        s3_targets.append(dictionary) 

try:
    response = client.create_crawler(
        Name= 'crawler-sprint2-group9',
        Role='arn:aws:iam::715841369216:role/service-role/AWSGlueServiceRole-crypto',
        DatabaseName='crypto_bros',
        Targets={
            'S3Targets': s3_targets
        },
        TablePrefix='group9_'
    )
    print("Successfully created crawler")
except Exception as e:
    print("error in creating crawler", e)




