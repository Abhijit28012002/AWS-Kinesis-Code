require('aws-sdk');

const s3 = new AWS.S3();

const kinesis = new AWS.kinesis();


exports.handler = async (event) =>{
    
    const bucketName = event.Records[0].s3.bucket.name;

    const keyName = event.Records[0].s3.object.key;

    console.log(keyName);

    const params = {
        Bucket: bucketName,
        Key: keyName
    }

    await s3.getObject(params).promise().then(async (data) => {
        const dataString = data.Body.toString();

        //console.log(dataString);

        const payload = {
            data: dataString
        }

        LWsendToKinesis(payload,'May2023');
    })

    
};

async function LWsendToKinesis(payload , partitionKey){
    const params = {
        Data: payload,
        PartitionKey: partitionKey,
        StreamName: 'LwProjectKDS'
    }

    await kinesis.putRecord(params).promise().then(response => {
        console.log(response);
    })
}
