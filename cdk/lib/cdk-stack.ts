import * as cdk from 'aws-cdk-lib';
import * as glue from 'aws-cdk-lib/aws-glue';
import * as iam from 'aws-cdk-lib/aws-iam';
import * as s3 from 'aws-cdk-lib/aws-s3';
import { Construct } from "constructs";
import * as dotenv from 'dotenv';
import { BucketDeployment, Source } from "aws-cdk-lib/aws-s3-deployment";

dotenv.config();

const inputBucketName = process.env.INPUT_BUCKET!.toLowerCase();
const inputKey = process.env.INPUT_KEY!;
const outputBucketName = process.env.OUTPUT_BUCKET!.toLowerCase();
const outputKey = process.env.OUTPUT_KEY!;
const jobBucketName = process.env.JOB_BUCKET!.toLowerCase();

export class GlueJobStack extends cdk.Stack {
  constructor(scope: Construct, id: string, props?: cdk.StackProps) {
    super(scope, id, props);


    // let inputBucket = s3.Bucket.fromBucketName(this, inputBucketName, inputBucketName);
    // if (inputBucket.bucketName !== inputBucketName) {
    const inputBucket = new s3.Bucket(this, inputBucketName, {bucketName: inputBucketName});
    // } else {
    //   console.log(`Bucket ${inputBucket.bucketName} already exists.`);
    // }


    // let outputBucket = s3.Bucket.fromBucketName(this, outputBucketName,  outputBucketName);
    // if (outputBucket.bucketName !== outputBucketName) {
    const outputBucket = new s3.Bucket(this, outputBucketName, {bucketName: outputBucketName});
    // } else {
    //   console.log(`Bucket ${outputBucket.bucketName} already exists.`);
    // }


    // let jobBucket = s3.Bucket.fromBucketName(this, jobBucketName, jobBucketName);
    // if (jobBucket.bucketName !== jobBucketName) {
    const jobBucket = new s3.Bucket(this, jobBucketName, {bucketName: jobBucketName});
    // } else {
    //   console.log(`Bucket ${jobBucket.bucketName} already exists.`);
    // }


    let role = new iam.Role(this, 'GlueJobRole', {
      // roleName: "read-raw-s3-write-inspec-s3",
      assumedBy: new iam.ServicePrincipal('glue.amazonaws.com')
    });

    // Add AWSGlueServiceRole to role.
    const gluePolicy = iam.ManagedPolicy.fromAwsManagedPolicyName("service-role/AWSGlueServiceRole");
    role.addManagedPolicy(gluePolicy);

    inputBucket.grantRead(role);
    outputBucket.grantWrite(role);
    jobBucket.grantReadWrite(role);

    let cfnJob = new glue.CfnJob(this, 'DictionaryInspecGlueJobId', {
      name: 'DictionaryInspecGlueJob',
      role: role.roleArn,
      glueVersion: '3.0',
      command: {
        name: 'glueetl',
        pythonVersion: '3',
        scriptLocation: `s3://${jobBucket.bucketName}/glue-job-script.py`
      },
      defaultArguments: {
        '--inputPath': inputBucket.bucketName,
        '--inputKey': inputKey,
        '--outputPath': outputBucket.bucketName,
        '--outputKey': outputKey
      },
    });

    new BucketDeployment(this, 'Default CSV', {
      sources: [Source.asset('../resources')],
      destinationBucket: inputBucket,
      destinationKeyPrefix: ''
    });

    new BucketDeployment(this, 'script.deployment', {
      sources: [Source.asset('../src')],
      destinationBucket: jobBucket,
      destinationKeyPrefix: ''
    });
  }
}


