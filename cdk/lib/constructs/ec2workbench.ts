import {Construct} from "constructs";
import {
    BastionHostLinux,
    BlockDeviceVolume,
    InstanceClass,
    InstanceSize,
    InstanceType,
    MachineImage,
    SecurityGroup,
    Vpc
} from "aws-cdk-lib/aws-ec2";
import {Effect, PolicyStatement} from "aws-cdk-lib/aws-iam";
import {CfnOutput, Stack} from "aws-cdk-lib";

export interface Ec2WorkBenchProps {
    readonly vpc: Vpc;
    readonly migrationBucketName: string;
    readonly domainName: string;
}

export class Ec2workbench extends Construct {

    readonly host: BastionHostLinux;

    constructor(scope: Construct, id: string, props: Ec2WorkBenchProps) {
        super(scope, id);

        const host = new BastionHostLinux(this, 'BastionHost', {
            vpc: props.vpc,
            instanceType: InstanceType.of(InstanceClass.T3, InstanceSize.MEDIUM),
            machineImage: MachineImage.latestAmazonLinux2023(),
            blockDevices: [{
                deviceName: '/dev/xvda',
                volume: BlockDeviceVolume.ebs(20, {
                    encrypted: true,
                }),
            }],
            securityGroup: new SecurityGroup(this, 'SecurityGroup', {
                vpc: props.vpc,
                allowAllOutbound: true,
            })
        });
        this.host = host;

        host.instance.addToRolePolicy(
            new PolicyStatement({
                sid: "S3Packages",
                actions: [
                    "es:ListPackagesForDomain",
                    "s3:PutObject",
                    "s3:GetObject",
                    "es:AssociatePackage",
                    "es:DissociatePackage",
                    "es:*"
                ],
                resources: [
                    `arn:aws:s3:::${props.migrationBucketName}`,
                    `arn:aws:s3:::${props.migrationBucketName}/migration_schema/*`,
                    `arn:aws:es:${Stack.of(this).region}:${Stack.of(this).account}:domain/${props.domainName}`,
                    `arn:aws:es:${Stack.of(this).region}:${Stack.of(this).account}:domain/${props.domainName}/*`
                ],
                effect: Effect.ALLOW
            })
        )

        host.instance.addToRolePolicy(
            new PolicyStatement({
                sid: "OSPackages",
                actions: [
                    "es:ListDomainsForPackage",
                    "es:CreatePackage",
                    "es:UpdatePackage",
                    "es:DescribePackages",
                    "es:GetPackageVersionHistory"
                ],
                resources: [
                    "*"
                ],
                effect: Effect.ALLOW
            })
        )
    }
}