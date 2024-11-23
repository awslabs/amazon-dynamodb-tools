#!/usr/bin/env node
import "source-map-support/register";
import * as cdk from "aws-cdk-lib";
import { DdbMigrationStack } from "../lib/ddb-migration-stack";

const app = new cdk.App();

let sourceTableArn = app.node.tryGetContext("sourceTableArn");
let destinationTableArn = app.node.tryGetContext("destinationTableArn");

if (!sourceTableArn) {
  throw new Error(
    'Context parameter "sourceTableArn" is required. Use -c sourceTableArn=<your-table-arn>'
  );
}
if (!destinationTableArn) {
  throw new Error(
    'Context parameter "destinationTableArn" is required. Use -c destinationTableArn=<your-table-arn>'
  );
}
// Extract table names from ARNs
const getTableNameFromArn = (arn: string) => {
  const parts = arn.split(":");
  return parts[parts.length - 1].split("/")[1];
};

let sourceTableName = getTableNameFromArn(sourceTableArn);
let destinationTableName = getTableNameFromArn(destinationTableArn);

// Sanitize the table name
sourceTableName = sourceTableName.replace(/^[^A-Za-z]+/, "");
sourceTableName = sourceTableName.replace(/[^A-Za-z0-9-]/g, "-");
destinationTableName = destinationTableName.replace(/^[^A-Za-z]+/, "");
destinationTableName = destinationTableName.replace(/[^A-Za-z0-9-]/g, "-");

// Validate the table name against the regex
const regex = /^[A-Za-z][A-Za-z0-9-]*$/;
if (!regex.test(sourceTableName) && !regex.test(destinationTableName)) {
  throw new Error(
    "Sanitized table name does not match the required pattern /^[A-Za-z][A-Za-z0-9-]*$/"
  );
}

// Generate a unique stack name
const stackName = `DdbMigration-${sourceTableName}-To-${destinationTableName}`;

new DdbMigrationStack(app, stackName, {
  sourceTableArn,
  sourceTableName: getTableNameFromArn(sourceTableArn),
  destinationTableArn,
  destinationTableName: getTableNameFromArn(destinationTableArn),
  env: {
    account: process.env.CDK_DEFAULT_ACCOUNT,
    region: process.env.CDK_DEFAULT_REGION,
  },
});
