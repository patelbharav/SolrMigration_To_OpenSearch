#!/usr/bin/env node
import * as cdk from 'aws-cdk-lib';

import {Solr2OsStack} from "../lib/cdk-stack";

const app = new cdk.App();
const stack = new Solr2OsStack(app,"Solr2OS")
