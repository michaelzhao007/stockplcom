#!/usr/bin/env bash
gunzip  -k fills.gz
gunzip  -k prices.gz
spark-submit --class=aqr.transformer.PositionService ../target/scala-2.11/aqr-project_2.11-0.1-SNAPSHOT.jar

