#!/bin/bash

cd ../src/transformation
zip -r transformation-lambda.zip transformation_utils
cd ../../
zip -r ./src/transformation/transformation-lambda.zip utils
cd ./src/transformation
zip transformation-lambda.zip transformation.py
