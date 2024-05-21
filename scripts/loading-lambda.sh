#!/bin/bash

cd ../
zip -r ./src/loading/loading-lambda.zip utils
cd ./src/loading
zip loading-lambda.zip loading.py
