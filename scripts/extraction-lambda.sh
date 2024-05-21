#!/bin/bash

cd ../
zip -r ./src/extraction/extraction-lambda.zip utils
cd ./src/extraction
zip extraction-lambda.zip extraction.py
