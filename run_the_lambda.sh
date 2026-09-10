#!/bin/bash

set -e

echo "Cleaning package directory..."
rm -rf package function.zip
mkdir -p package

echo "Installing dependencies..."
pip install -r requirements.txt \
  --platform manylinux2014_x86_64 \
  --implementation cp \
  --python-version 3.12 \
  --only-binary=:all: \
  -t package/

echo "Copying source files..."
cp *.py -r resources/ package/

echo "Creating ZIP package..."
powershell.exe -Command "Compress-Archive -Path package\* -DestinationPath function.zip -Force"
cd ..

echo "Build completed successfully."
echo "Output: function.zip"
``