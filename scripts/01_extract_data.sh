#!/bin/bash

# This bash script ingests an updated CSV file from an S3-compatible object storage
# and stores it temporarily in a local directory.


# Load S3 object storage credentials (e.g., OpenStack Swift)
source /opt/airflow/file-openrc.sh

echo "Connecting to S3 object storage"
echo "----------------------------------------"

export SURL="https://swift-yeg.cloud.cybera.ca:8080/v1/AUTH_35c57c9bf28a465395c6decd0ab9ddef" 

containerName="FileStorage"

echo "Listing all files in the container: $containerName..."
echo "----------------------------------------"
IFS=$'\n' read -r -d '' -a all_files < <(swift --os-storage-url="$SURL" list "$containerName" && printf '\0')

if [ ${#all_files[@]} -eq 0 ]; then
    echo "No files found in the container."
    exit 1
fi

echo "Files found in the container:"
echo "----------------------------------------"
printf '%s\n' "${all_files[@]}"

downloadPath="/opt/airflow/data/raw"

mkdir -p "$downloadPath"

echo "Downloading all files from the container: $containerName to $downloadPath"
echo "----------------------------------------"
for file in "${all_files[@]}"; do
    echo "Attempting to download \"$file\" to $downloadPath..."
    swift --os-storage-url="$SURL" download "$containerName" --output "$downloadPath/$file" "$file"
done

echo "Download process completed."

echo "Checking for zip files to extract and remove if available..."
echo "----------------------------------------"
while IFS= read -r -d '' zip_file; do
    echo "Extracting '$zip_file'..."
    unzip -o "$zip_file" -d "$downloadPath" 
    echo "Removing extracted zip file '$zip_file'..."
    rm "$zip_file" 
done < <(find "$downloadPath" -type f -name "*.zip" -print0)

echo "Extraction and removal process completed."
