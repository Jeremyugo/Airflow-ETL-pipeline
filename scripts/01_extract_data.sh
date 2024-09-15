#!/bin/bash

# This bash script ingests an updated CSV file from an S3-compatible object storage
# and stores it temporarily in a local directory. It also handles the extraction 
# of any .zip files after download.

# Load S3 object storage credentials (e.g., OpenStack Swift)
source /opt/airflow/file-openrc.sh

echo "Connecting to S3 object storage"
echo "----------------------------------------"

# Set the storage URL for the S3-compatible service
export SURL="https://swift-yeg.cloud.cybera.ca:8080/v1/AUTH_35c57c9bf28a465395c6decd0ab9ddef"

# Define the container name where files are stored
containerName="FileStorage"

# List all files in the specified container and store them in an array
echo "Listing all files in the container: $containerName..."
echo "----------------------------------------"
IFS=$'\n' read -r -d '' -a all_files < <(swift --os-storage-url="$SURL" list "$containerName" && printf '\0')

# If no files are found in the container, exit the script with an error
if [ ${#all_files[@]} -eq 0 ]; then
    echo "No files found in the container."
    exit 1
fi

# Display the list of files found in the container
echo "Files found in the container:"
echo "----------------------------------------"
printf '%s\n' "${all_files[@]}"

# Specify the local directory where files will be downloaded
downloadPath="/opt/airflow/data/raw"

# Create the local directory if it doesn't exist
mkdir -p "$downloadPath"

# Download all files from the container to the specified local directory
echo "Downloading all files from the container: $containerName to $downloadPath"
echo "----------------------------------------"
for file in "${all_files[@]}"; do
    echo "Attempting to download \"$file\" to $downloadPath..."
    swift --os-storage-url="$SURL" download "$containerName" --output "$downloadPath/$file" "$file"
done

echo "Download process completed."

# Unzip any .zip files in the download directory and then delete the original .zip files
echo "Checking for zip files to extract and remove..."
echo "----------------------------------------"
while IFS= read -r -d '' zip_file; do
    echo "Extracting '$zip_file'..."
    unzip -o "$zip_file" -d "$downloadPath"  # Extract the .zip file
    echo "Removing extracted zip file '$zip_file'..."
    rm "$zip_file"  # Remove the original .zip file after extraction
done < <(find "$downloadPath" -type f -name "*.zip" -print0)

echo "Extraction and removal process completed."
