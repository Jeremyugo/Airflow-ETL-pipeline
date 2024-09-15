#!/bin/bash

# To run this file, you must first place Kinetisense-openrc.sh in the same directory as this file. 
# Then, execute the bash code using ./data_ingestion_1_download_csv_files.sh The data will be stored at downloadPath="/home/ubuntu/kinetisense/data/raw/test"
# Source the openrc file (You will likely need to enter a password here)
echo "Sourcing Kinetisense-openrc.sh. Please enter your password if prompted."
source ~/FileStorage-openrc.sh

# List OpenStack projects
echo "Listing OpenStack projects..."
openstack project list

# Export SURL variable
export SURL="https://swift-yeg.cloud.cybera.ca:8080/v1/AUTH_35c57c9bf28a465395c6decd0ab9ddef"

# List containers
echo "Listing containers..."
swift --os-storage-url="$SURL" list

# # Ask the user to select a container
# echo "Please enter the name of the container you wish to download from:"
# read containerName

# # Validate input
# if [ -z "$containerName" ]; then
#     echo "You must enter a container name."
#     exit 1
# fi

containerName="FileStorage"

# List all files in the selected container and read them into an array
echo "Listing all files in the container: $containerName..."
IFS=$'\n' read -r -d '' -a all_files < <(swift --os-storage-url="$SURL" list "$containerName" && printf '\0')

if [ ${#all_files[@]} -eq 0 ]; then
    echo "No files found in the container."
    exit 1
fi

echo "Files found in the container:"
printf '%s\n' "${all_files[@]}"

# Specify the directory to which the files will be downloaded
downloadPath="/home/ubuntu/ds/ETL/data/raw"

# Create the directory if it does not exist
mkdir -p "$downloadPath"

# Download all files from the selected container to the specified directory
echo "Downloading all files from the container: $containerName to $downloadPath"
for file in "${all_files[@]}"; do
    echo "Attempting to download \"$file\" to $downloadPath..."
    swift --os-storage-url="$SURL" download "$containerName" --output "$downloadPath/$file" "$file"
done

echo "Download process completed."

# Unzip any .zip files in the downloadPath and then delete them
echo "Checking for zip files to extract and remove..."
while IFS= read -r -d '' zip_file; do
    echo "Extracting '$zip_file'..."
    unzip -o "$zip_file" -d "$downloadPath"
    echo "Removing extracted zip file '$zip_file'..."
    rm "$zip_file"
done < <(find "$downloadPath" -type f -name "*.zip" -print0)

echo "Extraction and removal process completed."
