#!/bin/bash
set -e

# Set default core name or use environment variable if provided
CORE_NAME=${SOLR_CORE_NAME:-mycore}

# Start Solr in the background
echo "Starting Solr in the background..."
solr start

# Wait for Solr to start completely
echo "Waiting for Solr to start..."
sleep 10

# Check if Solr is running
until $(curl --output /dev/null --silent --head --fail http://localhost:8983/solr/); do
    echo "Waiting for Solr to be available..."
    sleep 2
done

echo "Solr is up and running!"

# Create the core
echo "Creating Solr core: ${CORE_NAME}"
solr create_core -c ${CORE_NAME} -d /config

# Load documents from file
echo "Loading documents from data folder into core: $CORE_NAME"
if [ -f "/opt/solr/data/documents.json" ]; then
  curl -X POST -H 'Content-Type: application/json' "http://localhost:8983/solr/$CORE_NAME/update?commit=true" --data-binary @/opt/solr/data/documents.json
  
  # Check if the import was successful
  if [ $? -eq 0 ]; then
    echo "Successfully loaded documents into Solr."
    
    # Verify the documents were loaded
    COUNT=$(curl -s "http://localhost:8983/solr/$CORE_NAME/select?q=*:*&rows=0" | grep -o '"numFound":[0-9]*' | cut -d':' -f2)
    echo "Number of documents in Solr: $COUNT"
  else
    echo "Failed to load documents into Solr."
  fi
else
  echo "Documents file not found in data folder. No documents were loaded."
fi

# Keep the container running by following the logs
exec tail -f /var/solr/logs/solr.log