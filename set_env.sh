#!/bin/bash

# Define the keys you want to load from the Keychain
KEYS=(
  PURPLEAIR_API_KEY
  AIRTABLE_TOKEN
  #AWS_ACCESS_KEY_ID
  #AWS_SECRET_ACCESS_KEY
  # Add more keys here as needed
)

# Load each key from the Keychain and export it as an environment variable
for key in "${KEYS[@]}"; do
  value=$(security find-generic-password -s "$key" -w 2>/dev/null)
  if [[ $? -eq 0 ]]; then
    export "$key"="$value"
  else
    echo "⚠️  Warning: Could not load '$key' from Keychain."
  fi
done