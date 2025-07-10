#!/usr/bin/env python3
"""
Google Docs to CSV Export Script
Exports all Google Docs from a specified folder to CSV format
"""

import os
import csv
import io
from datetime import datetime
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
from google.oauth2 import service_account
import pandas as pd
import json
import sys


def setup_credentials():
    """Set up Google API credentials from service account key file"""
    try:
        credentials = service_account.Credentials.from_service_account_file(
            'service_account_key.json',
            scopes=['https://www.googleapis.com/auth/drive.readonly']
        )
        return credentials
    except Exception as e:
        print(f"Error setting up credentials: {e}")
        sys.exit(1)


def find_folder_id(service, folder_name, parent_name=None):
    """Find the Google Drive folder ID by name and optional parent"""
    query = f"name='{folder_name}' and mimeType='application/vnd.google-apps.folder'"
    
    try:
        results = service.files().list(
            q=query,
            spaces='drive',
            fields='files(id, name, parents)',
            supportsAllDrives=True,
            includeItemsFromAllDrives=True
        ).execute()
        
        folders = results.get('files', [])
        
        if not folders:
            return None
            
        # If parent name specified and multiple folders found, try to match parent
        if parent_name and len(folders) > 1:
            for folder in folders:
                if folder.get('parents'):
                    try:
                        parent = service.files().get(
                            fileId=folder['parents'][0],
                            fields='name',
                            supportsAllDrives=True
                        ).execute()
                        if parent.get('name') == parent_name:
                            return folder['id']
                    except Exception:
                        continue
        
        # Return first folder found
        return folders[0]['id']
        
    except Exception as e:
        print(f"Error finding folder: {e}")
        return None


def export_google_doc_to_text(service, file_id):
    """Export a Google Doc to plain text"""
    try:
        request = service.files().export_media(
            fileId=file_id,
            mimeType='text/plain'
        )
        
        file_content = io.BytesIO()
        downloader = MediaIoBaseDownload(file_content, request)
        
        done = False
        while not done:
            status, done = downloader.next_chunk()
        
        file_content.seek(0)
        text = file_content.read().decode('utf-8', errors='ignore')
        return text.strip()
        
    except Exception as e:
        print(f"Error exporting file {file_id}: {str(e)}")
        return ""


def get_files_in_folder(service, folder_id, folder_name=""):
    """Get all Google Docs in a folder and its subfolders recursively"""
    all_files = []
    
    try:
        query = f"'{folder_id}' in parents"
        results = service.files().list(
            q=query,
            fields='files(id, name, mimeType, createdTime, modifiedTime)',
            supportsAllDrives=True,
            includeItemsFromAllDrives=True
        ).execute()
        
        items = results.get('files', [])
        
        for item in items:
            if item['mimeType'] == 'application/vnd.google-apps.document':
                # It's a Google Doc
                item['folder'] = folder_name
                all_files.append(item)
            elif item['mimeType'] == 'application/vnd.google-apps.folder':
                # It's a subfolder - recurse into it
                subfolder_name = f"{folder_name}/{item['name']}" if folder_name else item['name']
                subfolder_files = get_files_in_folder(service, item['id'], subfolder_name)
                all_files.extend(subfolder_files)
                
    except Exception as e:
        print(f"Error getting files from folder {folder_id}: {e}")
    
    return all_files


def process_documents(service, folder_id):
    """Process all Google Docs in the folder and return CSV data"""
    print("\nFinding all Google Docs...")
    all_docs = get_files_in_folder(service, folder_id)
    print(f"Found {len(all_docs)} Google Docs")
    
    if not all_docs:
        print("No Google Docs found!")
        return []
    
    csv_data = []
    total_docs = len(all_docs)
    
    for i, doc in enumerate(all_docs):
        print(f"\nProcessing {i+1}/{total_docs}: {doc['name']}")
        
        # Export the document to text
        text_content = export_google_doc_to_text(service, doc['id'])
        
        # Parse dates
        created_date = doc['createdTime'][:10] if 'createdTime' in doc else ''
        modified_date = doc['modifiedTime'][:10] if 'modifiedTime' in doc else ''
        
        # Add to CSV data
        csv_data.append({
            'folder': doc['folder'],
            'filename': doc['name'],
            'file_id': doc['id'],
            'created_date': created_date,
            'modified_date': modified_date,
            'content_length': len(text_content),
            'content': text_content
        })
        
        # Show progress
        if (i + 1) % 5 == 0:
            print(f"Progress: {i+1}/{total_docs} documents processed")
    
    print(f"\nFinished processing {len(csv_data)} documents")
    return csv_data


def save_to_csv(csv_data):
    """Save processed data to CSV files"""
    if not csv_data:
        print("No data to save!")
        return None, None
    
    # Create DataFrame
    df = pd.DataFrame(csv_data)
    
    # Show summary
    print("\nDocument Summary:")
    if len(df) > 0:
        try:
            summary = df.groupby('folder').agg({
                'filename': 'count',
                'content_length': ['mean', 'sum']
            }).round(0)
            print(summary)
        except Exception as e:
            print(f"Could not generate summary: {e}")
    
    # Save full content CSV
    csv_filename = 'fathom_docs_content.csv'
    df.to_csv(csv_filename, index=False, encoding='utf-8')
    print(f"\nSaved to {csv_filename}")
    
    # Preview the data (without full content)
    if len(df) > 0:
        preview_df = df[['folder', 'filename', 'created_date', 'content_length']].head(10)
        print("\nPreview of documents:")
        print(preview_df)
    
    # Create metadata-only version
    metadata_df = df[['folder', 'filename', 'file_id', 'created_date', 'modified_date', 'content_length']]
    metadata_filename = 'fathom_docs_metadata.csv'
    metadata_df.to_csv(metadata_filename, index=False)
    
    print(f"\nCreated files: {csv_filename} and {metadata_filename}")
    return csv_filename, metadata_filename


def main():
    """Main execution function"""
    # Configuration from environment variables
    project_id = os.environ.get('PROJECT_ID', '')
    bucket_name = os.environ.get('BUCKET_NAME', '')
    shared_drive_name = 'MotherDuck Shared Drive'
    folder_path = 'GTM/Marketing DevRel/Fathom'
    
    print("Google Docs to CSV Export")
    print("=" * 50)
    print("Configuration:")
    print(f"  Project: {project_id}")
    print(f"  Bucket: {bucket_name}")
    print(f"  Looking for: {shared_drive_name}/{folder_path}")
    print("=" * 50)
    
    # Set up credentials and API service
    credentials = setup_credentials()
    service = build('drive', 'v3', credentials=credentials)
    
    # Find the target folder
    print("\nSearching for Fathom folder...")
    fathom_folder_id = find_folder_id(service, "Fathom", "Marketing DevRel")
    
    if not fathom_folder_id:
        # Try broader search
        print("Trying broader search...")
        fathom_folder_id = find_folder_id(service, "Fathom")
    
    if not fathom_folder_id:
        print("ERROR: Could not find Fathom folder!")
        print("Please check:")
        print("1. The folder name is correct")
        print("2. The service account has access to the shared drive")
        print("3. The folder exists in the specified location")
        sys.exit(1)
    
    print(f"Found Fathom folder with ID: {fathom_folder_id}")
    
    # Process all documents
    csv_data = process_documents(service, fathom_folder_id)
    
    if not csv_data:
        print("No documents processed!")
        sys.exit(1)
    
    # Save to CSV files
    csv_filename, metadata_filename = save_to_csv(csv_data)
    
    if csv_filename and metadata_filename:
        print(f"\nSuccess! Created {len(csv_data)} document exports")
        print(f"Files created:")
        print(f"  - {csv_filename}")
        print(f"  - {metadata_filename}")
    else:
        print("Failed to create CSV files!")
        sys.exit(1)


if __name__ == "__main__":
    main()