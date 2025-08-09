#!/usr/bin/env python3
"""
🚨 GOOGLE SHEETS 404 ERROR FIXER
================================
Quick fix for the 404 error you're experiencing.

The 404 error means the service account cannot access your Google Sheet.
This script will help you fix it step by step.
"""

import json
import os

def main():
    print("🚨 GOOGLE SHEETS 404 ERROR FIXER")
    print("=" * 40)
    print()
    
    # Step 1: Verify service account email
    print("📧 STEP 1: Get your service account email")
    creds_path = "bots-468513-17410ba88e8e.json"  # Your file
    
    try:
        with open(creds_path, 'r') as f:
            creds_data = json.load(f)
        
        service_email = creds_data['client_email']
        project_id = creds_data['project_id']
        
        print(f"✅ Service Account Email: {service_email}")
        print(f"🏗️  Project ID: {project_id}")
        
    except Exception as e:
        print(f"❌ Error reading credentials: {e}")
        return
    
    # Step 2: Extract correct Sheet ID
    print("\n📋 STEP 2: Extract correct Sheet ID")
    sheet_url = "https://docs.google.com/spreadsheets/d/1IjDWEcxxQUcb6y_Ha_bgCpp8vjOyV3Az-nOW2OSDxbQ/edit?usp=sharing"
    sheet_id = "1IjDWEcxxQUcb6y_Ha_bgCpp8vjOyV3Az-nOW2OSDxbQ"
    
    print(f"✅ Extracted Sheet ID: {sheet_id}")
    
    # Step 3: Check API enablement
    print("\n🔗 STEP 3: Verify APIs are enabled")
    print(f"🌐 Visit: https://console.cloud.google.com/apis/library?project={project_id}")
    print("📝 Make sure these APIs are ENABLED:")
    print("   1. ✅ Google Sheets API")
    print("   2. ✅ Google Drive API")
    
    # Step 4: Share the sheet
    print("\n📧 STEP 4: SHARE YOUR GOOGLE SHEET")
    print("🎯 THIS IS LIKELY THE ISSUE!")
    print()
    print("👉 DO THIS NOW:")
    print(f"   1. Open: {sheet_url}")
    print("   2. Click the 'Share' button (top-right corner)")
    print(f"   3. Add this email: {service_email}")
    print("   4. Set permission to 'Editor'")
    print("   5. Click 'Send' or 'Done'")
    print()
    print("⚠️  IMPORTANT: The service account email must have access!")
    print("   Without sharing, you'll get a 404 error.")
    
    # Step 5: Test connection
    print("\n🔄 STEP 5: Test the connection")
    print("After sharing the sheet, run this command:")
    print("   python setup_google_sheets.py")
    print()
    print("Or test with this Python code:")
    print(f"""
import gspread
from google.oauth2.service_account import Credentials

# Authenticate
scopes = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]
creds = Credentials.from_service_account_file('{creds_path}', scopes=scopes)
client = gspread.authorize(creds)

# Test access
try:
    sheet = client.open_by_key('{sheet_id}')
    print(f"✅ SUCCESS! Connected to: {{sheet.title}}")
except Exception as e:
    print(f"❌ Still failing: {{e}}")
""")
    
    print("\n🎉 SUMMARY:")
    print("The 404 error is almost certainly because the Google Sheet")
    print("hasn't been shared with your service account email.")
    print(f"📧 Share with: {service_email}")
    print("🔧 Permission: Editor")
    
if __name__ == "__main__":
    main()
