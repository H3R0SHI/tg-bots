#!/usr/bin/env python3
"""
Google Sheets Setup Helper for Reminder Bot

This script helps you set up Google Sheets integration for your reminder bot.

Prerequisites:
1. Install required packages: pip install gspread google-auth
2. Create a Google Service Account:
   - Go to https://console.cloud.google.com/
   - Create a new project or select existing
   - Enable Google Sheets API and Google Drive API
   - Create a Service Account
   - Generate a JSON key file
   - Share your spreadsheet with the service account email

3. Save the JSON key file and update the path below
"""

import json
import os
import sys
from pathlib import Path

def main():
    print("🔧 Google Sheets Setup Helper")
    print("=" * 40)
    
    # Check if credentials file exists
    creds_path = input("Enter path to your Google Service Account JSON file: ").strip()
    
    if not creds_path:
        print("❌ No path provided!")
        return
        
    if not os.path.exists(creds_path):
        print(f"❌ File not found: {creds_path}")
        return
    
    # Try to load and validate the credentials
    try:
        with open(creds_path, 'r') as f:
            creds_data = json.load(f)
            
        required_fields = ['type', 'project_id', 'private_key_id', 'private_key', 'client_email', 'client_id']
        missing = [field for field in required_fields if field not in creds_data]
        
        if missing:
            print(f"❌ Invalid credentials file. Missing fields: {missing}")
            return
            
        if creds_data.get('type') != 'service_account':
            print("❌ This is not a service account credentials file!")
            return
            
        print("✅ Valid service account credentials found!")
        print(f"📧 Service Account Email: {creds_data['client_email']}")
        print(f"🏗️  Project ID: {creds_data['project_id']}")
        
    except json.JSONDecodeError:
        print("❌ Invalid JSON file!")
        return
    except Exception as e:
        print(f"❌ Error reading file: {e}")
        return
    
    # Test connection to Google Sheets
    sheet_url = input("\nEnter your Google Sheets URL or ID: ").strip()
    
    if not sheet_url:
        print("❌ No sheet URL/ID provided!")
        return
    
    # Extract sheet ID from URL if full URL provided
    sheet_id = sheet_url
    if "docs.google.com/spreadsheets/d/" in sheet_url:
        try:
            sheet_id = sheet_url.split("/d/")[1].split("/")[0]
            print(f"📋 Extracted Sheet ID: {sheet_id}")
        except:
            print("❌ Could not extract sheet ID from URL!")
            return
        
    try:
        print("🔄 Testing connection to Google Sheets...")
        
        # Try importing required packages
        try:
            import gspread
            from google.oauth2.service_account import Credentials
        except ImportError as e:
            print(f"❌ Missing package: {e}")
            print("💡 Install with: pip install gspread google-auth")
            return
        
        # Test authentication and sheet access
        scopes = [
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive",
        ]
        
        print("🔐 Authenticating with Google APIs...")
        creds = Credentials.from_service_account_file(creds_path, scopes=scopes)
        client = gspread.authorize(creds)
        
        print("📊 Attempting to access spreadsheet...")
        try:
            sh = client.open_by_key(sheet_id)
        except gspread.exceptions.SpreadsheetNotFound:
            print("❌ Spreadsheet not found! This usually means:")
            print("   1. The sheet ID is incorrect")
            print("   2. The service account doesn't have access to the sheet")
            print(f"\n🔧 SOLUTION: Share your Google Sheet with this email:")
            print(f"   📧 {creds_data['client_email']}")
            print("   👉 Give it 'Editor' permissions")
            print("\n💡 How to share:")
            print("   1. Open your Google Sheet")
            print("   2. Click 'Share' button (top right)")
            print("   3. Add the service account email")
            print("   4. Set permission to 'Editor'")
            print("   5. Click 'Send' or 'Done'")
            return
        except Exception as e:
            print(f"❌ Access error: {e}")
            return
        
        print(f"✅ Successfully connected to spreadsheet: {sh.title}")
        print(f"📋 Worksheets: {[ws.title for ws in sh.worksheets()]}")
        
        # Update bot settings
        print("\n🔧 Updating bot settings...")
        
        settings_file = Path("settings.json")
        if settings_file.exists():
            with open(settings_file, 'r') as f:
                settings = json.load(f)
        else:
            settings = {}
            
        if 'spreadsheet' not in settings:
            settings['spreadsheet'] = {}
            
        settings['spreadsheet']['enabled'] = True
        settings['spreadsheet']['sheet_id'] = sheet_id
        settings['spreadsheet']['credentials_file'] = creds_path
        
        with open(settings_file, 'w') as f:
            json.dump(settings, f, indent=2)
            
        print(f"✅ Settings updated in {settings_file}")
        
        # Set environment variable as backup
        print(f"\n💡 You can also set environment variable:")
        print(f"export GOOGLE_APPLICATION_CREDENTIALS='{creds_path}'")
        
        print("\n🎉 Setup complete! Your bot should now sync to Google Sheets.")
        print("📝 Make sure the service account email has edit access to your spreadsheet:")
        print(f"   {creds_data['client_email']}")
        
    except gspread.exceptions.APIError as e:
        print(f"❌ Google Sheets API Error: {e}")
        print("\n🔧 TROUBLESHOOTING CHECKLIST:")
        print("   1. ✅ Google Sheets API enabled in your project")
        print("   2. ✅ Google Drive API enabled in your project")
        print("   3. ❓ Spreadsheet shared with service account email")
        print(f"      📧 Share with: {creds_data['client_email']}")
        print("\n🔗 Enable APIs here:")
        print(f"   https://console.cloud.google.com/apis/library?project={creds_data['project_id']}")
        
    except Exception as e:
        print(f"❌ Connection failed: {e}")
        print("\n🔧 COMMON SOLUTIONS:")
        print("   1. Check if the Google Sheet URL/ID is correct")
        print("   2. Ensure the service account has access to the sheet")
        print("   3. Verify APIs are enabled in Google Cloud Console")
        print("   4. Try creating a new Google Sheet and sharing it")


def create_test_sheet(creds_path, creds_data):
    """Create a test sheet automatically"""
    try:
        import gspread
        from google.oauth2.service_account import Credentials
        
        print("\n🛠️  Creating a test spreadsheet for you...")
        
        scopes = [
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive",
        ]
        
        creds = Credentials.from_service_account_file(creds_path, scopes=scopes)
        client = gspread.authorize(creds)
        
        # Create a new spreadsheet
        sh = client.create("ReminderBot Test Sheet")
        
        # Add some sample data
        worksheet = sh.sheet1
        worksheet.update('A1:D4', [
            ['Metric', 'Value', 'Description', 'Status'],
            ['Total Users', '0', 'Number of bot users', 'Active'],
            ['Premium Users', '0', 'Number of premium users', 'Active'],
            ['Total Reminders', '0', 'All reminders created', 'Active']
        ])
        
        print(f"✅ Test sheet created: {sh.title}")
        print(f"🔗 URL: https://docs.google.com/spreadsheets/d/{sh.id}")
        print(f"📋 Sheet ID: {sh.id}")
        
        return sh.id
        
    except Exception as e:
        print(f"❌ Could not create test sheet: {e}")
        return None


def enhanced_setup():
    """Enhanced setup with better error handling"""
    print("\n" + "="*50)
    print("🔧 ENHANCED GOOGLE SHEETS TROUBLESHOOTER")
    print("="*50)
    
    print("\n📋 Let's diagnose the issue step by step...")
    
    # Ask user about their setup
    print("\n❓ QUICK QUESTIONS:")
    has_enabled_apis = input("1. Have you enabled Google Sheets API & Google Drive API? (y/n): ").lower().startswith('y')
    has_shared_sheet = input("2. Have you shared the sheet with your service account email? (y/n): ").lower().startswith('y')
    wants_test_sheet = input("3. Would you like me to create a test sheet for you? (y/n): ").lower().startswith('y')
    
    if not has_enabled_apis:
        print("\n🔗 Please enable these APIs first:")
        print("   1. Go to: https://console.cloud.google.com/apis/library")
        print("   2. Search for 'Google Sheets API' and enable it")
        print("   3. Search for 'Google Drive API' and enable it")
        print("   4. Wait 1-2 minutes for activation")
        return
    
    if wants_test_sheet:
        creds_path = input("\nEnter path to your service account JSON: ").strip()
        if os.path.exists(creds_path):
            try:
                with open(creds_path, 'r') as f:
                    creds_data = json.load(f)
                
                test_sheet_id = create_test_sheet(creds_path, creds_data)
                if test_sheet_id:
                    print(f"\n✅ SUCCESS! Use this Sheet ID: {test_sheet_id}")
                    print("🔄 Now run the main setup again with this ID")
                
            except Exception as e:
                print(f"❌ Error: {e}")
        else:
            print("❌ Credentials file not found!")
    
    if not has_shared_sheet:
        print("\n📧 SHARING INSTRUCTIONS:")
        print("   1. Open your Google Sheet")
        print("   2. Click the 'Share' button (top-right)")
        print("   3. Add your service account email")
        print("   4. Set permission to 'Editor'")
        print("   5. Click 'Done'")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\n🛠️  Setup interrupted. Starting troubleshooter...")
        enhanced_setup()
    except Exception as e:
        print(f"\n❌ Unexpected error: {e}")
        print("\n🛠️  Starting enhanced troubleshooter...")
        enhanced_setup()
