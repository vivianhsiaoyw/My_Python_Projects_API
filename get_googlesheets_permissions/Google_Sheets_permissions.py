"""
This script automates the process of retrieving permissions from multiple Google Sheets at once. 
By looping through the list of Google Sheets, it ensures that each sheet's access permissions are granted appropriately, making the process more efficient and scalable.

Prerequisites:  
- Google Cloud credentials with access to the Google Drive API.  

Note:  
- Although this script works with Google Sheets, it specifically requires the Google Drive API instead of the Google Sheets API to retrieve permissions.  
"""


import httplib2
import pandas as pd
import os
import auth
from googleapiclient import discovery
from oauth2client.file import Storage

# Set the file location and name for saving outputs
save_dir = r'C:\Users\Vivian\Desktop' # file location
filename = 'GoogleSheet權限管理' # file name

# Set multiple Google Sheets
fileinfo = [ 
    # your googlesheet names and ids
    {'file': 'googlesheetname', 'id': 'googlesheetid', 'filename': f'{filename}_1'}, 
    {'file': 'googlesheetname', 'id': 'googlesheetid', 'filename': f'{filename}_2'}
]

# Set up Google Cloud credentials and Google Drive API
SCOPES = 'https://www.googleapis.com/auth/drive'
CLIENT_SECRET_FILE = 'client_secret.json'
APPLICATION_NAME = 'gcp_project_name' #your gcp project name
authInst = auth.auth(SCOPES,CLIENT_SECRET_FILE,APPLICATION_NAME)
credentials = authInst.getCredentials()
http = credentials.authorize(httplib2.Http())
drive_service = discovery.build('drive', 'v3', http=http)

# Prepare a function to save data to an Excel file
def fuct_to_csv(df, fn):
    df.to_csv(fn, sep='\t', encoding='utf_8_sig', date_format='string',
              index=False, chunksize=10**5)
def fuct_to_excel(df, fn):
    df.to_excel(fn, sheet_name='data', encoding='utf_8_sig',
                index=False)

class file_obj:
    """
    - Managing credentials for API access.
    - Retrieving all permissions for a file (display name, email, and role).
    - Exporting the permissions data to an Excel file.
    """
    def __init__(self, fileinfo):
        self.fileid = fileinfo.get('id', '')
        self.file = fileinfo.get('file', '')
        self.filename = fileinfo.get('filename', '')
    
    def get_credentials(self):
        home_dir = os.path.expanduser('~')
        credential_dir = os.path.join(home_dir, '.credentials')
        if not os.path.exists(credential_dir):
            os.makedirs(credential_dir)
        credential_path = os.path.join(credential_dir,
                                       'drive-python-quickstart.json')
    
        store = Storage(credential_path)
        credentials = store.get()
        if not credentials or credentials.invalid:
            if credentials and credentials.expired and credentials.refresh_token:
                credentials.refresh()
            else:
                pass
        return credentials
        
    def get_permissions(self): 
        """Return all permissions for a file.
        # Get user name, email, and permission role
        """
        perm_request = drive_service.permissions().list(fileId = self.fileid,
                                                        fields = "permissions(displayName,emailAddress,role)").execute()
        permission = perm_request.get('permissions', [])
        
        df = pd.DataFrame(permission)
        return df
    
    def to_excel(self):
        df = self.get_permissions()
        df['filename'] = self.file
        fuct_to_excel(df ,self.filename + '.xlsx')
        
def loop_fileinfo(fileinfo):
    """Loop through multiple Google Sheets."""
    os.chdir(save_dir)
    for file in fileinfo:
        obj = file_obj(file)
        obj.to_excel()
        print('finish : ', file['file'])

if __name__ == '__main__':
    loop_fileinfo(fileinfo)
