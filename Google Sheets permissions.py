import httplib2
import pandas as pd
import os
import auth
from googleapiclient import discovery
from oauth2client.file import Storage


save_dir = r'C:\Users\Vivian\Desktop' #save file location
filename = 'GoogleSheet權限管理' #save file name
#your googlesheet names and ids
fileinfo = [ 
    {'file': 'googlesheetname', 'id': 'googlesheetid', 'filename': f'{filename}_1'}, 
    {'file': 'googlesheetname', 'id': 'googlesheetid', 'filename': f'{filename}_2'}
]


#gcp google drive
SCOPES = 'https://www.googleapis.com/auth/drive'
CLIENT_SECRET_FILE = 'client_secret.json'
APPLICATION_NAME = 'gcp_project_name' #your gcp project name
authInst = auth.auth(SCOPES,CLIENT_SECRET_FILE,APPLICATION_NAME)
credentials = authInst.getCredentials()
http = credentials.authorize(httplib2.Http())
drive_service = discovery.build('drive', 'v3', http=http)

#create save to excel function
def fuct_to_csv(df, fn):
    df.to_csv(fn, sep='\t', encoding='utf_8_sig', date_format='string',
              index=False, chunksize=10**5)
def fuct_to_excel(df, fn):
    df.to_excel(fn, sheet_name='data', encoding='utf_8_sig',
                index=False)

#for google sheet user list save to excel file
class file_obj:
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
        """Get all permissions for a file.
        :param file_resource: The file to query permissions for.
        :return: All permissions on the file.
        """

        #get user name, email, and permission role
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
    os.chdir(save_dir)
    for file in fileinfo:
        obj = file_obj(file)
        obj.to_excel()
        print('finish : ', file['file'])

if __name__ == '__main__':
    loop_fileinfo(fileinfo)
