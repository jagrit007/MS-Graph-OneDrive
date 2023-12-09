import os
import time
import json
import msal
import requests
import webbrowser
import pathlib
import logging
import urllib.parse
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import as_completed

logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                    handlers=[logging.FileHandler('onedrivelog.txt'), logging.StreamHandler()],
                    level=logging.DEBUG)

LOGGER = logging.getLogger(__name__)

class ThreadTool():
    def __init__(self, maxThreadNum: int):
        self.allTask = []
        self.maxThreadNum = maxThreadNum
        self.thread = ThreadPoolExecutor(max_workers=maxThreadNum)

    def start(self, function, *args, **kwargs):
        if len(args) > 0 and len(kwargs) > 0:
            handle = self.thread.submit(function, *args, **kwargs)
        elif len(args) > 0:
            handle = self.thread.submit(function, *args)
        elif len(kwargs) > 0:
            handle = self.thread.submit(function, **kwargs)
        else:
            handle = self.thread.submit(function)

        self.allTask.append(handle)
        return handle

    def isFinish(self, handle):
        return handle.done()

    def getResult(self, handle):
        return handle.result()

    def waitAll(self):
        array = []
        for future in as_completed(self.allTask):
            data = future.result()
            array.append(data)
            self.allTask.remove(future)
        return array

    def waitAnyone(self):
        x = as_completed(self.allTask)
        self.allTask.remove(x)

    def isAllThreadsOccupied(self):
        return self.maxThreadNum <= len(self.allTask)

    def close(self):
        self.thread.shutdown(False)


threadtool = ThreadTool(10)

class MSGraph:
    def __init__(self, clientID, clientSecret, tenantID, scopes, grant_type="client_credentials"):
        self.tenant_id = tenantID
        self.client_id = clientID
        self.SCOPES = scopes
        self.chunk_size = 327680 * 100
        self.client_secret = clientSecret
        self.grant_type = "client_credentials"
        self.access_token_cache = msal.SerializableTokenCache()
        self.endpoint = "https://graph.microsoft.com/v1.0"

        # put expired token for testing
        # self.access_token = 'eyXXXX......'
        self.token_response = self.generateToken()
        self.access_token = self.token_response['access_token']
        self.base_headers = {'Authorization': 'Bearer ' + self.access_token}

        self.timeStart = 0
        self.uploadedBytes = 0
        self.totalBytes = 0
        self.timeElapsed = 0
        self.name = None
        self.__isCancelled = False

    def generateToken(self):

        if os.path.exists(os.path.join( os.path.dirname(os.path.abspath(__file__)), 'ms-graph.json' )):
            self.access_token_cache.deserialize(
                open(
                    os.path.join( 
                        os.path.dirname(
                            os.path.abspath(__file__)), 'ms-graph.json' ), 'r').read())

        self.client = msal.PublicClientApplication(client_id=self.client_id, token_cache=self.access_token_cache)

        self.accounts = self.client.get_accounts()
        if self.accounts:

            self.token_response = self.client.acquire_token_silent(self.SCOPES, account=self.accounts[0])
            # print(self.token_response)
        else:
            flow = self.client.initiate_device_flow(scopes=self.SCOPES)
            print('----------------------------')
            print('Login url: ' + flow['verification_uri'])
            print('Enter the user code in the login flow in browser: ' + flow['user_code'])
            print('----------------------------')
            webbrowser.open(flow['verification_uri'])

            self.token_response = self.client.acquire_token_by_device_flow(flow)
            print('Writing token to ms-graph.json. Do not share or push this file!')
            # print(self.token_response)


        with open(os.path.join( os.path.dirname(os.path.abspath(__file__)), 'ms-graph.json' ), 'w') as _f:
            _f.write(self.access_token_cache.serialize())
        
        self.token_expiration = time.time() + 3000 # renew before expiration!

        return self.token_response

    def __checkTokenExpiration(self):
        # if ((time.time()) - 1800) >= self.token_expiration:
        x, statuscode = self.getMe()
        print(x)
        if not(statuscode == 200 and 'error' not in x):
        # if (statuscode == 401 and 'error' in x):
            try:
                print('Token has expired! Renewing now...')
                self.token_response = self.client.acquire_token_silent(self.SCOPES, account=self.accounts[0])
                
                with open(os.path.join( os.path.dirname(os.path.abspath(__file__)), 'ms-graph.json' ), 'w') as _f:
                    _f.write(self.access_token_cache.serialize())
                    _f.close()

                self.token_expiration = time.time() + 3000
                self.access_token = self.token_response['access_token']
                self.base_headers = {'Authorization': 'Bearer ' + self.access_token}
                LOGGER.info("Renewed MS-Graph Access Token!")
                return self.token_response
            except Exception as e:
                LOGGER.error(str(e))
                time.sleep(10)
                self.generateToken()
                # print('Could not renew the token \nerr:', str(e))

    def getMe(self):
        # self.__checkTokenExpiration()
        path = self.endpoint + '/me'
        __req = requests.get(path, headers=self.base_headers)
        return (json.loads(__req.text), __req.status_code)

    def shareItem(self, itemID, permission='view'):
        """Permission: string	The type of sharing link to create. Either view, edit, or embed."""
        self.__checkTokenExpiration()
        path = self.endpoint + f'/me/drive/items/{itemID}/createLink'
        req_body = {
            'type': permission,
            'scope': 'anonymous'
        }

        __req = requests.post(path, headers=self.base_headers, json=req_body)
        _req_json = json.loads(__req.text)
        print('share_item function:', _req_json)
        return _req_json['link']['webUrl']

    def createFolder(self, folder_name, uploadpath):
        self.__checkTokenExpiration()
        path = self.endpoint + f'/me/drive/root:/{urllib.parse.quote(uploadpath)}:/children'
        req_body = {
            "name": folder_name,
            "folder": { }
        }

        __req = requests.post(path, headers=self.base_headers, json=req_body)
        return json.loads(__req.text)
        # print(json.loads(__req.text))
    
    def __searchItem(self, search_string):
        self.__checkTokenExpiration()
        path = self.endpoint + f"/me/drive/root/search(q='{urllib.parse.quote(search_string)}')"

        __req = requests.get(path, headers=self.base_headers)
        return json.loads(__req.text)
        # print(json.loads(__req.text))

    def __listDrive(self, folder=''):
        self.__checkTokenExpiration()
        if folder != '':
            # directory to list is provided by the user
            folder = f'root:/{folder}:'
        else:
            # no directory to list is provided
            folder = 'root'
        path = self.endpoint + f'/me/drive/{urllib.parse.quote(folder)}/children'
        __req = requests.get(path, headers=self.base_headers)
        # print(json.loads(__req.text))
    
    def searchDrive(self, search='', folder=''):
        if search == '':
            # list root or list given folder
            result = self.__listDrive(folder)
        else:
            result = self.__searchItem(search)
            
        print(result)
        results = []
        if 'value' in result:
            for item in result['value']:
                results.append({'id': item['id'],
                                'name': item['name'],
                                'url': item['webUrl'],
                                'size': item.get('size')})
            print(results)
            return results

    
    def deleteItem(self, itemID):
        self.__checkTokenExpiration()
        path = self.endpoint + f'/me/drive/items/{itemID}'

        __req = requests.delete(path, headers=self.base_headers)
        print(json.loads(__req.text))

    def cancel_upload(self):
        self.__isCancelled = True
        if self.self.__is_uploading:
            LOGGER.info(f"Cancelling Upload: {self.name}")
            self.__listener.onUploadError('your upload has been stopped and uploaded data has been deleted!')

    def cancelUpload(self, __sessionURL):
        self.__checkTokenExpiration()

        __req = requests.delete(__sessionURL, headers=self.base_headers)
        print(json.loads(__req.text))

    def getItemID(self, od_path):
        self.__checkTokenExpiration()
        path = self.endpoint + f'/me/drive/root:/{urllib.parse.quote(od_path)}'
        __req = requests.get(path, headers=self.base_headers)
        print('getItemID function:', __req.json())
        return __req.json()['id']
    
    def __getPathFileSize(self, path):
        if os.path.isfile(path):
            return os.path.getsize(path)
        total_size = 0
        for root, _, files in os.walk(path):
            for f in files:
                abs_path = os.path.join(root, f)
                total_size += os.path.getsize(abs_path)
        return total_size
    
    def speed(self):
        try:
            self.timeElapsed = time.time() - self.timeStart
            return self.uploadedBytes / self.timeElapsed
        except ZeroDivisionError:
            return 0
    
    def upload(self, path, upload_path=''):
        self.__checkTokenExpiration()
        self.uploadedBytes = 0
        self.name = pathlib.PurePath(path).name
        self.totalBytes = self.__getPathFileSize(path)
        self.timeStart = time.time()
        if os.path.isdir(path):
            # folderid = self.createFolder(os.path.basename(path), upload_path)
            self.uploadFolder(path, os.path.join(upload_path))
            try:
                folderID = self.getItemID(upload_path)
                print('trying to get uploaded folder id!::', folderID)
                link = self.shareItem(folderID)
                print(' id search shit got the link:', link )
            except Exception as e:
                print(str(e))
        else:
            link = self.upload_file(path, upload_path)
        
        print('got the link:', link)
        self.uploadedBytes = 0
        self.totalBytes = 0
        return link

    def uploadFolder(self, path, upload_path=''):
        self.__checkTokenExpiration()
        foldername = os.path.basename(path)
        # print('fname:', foldername)
        # self.createFolder(foldername, os.path.dirname(upload_path))

        list_dirs = os.listdir(path)
        # print(list_dirs)
        for item in list_dirs:
            print(item)
            current_file_name = os.path.join(path, item) # /home/jagrit/files/ , file1.xyz
            if not os.path.isdir(current_file_name):
                    # self.upload_file(
                    # current_file_name, 
                    # os.path.join(
                    #     upload_path,
                    #     foldername)
                    # )
                    threadtool.start(self.upload_file, current_file_name, os.path.join(upload_path, foldername))
            else:
                self.uploadFolder(current_file_name, os.path.join(
                    upload_path, foldername
                    ))
        threadtool.waitAll()

    
    def upload_file(self, filepath, uploadpath):
        self.__checkTokenExpiration()
        #4000000
        if uploadpath[-1:] == '/':
            uploadpath = uploadpath[:-1]

        if os.path.getsize(filepath) <= 4000000:
            link = self.upload_small_file(filepath, uploadpath)
        else:
            link = self.upload_large_file(filepath, uploadpath)

        return link


    def upload_small_file(self, filepath, uploadpath):
        self.__checkTokenExpiration()
        """For files under 200MB or size which can be easily uploaded without requirement for resumable upload link support"""
        filename = os.path.basename(filepath)
        with open(filepath, 'rb') as upload:
            media = upload.read()

        req_path = self.endpoint + f'/me/drive/items/root:/{urllib.parse.quote(uploadpath)}/{urllib.parse.quote(filename)}:/content'

        __req = requests.put(req_path, data=media, headers=self.base_headers)
        __req_json = __req.json()
        return self.shareItem(__req_json['id'])


    def upload_large_file(self, filepath, uploadpath):
        self.__checkTokenExpiration()
        filename = os.path.basename(filepath)
        # print(filename, filepath, uploadpath)
        file_request_body = {
            'item': {
                'name': filename
            }
        }

        # req_path = self.endpoint + f'/me/drive/items/root:/{uploadpath}/{filename}:/content'
        session_req_path = self.endpoint + f'/me/drive/items/root:/{urllib.parse.quote(uploadpath)}/{urllib.parse.quote(filename)}:/createUploadSession'
        
        retries = 0
        while retries < 5:
            upload_session_req = requests.post(session_req_path, headers=self.base_headers, json=file_request_body)
            if upload_session_req.status_code == 200:
                session_req_json = upload_session_req.json()
                print(session_req_json)
                try:
                    sessionUploadURL = session_req_json['uploadUrl']
                    # print(sessionUploadURL)
                    # upload_request = requests.put(sessionUploadURL, data=media_content)
                except Exception as e:
                    LOGGER.error(str(e))
                    # print(str(e))
                finally:
                    break
            else:
                retries += 1
                self.generateToken()
                time.sleep(retries*25)


        with open(filepath, 'rb') as upload:
            total_file_size = os.path.getsize(filepath)
            chunk_number = total_file_size // self.chunk_size
            chunk_leftover = total_file_size - (self.chunk_size * chunk_number)
            counter = 0

            while True:
                if self.__isCancelled:
                    self.cancelUpload(sessionUploadURL)
                # to-do: if download cancelled : self.cancelUpload(sessionUploadURL)
                chunk_data = upload.read(self.chunk_size)
                start_index = counter * self.chunk_size
                end_index = start_index + self.chunk_size

                if not chunk_data:
                    break
                if counter == chunk_number:
                    end_index = start_index + chunk_leftover
                
                headers = {
                    'Content-type': 'application/octet-stream',
                    'Content-Length': f'{self.chunk_size}',
                    'Content-Range': f'bytes {start_index}-{end_index-1}/{total_file_size}'
                }

                try:
                    retries = 0
                    while retries < 5:
                        chunk_data_upload_status = requests.put(
                            sessionUploadURL,
                            data=chunk_data,
                            headers=headers
                            )
                        if chunk_data_upload_status.status_code in [200, 202]:
                            break
                        else:
                            retries += 1
                            if 'error' in chunk_data_upload_status.json():
                                if chunk_data_upload_status.json()['error']['code'] == 'activityLimitReached':
                                    # time.sleep(10*60) #10mins sleep, alot of api requests!
                                    time.sleep(chunk_data_upload_status.json()['error']['retryAfterSeconds'] + 60)
                            else:
                                time.sleep(retries*20)
                            self.generateToken()
                    
                    print(chunk_data_upload_status.json())
                    if 'error' in chunk_data_upload_status.json():
                        if chunk_data_upload_status.json()['error']['code'] == 'invalidRequest':
                            return self.upload_large_file(filepath, uploadpath)
                    if 'createdBy' in chunk_data_upload_status.json():
                        # print(chunk_data_upload_status.json(), chunk_data_upload_status.json()['id'])
                        return self.shareItem(chunk_data_upload_status.json()['id'])
                    else:
                        print('upload progress: {0}'.format(chunk_data_upload_status.json()['nextExpectedRanges']))
                        # uploaded_bytes, file_total_size = chunk_data_upload_status.json()['nextExpectedRanges'][0].split('-')
                        self.uploadedBytes += self.chunk_size
                        counter +=1
                except Exception as e:
                    LOGGER.error(e)





if __name__ == '__main__':
    graphapi = MSGraph('CLIENT-ID', 'CLIENT-SECRET', 'MS-DOMAIN: eg. p5dq.onmicrosoft.com', ['Files.Read', 'Files.Read.All', 'Files.Read.Selected', 'Files.ReadWrite', 'Files.ReadWrite.All', 'Files.ReadWrite.AppFolder', 'Files.ReadWrite.Selected', 'Mail.Read', 'Mail.Send', 'Sites.Read.All', 'Sites.ReadWrite.All', 'User.Read', 'User.Read.All'])
    x, sc = graphapi.getMe()
    print(x, sc)
