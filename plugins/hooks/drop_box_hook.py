import dropbox


class dropbox_config:
    def __init__(self):
        self.DROP_BOX_SPECS = { "App_key" : "",
                   "App_secret" : "",
                   "Access_Token": ""}


class DropBoxHook(dropbox_config):
    def create_connection(self):
        '''Create dropbox connection object'''
        try:
            dbx = dropbox.Dropbox(self.DROP_BOX_SPECS["Access_Token"])
            print('DropBox Connection Successful')
            return dbx
        except Exception as exp_msg:
            print("DropBox Connection error: " + str(exp_msg))


