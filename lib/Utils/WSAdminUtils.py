from Workspace.WorkspaceClient import Workspace

# We have to use the administer method for all accesses


class WorkspaceAdminUtil:

    def __init__(self, config):
        wsurl = config.get('workspace-url')
        self.atoken = config.get('ws-admin-token')
        self.noadmin = False
        if self.atoken is None or self.atoken == '':
            self.noadmin = True
            self.atoken = config['token']
        self.ws = Workspace(wsurl, token=self.atoken)

    def list_objects(self, params):
        """
        Provide something that acts like a standard listObjects
        """
        if self.noadmin:
            return self.ws.list_objects(params)
        return self.ws.administer({'command': 'listObjects'}, params)

    def get_objects2(self, params):
        """
        Provide something that acts like a standard getObjects
        """
        if self.noadmin:
            return self.ws.get_objects2(params)
        return self.ws.administer({'command': 'getObjects'}, params)
