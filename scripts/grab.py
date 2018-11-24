from Workspace.WorkspaceClient import Workspace
import json
wsid = 16962
upa = '16962/3'
upa2 = '16962/23'


ws = Workspace('https://ci.kbase.us/services/ws')
d = ws.get_workspace_info({'id': wsid})
with open('get_workspace_info.json', 'w') as f:
    f.write(json.dumps(d))
d = ws.list_objects({'ids': [wsid]})
with open('list_objects.json', 'w') as f:
    f.write(json.dumps(d))
d = ws.get_objects2({'objects': [{'ref': upa}]})
with open('get_objects.json', 'w') as f:
    f.write(json.dumps(d))
d = ws.get_objects2({'objects': [{'ref': upa2}]})
with open('narrative_object.json', 'w') as f:
    f.write(json.dumps(d))
