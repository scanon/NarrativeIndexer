from ObjectIndexer import ObjectIndexer
# Special Indexer for Narrative Objects


class NarrativeObjectIndexer(ObjectIndexer):
    def index(self, upa):
        obj = self.ws.get_objects2({'objects': [{'ref': upa}]})['data'][0]
        metadata = obj['info'][10]
        rec = dict()
        print(metadata)
        rec['title'] = metadata['name']
        objdata = obj['data']
        source = []
        app_output = []
        app_info = []
        app_input = []
        job_ids = []
        for cell in objdata['cells']:
            source.append(cell['source'])
            kbm = cell['metadata'].get('kbase')
            if 'outputCell' in kbm:
                app_output.append(kbm['outputCell']['widget']['params'])
                job_ids.append(kbm['outputCell']['jobId'])
            if 'appCell' in kbm:
                app_info.append(kbm['appCell']['app']['spec']['info'])
                app_input.append(kbm['appCell']['params'])
        return rec
