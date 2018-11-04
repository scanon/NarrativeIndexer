from ObjectIndexer import ObjectIndexer
# Special Indexer for Narrative Objects


class NarrativeObjectIndexer(ObjectIndexer):
    def index(self, upa):
        obj = self.ws.get_objects2({'objects': [{'ref': upa}]})['data'][0]
        metadata = obj['info'][10]
        rec = dict()
        rec['title'] = metadata['name']
        source = []
        app_info = []
        job_ids = []
        for cell in obj['data']['cells']:
            ctype = cell['cell_type']
            if ctype == 'markdown':
                source.append(cell['source'])
            elif ctype == 'code' and 'kbase' in cell['metadata']:
                kbm = cell['metadata']['kbase']
                try:
                    app_info.append(kbm['outputCell']['widget']['params'])
                    job_ids.append(kbm['outputCell']['jobId'])
                except:
                    continue
        rec['app_info'] = app_info
        rec['jobs'] = job_ids
        return rec
