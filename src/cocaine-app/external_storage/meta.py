import functools
import uuid

from config import config
from db.mongo import MongoObject
from db.mongo.pool import Collection
import helpers


class ExternalStorageMapping(MongoObject):

    ID = 'id'

    EXTERNAL_STORAGE = 'external_storage'
    EXTERNAL_STORAGE_OPTIONS = 'external_storage_options'
    COUPLES = 'couples'
    NAMESPACE = 'namespace'

    PRIMARY_ID_KEY = ID

    def __init__(self, **init_params):
        super(ExternalStorageMapping, self).__init__()

        self.id = init_params.get(self.ID)
        if not self.id:
            # this is a new object, generate new id
            self.id = uuid.uuid4().hex
        self.external_storage = init_params[self.EXTERNAL_STORAGE]
        self.external_storage_options = init_params[self.EXTERNAL_STORAGE_OPTIONS]
        self.couples = init_params[self.COUPLES]
        self.namespace = init_params[self.NAMESPACE]

    @classmethod
    def new(cls, **kwargs):
        # TODO: move this to base class @new method?
        mapping = cls(**kwargs)
        mapping._dirty = True
        return mapping

    def dump(self):
        return {
            self.ID: self.id,
            self.EXTERNAL_STORAGE: self.external_storage,
            self.EXTERNAL_STORAGE_OPTIONS: self.external_storage_options,
            self.COUPLES: self.couples,
            self.NAMESPACE: self.namespace,
        }


class ExternalStorageMeta(object):

    def __init__(self, db):
        self.mapping = None
        if db:
            self.mapping = Collection(db[config['metadata']['external_storage']['db']], 'mapping')

    def _check_mapping(method):
        @functools.wraps(method)
        def wrapper(self, *args, **kwargs):
            if self.mapping is None:
                raise RuntimeError('External storage mapping is not set up')
            return method(self, *args, **kwargs)

        return wrapper

    @_check_mapping
    def set_mapping(self, external_storage, external_storage_options, couples, namespace):
        mapping = ExternalStorageMapping.new(
            external_storage=external_storage,
            external_storage_options=external_storage_options,
            couples=couples,
            namespace=namespace,
        )
        mapping.collection = self.mapping
        mapping.save()
        return mapping

    @_check_mapping
    def mapping_list(self, external_storage=None, couple=None, namespace=None):
        params = {}
        if external_storage is not None:
            params['external_storage'] = external_storage
        if couple is not None:
            params['couples'] = {'$in': couple}
        if namespace is not None:
            params['namespace'] = namespace
        mappings = []
        for m in self.mapping.find(params):
            mapping = ExternalStorageMapping(**m)
            mapping._dirty = False
            mappings.append(mapping)
        return mappings

    @_check_mapping
    @helpers.concurrent_handler
    def get_external_storage_mapping(self, request):

        params = {}

        external_storage = request.get('external_storage', None)
        if external_storage:
            params['external_storage'] = external_storage

        res = []
        for mapping in self.mapping_list(external_storage=external_storage):
            res.append(mapping.dump())

        return res
