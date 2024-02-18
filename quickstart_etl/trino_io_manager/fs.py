import fsspec
import os
from contextlib import contextmanager
from dagster import resource, Field, StringSource, ResourceDefinition
from dagster._annotations import public


def _create_fsspec_filesystem(config) -> fsspec.spec.AbstractFileSystem:
    fsspec_params = dict(config)
    return fsspec.filesystem(**fsspec_params) 

def build_fsspec_resource(fsspec_params) -> ResourceDefinition:
    """
    Builder for fsspec filesystem with the given parameters.

    Arguments:
        fsspec_params(dict): Dictionary containing arguments to be 
            passed as-is to a `fsspec_filesystem` initiation. 
            if the `protocol` parameter is not set, it defaults to the 
            local storage protocol `file`.
    """
    @resource(config_schema={
        'tmp_path': Field(
            StringSource, 
            is_required=True,
            description="Path where to stage temporary files. Will create a _dagster_tmp folder if it does not exist"
        ) 
    })
    def fsspec_resource(context):
        # init_context.log.info(f"IOManager: {init_context.resource_config}")
        return FsSpec(context.resource_config['tmp_path'], fsspec_params)
    
    return fsspec_resource

class FsSpec:
    """A Class that creates an fsspec filesystem for the desired storage protocol. 
    In general this class should not be directly instantiated, but rather used as a 
    resource with a type handler that requires access to the Trino underlying storage.
    """
    def __init__(self, tmp_path, fsspec_params):
        if "protocol" not in fsspec_params:
            fsspec_params["protocol"] = "file"
        self.protocol = fsspec_params['protocol']
        self.tmp_folder = os.path.join(tmp_path, '_dagster_tmp')
        self.params = fsspec_params

    @public
    @contextmanager
    def get_fs(self):
        fs = _create_fsspec_filesystem(self.params)
        fs.makedirs(self.tmp_folder, exist_ok=True)
        yield fs
        del(fs)
