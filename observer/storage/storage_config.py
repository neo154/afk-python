#!/usr/bin/env python3
"""storage_config.py

Author: neo154
Version: 0.0.1
Date Modified: 2022-06-05

Storage configuration dictionary declaration
"""

from typing import Union, List
from observer.storage.models.storage_models import StorageLocation, StorageItem

def _check_item(item: Union[dict, StorageLocation]) -> StorageLocation:
    """
    Helper to resolve config dictionaries to storage Items and then locations

    :param item: Dictionary or StorageLocation variable
    """
    if isinstance(item, StorageItem):
        return item
    return StorageItem(**item).resolve_location()

def _generate_default(prefix_loc: StorageLocation, default_str: str,
        as_dir: bool=False) -> StorageLocation:
    """
    Helper to generate another storage location object based on the base and a string

    :param prefix_loc: Base location storage object
    :param default_str: String defaults to add to config
    :param as_dir: Booolean for whether or not ne wlocation should be treated like a dir
    :returns: StorageLocation object for default
    """
    return prefix_loc.join_loc(default_str, as_dir=as_dir)

_SingleLocType = Union[dict, StorageLocation]
_MultiLocType = Union[dict, StorageLocation, List[dict], List[StorageLocation]]

class StorageConfig(dict):
    """FileSystemConfig that gives references for tracking """

    def __init__(self, base_loc: _SingleLocType, tmp_loc: _SingleLocType=None,
            data_loc: _SingleLocType=None, archive_loc: _SingleLocType=None,
            mutex_loc: _SingleLocType=None, report_loc: _SingleLocType=None,
            archive_files: _MultiLocType=None, required_files: _MultiLocType=None,
            halt_files: _MultiLocType=None, mutex_max_age: int=None,
            compression_level: int=9) -> None:
        super().__init__()
        self['base_loc'] = _check_item(base_loc)
        self._eval_arg(
            attr_name='tmp_loc', prefix_loc=self['base_loc'],
            default_str='tmp', as_dir=True, loc_arg=tmp_loc
        )
        self._eval_arg(
            attr_name='archive_loc', prefix_loc=self['base_loc'],
            default_str='archive', as_dir=True, loc_arg=archive_loc
        )
        self._eval_arg(
            attr_name='data_loc', prefix_loc=self['base_loc'],
            default_str='data', as_dir=True, loc_arg=data_loc
        )
        self._eval_arg(
            attr_name='mutex_loc', prefix_loc=self['base_loc'],
            default_str='tmp', as_dir=True, loc_arg=mutex_loc
        )
        self._eval_arg(
            attr_name='report_loc', prefix_loc=self['base_loc'],
            default_str='reports', as_dir=True, loc_arg=report_loc
        )
        self._eval_arg_list('archive_files', archive_files)
        self._eval_arg_list('required_files', required_files)
        self._eval_arg_list('halt_files', halt_files)
        self['mutex_max_age'] = mutex_max_age
        self['compression_level'] = compression_level

    def _eval_arg(self, attr_name: str, prefix_loc: StorageLocation, default_str: str,
            as_dir: bool=False, loc_arg: _SingleLocType=None) -> None:
        """
        Evaluates arguments and set attributes for each argument as a simple shortcut to
        see if values are good or not

        :param attr_name: String of attribute to be used
        :param prefix_loc: Storage location argument to utilize as a base argument
        :param default_str: String default of location after prefix for default location
        :param as_dir: Boolean argument to see if this location needs to be a dir or not
        :param loc_arg: Storage location or config that is being evaluated
        :returns: None
        """
        if loc_arg is not None:
            self[attr_name] = _check_item(loc_arg)
        else:
            self[attr_name] = _generate_default(
                prefix_loc=prefix_loc, default_str=default_str, as_dir=as_dir
            )

    def _eval_arg_list(self, attr_name: str, arg_list: _MultiLocType=None) -> None:
        """
        Evaluates a list of storage location argumentss for storage or other checks

        :param attr_name: String name of the attribute for the dict
        :param arg_list: Single or list of dict or storage locations to be evaluated and attached
        :returns: None
        """
        final_list = []
        if arg_list is None:
            self[attr_name] = final_list
            return
        if not isinstance(arg_list, list):
            arg_list = [arg_list]
        for arg in arg_list:
            final_list.append(_check_item(arg))
        self[attr_name] = final_list
