#coding:utf-8

from abc import (
    ABCMeta,
    abstractmethod
)
import pickle
import os.path
import codecs

class Serializable(object):
    __metaclass__ = ABCMeta

    def __init__(self, cache_path):
        self.serialize_list = []
        self.cache_path = cache_path
        self.caches()

        # To record if the single data is loaded
        self.data_loaded = {}
        for name in self.serialize_list:
            self.data_loaded[name] = False

    @abstractmethod
    def caches(self):
        # call self.register_cache()
        raise NotImplementedError

    def register_cache(self, name):
        if name in self.serialize_list:
            raise Exception('Duplicated register for {name}'.format(
                name=name
            ))
        self.serialize_list.append(name)

    def cache_exist(self, cache_path):
        if len(self.serialize_list) == 0:
            return False
        for name in self.serialize_list:
            if not os.path.exists(os.path.join(cache_path, name)):
                return False
        return True

    def dump(self, cache_path):
        for name in self.serialize_list:
            var = getattr(self, name)
            if var is None:
                raise Exception('Dumping None object {name}'.format(
                    name=name
                ))
            pickle.dump(file=os.path.join(cache_path, name), obj=var)

    def load_single_data(self, cache_path, name):
        if name not in self.serialize_list:
            raise Exception('Data {name} is not in cache'.format(
                name=name
            ))
        obj = pickle.load(open(os.path.join(cache_path, name), 'rb'))
        if obj is None:
            raise Exception('Loading None object {name}'.format(
                name=name
            ))
        setattr(self, name, obj)
        self.data_loaded[name] = True

    def load_all_data(self, cache_path):
        for name in self.serialize_list:
            self.load_single_data(cache_path, name)

    # def load_data(self):
    #     if self.cache_exist(self.cache_path):
    #         self.load(self.cache_path)
    #     else:
    #         raise Exception('No cache_path found')
