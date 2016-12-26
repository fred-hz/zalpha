from abc import (
    ABCMeta,
    abstractmethod
)
import pickle
import os.path


class Serializable(object):
    __metaclass__ = ABCMeta

    def __init__(self, cache_path):
        self.serialize_list = []
        self.cache_path = cache_path

    def register_serialization(self, name):
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

    def load(self, cache_path):
        for name in self.serialize_list:
            var = pickle.load(file=os.path.join(cache_path, name))
            if var is None:
                raise Exception('Loading None object {name}'.format(
                    name=name
                ))
            setattr(self, name, var)

    def load_data(self):
        if self.cache_exist(self.cache_path):
            self.load(self.cache_path)
        else:
            self.compute_cache()
            self.dump(self.cache_path)

    @abstractmethod
    def compute_cache(self):
        raise Exception('Not implemented as an abstractmethod.')