class BaseEngine:
    def __init__(self):
        pass

    def get_latest_rid(self, rid_config):
        raise NotImplemented

    def start_get_inc(self, src_config, min_rid):
        raise NotImplemented

    def start_get_all(self, src_config):
        raise NotImplemented

    def get_batch(self, batch_size):
        raise NotImplemented

    def insert_batch(self, dst_config, batch, names):
        raise NotImplemented

    def truncate(self, dst_config):
        raise NotImplemented

    def create(self, dst_config):
        raise NotImplemented