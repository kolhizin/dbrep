
# Performance notes (based on simple test, without detailed hyperparameters and size comparisons):
# 1) pandas uses sqlalchemy with execute(table.insert(), data) -- set it as baseline
#    there are 3 option for insert method: None, 'multi' and custom (e.g. PG COPY)
# 2) None is versatile and fastest (100% time)
# 3) 'multi' is unexpectedly slower (700% time)
# 4) PG COPY is much faster (30% time)
# 5) None performance is attained using sqlalchemy.execute and passing list of dictionaries for bind params
# 6) Much faster alternative is insert values (), (), (), () using mogrify on python side ~50% time
#
# HENCE primary solution to test against pandas: mogrify inside python, insert with several simple executes

from .engine_base import BaseEngine

class SQLAlchemyEngine(BaseEngine):
    def __init__(self, connection_config):
        import sqlalchemy #import only here when it will be actually used
        def make_table_(table_name, col_names):
            return sqlalchemy.Table(table_name, sqlalchemy.MetaData(),
                        *[sqlalchemy.Column(x) for x in col_names]
                    )

        self.engine = sqlalchemy.create_engine(connection_config['conn-str'])
        self.conn = self.engine.connect()
        self.template_select_inc = 'select * from {src} where {rid} > {rid_value} order by {rid}'
        self.template_select_inc_null = 'select * from {src} order by {rid}'
        self.template_select_all = 'select * from {src}'
        self.template_select_rid = 'select max({rid}) from {src}'
        self.make_query = sqlalchemy.text
        self.make_table = lambda table_name, col_names: make_table_(table_name, col_names)
        self.active_cursor = None

    def get_latest_rid(self, rid_config):
        query = self.make_query(self.template_select_rid.format(
            src='({}) t'.format(rid_config['query']) if 'query' in rid_config else rid_config['table'],
            rid=rid_config['rid']
        ))
        res = self.conn.execute(query).fetchall()
        if res is None or len(res) == 0:
            return None
        return res[0][0]

    def start_get_inc(self, src_config, min_rid):
        template = self.template_select_inc if min_rid else self.template_select_inc_null
        query = self.make_query(template.format(
            src='({}) t'.format(src_config['query']) if 'query' in src_config else src_config['table'],
            rid=src_config['rid'],
            rid_value=min_rid
        ))
        self.active_cursor = self.conn.execute(query)

    def start_get_all(self, src_config):
        query = self.make_query(self.template_select_all.format(
            src='({}) t'.format(src_config['query']) if 'query' in src_config else src_config['table']
        ))
        self.active_cursor = self.conn.execute(query)

    def get_batch(self, batch_size):
        if not self.active_cursor:
            raise Exception()
        keys = list(self.active_cursor.keys())
        return keys, self.active_cursor.fetchmany(batch_size)

    def insert_batch(self, dst_config, batch, names):
        table = self.make_table(dst_config['table'], names)
        self.conn.execute(table.insert(), [dict(zip(names, x)) for x in batch])

    def truncate(self, dst_config):
        raise NotImplemented

    def create(self, dst_config):
        raise NotImplemented