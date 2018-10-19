import asyncio
import logging; logging.basicConfig(level=logging.INFO)
import aiomysql


@asyncio.coroutine
def create_pool(loop, **kw):
    """创建连接池"""
    logging.info("create database connection pool")
    global __pool
    __pool = yield from aiomysql.create_pool(
        host=kw.get('host', 'localhost'),
        port=kw.get('port', 3306),
        user=kw['user'],
        password=kw['password'],
        db=kw['db'],
        charset=kw.get('charset', 'utf8'),
        autocommit=kw.get('autocommit', True),
        maxsize=kw.get('maxsize', 10),
        minsize=kw.get('minsize', 1),
        loop=loop
    )


@asyncio.coroutine
def select(sql, args, size=None):
    log(sql, args)
    global __pool
    with (yield from __pool) as conn:
        cur = yield from conn.cursor(aiomysql.DictCursor)
        yield from cur.execute(sql.replace('?', '%s'), args or ())
        if size:
            rs = yield from cur.fetchmany(size)
        else:
            rs = yield from cur.fetchall()
        yield from cur.close()
        logging.info('rows returned: %s' % len(rs))
        return rs


@asyncio.coroutine
def execute(sql, args):
    log(sql)
    with (yield from __pool) as conn:
        try:
            cur = yield from conn.cursor()
            yield from cur.execute(sql.replace('?', '%s'), args)
            affected = cur.rowcount
            yield from cur.close()
        except BaseException as e:
            raise e
        return affected

class Field(object):
    """字段基类"""
    def __init__(self, column_typename, primary):


class ModelMetaclass(type):
    """
    自定义的元类, 元类：能创建类的类
    """
    def __new__(mcs, name, upper_class, dct):
        if name == 'Model':  # 如果通过这个元类创建的类是Model, 什么也不修改, 直接通过type元类来创建这个类
            return super(ModelMetaclass, mcs).__new__(mcs, name, upper_class, dct)

        # 获取table 名称
        table_name = dct.get('__table__', None) or name
        logging.info('found model: {}, (table name:{})'.format(name, table_name))

        # 获取所有的field和主键名称
        mappings = dict()
        fields = []
        primary = None
        for k, v in dct.items():
            if isinstance(v, Field):



class Model(dict, metaclass=ModelMetaclass):

    def __init__(self, **kw):
        super(Model, self).__init__(**kw)

    def __getattr__(self, key):
        try:
            return self[key]
        except KeyError:
            raise AttributeError(r"'Model' object has no attribute '%s'" % key)

    def __setattr__(self, key, value):
        self[key] = value

    def get_value(self, key):
        return getattr(self, key, None)

    def get_value_or_object(self, key):
        value = getattr(self, key, None)
        if value is None:
            field = self.__mapping__[key]


