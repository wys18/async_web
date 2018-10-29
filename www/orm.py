import asyncio
import logging; logging.basicConfig(level=logging.INFO)
import aiomysql


def log(sql, args=()):
    logging.info('SQL: %s' % sql)


async def create_pool(loop, **kw):
    """创建连接池"""
    logging.info("create database connection pool")
    global __pool  # 声明全局变量__pool 客户端连接池
    __pool = await aiomysql.create_pool(
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


def create_args_string(num):
    l = []
    for n in range(num):
        l.append('?')
    return ', '.join(l)


async def select(sql, args, size=None):
    log(sql, args)
    global __pool
    async with __pool.get() as conn:  # 从连接池中取出一个连接
        async with conn.cursor(aiomysql.DictCursor) as cur:
            await cur.execute(sql.replace('?', '%s'), args or ())
            if size:
                rs = await cur.fetchmany(size)
            else:
                rs = await cur.fetchall()

        logging.info('rows returned: %s' % len(rs))
        return rs


async def execute(sql, args, autocommit=True):
    log(sql)
    async with __pool.get() as conn:
        if not autocommit:
            await conn.begin()
        try:
            async with conn.cursor(aiomysql.DictCursor) as cur:
                await cur.execute(sql.replace('?', '%s'), args)
                affected = cur.rowcount
            if not autocommit:
                await conn.commit()
        except BaseException as e:
            if not autocommit:
                await conn.rollback()
            raise e
        return affected


class Field(object):
    """字段基类"""
    def __init__(self, name, column_type, primary_key, default):
        self.name = name
        self.column_type = column_type
        self.primary_key = primary_key
        self.default = default

    def __str__(self):
        return '<%s, %s:%s>' % (self.__class__.__name__, self.column_type, self.name)


class StringField(Field):

    def __init__(self, name=None, primary_key=False, default=None, ddl='varchra(100)'):
        super(StringField, self).__init__(name, ddl, primary_key, default)


class BooleanField(Field):
    def __init__(self, name=None, default=False):
        super(BooleanField, self).__init__(name, 'boolean', False, default)


class IntegerField(Field):
    def __init__(self, name=None, primary_key=False, default=0):
        super(IntegerField, self).__init__(name, 'bigint', primary_key, default)


class FloatField(Field):
    def __init__(self, name=None, primary_key=False, default=0.0):
        super(FloatField, self).__init__(name, 'real', primary_key, default)


class TextField(Field):
    def __init__(self, name=None, default=None):
        super(TextField, self).__init__(name, 'text', False, default)


class ModelMetaclass(type):
    """
    自定义的元类, 元类：能创建类的类
    """
    def __new__(mcs, name, upper_class, dct):
        """

        :param name: 要创建的类的名称
        :param upper_class: 要继承的父类元组
        :param dct: 类的初始属性字典
        :return:
        """
        if name == 'Model':  # 如果通过这个元类创建的类是Model, 什么也不修改, 直接通过type元类来创建这个类
            return super(ModelMetaclass, mcs).__new__(mcs, name, upper_class, dct)

        # 获取table 名称
        table_name = dct.get('__table__', None) or name
        logging.info('found model: {}, (table name:{})'.format(name, table_name))

        # 获取所有的field和主键名称
        mappings = dict()  # 存储字段名称与字段类型的映射
        fields = []  # 存放除主键外的字段名称
        primary = None  # 存放主键的字段名称
        for k, v in dct.items():
            if isinstance(v, Field):
                logging.info('found mapping: %s ==> %s' % (k, v))
                mappings[k] = v
                # 如果该字段被设置为主键
                if v.primary_key:
                    # 看是否已经有字段声明为主键了，如果有，抛出异常
                    if primary:
                        raise BaseException('Duplicate primary key for field: %s' % k)
                    primary = k
                else:
                    fields.append(k)

        # 如果循环完了都没有发现主键，抛出异常
        if not primary:
            raise BaseException('Primary key not found')

        # 已经保存到了mapping 中的字段对应关系，从相应的dct属性中删除
        for k in mappings.keys():
            dct.pop(k)
        escaped_fields = list(map(lambda f: '`%s`' % f, fields))

        dct['__mappings__'] = mappings  # 保存属性和列的映射关系
        dct['__table__'] = table_name  # 保存table name
        dct['__primary_key__'] = primary  # 主键属性名称
        dct['__fields__'] = fields  # 除主键外的属性名
        dct['__select__'] = 'select `%s`, %s from `%s`' % (primary, ', '.join(escaped_fields), table_name)
        dct['__insert__'] = 'insert into `%s` (%s, `%s`) values (%s)' % (table_name, ', '.join(
            escaped_fields), primary, create_args_string(len(escaped_fields) + 1))
        dct['__update__'] = 'update `%s` set %s where `%s`=?' % (table_name, ', '.join(map(lambda f: '`%s`=?' % (mappings.get(f).name or f), fields)), primary)
        dct['__delete__'] = 'delete from `%s` where `%s`=?' % (table_name, primary)
        return type.__new__(mcs, name, upper_class, dct)


class Model(dict, metaclass=ModelMetaclass):

    def __init__(self, **kw):
        super(Model, self).__init__(**kw)

    # 实现这个方法的目的，可以通过 对象.属性的方式来获取dict中的内容
    def __getattr__(self, key):
        try:
            return self[key]
        except KeyError:
            raise AttributeError(r"'Model' object has no attribute '%s'" % key)

    # 实现这个方法的目的，可以通过 对象.属性的方式来设置dict中的内容
    def __setattr__(self, key, value):
        self[key] = value

    def get_value(self, key):
        return getattr(self, key, None)

    def get_value_or_default(self, key):
        """
        获取这个属性的时候，发现没有，然后发现有默认值，就返回默认值，且把默认值设置为这个属性的值
        :param key: 字段名
        :return:
        """
        value = getattr(self, key, None)  # 获取字段名对应的值
        if value is None:  # 如果该字段的获取到的是None
            field = self.__mapping__[key]  # 获取该字段名对应的对象
            if field.default is not None:  # 如果该字段对象的default属性不是None
                value = field.default() if callable(field.default) else field.default
                logging.debug('using default value for %s: %s' % (key, str(value)))
                setattr(self, key, value)
        return value

    @classmethod
    async def find_all(cls, where=None, args=None, **kw):
        """通过where条件查找数据"""
        sql = [cls.__select__]
        if where:
            sql.append('where')  # sql语句中加入where
            sql.append(where)  # 加入where真实值
        if args is None:
            args = []

        order_by = kw.get('order_by', None)
        if order_by:
            sql.append('order by')  # 如果从参数中有order_by，sql语句加入'order by'
            sql.append(order_by)

        limit = kw.get('limit', None)
        if limit is not None:
            sql.append('limit')
            if isinstance(limit, int):  # limit 只有条数
                sql.append('?')
                args.append(limit)
            elif isinstance(limit, tuple):  # 如果limit带起始位置和条数
                sql.append('?, ?')
                args.extent(limit)
            else:
                raise ValueError('Invalid limit value: %s' % str(limit))
        rs = await select(' '.join(sql), args)
        # 返回对应的model对象列表
        return [cls(**r) for r in rs]

    @classmethod
    async def find_number(cls, select_field, where=None, args=None):
        sql = ['select %s _num_ from `%s`' % (select_field, cls.__table__)]
        if where:
            sql.append('where')
            sql.append(where)
        rs = await select(' '.join(sql), args, 1)
        if len(rs) == 0:
            return None
        return rs[0]['_num_']

    @classmethod
    async def find(cls, pk):
        """find object by primary key."""
        rs = await select('%s where `%s`=?' % (cls.__select__, cls.__primary_key__), [pk], 1)
        if len(rs) == 0:
            return None
        return cls(**rs[0])

    async def save(self):
        args = list(map(self.get_value_or_default, self.__fields__))  # 获取到字段的值
        args.append(self.get_value_or_default(self.__primary_key__))  # 获取到主键的值
        rows = await execute(self.__insert__, args)
        if rows != 1:
            logging.warning('failed to update by primary key: affected rows: %s' % rows)

    async def update(self):
        args = list(map(self.get_value, self.__fields__))
        args.append(self.get_value(self.__primary_key__))
        rows = await execute(self.__update__, args)
        if rows != 1:
            logging.warning('failed to update by primary key: affected rows: %s' % rows)

    async def remove(self):
        args = [self.get_value(self.__primary_key__)]
        rows = await execute(self.__delete__, args)
        if rows != 1:
            logging.warning('failed to remove by primary key: affected rows: %s' % rows)

