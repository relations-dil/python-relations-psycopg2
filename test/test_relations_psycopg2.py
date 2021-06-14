import unittest
import unittest.mock

import os
import copy
import psycopg2.extras

import ipaddress

import relations
import relations_psycopg2

class SourceModel(relations.Model):
    SOURCE = "PsycoPg2Source"

class Simple(SourceModel):
    id = int
    name = str

class Plain(SourceModel):
    ID = None
    simple_id = int
    name = str

relations.OneToMany(Simple, Plain)

class Meta(SourceModel):
    id = int
    name = str
    flag = bool
    spend = float
    stuff = list
    things = dict, {"extract": "for__0___1"}
    push = str, {"inject": "stuff__-1__relations.io___1"}

def subnet_attr(values, value):

    values["address"] = str(value)
    min_ip = value[0]
    max_ip = value[-1]
    values["min_address"] = str(min_ip)
    values["min_value"] = int(min_ip)
    values["max_address"] = str(max_ip)
    values["max_value"] = int(max_ip)

class Net(SourceModel):

    id = int
    ip = ipaddress.IPv4Address, {
        "attr": {"compressed": "address", "__int__": "value"},
        "init": "address",
        "label": "address",
        "extract": {"address": str, "value": int}
    }
    subnet = ipaddress.IPv4Network, {
        "attr": subnet_attr,
        "init": "address",
        "label": "address"
    }

    LABEL = "ip__address"
    INDEX = "ip__value"

class Unit(SourceModel):
    id = int
    name = str, {"format": "fancy"}

class Test(SourceModel):
    id = int
    unit_id = int
    name = str, {"format": "shmancy"}

class Case(SourceModel):
    id = int
    test_id = int
    name = str

relations.OneToMany(Unit, Test)
relations.OneToOne(Test, Case)

class TestSource(unittest.TestCase):

    maxDiff = None

    def setUp(self):

        self.connection = psycopg2.connect(
            user="postgres", host=os.environ["POSTGRES_HOST"], port=int(os.environ["POSTGRES_PORT"]),
            cursor_factory=psycopg2.extras.RealDictCursor
        )

        self.connection.autocommit = True

        cursor = self.connection.cursor()
        cursor.execute('DROP DATABASE IF EXISTS "test_source"')
        cursor.execute('CREATE DATABASE "test_source"')

        self.source = relations_psycopg2.Source(
            "PsycoPg2Source", "test_source", user="postgres", host=os.environ["POSTGRES_HOST"], port=int(os.environ["POSTGRES_PORT"])
        )

    def tearDown(self):

        self.source.connection.close()

        cursor = self.connection.cursor()
        cursor.execute('DROP DATABASE "test_source"')
        self.connection.close()

    @unittest.mock.patch("relations.SOURCES", {})
    @unittest.mock.patch("psycopg2.connect", unittest.mock.MagicMock())
    def test___init__(self):

        source = relations_psycopg2.Source("unit", "init", connection="corkneckshurn")
        self.assertFalse(source.created)
        self.assertEqual(source.name, "unit")
        self.assertEqual(source.database, "init")
        self.assertIsNone(source.schema)
        self.assertEqual(source.connection, "corkneckshurn")
        self.assertEqual(relations.SOURCES["unit"], source)

        source = relations_psycopg2.Source("test", "init", schema="private", extra="stuff")
        self.assertTrue(source.created)
        self.assertEqual(source.name, "test")
        self.assertEqual(source.database, "init")
        self.assertEqual(source.schema, "private")
        self.assertEqual(source.connection, psycopg2.connect.return_value)
        self.assertEqual(relations.SOURCES["test"], source)
        psycopg2.connect.assert_called_once_with(cursor_factory=psycopg2.extras.RealDictCursor, dbname="init", extra="stuff")

    @unittest.mock.patch("relations.SOURCES", {})
    @unittest.mock.patch("psycopg2.connect", unittest.mock.MagicMock())
    def test___del__(self):

        source = relations_psycopg2.Source("test", "init", schema="private", extra="stuff")
        source.connection = None
        del relations.SOURCES["test"]
        psycopg2.connect.return_value.close.assert_not_called()

        relations_psycopg2.Source("test", "init", schema="private", extra="stuff")
        del relations.SOURCES["test"]
        psycopg2.connect.return_value.close.assert_called_once_with()

    def test_table(self):

        model = unittest.mock.MagicMock()
        model.SCHEMA = None

        self.source.schema = "public"
        model.TABLE = "people"
        self.assertEqual(self.source.table(model), '"public"."people"')

        model.SCHEMA = "things"
        self.assertEqual(self.source.table(model), '"things"."people"')

    def test_encode(self):

        model = unittest.mock.MagicMock()
        people = unittest.mock.MagicMock()
        stuff = unittest.mock.MagicMock()
        things = unittest.mock.MagicMock()

        people.kind = str
        stuff.kind = list
        things.kind = dict

        people.store = "people"
        stuff.store = "stuff"
        things.store = "things"

        model._fields._order = [people, stuff, things]

        values = {
            "people": "sure",
            "stuff": None,
            "things": None
        }

        self.assertEqual(self.source.encode(model, values), {
            "people": "sure",
            "stuff": None,
            "things": None
        })

        values = {
            "people": "sure",
            "stuff": [],
            "things": {}
        }

        self.assertEqual(self.source.encode(model, values), {
            "people": "sure",
            "stuff": '[]',
            "things": '{}'
        })

    def test_walk(self):

        self.assertEqual(self.source.walk("a__b__0___1"), '{a,b,0,"1"}')

    def test_field_init(self):

        class Field:
            pass

        field = Field()

        self.source.field_init(field)

        self.assertIsNone(field.primary_key)
        self.assertIsNone(field.serial)
        self.assertIsNone(field.definition)

    def test_model_init(self):

        class Check(relations.Model):
            id = int
            name = str

        model = Check()

        self.source.model_init(model)

        self.assertIsNone(model.DATABASE)
        self.assertIsNone(model.SCHEMA)
        self.assertEqual(model.TABLE, "check")
        self.assertEqual(model.QUERY.get(), 'SELECT * FROM "check"')
        self.assertIsNone(model.DEFINITION)
        self.assertTrue(model._fields._names["id"].primary_key)
        self.assertTrue(model._fields._names["id"].serial)
        self.assertTrue(model._fields._names["id"].auto)

    def test_field_define(self):

        def deffer():
            pass

        # Specific

        field = relations.Field(int, definition='id')
        self.source.field_init(field)
        definitions = []
        self.source.field_define(field, definitions)
        self.assertEqual(definitions, ['id'])

        # BOOLEAN

        field = relations.Field(bool, store='_flag')
        self.source.field_init(field)
        definitions = []
        self.source.field_define(field, definitions)
        self.assertEqual(definitions, ['"_flag" BOOLEAN'])

        # BOOLEAN default

        field = relations.Field(bool, store='_flag', default=False)
        self.source.field_init(field)
        definitions = []
        self.source.field_define(field, definitions)
        self.assertEqual(definitions, ['"_flag" BOOLEAN NOT NULL DEFAULT False'])

        # BOOLEAN function default

        field = relations.Field(bool, store='_flag', default=deffer)
        self.source.field_init(field)
        definitions = []
        self.source.field_define(field, definitions)
        self.assertEqual(definitions, ['"_flag" BOOLEAN NOT NULL'])

        # BOOLEAN none

        field = relations.Field(bool, store='_flag', none=False)
        self.source.field_init(field)
        definitions = []
        self.source.field_define(field, definitions)
        self.assertEqual(definitions, ['"_flag" BOOLEAN NOT NULL'])

        # INT

        field = relations.Field(int, store='_id')
        self.source.field_init(field)
        definitions = []
        self.source.field_define(field, definitions)
        self.assertEqual(definitions, ['"_id" INT'])

        # INT default

        field = relations.Field(int, store='_id', default=0)
        self.source.field_init(field)
        definitions = []
        self.source.field_define(field, definitions)
        self.assertEqual(definitions, ['"_id" INT NOT NULL DEFAULT 0'])

        # INT function default

        field = relations.Field(int, store='_id', default=deffer)
        self.source.field_init(field)
        definitions = []
        self.source.field_define(field, definitions)
        self.assertEqual(definitions, ['"_id" INT NOT NULL'])

        # INT none

        field = relations.Field(int, store='_id', none=False)
        self.source.field_init(field)
        definitions = []
        self.source.field_define(field, definitions)
        self.assertEqual(definitions, ['"_id" INT NOT NULL'])

        # INT primary

        field = relations.Field(int, store='_id', primary_key=True)
        self.source.field_init(field)
        definitions = []
        self.source.field_define(field, definitions)
        self.assertEqual(definitions, ['"_id" INT PRIMARY KEY'])

        # INT full

        field = relations.Field(int, store='_id', none=False, primary_key=True, serial=True)
        self.source.field_init(field)
        definitions = []
        self.source.field_define(field, definitions)
        self.assertEqual(definitions, ['"_id" SERIAL NOT NULL PRIMARY KEY'])

        # FLOAT

        field = relations.Field(float, store='spend')
        self.source.field_init(field)
        definitions = []
        self.source.field_define(field, definitions)
        self.assertEqual(definitions, ['"spend" FLOAT'])

        # FLOAT default

        field = relations.Field(float, store='spend', default=0.1)
        self.source.field_init(field)
        definitions = []
        self.source.field_define(field, definitions)
        self.assertEqual(definitions, ['"spend" FLOAT NOT NULL DEFAULT 0.1'])

        # FLOAT function default

        field = relations.Field(float, store='spend', default=deffer)
        self.source.field_init(field)
        definitions = []
        self.source.field_define(field, definitions)
        self.assertEqual(definitions, ['"spend" FLOAT NOT NULL'])

        # FLOAT none

        field = relations.Field(float, store='spend', none=False)
        self.source.field_init(field)
        definitions = []
        self.source.field_define(field, definitions)
        self.assertEqual(definitions, ['"spend" FLOAT NOT NULL'])

        # VARCHAR

        field = relations.Field(str, name='name')
        self.source.field_init(field)
        definitions = []
        self.source.field_define(field, definitions)
        self.assertEqual(definitions, ['"name" VARCHAR(255)'])

        # VARCHAR length

        field = relations.Field(str, name='name', length=32)
        self.source.field_init(field)
        definitions = []
        self.source.field_define(field, definitions)
        self.assertEqual(definitions, ['"name" VARCHAR(32)'])

        # VARCHAR default

        field = relations.Field(str, name='name', default='ya')
        self.source.field_init(field)
        definitions = []
        self.source.field_define(field, definitions)
        self.assertEqual(definitions, ['"name" VARCHAR(255) NOT NULL DEFAULT \'ya\''])

        # VARCHAR function default

        field = relations.Field(str, name='name', default=deffer)
        self.source.field_init(field)
        definitions = []
        self.source.field_define(field, definitions)
        self.assertEqual(definitions, ['"name" VARCHAR(255) NOT NULL'])

        # VARCHAR none

        field = relations.Field(str, name='name', none=False)
        self.source.field_init(field)
        definitions = []
        self.source.field_define(field, definitions)
        self.assertEqual(definitions, ['"name" VARCHAR(255) NOT NULL'])

        # VARCHAR full

        field = relations.Field(str, name='name', length=32, none=False, default='ya')
        self.source.field_init(field)
        definitions = []
        self.source.field_define(field, definitions)
        self.assertEqual(definitions, ['"name" VARCHAR(32) NOT NULL DEFAULT \'ya\''])

        # JSON (list)

        field = relations.Field(list, name='stuff')
        self.source.field_init(field)
        definitions = []
        self.source.field_define(field, definitions)
        self.assertEqual(definitions, ['"stuff" JSONB NOT NULL DEFAULT \'[]\''])

        # JSON (dict)

        field = relations.Field(dict, name='things')
        self.source.field_init(field)
        definitions = []
        self.source.field_define(field, definitions)
        self.assertEqual(definitions, ['"things" JSONB NOT NULL DEFAULT \'{}\''])

        # JSON (anything)

        field = relations.Field(ipaddress.IPv4Address, name='ip', attr="whatev")
        self.source.field_init(field)
        definitions = []
        self.source.field_define(field, definitions)
        self.assertEqual(definitions, ['"ip" JSONB'])

        # EXTRACTED

        field = relations.Field(dict, name='grab', extract={
            "a__b__0___1": bool,
            "c__b__0___1": int,
            "c__d__0___1": float,
            "c__d__1___1": str,
            "c__d__1___2": list
        })
        self.source.field_init(field)
        definitions = []
        self.source.field_define(field, definitions)
        self.assertEqual(definitions, [
            '"grab" JSONB NOT NULL DEFAULT \'{}\'',
            '"grab__a__b__0___1" BOOLEAN GENERATED ALWAYS AS (("grab"#>>\'{a,b,0,"1"}\')::BOOLEAN) STORED',
            '"grab__c__b__0___1" INT GENERATED ALWAYS AS (("grab"#>>\'{c,b,0,"1"}\')::INT) STORED',
            '"grab__c__d__0___1" FLOAT GENERATED ALWAYS AS (("grab"#>>\'{c,d,0,"1"}\')::FLOAT) STORED',
            '"grab__c__d__1___1" VARCHAR(255) GENERATED ALWAYS AS (("grab"#>>\'{c,d,1,"1"}\')::VARCHAR(255)) STORED',
            '"grab__c__d__1___2" JSONB GENERATED ALWAYS AS (("grab"#>>\'{c,d,1,"2"}\')::JSONB) STORED'
        ])

        # INJECTED

        field = relations.Field(str, name='grab', inject="things__a__b__0___1")
        self.source.field_init(field)
        definitions = []
        self.source.field_define(field, definitions)
        self.assertEqual(definitions, [])

    def test_model_define(self):

        class Simple(relations.Model):

            SOURCE = "PsycoPg2Source"
            DEFINITION = "whatever"

            id = int
            name = str

            INDEX = "id"

        self.assertEqual(Simple.define(), "whatever")

        Simple.DEFINITION = None
        self.assertEqual(Simple.define(), [
            """CREATE TABLE IF NOT EXISTS "simple" (
  "id" SERIAL PRIMARY KEY,
  "name" VARCHAR(255) NOT NULL
)""",
            """CREATE UNIQUE INDEX "simple_name" ON "simple" ("name")""",
            """CREATE INDEX "simple_id" ON "simple" ("id")"""
        ])

        cursor = self.source.connection.cursor()
        [cursor.execute(statement) for statement in Simple.define()]
        cursor.close()

    def test_field_create(self):

        # Standard

        field = relations.Field(int, name="id")
        self.source.field_init(field)
        fields = []
        clause = []
        self.source.field_create( field, fields, clause)
        self.assertEqual(fields, ['"id"'])
        self.assertEqual(clause, ["%(id)s"])

        # auto

        field = relations.Field(int, name="id", auto=True)
        self.source.field_init(field)
        fields = []
        clause = []
        self.source.field_create( field, fields, clause)
        self.assertEqual(fields, [])
        self.assertEqual(clause, [])

        # inject

        field = relations.Field(int, name="id", inject=True)
        self.source.field_init(field)
        fields = []
        clause = []
        self.source.field_create( field, fields, clause)
        self.assertEqual(fields, [])
        self.assertEqual(clause, [])

    def test_model_create(self):

        simple = Simple("sure")
        simple.plain.add("fine")

        cursor = self.source.connection.cursor()
        [cursor.execute(statement) for statement in Simple.define() + Plain.define() + Meta.define()]

        simple.create()

        self.assertEqual(simple._action, "update")
        self.assertEqual(simple._record._action, "update")
        self.assertEqual(simple.plain[0].simple_id, simple.id)
        self.assertEqual(simple.plain._action, "update")
        self.assertEqual(simple.plain[0]._record._action, "update")

        cursor.execute("SELECT * FROM simple")
        self.assertEqual(cursor.fetchone(), {"id": 1, "name": "sure"})

        simples = Simple.bulk().add("ya").create()
        self.assertEqual(simples._models, [])

        cursor.execute("SELECT * FROM simple WHERE name='ya'")
        self.assertEqual(cursor.fetchone(), {"id": 2, "name": "ya"})

        cursor.execute("SELECT * FROM plain")
        self.assertEqual(cursor.fetchone(), {"simple_id": 1, "name": "fine"})

        Meta("yep", True, 3.50, [1, None], {"for": [{"1": "yep"}]}, "sure").create()
        cursor.execute("SELECT * FROM meta")
        self.assertEqual(cursor.fetchone(), {
            "id": 1,
            "name": "yep",
            "flag": True,
            "spend": 3.50,
            "stuff": [1, {"relations.io": {"1": "sure"}}],
            "things": {"for": [{"1": "yep"}]},
            "things__for__0___1": "yep"
        })

        cursor.close()

    def test_field_retrieve(self):

        # IN

        field = relations.Field(int, name='id')
        self.source.field_init(field)
        field.filter([1, 2, 3], 'in')
        query = relations.query.Query()
        values = []
        self.source.field_retrieve( field, query, values)
        self.assertEqual(query.wheres, '"id" IN (%s,%s,%s)')
        self.assertEqual(values, [1, 2, 3])

        # NOT IN

        field = relations.Field(int, name='id')
        self.source.field_init(field)
        field.filter([1, 2, 3], 'ne')
        query = relations.query.Query()
        values = []
        self.source.field_retrieve( field, query, values)
        self.assertEqual(query.wheres, '"id" NOT IN (%s,%s,%s)')
        self.assertEqual(values, [1, 2, 3])

        # LIKE

        field = relations.Field(int, name='id')
        self.source.field_init(field)
        field.filter(1, 'like')
        query = relations.query.Query()
        values = []
        self.source.field_retrieve( field, query, values)
        self.assertEqual(query.wheres, '"id"::VARCHAR(255) ILIKE %s')
        self.assertEqual(values, ["%1%"])

        # NOT LIKE

        field = relations.Field(int, name='id')
        self.source.field_init(field)
        field.filter(1, 'notlike')
        query = relations.query.Query()
        values = []
        self.source.field_retrieve( field, query, values)
        self.assertEqual(query.wheres, '"id"::VARCHAR(255) NOT ILIKE %s')
        self.assertEqual(values, ["%1%"])

        # IS NULL

        field = relations.Field(int, name='id')
        self.source.field_init(field)
        field.filter(True, 'null')
        query = relations.query.Query()
        values = []
        self.source.field_retrieve( field, query, values)
        self.assertEqual(query.wheres, '"id" IS NULL')
        self.assertEqual(values, [])

        # IS NOT NULL

        field = relations.Field(int, name='id')
        self.source.field_init(field)
        field.filter(False, 'null')
        query = relations.query.Query()
        values = []
        self.source.field_retrieve( field, query, values)
        self.assertEqual(query.wheres, '"id" IS NOT NULL')
        self.assertEqual(values, [])

        # JSON

        field = relations.Field(dict, name='meta')
        self.source.field_init(field)
        field.filter(1, 'a__b__0___1')
        query = relations.query.Query()
        values = []
        self.source.field_retrieve(field, query, values)
        self.assertEqual(query.wheres, """("meta"#>>%s)::INT=%s""")
        self.assertEqual(values, ['{a,b,0,"1"}', 1])

        field = relations.Field(dict, name='meta')
        self.source.field_init(field)
        field.filter(1.0, 'a__b__0___1')
        query = relations.query.Query()
        values = []
        self.source.field_retrieve(field, query, values)
        self.assertEqual(query.wheres, """("meta"#>>%s)::FLOAT=%s""")
        self.assertEqual(values, ['{a,b,0,"1"}', 1.0])

        field = relations.Field(dict, name='meta', extract="a__b__0___1")
        self.source.field_init(field)
        field.filter(1.0, 'a__b__0___1')
        query = relations.query.Query()
        values = []
        self.source.field_retrieve(field, query, values)
        self.assertEqual(query.wheres, """"meta__a__b__0___1"=%s""")
        self.assertEqual(values, [1.0])

        # =

        field = relations.Field(int, name='id')
        self.source.field_init(field)
        field.filter(1)
        query = relations.query.Query()
        values = []
        self.source.field_retrieve( field, query, values)
        self.assertEqual(query.wheres, '"id"=%s')
        self.assertEqual(values, [1])

        # >

        field = relations.Field(int, name='id')
        self.source.field_init(field)
        field.filter(1, 'gt')
        query = relations.query.Query()
        values = []
        self.source.field_retrieve( field, query, values)
        self.assertEqual(query.wheres, '"id">%s')
        self.assertEqual(values, [1])

        # >=

        field = relations.Field(int, name='id')
        self.source.field_init(field)
        field.filter(1, 'gte')
        query = relations.query.Query()
        values = []
        self.source.field_retrieve( field, query, values)
        self.assertEqual(query.wheres, '"id">=%s')
        self.assertEqual(values, [1])

        # <

        field = relations.Field(int, name='id')
        self.source.field_init(field)
        field.filter(1, 'lt')
        query = relations.query.Query()
        values = []
        self.source.field_retrieve( field, query, values)
        self.assertEqual(query.wheres, '"id"<%s')
        self.assertEqual(values, [1])

        # <=

        field = relations.Field(int, name='id')
        self.source.field_init(field)
        field.filter(1, 'lte')
        query = relations.query.Query()
        values = []
        self.source.field_retrieve( field, query, values)
        self.assertEqual(query.wheres, '"id"<=%s')
        self.assertEqual(values, [1])

        # Extract

    def test_model_like(self):

        cursor = self.source.connection.cursor()

        [cursor.execute(statement) for statement in Unit.define() + Test.define() + Case.define()]

        Unit([["stuff"], ["people"]]).create()

        unit = Unit.one()

        query = copy.deepcopy(unit.QUERY)
        values = []
        self.source.model_like(unit, query, values)
        self.assertEqual(query.wheres, '')
        self.assertEqual(values, [])

        unit = Unit.one(like="p")
        query = copy.deepcopy(unit.QUERY)
        values = []
        self.source.model_like(unit, query, values)
        self.assertEqual(query.wheres, '("name"::VARCHAR(255) ILIKE %s)')
        self.assertEqual(values, ['%p%'])

        unit = Unit.one(name="people")
        unit.test.add("things")[0]
        unit.update()

        test = Test.many(like="p")
        query = copy.deepcopy(test.QUERY)
        values = []
        self.source.model_like(test, query, values)
        self.assertEqual(query.wheres, '("unit_id" IN (%s) OR "name"::VARCHAR(255) ILIKE %s)')
        self.assertEqual(values, [unit.id, '%p%'])
        self.assertFalse(test.overflow)

        test = Test.many(like="p", _chunk=1)
        query = copy.deepcopy(test.QUERY)
        values = []
        self.source.model_like(test, query, values)
        self.assertEqual(query.wheres, '("unit_id" IN (%s) OR "name"::VARCHAR(255) ILIKE %s)')
        self.assertEqual(values, [unit.id, '%p%'])
        self.assertTrue(test.overflow)

        class Nut(SourceModel):

            id = int
            name = str
            ip = ipaddress.IPv4Address, {"attr": {"compressed": "address", "__int__": "value"}, "init": "address", "label": ["address", "value"], "extract": "address"}
            subnet = ipaddress.IPv4Network, {"attr": subnet_attr, "init": "address", "label": "address"}

            LABEL = ["ip", "subnet__min_address"]
            UNIQUE = False

        net = Nut.many(like="p")
        query = copy.deepcopy(net.QUERY)
        values = []
        self.source.model_like(net, query, values)
        self.assertEqual(query.wheres, '("ip__address"::VARCHAR(255) ILIKE %s OR ("ip"#>>%s)::VARCHAR(255) ILIKE %s OR ("subnet"#>>%s)::VARCHAR(255) ILIKE %s)')
        self.assertEqual(values, ['%p%', '{value}', '%p%', '{min_address}', '%p%'])


    def test_model_sort(self):

        unit = Unit.one()

        query = copy.deepcopy(unit.QUERY)
        self.source.model_sort(unit, query)
        self.assertEqual(query.order_bys, '"name"')

        unit._sort = ['-id']
        query = copy.deepcopy(unit.QUERY)
        self.source.model_sort(unit, query)
        self.assertEqual(query.order_bys, '"id" DESC')
        self.assertIsNone(unit._sort)

    def test_model_limit(self):

        unit = Unit.one()

        query = copy.deepcopy(unit.QUERY)
        values = []
        self.source.model_limit(unit, query, values)
        self.assertEqual(query.limits, '')
        self.assertEqual(values, [])

        unit._limit = 2
        query = copy.deepcopy(unit.QUERY)
        values = []
        self.source.model_limit(unit, query, values)
        self.assertEqual(query.limits, '%s')
        self.assertEqual(values, [2])

        unit._offset = 1
        query = copy.deepcopy(unit.QUERY)
        values = []
        self.source.model_limit(unit, query, values)
        self.assertEqual(query.limits, '%s OFFSET %s')
        self.assertEqual(values, [2, 1])

    def test_model_count(self):

        cursor = self.source.connection.cursor()

        [cursor.execute(statement) for statement in Unit.define() + Test.define() + Case.define()]

        Unit([["stuff"], ["people"]]).create()

        self.assertEqual(Unit.many().count(), 2)

        self.assertEqual(Unit.many(name="people").count(), 1)

        self.assertEqual(Unit.many(like="p").count(), 1)

    def test_model_retrieve(self):

        cursor = self.source.connection.cursor()

        [cursor.execute(statement) for statement in Unit.define() + Test.define() + Case.define() + Meta.define() + Net.define()]

        Unit([["stuff"], ["people"]]).create()

        models = Unit.one(name__in=["people", "stuff"])
        self.assertRaisesRegex(relations.ModelError, "unit: more than one retrieved", models.retrieve)

        model = Unit.one(name="things")
        self.assertRaisesRegex(relations.ModelError, "unit: none retrieved", model.retrieve)

        self.assertIsNone(model.retrieve(False))

        unit = Unit.one(name="people")

        self.assertEqual(unit.id, 2)
        self.assertEqual(unit._action, "update")
        self.assertEqual(unit._record._action, "update")

        self.assertTrue(Unit.many(name="people").limit(1).retrieve().overflow)
        self.assertFalse(Unit.many(name="people").limit(2).retrieve().overflow)

        unit.test.add("things")[0].case.add("persons")
        unit.update()

        model = Unit.many(test__name="things")

        self.assertEqual(model.id, [2])
        self.assertEqual(model[0]._action, "update")
        self.assertEqual(model[0]._record._action, "update")
        self.assertEqual(model[0].test[0].id, 1)
        self.assertEqual(model[0].test[0].case.name, "persons")

        self.assertEqual(Unit.many().name, ["people", "stuff"])
        self.assertEqual(Unit.many().sort("-name").name, ["stuff", "people"])
        self.assertEqual(Unit.many().sort("-name").limit(1, 1).name, ["people"])
        self.assertEqual(Unit.many().sort("-name").limit(0).name, [])
        self.assertEqual(Unit.many(name="people").limit(1).name, ["people"])

        model = Unit.many(like="p")
        self.assertEqual(model.name, ["people"])

        model = Test.many(like="p").retrieve()
        self.assertEqual(model.name, ["things"])
        self.assertFalse(model.overflow)

        model = Test.many(like="p", _chunk=1).retrieve()
        self.assertEqual(model.name, ["things"])
        self.assertTrue(model.overflow)

        Meta("dive", stuff=[1, 2, 3, None], things={"a": {"b": [1], "c": "sure"}, "4": 5, "for": [{"1": "yep"}]}).create()

        model = Meta.many(stuff__1=2)
        self.assertEqual(model[0].name, "dive")

        model = Meta.many(things__a__b__0=1)
        self.assertEqual(model[0].name, "dive")

        model = Meta.many(things__a__c__like="su")
        self.assertEqual(model[0].name, "dive")

        model = Meta.many(things__a__d__null=True)
        self.assertEqual(model[0].name, "dive")

        model = Meta.many(things___4=5)
        self.assertEqual(model[0].name, "dive")

        model = Meta.many(things__a__b__0__gt=1)
        self.assertEqual(len(model), 0)

        model = Meta.many(things__a__c__notlike="su")
        self.assertEqual(len(model), 0)

        model = Meta.many(things__a__d__null=False)
        self.assertEqual(len(model), 0)

        model = Meta.many(things___4=6)
        self.assertEqual(len(model), 0)

        Net(ip="1.2.3.4", subnet="1.2.3.0/24").create()
        Net().create()

        model = Net.many(like='1.2.3.')
        self.assertEqual(model[0].ip.compressed, "1.2.3.4")

        model = Net.many(ip__address__like='1.2.3.')
        self.assertEqual(model[0].ip.compressed, "1.2.3.4")

        model = Net.many(ip__value__gt=int(ipaddress.IPv4Address('1.2.3.0')))
        self.assertEqual(model[0].ip.compressed, "1.2.3.4")

        model = Net.many(subnet__address__like='1.2.3.')
        self.assertEqual(model[0].ip.compressed, "1.2.3.4")

        model = Net.many(subnet__min_value=int(ipaddress.IPv4Address('1.2.3.0')))
        self.assertEqual(model[0].ip.compressed, "1.2.3.4")

        model = Net.many(ip__address__notlike='1.2.3.')
        self.assertEqual(len(model), 0)

        model = Net.many(ip__value__lt=int(ipaddress.IPv4Address('1.2.3.0')))
        self.assertEqual(len(model), 0)

        model = Net.many(subnet__address__notlike='1.2.3.')
        self.assertEqual(len(model), 0)

        model = Net.many(subnet__max_value=int(ipaddress.IPv4Address('1.2.3.0')))
        self.assertEqual(len(model), 0)

    def test_model_labels(self):

        cursor = self.source.connection.cursor()

        [cursor.execute(statement) for statement in Unit.define() + Test.define() + Case.define() + Net.define()]

        Unit("people").create().test.add("stuff").add("things").create()

        labels = Unit.many().labels()

        self.assertEqual(labels.id, "id")
        self.assertEqual(labels.label, ["name"])
        self.assertEqual(labels.parents, {})
        self.assertEqual(labels.format, ["fancy"])

        self.assertEqual(labels.ids, [1])
        self.assertEqual(labels.labels,{1: ["people"]})

        labels = Test.many().labels()

        self.assertEqual(labels.id, "id")
        self.assertEqual(labels.label, ["unit_id", "name"])

        self.assertEqual(labels.parents["unit_id"].id, "id")
        self.assertEqual(labels.parents["unit_id"].label, ["name"])
        self.assertEqual(labels.parents["unit_id"].parents, {})
        self.assertEqual(labels.parents["unit_id"].format, ["fancy"])

        self.assertEqual(labels.format, ["fancy", "shmancy"])

        self.assertEqual(labels.ids, [1, 2])
        self.assertEqual(labels.labels, {
            1: ["people", "stuff"],
            2: ["people", "things"]
        })

        Net(ip="1.2.3.4", subnet="1.2.3.0/24").create()

        self.assertEqual(Net.many().labels().labels, {
            1: ["1.2.3.4"]
        })

    def test_field_update(self):

        # Standard

        field = relations.Field(int, name="id")
        self.source.field_init(field)
        clause = []
        values = []
        self.source.field_update(field, {"id": 1}, clause, values)
        self.assertEqual(clause, ['"id"=%s'])
        self.assertEqual(values, [1])

        # Non standard

        field = relations.Field(dict, name="id")
        self.source.field_init(field)
        clause = []
        values = []
        self.source.field_update(field, {"id": {"a": 1}}, clause, values)
        self.assertEqual(clause, ['"id"=%s'])
        self.assertEqual(values, ['{"a": 1}'])

        # Non existent

        field = relations.Field(dict, name="id")
        self.source.field_init(field)
        clause = []
        values = []
        self.source.field_update(field, {}, clause, values)
        self.assertEqual(clause, [])
        self.assertEqual(values, [])

    def test_model_update(self):

        cursor = self.source.connection.cursor()

        [cursor.execute(statement) for statement in Unit.define() + Test.define() + Case.define() + Meta.define() + Net.define()]

        Unit([["people"], ["stuff"]]).create()

        unit = Unit.many(id=2).set(name="things")

        self.assertEqual(unit.update(), 1)

        unit = Unit.one(2)

        unit.name = "thing"
        unit.test.add("moar")

        self.assertEqual(unit.update(), 1)
        self.assertEqual(unit.name, "thing")
        self.assertEqual(unit.test[0].unit_id, unit.id)
        self.assertEqual(unit.test[0].name, "moar")

        Meta("yep", True, 1.1, [1, None], {"a": 1}).create()

        Meta.one(name="yep").set(flag=False, stuff=[], things={}).update()
        cursor.execute("SELECT * FROM meta")
        self.assertEqual(cursor.fetchone(), {"id": 1, "name": "yep", "flag": False, "spend": 1.1, "stuff": [], "things": {}, "things__for__0___1": None})

        plain = Plain.one()
        self.assertRaisesRegex(relations.ModelError, "plain: nothing to update from", plain.update)

        ping = Net(ip="1.2.3.4", subnet="1.2.3.0/24").create()
        pong = Net(ip="5.6.7.8", subnet="5.6.7.0/24").create()

        Net.many().set(subnet="9.10.11.0/24").update()

        self.assertEqual(Net.one(ping.id).subnet.compressed, "9.10.11.0/24")
        self.assertEqual(Net.one(pong.id).subnet.compressed, "9.10.11.0/24")

        Net.one(ping.id).set(ip="13.14.15.16").update()
        self.assertEqual(Net.one(ping.id).ip.compressed, "13.14.15.16")
        self.assertEqual(Net.one(pong.id).ip.compressed, "5.6.7.8")

    def test_model_delete(self):

        cursor = self.source.connection.cursor()

        [cursor.execute(statement) for statement in Unit.define() + Test.define() + Case.define()]

        unit = Unit("people")
        unit.test.add("stuff").add("things")
        unit.create()

        self.assertEqual(Test.one(id=2).delete(), 1)
        self.assertEqual(len(Test.many()), 1)

        self.assertEqual(Unit.one(1).test.delete(), 1)
        self.assertEqual(Unit.one(1).retrieve().delete(), 1)
        self.assertEqual(len(Unit.many()), 0)
        self.assertEqual(len(Test.many()), 0)

        [cursor.execute(statement) for statement in Plain.define()]

        plain = Plain(0, "nope").create()
        self.assertRaisesRegex(relations.ModelError, "plain: nothing to delete from", plain.delete)
