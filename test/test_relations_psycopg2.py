import unittest
import unittest.mock

import os
import shutil
import pathlib
import copy
import json
import psycopg2.extras

import ipaddress

import relations
import relations_sql
import relations_postgresql
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
    people = set
    stuff = list
    things = dict, {"extract": "for__0____1"}
    push = str, {"inject": "stuff___1__relations.io____1"}

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
        "titles": "address",
        "extract": {"address": str, "value": int}
    }
    subnet = ipaddress.IPv4Network, {
        "attr": subnet_attr,
        "init": "address",
        "titles": "address"
    }

    TITLES = "ip__address"
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
            "PsycoPg2Source", "test_source", schema="test_source", user="postgres", host=os.environ["POSTGRES_HOST"], port=int(os.environ["POSTGRES_PORT"])
        )

        self.source.connection.cursor().execute('CREATE SCHEMA "test_source"')

        shutil.rmtree("ddl", ignore_errors=True)
        os.makedirs("ddl", exist_ok=True)

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

    def test_execute(self):

        self.source.execute("")

        self.source.execute(relations_sql.SQL("""CREATE TABLE IF NOT EXISTS "simple" (
  "id" SERIAL PRIMARY KEY,
  "name" VARCHAR(255) NOT NULL
);"""))

        cursor = self.source.connection.cursor()

        cursor.execute("SELECT * FROM information_schema.columns WHERE table_name='simple'")

        id = cursor.fetchone()
        self.assertEqual(id["column_name"], "id")
        self.assertEqual(id["data_type"], "integer")

        name = cursor.fetchone()
        self.assertEqual(name["column_name"], "name")
        self.assertEqual(name["data_type"], "character varying")

    def test_init(self):

        class Check(relations.Model):
            id = int
            name = str

        model = Check()

        self.source.init(model)

        self.assertEqual(model.SCHEMA, "test_source")
        self.assertEqual(model.STORE, "check")
        self.assertTrue(model._fields._names["id"].auto)

    def test_define(self):

        self.assertEqual(self.source.define(Simple.thy().define()),
"""CREATE TABLE IF NOT EXISTS "test_source"."simple" (
  "id" BIGSERIAL,
  "name" VARCHAR(255) NOT NULL,
  PRIMARY KEY ("id")
);

CREATE UNIQUE INDEX "simple_name" ON "test_source"."simple" ("name");
""")

        self.source.execute(self.source.define(Simple.thy().define()))

    def test_create_query(self):

        query = Simple("sure").query()
        query.generate()

        self.assertEqual(query.sql, """INSERT INTO "test_source"."simple" ("name") VALUES (%s)""")
        self.assertEqual(query.args, ["sure"])

        query = Simple.bulk().add("sure").add("fine").query()
        query.generate()

        self.assertEqual(query.sql, """INSERT INTO "test_source"."simple" ("name") VALUES (%s),(%s)""")
        self.assertEqual(query.args, ["sure", "fine"])

        model = Simple([["sure"], ["fine"]])
        self.assertRaisesRegex(relations.ModelError, "only one create query at a time", model.query)

    def test_create_id(self):

        self.source.execute(Simple.define())
        self.source.execute(Plain.define())
        self.source.execute(Meta.define())

        simple = Simple("sure")

        query = self.source.create_query(simple)

        cursor = self.source.connection.cursor()

        self.source.create_id(cursor, simple, query)

        cursor.execute("SELECT * FROM test_source.simple")
        self.assertEqual(cursor.fetchone()["id"], simple.id)

        cursor.close()

    def test_create(self):

        simple = Simple("sure")
        simple.plain.add("fine")

        self.source.execute(Simple.define())
        self.source.execute(Plain.define())
        self.source.execute(Meta.define())

        simple.create()

        self.assertEqual(simple.id, 1)
        self.assertEqual(simple._action, "update")
        self.assertEqual(simple._record._action, "update")
        self.assertEqual(simple.plain[0].simple_id, 1)
        self.assertEqual(simple.plain._action, "update")
        self.assertEqual(simple.plain[0]._record._action, "update")

        cursor = self.source.connection.cursor()

        cursor.execute("SELECT * FROM test_source.simple")
        self.assertEqual(cursor.fetchone(), {"id": 1, "name": "sure"})

        simples = Simple.bulk().add("ya").create()
        self.assertEqual(simples._models, [])

        cursor.execute("SELECT * FROM test_source.simple WHERE name='ya'")
        self.assertEqual(cursor.fetchone(), {"id": 2, "name": "ya"})

        cursor.execute("SELECT * FROM test_source.plain")
        self.assertEqual(cursor.fetchone(), {"simple_id": 1, "name": "fine"})

        model = Meta("yep", True, 3.50, {"tom", "mary"}, [1, None], {"for": [{"1": "yep"}]}, "sure").create()
        cursor.execute("SELECT * FROM test_source.meta")
        self.assertEqual(self.source.values_retrieve(model, cursor.fetchone()), {
            "id": 1,
            "name": "yep",
            "flag": 1,
            "spend": 3.50,
            "people": ["mary", "tom"],
            "stuff": [1, {"relations.io": {"1": "sure"}}],
            "things": {"for": [{"1": "yep"}]},
            "things__for__0____1": "yep"
        })

        cursor.close()

    def test_retrieve_field(self):

        field = relations.Field(int, name="id")
        self.source.field_init(field)
        field.filter(1)
        query = self.source.SELECT()
        self.source.retrieve_field(field, query)
        query.generate()
        self.assertEqual(query.sql, """SELECT WHERE "id"=%s""")
        self.assertEqual(query.args, [1])

        field = relations.Field(dict, name="things", extract={"for__0____1": str})
        self.source.field_init(field)
        field.filter({"a": 1})
        query = self.source.SELECT()
        self.source.retrieve_field(field, query)
        query.generate()
        self.assertEqual(query.sql, """SELECT WHERE "things"=(%s)::JSONB""")
        self.assertEqual(query.args, ['{"a": 1}'])

        field = relations.Field(dict, name="things", extract={"for__0____1": str})
        self.source.field_init(field)
        field.filter("yes", "a__b")
        query = self.source.SELECT()
        self.source.retrieve_field(field, query)
        query.generate()
        self.assertEqual(query.sql, """SELECT WHERE ("things"#>>%s)::JSONB=(%s)::JSONB""")
        self.assertEqual(query.args, ['{a,b}', '"yes"'])

        field = relations.Field(dict, name="things", extract={"for__0____1": str})
        self.source.field_init(field)
        field.filter("yes", "for__0____1")
        query = self.source.SELECT()
        self.source.retrieve_field(field, query)
        query.generate()
        self.assertEqual(query.sql, """SELECT WHERE "things__for__0____1"=%s""")
        self.assertEqual(query.args, ['yes'])

    def test_like(self):

        self.source.execute(Unit.define())
        self.source.execute(Test.define())
        self.source.execute(Case.define())
        self.source.execute(Meta.define())
        self.source.execute(Net.define())

        Unit([["stuff"], ["people"]]).create()

        unit = Unit.one()
        query = self.source.SELECT()
        self.source.like(unit, query)
        query.generate()
        self.assertEqual(query.sql, """SELECT """)
        self.assertEqual(query.args, [])

        unit = Unit.one(like="p")
        query = self.source.SELECT()
        self.source.like(unit, query)
        query.generate()
        self.assertEqual(query.sql, """SELECT WHERE (("name")::VARCHAR(255) LIKE (%s)::VARCHAR(255))""")
        self.assertEqual(query.args, ['%p%'])

        unit = Unit.one(name="people")
        unit.test.add("things")[0]
        unit.update()

        test = Test.many(like="p")
        query = self.source.SELECT()
        self.source.like(test, query)
        query.generate()
        self.assertEqual(query.sql, """SELECT WHERE ("unit_id" IN (%s) OR ("name")::VARCHAR(255) LIKE (%s)::VARCHAR(255))""")
        self.assertEqual(query.args, [unit.id, '%p%'])
        self.assertFalse(test.overflow)

        test = Test.many(like="p", _chunk=1)
        query = self.source.SELECT()
        self.source.like(test, query)
        query.generate()
        self.assertEqual(query.sql, """SELECT WHERE ("unit_id" IN (%s) OR ("name")::VARCHAR(255) LIKE (%s)::VARCHAR(255))""")
        self.assertEqual(query.args, [unit.id, '%p%'])
        self.assertTrue(test.overflow)

        Unit.many().delete()
        test = Test.many(like="p")
        query = self.source.SELECT()
        self.source.like(test, query)
        query.generate()
        self.assertEqual(query.sql, """SELECT WHERE (("name")::VARCHAR(255) LIKE (%s)::VARCHAR(255))""")
        self.assertEqual(query.args, ['%p%'])

        class Nut(SourceModel):

            id = int
            name = str
            ip = ipaddress.IPv4Address, {"attr": {"compressed": "address", "__int__": "value"}, "init": "address", "titles": ["address", "value"], "extract": "address"}
            subnet = ipaddress.IPv4Network, {"attr": subnet_attr, "init": "address", "titles": "address"}

            TITLES = ["ip", "subnet__min_address"]
            UNIQUE = False

        net = Nut.many(like="p")
        query = self.source.SELECT()
        self.source.like(net, query)
        query.generate()
        self.assertEqual(query.sql, """SELECT WHERE (("ip__address")::VARCHAR(255) LIKE (%s)::VARCHAR(255) OR ("ip"#>>%s)::VARCHAR(255) LIKE (%s)::VARCHAR(255) OR ("subnet"#>>%s)::VARCHAR(255) LIKE (%s)::VARCHAR(255))""")
        self.assertEqual(query.args, ['%p%', '{value}', '%p%', '{min_address}', '%p%'])

    def test_sort(self):

        unit = Unit.one()

        query = self.source.SELECT()
        self.source.sort(unit, query)
        query.generate()
        self.assertEqual(query.sql, """SELECT ORDER BY "name" ASC""")
        self.assertEqual(query.args, [])

        unit._sort = ['-id']
        query = self.source.SELECT()
        self.source.sort(unit, query)
        query.generate()
        self.assertEqual(query.sql, """SELECT ORDER BY "id" DESC""")
        self.assertEqual(query.args, [])
        self.assertIsNone(unit._sort)

    def test_limit(self):

        unit = Unit.one()

        query = self.source.SELECT()
        self.source.limit(unit, query)
        query.generate()
        self.assertEqual(query.sql, """SELECT """)
        self.assertEqual(query.args, [])

        query = self.source.SELECT()
        unit._limit = 2
        self.source.limit(unit, query)
        query.generate()
        self.assertEqual(query.sql, """SELECT LIMIT %s""")
        self.assertEqual(query.args, [2])

        query = self.source.SELECT()
        unit._offset = 1
        self.source.limit(unit, query)
        query.generate()
        self.assertEqual(query.sql, """SELECT LIMIT %s OFFSET %s""")
        self.assertEqual(query.args, [2, 1])

    def test_count_query(self):

        self.source.execute(Unit.define())
        self.source.execute(Test.define())
        self.source.execute(Case.define())

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

        unit.test.add("things")[0].case.add("persons")
        unit.update()

        model = Unit.many(test__name="things", like="p")

        query = self.source.count_query(model)

        query.generate(indent=2)

        self.assertEqual(query.sql,
"""SELECT
  COUNT(*) AS "total"
FROM
  "test_source"."unit"
WHERE
  "id" IN (
    %s
  ) AND
  (
    ("name")::VARCHAR(255) LIKE (%s)::VARCHAR(255)
  )""")
        self.assertEqual(query.args, [2, '%p%'])

    def test_retrieve_query(self):

        self.source.execute(Unit.define())
        self.source.execute(Test.define())
        self.source.execute(Case.define())

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

        unit.test.add("things")[0].case.add("persons")
        unit.update()

        model = Unit.many(test__name="things", like="p").limit(5)

        query = self.source.retrieve_query(model)

        query.generate(indent=2)

        self.assertEqual(query.sql,
"""SELECT
  *
FROM
  "test_source"."unit"
WHERE
  "id" IN (
    %s
  ) AND
  (
    ("name")::VARCHAR(255) LIKE (%s)::VARCHAR(255)
  )
ORDER BY
  "name" ASC
LIMIT %s""")
        self.assertEqual(query.args, [2, '%p%', 5])

    def test_titles_query(self):

        self.source.execute(Unit.define())
        self.source.execute(Test.define())
        self.source.execute(Case.define())

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

        unit.test.add("things")[0].case.add("persons")
        unit.update()

        model = Unit.many(test__name="things", like="p").limit(5)

        query = self.source.titles_query(model)

        query.generate(indent=2)

        self.assertEqual(query.sql,
"""SELECT
  *
FROM
  "test_source"."unit"
WHERE
  "id" IN (
    %s
  ) AND
  (
    ("name")::VARCHAR(255) LIKE (%s)::VARCHAR(255)
  )
ORDER BY
  "name" ASC
LIMIT %s""")
        self.assertEqual(query.args, [2, '%p%', 5])

    def test_count(self):

        self.source.execute(Unit.define())
        self.source.execute(Test.define())
        self.source.execute(Case.define())

        Unit([["stuff"], ["people"]]).create()

        self.assertEqual(Unit.many().count(), 2)

        self.assertEqual(Unit.many(name="people").count(), 1)

        self.assertEqual(Unit.many(like="p").count(), 1)

    def test_values_retrieve(self):

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

        self.assertEqual(self.source.values_retrieve(model, values), {
            "people": "sure",
            "stuff": None,
            "things": None
        })

        values = {
            "people": "sure",
            "stuff": '[]',
            "things": '{}'
        }

        self.assertEqual(self.source.values_retrieve(model, values), {
            "people": "sure",
            "stuff": [],
            "things": {}
        })

    def test_retrieve(self):

        self.source.execute(Unit.define())
        self.source.execute(Test.define())
        self.source.execute(Case.define())
        self.source.execute(Meta.define())
        self.source.execute(Net.define())

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

        unit.test.add("things")[0].case.add("persons")
        unit.update()

        model = Unit.many(test__name="things")

        self.assertEqual(model.id, [2])
        self.assertEqual(model[0]._action, "update")
        self.assertEqual(model[0]._record._action, "update")
        self.assertEqual(model[0].test[0].id, 1)
        self.assertEqual(model[0].test[0].case.name, "persons")

        model = Unit.many(like="p")
        self.assertEqual(model.name, ["people"])

        model = Test.many(like="p").retrieve()
        self.assertEqual(model.name, ["things"])
        self.assertFalse(model.overflow)

        model = Test.many(like="p", _chunk=1).retrieve()
        self.assertEqual(model.name, ["things"])
        self.assertTrue(model.overflow)

        Meta("yep", True, 1.1, {"tom"}, [1, None], {"a": 1}).create()
        model = Meta.one(name="yep")

        self.assertEqual(model.flag, True)
        self.assertEqual(model.spend, 1.1)
        self.assertEqual(model.people, {"tom"})
        self.assertEqual(model.stuff, [1, {"relations.io": {"1": None}}])
        self.assertEqual(model.things, {"a": 1})

        self.assertEqual(Unit.many().name, ["people", "stuff"])
        self.assertEqual(Unit.many().sort("-name").name, ["stuff", "people"])
        self.assertEqual(Unit.many().sort("-name").limit(1, 1).name, ["people"])
        self.assertEqual(Unit.many().sort("-name").limit(0).name, [])
        self.assertEqual(Unit.many(name="people").limit(1).name, ["people"])

        Meta("dive", people={"tom", "mary"}, stuff=[1, 2, 3, None], things={"a": {"b": [1, 2], "c": "sure"}, "4": 5, "for": [{"1": "yep"}]}).create()

        model = Meta.many(people={"tom", "mary"})
        self.assertEqual(model[0].name, "dive")

        model = Meta.many(stuff=[1, 2, 3, {"relations.io": {"1": None}}])
        self.assertEqual(model[0].name, "dive")

        model = Meta.many(things={"a": {"b": [1, 2], "c": "sure"}, "4": 5, "for": [{"1": "yep"}]})
        self.assertEqual(model[0].name, "dive")

        model = Meta.many(stuff__1=2)
        self.assertEqual(model[0].name, "dive")

        model = Meta.many(things__a__b__0=1)
        self.assertEqual(model[0].name, "dive")

        model = Meta.many(things__a__c__like="su")
        self.assertEqual(model[0].name, "dive")

        model = Meta.many(things__a__d__null=True)
        self.assertEqual(model[0].name, "dive")

        model = Meta.many(things____4=5)
        self.assertEqual(model[0].name, "dive")

        model = Meta.many(things__a__b__0__gt=1)
        self.assertEqual(len(model), 0)

        model = Meta.many(things__a__c__notlike="su")
        self.assertEqual(len(model), 0)

        model = Meta.many(things__a__d__null=False)
        self.assertEqual(len(model), 0)

        model = Meta.many(things____4=6)
        self.assertEqual(len(model), 0)

        model = Meta.many(things__a__b__has=1)
        self.assertEqual(len(model), 1)

        model = Meta.many(things__a__b__has=3)
        self.assertEqual(len(model), 0)

        model = Meta.many(things__a__b__any=[1, 3])
        self.assertEqual(len(model), 1)

        model = Meta.many(things__a__b__any=[4, 3])
        self.assertEqual(len(model), 0)

        model = Meta.many(things__a__b__all=[2, 1])
        self.assertEqual(len(model), 1)

        model = Meta.many(things__a__b__all=[3, 2, 1])
        self.assertEqual(len(model), 0)

        model = Meta.many(people__has="mary")
        self.assertEqual(len(model), 1)

        model = Meta.many(people__has="dick")
        self.assertEqual(len(model), 0)

        model = Meta.many(people__any=["mary", "dick"])
        self.assertEqual(len(model), 1)

        model = Meta.many(people__any=["harry", "dick"])
        self.assertEqual(len(model), 0)

        model = Meta.many(people__all=["mary", "tom"])
        self.assertEqual(len(model), 1)

        model = Meta.many(people__all=["tom", "dick", "mary"])
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

    def test_titles(self):

        self.source.execute(Unit.define())
        self.source.execute(Test.define())
        self.source.execute(Case.define())
        self.source.execute(Meta.define())
        self.source.execute(Net.define())

        Unit("people").create().test.add("stuff").add("things").create()

        titles = Unit.many().titles()

        self.assertEqual(titles.id, "id")
        self.assertEqual(titles.fields, ["name"])
        self.assertEqual(titles.parents, {})
        self.assertEqual(titles.format, ["fancy"])

        self.assertEqual(titles.ids, [1])
        self.assertEqual(titles.titles,{1: ["people"]})

        titles = Test.many().titles()

        self.assertEqual(titles.id, "id")
        self.assertEqual(titles.fields, ["unit_id", "name"])

        self.assertEqual(titles.parents["unit_id"].id, "id")
        self.assertEqual(titles.parents["unit_id"].fields, ["name"])
        self.assertEqual(titles.parents["unit_id"].parents, {})
        self.assertEqual(titles.parents["unit_id"].format, ["fancy"])

        self.assertEqual(titles.format, ["fancy", "shmancy"])

        self.assertEqual(titles.ids, [1, 2])
        self.assertEqual(titles.titles, {
            1: ["people", "stuff"],
            2: ["people", "things"]
        })

        Net(ip="1.2.3.4", subnet="1.2.3.0/24").create()

        self.assertEqual(Net.many().titles().titles, {
            1: ["1.2.3.4"]
        })

    def test_update_field(self):

        # Standard

        field = relations.Field(int, name="id")
        query = self.source.UPDATE("table")
        self.source.update_field(field, {"id": 1}, query)
        query.generate()
        self.assertEqual(query.sql, """UPDATE "table" SET "id"=%s""")
        self.assertEqual(query.args, [1])

        # Non standard

        field = relations.Field(dict, name="id")
        query = self.source.UPDATE("table")
        self.source.update_field(field, {"id": {"a": 1}}, query)
        query.generate()
        self.assertEqual(query.sql, """UPDATE "table" SET "id"=(%s)::JSONB""")
        self.assertEqual(query.args, ['{"a": 1}'])

        # Non existent

        field = relations.Field(dict, name="id")
        query = self.source.UPDATE("table")
        self.source.update_field(field, {}, query)
        query.generate()
        self.assertEqual(query.sql, """UPDATE "table\"""")
        self.assertEqual(query.args, [])

    def test_update_query(self):

        self.source.execute(Unit.define())
        self.source.execute(Test.define())
        self.source.execute(Case.define())
        self.source.execute(Plain.define())

        query = Unit.many().set(name="fun").query("update")
        query.generate()

        self.assertEqual(query.sql, """UPDATE "test_source"."unit" SET "name"=%s""")
        self.assertEqual(query.args, ["fun"])

        model = Unit([["people"], ["stuff"]]).create()

        query = model[0].set(name="fun").query()
        query.generate()

        self.assertEqual(query.sql, """UPDATE "test_source"."unit" SET "name"=%s WHERE "id"=%s""")
        self.assertEqual(query.args, ["fun", model[0].id])

        self.assertRaisesRegex(relations.ModelError, "only one update query at a time", model.query)

        model = Plain(name="yep").create()

        self.assertRaisesRegex(relations.ModelError, "nothing to update from", model.query)

    def test_update(self):

        self.source.execute(Unit.define())
        self.source.execute(Test.define())
        self.source.execute(Case.define())
        self.source.execute(Meta.define())
        self.source.execute(Net.define())

        Unit([["people"], ["stuff"]]).create()

        unit = Unit.many(id=2).set(name="things")

        self.assertEqual(unit.update(), 1)

        unit = Unit.one(2)

        unit.name = "thing"
        unit.test.add("moar")

        self.assertEqual(unit.update(), 1)
        self.assertEqual(unit.name, "thing")
        self.assertEqual(unit.test[0].id, 1)
        self.assertEqual(unit.test[0].name, "moar")

        Meta("yep", True, 1.1, {"tom"}, [1, None], {"a": 1}).create()
        Meta.one(name="yep").set(flag=False, people=set(), stuff=[], things={}).update()

        model = Meta.one(name="yep")
        self.assertEqual(model.flag, False)
        self.assertEqual(model.spend, 1.1)
        self.assertEqual(model.people, set())
        self.assertEqual(model.stuff, [])
        self.assertEqual(model.things, {})

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

    def test_delete_query(self):

        self.source.execute(Unit.define())
        self.source.execute(Test.define())
        self.source.execute(Case.define())
        self.source.execute(Plain.define())

        query = Unit.many().query("delete")
        query.generate()

        self.assertEqual(query.sql, """DELETE FROM "test_source"."unit\"""")
        self.assertEqual(query.args, [])

        model = Unit([["people"], ["stuff"]]).create()

        query = model.query("delete")
        query.generate()

        self.assertEqual(query.sql, """DELETE FROM "test_source"."unit" WHERE "id" IN (%s,%s)""")
        self.assertEqual(query.args, model.id)

        model = Plain(name="yep").create()

        self.assertRaisesRegex(relations.ModelError, "nothing to delete from", model.query, "delete")

    def test_delete(self):

        self.source.execute(Unit.define())
        self.source.execute(Test.define())
        self.source.execute(Case.define())
        self.source.execute(Plain.define())

        unit = Unit("people")
        unit.test.add("stuff").add("things")
        unit.create()

        self.assertEqual(Test.one(id=2).delete(), 1)
        self.assertEqual(len(Test.many()), 1)

        self.assertEqual(Unit.one(1).test.delete(), 1)
        self.assertEqual(Unit.one(1).retrieve().delete(), 1)
        self.assertEqual(len(Unit.many()), 0)
        self.assertEqual(len(Test.many()), 0)

        self.assertEqual(Test.many().delete(), 0)

        plain = Plain(0, "nope").create()
        self.assertRaisesRegex(relations.ModelError, "plain: nothing to delete from", plain.delete)

    def test_definition(self):

        with open("ddl/general.json", 'w') as ddl_file:
            json.dump({
                "simple": Simple.thy().define(),
                "plain": Plain.thy().define()
            }, ddl_file)

        os.makedirs("ddl/sourced", exist_ok=True)

        self.source.definition("ddl/general.json", "ddl/sourced")

        with open("ddl/sourced/general.sql", 'r') as ddl_file:
            self.assertEqual(ddl_file.read(),
"""CREATE TABLE IF NOT EXISTS "test_source"."plain" (
  "simple_id" INT8,
  "name" VARCHAR(255) NOT NULL
);

CREATE UNIQUE INDEX "plain_simple_id_name" ON "test_source"."plain" ("simple_id","name");

CREATE TABLE IF NOT EXISTS "test_source"."simple" (
  "id" BIGSERIAL,
  "name" VARCHAR(255) NOT NULL,
  PRIMARY KEY ("id")
);

CREATE UNIQUE INDEX "simple_name" ON "test_source"."simple" ("name");
""")

    def test_migration(self):

        with open("ddl/general.json", 'w') as ddl_file:
            json.dump({
                "add": {"simple": Simple.thy().define()},
                "remove": {"simple": Simple.thy().define()},
                "change": {
                    "simple": {
                        "definition": Simple.thy().define(),
                        "migration": {
                            "source": "PyMySQLSource",
                            "store": "simples"
                        }
                    }
                }
            }, ddl_file)

        os.makedirs("ddl/sourced", exist_ok=True)

        self.source.migration("ddl/general.json", "ddl/sourced")

        with open("ddl/sourced/general.sql", 'r') as ddl_file:
            self.assertEqual(ddl_file.read(),
"""CREATE TABLE IF NOT EXISTS "test_source"."simple" (
  "id" BIGSERIAL,
  "name" VARCHAR(255) NOT NULL,
  PRIMARY KEY ("id")
);

CREATE UNIQUE INDEX "simple_name" ON "test_source"."simple" ("name");

DROP TABLE IF EXISTS "test_source"."simple";

ALTER TABLE "test_source"."simple" RENAME TO "simples";
""")

    def test_load(self):

        self.source.ids = {}
        self.source.data = {}

        migrations = relations.Migrations()

        migrations.generate([Unit])
        migrations.convert(self.source.name)

        self.source.load(f"ddl/{self.source.name}/{self.source.KIND}/definition.sql")

        cursor = self.source.connection.cursor()

        cursor.execute("""SELECT COUNT(*) as "total" FROM "test_source"."unit\"""")

        self.assertEqual(cursor.fetchone()["total"], 0)

    def test_list(self):

        os.makedirs(f"ddl/{self.source.name}/{self.source.KIND}")

        pathlib.Path(f"ddl/{self.source.name}/{self.source.KIND}/definition.json").touch()
        pathlib.Path(f"ddl/{self.source.name}/{self.source.KIND}/definition-2012-07-07.sql").touch()
        pathlib.Path(f"ddl/{self.source.name}/{self.source.KIND}/migration-2012-07-07.sql").touch()
        pathlib.Path(f"ddl/{self.source.name}/{self.source.KIND}/definition-2012-07-08.sql").touch()
        pathlib.Path(f"ddl/{self.source.name}/{self.source.KIND}/migration-2012-07-08.sql").touch()

        self.assertEqual(self.source.list(f"ddl/{self.source.name}/{self.source.KIND}"), {
            "2012-07-07": {
                "definition": "definition-2012-07-07.sql",
                "migration": "migration-2012-07-07.sql"
            },
            "2012-07-08": {
                "definition": "definition-2012-07-08.sql",
                "migration": "migration-2012-07-08.sql"
            }
        })

    def test_migrate(self):

        migrations = relations.Migrations()

        migrations.generate([Unit])
        migrations.generate([Unit, Test])
        migrations.convert(self.source.name)

        self.assertTrue(self.source.migrate(f"ddl/{self.source.name}/{self.source.KIND}"))

        self.assertEqual(Unit.many().count(), 0)
        self.assertEqual(Test.many().count(), 0)

        self.assertFalse(self.source.migrate(f"ddl/{self.source.name}/{self.source.KIND}"))

        migrations.generate([Unit, Test, Case])
        migrations.convert(self.source.name)

        self.assertTrue(self.source.migrate(f"ddl/{self.source.name}/{self.source.KIND}"))

        self.assertEqual(Case.many().count(), 0)

        self.assertFalse(self.source.migrate(f"ddl/{self.source.name}/{self.source.KIND}"))
