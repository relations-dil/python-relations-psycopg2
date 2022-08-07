# relations-psycopg2

DB Modeling for PostgreSQL using the psycopg2 library

Relations overall is designed to be a simple, straight forward, flexible DIL (data interface layer).

Quite different from other DIL's, it has the singular, microservice based purpose to:
- Create models with very little code, independent of backends
- Create CRUD API with a database backend from those models with very little code
- Create microservices to use those same models but with that CRUD API as the backend

Ya, that last one is kinda new I guess.

Say we create a service, composed of microservices, which in turn is to be consumed by other services made of microservices.

You should only need to define the model once. Your conceptual structure is the same, to the DB, the API, and anything using that API. You shouldn't have say that structure over and over. You shouldn't have to define CRUD endpoints over and over. That's so boring, tedious, and unnecessary.

Furthermore, the conceptual structure is based not the backend of what you've going to use at that moment of time (scaling matters) but on the relations, how the pieces interact. If you know the structure of the data, that's all you need to interact with the data.

So with Relations, Models and Fields are defined independent of any backend, which instead is set at runtime. So the API will use a DB, everything else will use that API.

This is just the PostgreSQL backend of models and what not.

Don't have great docs yet so I've included some of the unittests to show what's possible.

# Example

## define

```python

import relations
import relations_psycopg2

# The source is a string, the backend of which is defined at runtime

class SourceModel(relations.Model):
    SOURCE = "PsycoPg2Source"

class Simple(SourceModel):
    id = int
    name = str

class Plain(SourceModel):
    ID = None # This table has no primary id field
    simple_id = int
    name = str

# This makes Simple a parent of Plain

relations.OneToMany(Simple, Plain)

class Meta(SourceModel):
    id = int
    name = str
    flag = bool
    spend = float
    people = set # JSON storage
    stuff = list # JSON stroage
    things = dict, {"extract": "for__0____1"} # Extracts things["for"][0][-1] as a virtual column
    push = str, {"inject": "stuff___1__relations.io____1"} # Injects this value into stuff[-1]["relations.io"]["1"]

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
    ip = ipaddress.IPv4Address, { # The field type is that of a class, with the storage being JSON
        "attr": {
            "compressed": "address", # Storge compressed attr as address key in JSON
            "__int__": "value"       # Storge int() as value key in JSON
        },
        "init": "address",           # Initilize with address from JSON
        "titles": "address",         # Use address from JSON as the how to list this field
        "extract": {
            "address": str,          # Extract address as virtual column
            "value": int             # Extra value as virtual column
        }
    }
    subnet = ipaddress.IPv4Network, {
        "attr": subnet_attr,
        "init": "address",
        "titles": "address"
    }

    TITLES = "ip__address" # When listing, use ip["address"] as display value
    INDEX = "ip__value"    # Create an index on the virtual column ip __value

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

# With this statement, all the above models now how this PostgreSQL database as a backend

self.source = relations_psycopg2.Source(
    "PsycoPg2Source", "test_source", schema="test_source", user="postgres", pass="passwd")
)

# Simple.thy().define() = model definition independent of source
# self.source.define(Simple.thy().define()) = model definition for source (SQL in this case)

self.assertEqual(self.source.define(Simple.thy().define()),
"""CREATE TABLE IF NOT EXISTS "test_source"."simple" (
  "id" BIGSERIAL,
  "name" VARCHAR(255) NOT NULL,
  PRIMARY KEY ("id")
);

CREATE UNIQUE INDEX "simple_name" ON "test_source"."simple" ("name");
""")

# Create the schema

self.source.connection.cursor().execute('CREATE SCHEMA "test_source"')

# Create tables in database from models

self.source.execute(Unit.define())
self.source.execute(Test.define())
self.source.execute(Case.define())
self.source.execute(Meta.define())
self.source.execute(Net.define())
```

## create

```python
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
```

## retrieve

```python
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
```

## update

```python
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
```

## delete

```python
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
```
