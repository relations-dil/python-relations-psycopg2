"""
Module for intersting with PyMySQL
"""

# pylint: disable=arguments-differ

import copy
import json

import psycopg2
import psycopg2.extras

import relations
import relations.query

class Source(relations.Source):
    """
    PyMySQL Source
    """

    RETRIEVE = {
        'eq': '=',
        'gt': '>',
        'gte': '>=',
        'lt': '<',
        'lte': '<='
    }

    database = None   # Database to use
    schema = None     # Schema to use
    connection = None # Connection
    created = False   # If we created the connection

    def __init__(self, name, database, schema=None, connection=None, **kwargs):

        self.database = database
        self.schema = schema

        if connection is not None:
            self.connection = connection
        else:
            self.created = True
            self.connection = psycopg2.connect(
                dbname=self.database, cursor_factory=psycopg2.extras.RealDictCursor,
                **{name: arg for name, arg in kwargs.items() if name not in ["name", "database", "schema", "connection"]}
            )

    def __del__(self):

        if self.created and self.connection:
            self.connection.close()

    def table(self, model):
        """
        Get the full table name
        """

        table = []

        if model.SCHEMA is not None:
            table.append(f'"{model.SCHEMA}"')
        elif self.schema is not None:
            table.append(f'"{self.schema}"')

        table.append(f'"{model.TABLE}"')

        return ".".join(table)

    @staticmethod
    def encode(model, values):
        """
        Encodes the fields in json if needed
        """
        for field in model._fields._order:
            if values.get(field.store) is not None and field.kind not in [bool, int, float, str]:
                values[field.store] = json.dumps(values[field.store])

        return values

    @staticmethod
    def walk(path):
        """
        Generates the JSON pathing for a field
        """

        if isinstance(path, str):
            path = path.split('__')

        places = []

        for place in path:

            if place[0] == '_':
                places.append(f'"{place[1:]}"')
            else:
                places.append(place)

        return f"{{{','.join(places)}}}"

    def field_init(self, field):
        """
        Make sure there's primary_key
        """

        self.ensure_attribute(field, "primary_key")
        self.ensure_attribute(field, "serial")
        self.ensure_attribute(field, "definition")

    def model_init(self, model):
        """
        Init the model
        """

        self.record_init(model._fields)

        self.ensure_attribute(model, "DATABASE")
        self.ensure_attribute(model, "SCHEMA")
        self.ensure_attribute(model, "TABLE")
        self.ensure_attribute(model, "QUERY")
        self.ensure_attribute(model, "DEFINITION")

        if model.TABLE is None:
            model.TABLE = model.NAME

        if model.QUERY is None:
            model.QUERY = relations.query.Query(selects='*', froms=self.table(model))

        if model._id is not None:

            if model._fields._names[model._id].primary_key is None:
                model._fields._names[model._id].primary_key = True

            if model._fields._names[model._id].serial is None:
                model._fields._names[model._id].serial = True
                model._fields._names[model._id].readonly = True

    def field_define(self, field, definitions, model): # pylint: disable=too-many-branches
        """
        Add what this field is the definition
        """

        if field.inject:
            return

        if field.definition is not None:
            definitions.append(field.definition)
            return

        definition = [f'"{field.store}"']

        default = None

        if field.kind == bool:

            definition.append("BOOLEAN")

            if field.default is not None and not callable(field.default):
                default = f"DEFAULT {field.default}"

        elif field.kind == int:

            if field.serial:
                definition.append("SERIAL")
            else:
                definition.append("INT")

            if field.default is not None and not callable(field.default):
                default = f"DEFAULT {field.default}"

        elif field.kind == float:

            definition.append("FLOAT")

            if field.default is not None and not callable(field.default):
                default = f"DEFAULT {field.default}"

        elif field.kind == str:

            length = field.length if field.length is not None else 255

            definition.append(f"VARCHAR({length})")

            if field.default is not None and not callable(field.default):
                default = f"DEFAULT '{field.default}'"

        else:

            definition.append("JSONB")

            if field.default is not None:
                default = f"DEFAULT '{json.dumps(field.default() if callable(field.default) else field.default) or '{}'}'"

        if not field.none:
            definition.append("NOT NULL")

        if field.primary_key:
            definition.append("PRIMARY KEY")

        if default:
            definition.append(default)

        if field.extract:
            path = field.extract.split('__')
            extracted = model._fields._names[path.pop(0)]

            if field.kind == bool:
                cast = "::BOOLEAN"
            elif field.kind == int:
                cast = "::INT"
            elif field.kind == float:
                cast = "::FLOAT"
            else:
                cast = "::VARCHAR(255)"

            definition.append(f'GENERATED ALWAYS AS (("{extracted.store}"#>>\'{self.walk(path)}\'){cast}) STORED')

        definitions.append(" ".join(definition))

    def model_define(self, cls):

        model = cls.thy()

        if model.DEFINITION is not None:
            return model.DEFINITION

        definitions = []

        self.record_define(model._fields, definitions, model)

        sep = ',\n  '

        statements = [
            f"CREATE TABLE IF NOT EXISTS {self.table(model)} (\n  {sep.join(definitions)}\n)"
        ]

        for unique in model._unique:
            name = f"{model.TABLE}_{unique.replace('-', '_')}"
            fields = '","'.join(model._unique[unique])
            statements.append(f'CREATE UNIQUE INDEX "{name}" ON {self.table(model)} ("{fields}")')

        for index in model._index:
            name = f"{model.TABLE}_{index.replace('-', '_')}"
            fields = '","'.join(model._index[index])
            statements.append(f'CREATE INDEX "{name}" ON {self.table(model)} ("{fields}")')

        return statements

    def field_create(self, field, fields, clause):
        """
        Adds values to clause if not readonly
        """

        if not field.readonly and not field.inject:
            fields.append(f'"{field.store}"')
            clause.append(f"%({field.store})s")
            field.changed = False

    def model_create(self, model):
        """
        Executes the create
        """

        cursor = self.connection.cursor()

        # Create the insert query

        fields = []
        clause = []

        self.record_create(model._fields, fields, clause)

        if not model._bulk and model._id is not None and model._fields._names[model._id].serial:

            store = model._fields._names[model._id].store

            query = f'INSERT INTO {self.table(model)} ({",".join(fields)}) VALUES({",".join(clause)}) RETURNING {store}'

            for creating in model._each("create"):
                cursor.execute(query, self.encode(creating, creating._record.write({})))
                creating[model._id] = cursor.fetchone()[store]
        else:

            query = f'INSERT INTO {self.table(model)} ({",".join(fields)}) VALUES %s'

            psycopg2.extras.execute_values(cursor, query, [
                self.encode(creating, creating._record.write({})) for creating in model._each("create")
            ], f'({",".join(clause)})')

        cursor.close()

        if not model._bulk:

            for creating in model._each("create"):
                for parent_child in creating.CHILDREN:
                    if creating._children.get(parent_child):
                        creating._children[parent_child].create()
                creating._action = "update"
                creating._record._action = "update"

            model._action = "update"

        else:

            model._models = []

        return model

    def field_retrieve(self, field, query, values): # pylint: disable=too-many-branches
        """
        Adds where caluse to query
        """

        for operator, value in (field.criteria or {}).items():

            if operator not in relations.Field.OPERATORS:

                path = operator.split("__")
                operator = path.pop()

                values.append(self.walk(path))

                cast = value[0] if isinstance(value, list) and value else value

                if isinstance(cast, bool):
                    store = f'("{field.store}"#>>%s)::BOOLEAN'
                elif isinstance(cast, int):
                    store = f'("{field.store}"#>>%s)::INT'
                elif isinstance(cast, float):
                    store = f'("{field.store}"#>>%s)::FLOAT'
                else:
                    store = f'("{field.store}"#>>%s)'

            else:

                store = f'"{field.store}"'

            if operator == "in":
                query.add(wheres=f'{store} IN ({",".join(["%s" for _ in value])})')
                values.extend(value)
            elif operator == "ne":
                query.add(wheres=f'{store} NOT IN ({",".join(["%s" for _ in value])})')
                values.extend(value)
            elif operator == "like":
                query.add(wheres=f'{store}::VARCHAR(255) ILIKE %s')
                values.append(f"%{value}%")
            elif operator == "notlike":
                query.add(wheres=f'{store}::VARCHAR(255) NOT ILIKE %s')
                values.append(f"%{value}%")
            elif operator == "null":
                query.add(wheres=f"{store} {'IS' if value else 'IS NOT'} NULL")
            else:
                query.add(wheres=f'{store}{self.RETRIEVE[operator]}%s')
                values.append(value)

    @classmethod
    def model_like(cls, model, query, values):
        """
        Adds like information to the query
        """

        if model._like is None:
            return

        ors = []

        for name in model._label:

            path = name.split("__", 1)
            name = path.pop(0)

            field = model._fields._names[name]

            parent = False

            for relation in model.PARENTS.values():
                if field.name == relation.child_field:
                    parent = relation.Parent.many(like=model._like).limit(model._chunk)
                    ors.append(f'"{field.store}" IN ({",".join(["%s" for _ in parent[relation.parent_field]])})')
                    values.extend(parent[relation.parent_field])
                    model.overflow = model.overflow or parent.overflow

            if not parent:

                paths = [path] if path else field.label

                if paths:

                    for path in paths:

                        ors.append(f'("{field.store}"#>>%s)::VARCHAR(255) ILIKE %s')
                        values.append(cls.walk(path))
                        values.append(f"%{model._like}%")

                else:

                    ors.append(f'"{field.store}"::VARCHAR(255) ILIKE %s')
                    values.append(f"%{model._like}%")

        query.add(wheres="(%s)" % " OR ".join(ors))

    @staticmethod
    def model_sort(model, query):
        """
        Adds sort information to the query
        """

        sort = model._sort or model._order

        if sort:
            order_bys = []
            for field in sort:
                order_bys.append(f'"{field[1:]}"' if field[0] == "+" else f'"{field[1:]}" DESC')
            query.add(order_bys=order_bys)

        model._sort = None

    @staticmethod
    def model_limit(model, query, values):
        """
        Adds sort informaiton to the query
        """

        if model._limit is None:
            return

        if model._offset:
            query.add(limits="%s OFFSET %s")
            values.extend([model._limit, model._offset])
        else:
            query.add(limits="%s")
            values.append(model._limit)

    def model_retrieve(self, model, verify=True):
        """
        Executes the retrieve
        """

        model._collate()

        cursor = self.connection.cursor()

        query = copy.deepcopy(model.QUERY)
        values = []

        self.record_retrieve(model._record, query, values)

        self.model_like(model, query, values)
        self.model_sort(model, query)
        self.model_limit(model, query, values)

        cursor.execute(query.get(), values)

        if model._mode == "one" and cursor.rowcount > 1:
            raise relations.model.ModelError(model, "more than one retrieved")

        if model._mode == "one" and model._role != "child":

            if cursor.rowcount < 1:

                if verify:
                    raise relations.model.ModelError(model, "none retrieved")
                return None

            model._record = model._build("update", _read=cursor.fetchone())

        else:

            model._models = []

            while len(model._models) < cursor.rowcount:
                model._models.append(model.__class__(_read=cursor.fetchone()))

            if model._limit is not None:
                model.overflow = model.overflow or len(model._models) >= model._limit

            model._record = None

        model._action = "update"

        cursor.close()

        return model

    def model_labels(self, model):
        """
        Creates the labels structure
        """

        if model._action == "retrieve":
            self.model_retrieve(model)

        labels = relations.Labels(model)

        for labeling in model._each():
            labels.add(labeling)

        return labels

    def field_update(self, field, clause, values, changed=None):
        """
        Preps values to dict (if not readonly)
        """

        if not field.readonly and not field.inject:
            if field.replace and not field.changed:
                field.value = field.default() if callable(field.default) else field.default
            if changed is None or field.changed == changed:
                clause.append(f'"{field.store}"=%s')
                if field.kind in [list, dict] and field.value is not None:
                    values.append(json.dumps(field.value))
                else:
                    values.append(field.value)
                field.changed = False

    def model_update(self, model):
        """
        Executes the update
        """

        cursor = self.connection.cursor()

        updated = 0

        # If the overall model is retrieving and the record has values set

        if model._action == "retrieve" and model._record._action == "update":

            # Build the SET clause first

            clause = []
            values = []

            self.record_update(model._record, clause, values, changed=True)

            # Build the WHERE clause next

            where = relations.query.Query()
            self.record_retrieve(model._record, where, values)

            query = f"UPDATE {self.table(model)} SET {relations.sql.assign_clause(clause)} {where.get()}"

            cursor.execute(query, values)

            updated = cursor.rowcount

        elif model._id:

            store = model._fields._names[model._id].store

            for updating in model._each("update"):

                clause = []
                values = []

                self.record_update(updating._record, clause, values)

                values.append(updating[model._id])

                query = f'UPDATE {self.table(model)} SET {relations.sql.assign_clause(clause)} WHERE "{store}"=%s'

                cursor.execute(query, values)

                for parent_child in updating.CHILDREN:
                    if updating._children.get(parent_child):
                        updating._children[parent_child].create().update()

                updated += cursor.rowcount

        else:

            raise relations.model.ModelError(model, "nothing to update from")

        return updated

    def model_delete(self, model):
        """
        Executes the delete
        """

        cursor = self.connection.cursor()

        if model._action == "retrieve":

            where = relations.query.Query()
            values = []
            self.record_retrieve(model._record, where, values)

            query = f"DELETE FROM {self.table(model)} {where.get()}"

        elif model._id:

            store = model._fields._names[model._id].store
            values = []

            for deleting in model._each():
                values.append(deleting[model._id])

            query = f'DELETE FROM {self.table(model)} WHERE "{store}" IN ({",".join(["%s"] * len(values))})'

        else:

            raise relations.model.ModelError(model, "nothing to delete from")

        cursor.execute(query, values)

        return cursor.rowcount
