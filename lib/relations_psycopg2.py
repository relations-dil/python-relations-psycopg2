"""
Module for intersting with PyMySQL
"""

# pylint: disable=arguments-differ

import glob
import copy
import json

import psycopg2
import psycopg2.extras

import relations
import relations.query

class Source(relations.Source): # pylint: disable=too-many-public-methods
    """
    PsycoPg2 Source
    """

    KIND = "postgresql"

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

    def table_names(self, model):
        """
        Gets the base table and names for schemas
        """

        if isinstance(model, dict):
            schema = model.get("schema")
            table = model['table']
        else:
            schema = model.SCHEMA
            table = model.TABLE

        names = []

        if schema is not None:
            names.append(f'"{schema}"')
        elif self.schema is not None:
            names.append(f'"{self.schema}"')

        return table, names

    def table(self, model):
        """
        Get the full table name
        """

        table, names = self.table_names(model)

        names.append(f'"{table}"')

        return ".".join(names)

    def index(self, model, index, full=True):
        """
        Get the full index name
        """

        table, names = self.table_names(model)

        name = f'"{table}_{index.replace("-", "_")}"'

        if not full:
            return name

        names.append(name)

        return ".".join(names)

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

        model.UNDEFINE.append("QUERY")

        if model.TABLE is None:
            model.TABLE = model.NAME

        if model.QUERY is None:
            model.QUERY = relations.query.Query(selects='*', froms=self.table(model))

        if model._id is not None:

            if model._fields._names[model._id].primary_key is None:
                model._fields._names[model._id].primary_key = True

            if model._fields._names[model._id].serial is None:
                model._fields._names[model._id].serial = True
                model._fields._names[model._id].auto = True

    @staticmethod
    def column_define(field): # pylint: disable=too-many-branches
        """
        Defines just the column for field
        """

        if field.get('definition') is not None:
            return field['definition']

        definition = [f'"{field["store"]}"']

        default = None

        if field["kind"] == 'bool':

            definition.append("BOOLEAN")

            if field.get('default') is not None:
                default = f"DEFAULT {field['default']}"

        elif field["kind"] == 'int':

            if field.get("serial"):
                definition.append("SERIAL")
            else:
                definition.append("INT")

            if field.get('default') is not None:
                default = f"DEFAULT {field['default']}"

        elif field["kind"] == 'float':

            definition.append("FLOAT")

            if field.get('default') is not None:
                default = f"DEFAULT {field['default']}"

        elif field["kind"] == 'str':

            length = field.get("length", 255)

            definition.append(f"VARCHAR({length})")

            if field.get('default') is not None:
                default = f"DEFAULT '{field['default']}'"

        else:

            definition.append("JSONB")

            if field["kind"] == 'list':
                default = f"DEFAULT '{json.dumps(field.get('default', []))}'"
            elif field["kind"] == 'dict':
                default = f"DEFAULT '{json.dumps(field.get('default', {}))}'"

        if not field["none"]:
            definition.append("NOT NULL")

        if field.get("primary_key"):
            definition.append("PRIMARY KEY")

        if default:
            definition.append(default)

        return " ".join(definition)

    def extract_define(self, store, path, kind):
        """
        Createa and extract store
        """

        definition = [f'"{store}__{path}"']

        if kind == 'bool':
            cast = "BOOLEAN"
        elif kind == 'int':
            cast = "INT"
        elif kind == 'float':
            cast = "FLOAT"
        elif kind == 'str':
            cast = "VARCHAR(255)"
        else:
            cast = "JSONB"

        definition.append(cast)

        definition.append(f'GENERATED ALWAYS AS (("{store}"#>>\'{self.walk(path)}\')::{cast}) STORED')

        return " ".join(definition)

    def field_define(self, field, definitions, extract=True): # pylint: disable=too-many-branches,too-many-statements
        """
        Add what this field is the definition
        """

        if field.get('inject'):
            return

        definitions.append(self.column_define(field))

        if extract:
            for path in sorted(field.get('extract', {}).keys()):
                definitions.append(self.extract_define(field['store'], path, field['extract'][path]))

    def index_define(self, model, name, fields, unique=False):
        """
        Defines an index
        """

        kind = "UNIQUE INDEX" if unique else "INDEX"
        index = self.index(model, name, full=False)
        table = self.table(model)
        fields = '","'.join(fields)

        return f'CREATE {kind} {index} ON {table} ("{fields}")'

    def model_define(self, model):

        if model.get('definition') is not None:
            return [model['definition']]

        definitions = []

        self.record_define(model['fields'], definitions)

        sep = ',\n  '

        statements = [
            f"CREATE TABLE IF NOT EXISTS {self.table(model)} (\n  {sep.join(definitions)}\n)"
        ]

        for unique in sorted(model['unique'].keys()):
            statements.append(self.index_define(model, unique, model['unique'][unique], unique=True))

        for index in sorted(model['index'].keys()):
            statements.append(self.index_define(model, index, model['index'][index]))

        return statements

    def field_add(self, migration, migrations):
        """
        add the field
        """

        if migration.get('inject'):
            return

        migrations.append(f"ADD {self.column_define(migration)}")

        for path in sorted(migration.get('extract', {}).keys()):
            migrations.append(f"ADD {self.extract_define(migration['store'], path, migration['extract'][path])}")

    def field_remove(self, definition, migrations):
        """
        remove the field
        """

        if definition.get('inject'):
            return

        migrations.append(f'DROP "{definition["store"]}"')

        for store in sorted(definition.get('extract', {}).keys()):
            migrations.append(f'DROP "{definition["store"]}__{store}"')

    @staticmethod
    def column_change(store, definition, migration): # pylint: disable=too-many-branches
        """
        Creates a list of column changes to that all can be applied at once
        """

        migrations = []

        if "kind" in migration:

            if migration["kind"] == 'bool':
                cast = "BOOLEAN"
            elif migration["kind"] == 'int':
                cast = "INT"
            elif migration["kind"] == 'float':
                cast = "FLOAT"
            elif migration["kind"] == 'str':
                length = migration.get("length", 255)
                cast = f"VARCHAR({length})"
            else:
                cast = "JSONB"

            migrations.append(f'ALTER "{store}" TYPE {cast} USING "{store}"::{cast}')

            if migration['kind'] in ['list', 'dict'] and "default" not in migration:
                default = json.dumps([] if migration['kind'] == 'list' else {})
                migrations.append(f'ALTER "{store}" SET DEFAULT \'{default}\'')

        kind = migration.get('kind', definition['kind'])

        if "default" in migration:

            if migration["default"] is not None:

                if kind in ['bool', 'int', 'float']:
                    default = migration["default"]
                elif kind == 'str':
                    default = f"'{migration['default']}'"
                else:
                    default = f"'{json.dumps(migration['default'])}'"

                migrations.append(f'ALTER "{store}" SET DEFAULT {default}')

            else:

                migrations.append(f'ALTER "{store}" DROP DEFAULT')

        if "none" in migration:

            if migration["none"]:
                migrations.append(f'ALTER "{store}" DROP NOT NULL')
            else:
                migrations.append(f'ALTER "{store}" SET NOT NULL')

        return migrations

    def field_change(self, definition, migration, migrations): # pylint: disable=too-many-branches
        """
        change the field
        """

        if definition.get('inject'):
            return

        store = migration.get('store', definition['store'])
        extract = migration.get('extract', definition.get('extract', {}))

        if definition['store'] != store:
            migrations.append(f'RENAME "{definition["store"]}" TO "{store}"')

        column = self.column_change(store, definition, migration)

        if column:
            migrations.append(f'{" ".join(column)}')

        # Remove all the ones that were there and now aren't

        for path in sorted(definition.get('extract', {}).keys()):
            if path not in extract:
                migrations.append(f'DROP "{definition["store"]}__{path}"')

        for path in sorted(extract.keys()):

            # Add the ones that are new

            if path not in definition.get("extract"):

                migrations.append(f'ADD {self.extract_define(store, path, migration["extract"][path])}')

            else:

                # if the field name changed, rename

                if definition['store'] != store:

                    migrations.append(f'RENAME "{definition["store"]}__{path}" "{store}__{path}"')

                # If the kind changed, recast

                if definition["extract"][path] != extract[path]:

                    if extract[path] == 'bool':
                        cast = "BOOLEAN"
                    elif extract[path] == 'int':
                        cast = "INT"
                    elif extract[path] == 'float':
                        cast = "FLOAT"
                    elif extract[path] == 'str':
                        cast = "VARCHAR(255)"
                    else:
                        cast = "JSONB"

                    migrations.append(f'ALTER "{store}__{path}" TYPE {cast} USING "{store}__{path}"::{cast}')

    def model_add(self, definition):
        """
        migrate the model
        """

        return self.model_define(definition)

    def model_remove(self, definition):
        """
        remove the model
        """

        return [f"DROP TABLE IF EXISTS {self.table(definition)}"]

    def model_change(self, definition, migration):
        """
        change the model
        """

        migrations = []

        schema = migration.get("schema", definition.get("schema"))
        table = migration.get("table", definition["table"])
        model = {**definition, **migration}

        if definition.get('schema') != schema:
            migrations.append(f'ALTER TABLE {self.table(definition)} SET SCHEMA "{schema or "public"}"')

        if definition["table"] != table:

            definition_table = self.table({**definition, "schema": schema})
            migration_table = self.table(model)

            migrations.append(f"ALTER TABLE {definition_table} RENAME TO {migration_table}")

            for name in sorted(definition["unique"].keys()):

                definition_index = self.index({**definition, "schema": schema}, name)
                migration_index = self.index(model, name)

                migrations.append(f"ALTER INDEX {definition_index} RENAME TO {migration_index}")

            for name in sorted(definition["index"].keys()):

                definition_index = self.index({**definition, "schema": schema}, name)
                migration_index = self.index(model, name)

                migrations.append(f"ALTER INDEX {definition_index} RENAME TO {migration_index}")

        for name in sorted(migration.get("unique", {}).get("remove", [])):
            migrations.append(f'DROP INDEX {self.index(model, name)}')

        for name in sorted(migration.get("index", {}).get("remove", [])):
            migrations.append(f'DROP INDEX {self.index(model, name)}')

        fields = []

        self.record_change(definition['fields'], migration.get("fields", {}), fields)

        migrations.extend([f'ALTER TABLE {self.table(model)} {field}' for field in fields])

        for name in sorted(migration.get("unique", {}).get("add", {}).keys()):
            migrations.append(self.index_define(model, name, migration['unique']['add'][name], unique=True))

        for name in sorted(migration.get("unique", {}).get("rename", {}).keys()):
            migrations.append(f'ALTER INDEX {self.index(model, name)} RENAME TO {self.index(model, migration["unique"]["rename"][name])}')

        for name in sorted(migration.get("index", {}).get("add", {}).keys()):
            migrations.append(self.index_define(model, name, migration['index']['add'][name]))

        for name in sorted(migration.get("index", {}).get("rename", {}).keys()):
            migrations.append(f'ALTER INDEX {self.index(model, name)} RENAME TO {self.index(model, migration["index"]["rename"][name])}')

        return migrations

    def field_create(self, field, fields, clause):
        """
        Adds values to clause if not auto
        """

        if not field.auto and not field.inject:
            fields.append(f'"{field.store}"')
            clause.append(f"%({field.store})s")

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
                cursor.execute(query, self.encode(creating, creating._record.create({})))
                creating[model._id] = cursor.fetchone()[store]
        else:

            query = f'INSERT INTO {self.table(model)} ({",".join(fields)}) VALUES %s'

            psycopg2.extras.execute_values(cursor, query, [
                self.encode(creating, creating._record.create({})) for creating in model._each("create")
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

                path, operator = operator.rsplit("__", 1)

                if path in (field.extract or {}):

                    store = store = f'"{field.store}__{path}"'

                else:

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
                if value:
                    query.add(wheres=f'{store} IN ({",".join(["%s" for _ in value])})')
                    values.extend(value)
                else:
                    query.add(wheres='FALSE')
            elif operator == "ne":
                if value:
                    query.add(wheres=f'{store} NOT IN ({",".join(["%s" for _ in value])})')
                    values.extend(value)
                else:
                    query.add(wheres='TRUE')
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
                    if parent[relation.parent_field]:
                        ors.append(f'"{field.store}" IN ({",".join(["%s" for _ in parent[relation.parent_field]])})')
                        values.extend(parent[relation.parent_field])
                        model.overflow = model.overflow or parent.overflow
                    else:
                        parent = True

            if not parent:

                paths = path if path else field.label

                if paths:

                    for path in paths:

                        if path in (field.extract or {}):

                            ors.append(f'"{field.store}__{path}"::VARCHAR(255) ILIKE %s')
                            values.append(f"%{model._like}%")

                        else:

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

    def model_count(self, model):
        """
        Executes the count
        """

        model._collate()

        cursor = self.connection.cursor()

        query = copy.deepcopy(model.QUERY)
        query.set(selects="COUNT(*) AS total")

        values = []

        self.record_retrieve(model._record, query, values)

        self.model_like(model, query, values)

        cursor.execute(query.get(), values)

        total = cursor.fetchone()["total"] if cursor.rowcount else 0

        cursor.close()

        return total

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

    def field_update(self, field, updates, clause, values):
        """
        Preps values from dict
        """

        if field.store in updates:
            clause.append(f'"{field.store}"=%s')
            if field.kind not in [bool, int, float, str] and updates[field.store] is not None:
                values.append(json.dumps(updates[field.store]))
            else:
                values.append(updates[field.store])

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

            self.record_update(model._record, model._record.mass({}), clause, values)

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

                self.record_update(updating._record, updating._record.update({}), clause, values)

                if clause:

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

    def definition_convert(self, file_path, source_path):
        """"
        Converts a definition file to a MySQL definition file
        """

        definitions = []

        with open(file_path, "r") as definition_file:
            definition = json.load(definition_file)
            for name in sorted(definition.keys()):
                if definition[name]["source"] == self.name:
                    definitions.extend(self.model_define(definition[name]))

        if definitions:
            file_name = file_path.split("/")[-1].split('.')[0]
            with open(f"{source_path}/{file_name}.sql", "w") as source_file:
                source_file.write(";\n\n".join(definitions))
                source_file.write(";\n")

    def migration_convert(self, file_path, source_path):
        """"
        Converts a migration file to a source definition file
        """

        migrations = []

        with open(file_path, "r") as migration_file:
            migration = json.load(migration_file)

            for add in sorted(migration.get('add', {}).keys()):
                if migration['add'][add]["source"] == self.name:
                    migrations.extend(self.model_add(migration['add'][add]))

            for remove in sorted(migration.get('remove', {}).keys()):
                if migration['remove'][remove]["source"] == self.name:
                    migrations.extend(self.model_remove(migration['remove'][remove]))

            for change in sorted(migration.get('change', {}).keys()):
                if migration['change'][change]['definition']["source"] == self.name:
                    migrations.extend(
                        self.model_change(migration['change'][change]['definition'], migration['change'][change]['migration'])
                    )

        if migrations:
            file_name = file_path.split("/")[-1].split('.')[0]
            with open(f"{source_path}/{file_name}.sql", "w") as source_file:
                source_file.write(";\n\n".join(migrations))
                source_file.write(";\n")

    def execute(self, commands):
        """
        Execute one or more commands
        """

        if not isinstance(commands, list):
            commands = [commands]

        cursor = self.connection.cursor()

        for command in commands:
            if command.strip():
                cursor.execute(command)

        self.connection.commit()

        cursor.close()

    def migrate(self, source_path):
        """
        Migrate all the existing files to where we are
        """

        migrated = False

        cursor = self.connection.cursor()

        cursor.execute("""
            SELECT COUNT(*) AS "migrations"
            FROM information_schema.tables
            WHERE table_schema = %s AND table_name = %s
            LIMIT 1
        """, (self.schema or "public", "_relations_migrations"))

        migrations = cursor.fetchone()['migrations']

        migration_paths = sorted(glob.glob(f"{source_path}/migration-*.sql"))

        table = self.table({"table": "_relations_migrations"})

        if not migrations:

            cursor.execute(f"""
                CREATE TABLE {table} (
                    "migration" VARCHAR(255) NOT NULL,
                    PRIMARY KEY ("migration")
                );
            """)

            with open(f"{source_path}/definition.sql", 'r') as definition_file:
                self.execute(definition_file.read().split(";\n"))
                migrated = True

        else:

            cursor.execute(f'SELECT "migration" FROM {table} ORDER BY "migration"')

            migrations = [row['migration'] for row in cursor.fetchall()]

            for migration_path in migration_paths:
                if migration_path.rsplit("/migration-", 1)[-1].split('.')[0] not in migrations:
                    with open(migration_path, 'r') as migration_file:
                        self.execute(migration_file.read().split(";\n"))
                    migrated = True

        for migration_path in migration_paths:
            migration = migration_path.rsplit("/migration-", 1)[-1].split('.')[0]
            if not migrations or migration not in migrations:
                cursor.execute(f'INSERT INTO {table} VALUES (%s)', (migration, ))

        self.connection.commit()

        return migrated
