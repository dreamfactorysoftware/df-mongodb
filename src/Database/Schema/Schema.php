<?php
namespace DreamFactory\Core\MongoDb\Database\Schema;

use DreamFactory\Core\Database\Schema\ColumnSchema;
use DreamFactory\Core\Database\Schema\TableSchema;
use Jenssegers\Mongodb\Connection;
use MongoDB\Model\CollectionInfo;

/**
 * Schema is the class for retrieving metadata information from a MongoDB database (version 4.1.x and 5.x).
 */
class Schema extends \DreamFactory\Core\Database\Schema\Schema
{
    /**
     * @var Connection
     */
    protected $connection;

    /**
     * @return boolean
     */
    public function supportsFunctions()
    {
        return false;
    }

    /**
     * @return boolean
     */
    public function supportsProcedures()
    {
        return false;
    }

    /**
     * @inheritdoc
     */
    protected function loadTable(TableSchema $table)
    {
        if (null !== $template = $this->connection->table($table->name)->first()) {
            foreach ($template as $name => $value) {
                $c = new ColumnSchema(['name' => $name, 'allowNull' => true, 'type' => gettype($value)]);
                if ('_id' === $name) {
                    $c->isPrimaryKey = true;
                    $c->autoIncrement = true;
                    $c->allowNull = false;
                }
                $table->addColumn($c);
            }
        } else {
            $c =
                new ColumnSchema([
                    'name'          => '_id',
                    'type'          => 'string',
                    'isPrimaryKey'  => true,
                    'autoIncrement' => true
                ]);
            $table->addColumn($c);
        }

        return $table;
    }

    /**
     * Returns all table names in the database.
     *
     * @param string $schema the schema of the tables. Defaults to empty string, meaning the current or default schema.
     *                       If not empty, the returned table names will be prefixed with the schema name.
     * @param bool   $include_views
     *
     * @return array all table names in the database.
     */
    protected function findTableNames($schema = '', $include_views = true)
    {
        $tables = [];
        /** @type \MongoDB\Database $db */
        /** @noinspection PhpUndefinedMethodInspection */
        $db = $this->connection->getMongoDB();
        $schema = $db->getDatabaseName(); // only one supported currently
        /** @type CollectionInfo[] $collections */
        $collections = $db->listCollections();
        foreach ($collections as $collection) {
            $name = $collection->getName();
            $tables[strtolower($name)] = new TableSchema([
                'schemaName' => $schema,
                'tableName'  => $name,
                'name'       => $name,
                'primaryKey' => '_id',
            ]);
        }

        return $tables;
    }

    public function createTable($table, $schema, $options = null)
    {
        if (empty($schema['field'])) {
            throw new \Exception("No valid fields exist in the received table schema.");
        }

        $results = $this->buildTableFields($table, $schema['field']);
        if (empty($results['columns'])) {
            throw new \Exception("No valid fields exist in the received table schema.");
        }

        $cols = [];
        foreach ($results['columns'] as $name => $type) {
            if (is_string($name)) {
                $cols[] = "\t" . $this->quoteColumnName($name) . ' ' . $this->getColumnType($type);
            } else {
                $cols[] = "\t" . $type;
            }
        }

        if (!is_array($options)){
            $options = [];
        }
        $this->connection->getMongoDB()->createCollection($table, $options);

        return $results;
    }

    /**
     * @param string $table_name
     * @param array  $schema
     * @param bool   $allow_delete
     *
     * @throws \Exception
     * @return array
     */
    protected function updateTable($table_name, $schema, $allow_delete = false)
    {
        if (empty($table_name)) {
            throw new \Exception("Table schema received does not have a valid name.");
        }

        // does it already exist
        if (!$this->doesTableExist($table_name)) {
            throw new \Exception("Update schema called on a table with name '$table_name' that does not exist in the database.");
        }

        //  Is there a name update
        if (!empty($schema['new_name'])) {
            // todo change table name, has issue with references
        }

        $oldSchema = $this->getTable($table_name);

        // update column types

        $results = [];
        if (!empty($schema['field'])) {
            $results =
                $this->buildTableFields($table_name, $schema['field'], $oldSchema, true, $allow_delete);
            if (isset($results['columns']) && is_array($results['columns'])) {
                foreach ($results['columns'] as $name => $definition) {
//                    $this->connection->statement($this->addColumn($table_name, $name, $definition));
                }
            }
            if (isset($results['alter_columns']) && is_array($results['alter_columns'])) {
                foreach ($results['alter_columns'] as $name => $definition) {
//                    $this->connection->statement($this->alterColumn($table_name, $name, $definition));
                }
            }
            if (isset($results['drop_columns']) && is_array($results['drop_columns'])) {
                foreach ($results['drop_columns'] as $name) {
//                    $this->connection->statement($this->dropColumn($table_name, $name));
                }
            }
        }

        return $results;
    }

    /**
     * Builds and executes a SQL statement for dropping a DB table.
     *
     * @param string $table the table to be dropped. The name will be properly quoted by the method.
     *
     * @return integer 0 is always returned. See {@link http://php.net/manual/en/pdostatement.rowcount.php} for more
     *                 information.
     */
    public function dropTable($table)
    {
        $this->connection->getCollection($table)->drop();
        $this->removeSchemaExtrasForTables($table);

        //  Any changes here should refresh cached schema
        $this->refresh();

        return ['name' => $table];
    }

    public function dropColumn($table, $column)
    {
        $result = 0;
        $tableInfo = $this->getTable($table);
        if (($columnInfo = $tableInfo->getColumn($column)) && (ColumnSchema::TYPE_VIRTUAL !== $columnInfo->type)) {
        }
        $this->removeSchemaExtrasForFields($table, $column);

        //  Any changes here should refresh cached schema
        $this->refresh();

        return $result;
    }

    /**
     * @param string $table_name
     * @param array  $fields
     * @param bool   $allow_update
     * @param bool   $allow_delete
     *
     * @return array
     * @throws \Exception
     */
    public function updateFields($table_name, $fields, $allow_update = false, $allow_delete = false)
    {
        if (empty($table_name)) {
            throw new \Exception("Table schema received does not have a valid name.");
        }

        // does it already exist
        if (!$this->doesTableExist($table_name)) {
            throw new \Exception("Update schema called on a table with name '$table_name' that does not exist in the database.");
        }

        $oldSchema = $this->getTable($table_name);

        $names = [];
        $results = $this->buildTableFields($table_name, $fields, $oldSchema, $allow_update, $allow_delete);
        if (isset($results['columns']) && is_array($results['columns'])) {
            foreach ($results['columns'] as $name => $definition) {
//                $this->connection->statement($this->addColumn($table_name, $name, $definition));
                $names[] = $name;
            }
        }
        if (isset($results['alter_columns']) && is_array($results['alter_columns'])) {
            foreach ($results['alter_columns'] as $name => $definition) {
//                $this->connection->statement($this->alterColumn($table_name, $name, $definition));
                $names[] = $name;
            }
        }
        if (isset($results['drop_columns']) && is_array($results['drop_columns'])) {
            foreach ($results['drop_columns'] as $name) {
//                $this->connection->statement($this->dropColumn($table_name, $name));
                $names[] = $name;
            }
        }

        $references = (isset($results['references'])) ? $results['references'] : [];
        $this->createFieldReferences($references);

        $indexes = (isset($results['indexes'])) ? $results['indexes'] : [];
        $this->createFieldIndexes($indexes);

        $extras = (isset($results['extras'])) ? $results['extras'] : [];
        if (!empty($extras)) {
            $this->setSchemaFieldExtras($extras);
        }

        $extras = (isset($results['drop_extras'])) ? $results['drop_extras'] : [];
        if (!empty($extras)) {
            foreach ($extras as $table => $dropFields) {
                $this->removeSchemaExtrasForFields($table, $dropFields);
            }
        }

        return ['names' => $names];
    }

    protected function createFieldReferences($references)
    {
        // Do nothing here for now
    }

    /**
     * @param array $indexes
     *
     * @return array
     */
    protected function createFieldIndexes($indexes)
    {
        // Do nothing here for now
    }

    public function parseValueForSet($value, $field_info)
    {
        switch ($field_info->type) {
            case ColumnSchema::TYPE_BOOLEAN:
                $value = (filter_var($value, FILTER_VALIDATE_BOOLEAN) ? 1 : 0);
                break;
        }

        return $value;
    }
}
