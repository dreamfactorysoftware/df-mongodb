<?php
namespace DreamFactory\Core\MongoDb\Database\Schema;

use DreamFactory\Core\Database\Schema\TableSchema;
use DreamFactory\Core\Enums\DbSimpleTypes;
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
     * @inheritdoc
     */
    protected function findColumns(TableSchema $table)
    {
        $columns = [
            [
                'name'           => '_id',
                'db_type'        => 'string',
                'is_primary_key' => true,
                'auto_increment' => true,
            ]
        ];

        return $columns;
    }

    /**
     * @inheritdoc
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
            ]);
        }

        return $tables;
    }

    /**
     * @inheritdoc
     */
    public function createTable($table, $schema, $options = null)
    {
        if (!is_array($options)) {
            $options = [];
        }

        return $this->connection->getMongoDB()->createCollection($table, $options);
    }

    /**
     * @inheritdoc
     */
    protected function updateTable($table_name, $schema)
    {
        // nothing to do here
    }

    /**
     * @inheritdoc
     */
    public function dropTable($table)
    {
        /** @noinspection PhpUndefinedMethodInspection */
        $this->connection->getCollection($table)->drop();

        return 0;
    }

    /**
     * @param $table
     * @param $column
     *
     * @return bool|int
     */
    public function dropColumn($table, $column)
    {
        // Do nothing here for now
        return false;
    }

    /**
     * @inheritdoc
     */
    protected function createFieldReferences($references)
    {
        // Do nothing here for now
    }

    /**
     * @inheritdoc
     */
    protected function createFieldIndexes($indexes)
    {
        // Do nothing here for now
    }

    /**
     * @inheritdoc
     */
    public function parseValueForSet($value, $field_info)
    {
        switch ($field_info->type) {
            case DbSimpleTypes::TYPE_BOOLEAN:
                $value = (filter_var($value, FILTER_VALIDATE_BOOLEAN) ? 1 : 0);
                break;
        }

        return $value;
    }
}
