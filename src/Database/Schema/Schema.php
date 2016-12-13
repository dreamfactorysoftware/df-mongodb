<?php
namespace DreamFactory\Core\MongoDb\Database\Schema;

use DreamFactory\Core\Database\Schema\TableSchema;
use Jenssegers\Mongodb\Connection;
use MongoDB\Model\CollectionInfo;

/**
 * Schema is the class for retrieving metadata information from a MongoDB database (version 4.1.x and 5.x).
 */
class Schema extends \DreamFactory\Core\Database\Components\Schema
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
    protected function findTableNames($schema = '')
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
            $internalName = $quotedName = $tableName = $name;
            $settings = compact('tableName', 'name', 'internalName','quotedName');
            $tables[strtolower($name)] = new TableSchema($settings);
        }

        return $tables;
    }

    /**
     * @inheritdoc
     */
    protected function createTable($table, $options)
    {
        if (empty($tableName = array_get($table, 'name'))) {
            throw new \Exception("No valid name exist in the received table schema.");
        }

        $options = [];
        if (!empty($native = array_get($table, 'native'))) {
        }

        return $this->connection->getMongoDB()->createCollection($tableName, $options);
    }

    /**
     * @inheritdoc
     */
    protected function updateTable($table, $changes)
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
}
