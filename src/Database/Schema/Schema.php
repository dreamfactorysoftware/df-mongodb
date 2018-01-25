<?php
namespace DreamFactory\Core\MongoDb\Database\Schema;

use DreamFactory\Core\Database\Schema\ColumnSchema;
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
    protected function loadTableColumns(TableSchema $table)
    {
        $table->addPrimaryKey('_id');
        $c = new ColumnSchema([
            'name'           => '_id',
            'db_type'        => 'string',
            'is_primary_key' => true,
            'auto_increment' => true,
        ]);
        $c->quotedName = $this->quoteColumnName($c->name);

        $table->addColumn($c);
    }

    /**
     * @inheritdoc
     */
    protected function getTableNames($schema = '')
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
            $tables[strtolower($name)] = new TableSchema(['name' => $name]);
        }

        return $tables;
    }

    /**
     * @inheritdoc
     */
    public function createTable($table, $options)
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
    public function updateTable($tableSchema, $changes)
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
     * @inheritdoc
     */
    public function dropColumns($table, $column)
    {
        // Do nothing here for now
        return false;
    }

    /**
     * @inheritdoc
     */
    public function createFieldReferences($references)
    {
        // Do nothing here for now
    }

    /**
     * @inheritdoc
     */
    public function createFieldIndexes($indexes)
    {
        // Do nothing here for now
    }
}
