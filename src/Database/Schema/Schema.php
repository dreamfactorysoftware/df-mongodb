<?php
namespace DreamFactory\Core\MongoDb\Database\Schema;

use DreamFactory\Core\Database\ColumnSchema;
use DreamFactory\Core\Database\TableSchema;
use MongoDB\Model\CollectionInfo;

/**
 * Schema is the class for retrieving metadata information from a MongoDB database (version 4.1.x and 5.x).
 */
class Schema extends \DreamFactory\Core\Database\Schema\Schema
{
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
        $c = new ColumnSchema(['name' => '_id', 'isPrimaryKey' => true, 'autoIncrement' => true]);
        $table->addColumn($c);

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
        $db = $this->connection->getMongoDb();
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
