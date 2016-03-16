<?php
namespace DreamFactory\Core\MongoDb\Resources;

use DreamFactory\Core\Exceptions\BadRequestException;
use DreamFactory\Core\Exceptions\InternalServerErrorException;
use DreamFactory\Core\Resources\BaseNoSqlDbSchemaResource;
use DreamFactory\Core\MongoDb\Services\MongoDb;
use MongoDB\Collection;

class Schema extends BaseNoSqlDbSchemaResource
{
    //*************************************************************************
    //	Members
    //*************************************************************************

    /**
     * @var null|MongoDb
     */
    protected $parent = null;

    //*************************************************************************
    //	Methods
    //*************************************************************************

    /**
     * @return null|MongoDb
     */
    public function getService()
    {
        return $this->parent;
    }

    /**
     * @param $name
     *
     * @return Collection|null
     */
    public function selectTable($name)
    {
        $coll = $this->parent->getConnection()->selectCollection($name);

        return $coll;
    }

    /**
     * {@inheritdoc}
     */
    public function describeTable($table, $refresh = true)
    {
        $name = (is_array($table)) ? array_get($table, 'name') : $table;

        try {
            $collection = $this->selectTable($name);
            $indexes = [];
            foreach ($collection->listIndexes() as $index) {
                $indexes[] = $index->__debugInfo();
            }
            $out =
                [
                    'name'      => $collection->getCollectionName(),
                    'namespace' => $collection->getNamespace(),
                    'indexes'   => $indexes,
                    'access' => $this->getPermissions($name)
                ];

            return $out;
        } catch (\Exception $ex) {
            throw new InternalServerErrorException(
                "Failed to get table properties for table '$name'.\n{$ex->getMessage()}"
            );
        }
    }

    /**
     * {@inheritdoc}
     */
    public function createTable($table, $properties = [], $check_exist = false, $return_schema = false)
    {
        if (empty($table)) {
            throw new BadRequestException("No 'name' field in data.");
        }

        try {
            $result = $this->parent->getConnection()->createCollection($table);
            $this->refreshCachedTables();

            return $this->describeTable($table);
        } catch (\Exception $ex) {
            throw new InternalServerErrorException("Failed to create table '$table'.\n{$ex->getMessage()}");
        }
    }

    /**
     * {@inheritdoc}
     */
    public function updateTable($table, $properties = [], $allow_delete_fields = false, $return_schema = false)
    {
        if (empty($table)) {
            throw new BadRequestException("No 'name' field in data.");
        }

        $this->selectTable($table);
        $this->refreshCachedTables();

//		throw new InternalServerErrorException( "Failed to update table '$name'." );
        return ['name' => $table];
    }

    /**
     * {@inheritdoc}
     */
    public function deleteTable($table, $check_empty = false)
    {
        $name = (is_array($table)) ? array_get($table, 'name') : $table;
        if (empty($name)) {
            throw new BadRequestException('Table name can not be empty.');
        }

        try {
            $this->selectTable($table)->drop();
            $this->refreshCachedTables();

            return ['name' => $name];
        } catch (\Exception $ex) {
            throw new InternalServerErrorException("Failed to delete table '$name'.\n{$ex->getMessage()}");
        }
    }
}