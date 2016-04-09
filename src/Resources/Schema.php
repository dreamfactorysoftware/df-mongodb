<?php
namespace DreamFactory\Core\MongoDb\Resources;

use DreamFactory\Core\Exceptions\BadRequestException;
use DreamFactory\Core\Exceptions\InternalServerErrorException;
use DreamFactory\Core\Exceptions\NotFoundException;
use DreamFactory\Core\Exceptions\RestException;
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
        $coll = $this->parent->getConnection()->getCollection($name);

        return $coll;
    }

    /**
     * {@inheritdoc}
     */
    public function describeTable($table, $refresh = true)
    {
        $name = (is_array($table)) ? array_get($table, 'name') : $table;

        try {
            $table = $this->parent->getSchema()->getTable($name, $refresh);
            if (!$table) {
                throw new NotFoundException("Table '$name' does not exist in the database.");
            }

            $result = $table->toArray();
            $result['access'] = $this->getPermissions($name);

            return $result;
        } catch (RestException $ex) {
            throw $ex;
        } catch (\Exception $ex) {
            throw new InternalServerErrorException("Failed to query database schema.\n{$ex->getMessage()}");
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

        $properties = (is_array($properties) ? $properties : []);

        try {
            $this->parent->getConnection()->getMongoDB()->createCollection($table, $properties);
        } catch (\Exception $ex) {
            throw new InternalServerErrorException("Failed to create table '$table'.\n{$ex->getMessage()}");
        }

        //  Any changes here should refresh cached schema
        $this->refreshCachedTables();

        if ($return_schema) {
            return $this->describeTable($table);
        }

        return ['name' => $table];
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