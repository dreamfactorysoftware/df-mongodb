<?php
 namespace DreamFactory\Core\MongoDb\Resources;

use DreamFactory\Library\Utility\ArrayUtils;
use DreamFactory\Core\Exceptions\BadRequestException;
use DreamFactory\Core\Exceptions\InternalServerErrorException;
use DreamFactory\Core\Resources\BaseNoSqlDbSchemaResource;
use DreamFactory\Core\MongoDb\Services\MongoDb;

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
     * @return \MongoCollection|null
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
        $name = (is_array($table)) ? ArrayUtils::get($table, 'name') : $table;

        try {
            $coll = $this->selectTable($name);
            $out = array('name' => $coll->getName());
            $out['indexes'] = $coll->getIndexInfo();
            $out['access'] = $this->getPermissions($name);

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
    public function createTable($table, $properties = array(), $check_exist = false, $return_schema = false)
    {
        if (empty($table)) {
            throw new BadRequestException("No 'name' field in data.");
        }

        try {
            $result = $this->parent->getConnection()->createCollection($table);
            $out = array('name' => $result->getName());
            $out['indexes'] = $result->getIndexInfo();

            $this->refreshCachedTables();

            return $out;
        } catch (\Exception $ex) {
            throw new InternalServerErrorException("Failed to create table '$table'.\n{$ex->getMessage()}");
        }
    }

    /**
     * {@inheritdoc}
     */
    public function updateTable($table, $properties = array(), $allow_delete_fields = false, $return_schema = false)
    {
        if (empty($table)) {
            throw new BadRequestException("No 'name' field in data.");
        }

        $this->selectTable($table);
        $this->refreshCachedTables();

//		throw new InternalServerErrorException( "Failed to update table '$name'." );
        return array('name' => $table);
    }

    /**
     * {@inheritdoc}
     */
    public function deleteTable($table, $check_empty = false)
    {
        $name = (is_array($table)) ? ArrayUtils::get($table, 'name') : $table;
        if (empty($name)) {
            throw new BadRequestException('Table name can not be empty.');
        }

        try {
            $this->selectTable($table)->drop();
            $this->refreshCachedTables();

            return array('name' => $name);
        } catch (\Exception $ex) {
            throw new InternalServerErrorException("Failed to delete table '$name'.\n{$ex->getMessage()}");
        }
    }
}