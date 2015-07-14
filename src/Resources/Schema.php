<?php
 namespace DreamFactory\Core\MongoDb\Resources;

use DreamFactory\Library\Utility\ArrayUtils;
use DreamFactory\Library\Utility\Inflector;
use DreamFactory\Core\Exceptions\BadRequestException;
use DreamFactory\Core\Exceptions\InternalServerErrorException;
use DreamFactory\Core\Resources\BaseNoSqlDbSchemaResource;
use DreamFactory\Core\Utility\DbUtilities;
use DreamFactory\Core\MongoDb\Services\MongoDb;

class Schema extends BaseNoSqlDbSchemaResource
{
    //*************************************************************************
    //	Members
    //*************************************************************************

    /**
     * @var null|MongoDb
     */
    protected $service = null;

    //*************************************************************************
    //	Methods
    //*************************************************************************

    /**
     * @return null|MongoDb
     */
    public function getService()
    {
        return $this->service;
    }

    /**
     * @param $name
     *
     * @return \MongoCollection|null
     */
    public function selectTable($name)
    {
        $_coll = $this->service->getConnection()->selectCollection($name);

        return $_coll;
    }

    /**
     * {@inheritdoc}
     */
    public function listResources($fields = null)
    {
//        $refresh = $this->request->queryBool('refresh');

        $_names = $this->service->getConnection()->getCollectionNames();

        if (empty($fields)) {
            return $this->cleanResources($_names);
        }

        $_extras =
            DbUtilities::getSchemaExtrasForTables($this->service->getServiceId(), $_names, false, 'table,label,plural');

        $_tables = [];
        foreach ($_names as $name) {
            $label = '';
            $plural = '';
            foreach ($_extras as $each) {
                if (0 == strcasecmp($name, ArrayUtils::get($each, 'table', ''))) {
                    $label = ArrayUtils::get($each, 'label');
                    $plural = ArrayUtils::get($each, 'plural');
                    break;
                }
            }

            if (empty($label)) {
                $label = Inflector::camelize($name, ['_', '.'], true);
            }

            if (empty($plural)) {
                $plural = Inflector::pluralize($label);
            }

            $_tables[] = ['name' => $name, 'label' => $label, 'plural' => $plural];
        }

        return $this->cleanResources($_tables, 'name', $fields);
    }

    /**
     * {@inheritdoc}
     */
    public function describeTable($table, $refresh = true)
    {
        $_name = (is_array($table)) ? ArrayUtils::get($table, 'name') : $table;

        try {
            $_coll = $this->selectTable($_name);
            $_out = array('name' => $_coll->getName());
            $_out['indexes'] = $_coll->getIndexInfo();
            $_out['access'] = $this->getPermissions($_name);

            return $_out;
        } catch (\Exception $_ex) {
            throw new InternalServerErrorException(
                "Failed to get table properties for table '$_name'.\n{$_ex->getMessage()}"
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
            $_result = $this->service->getConnection()->createCollection($table);
            $_out = array('name' => $_result->getName());
            $_out['indexes'] = $_result->getIndexInfo();

            return $_out;
        } catch (\Exception $_ex) {
            throw new InternalServerErrorException("Failed to create table '$table'.\n{$_ex->getMessage()}");
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

//		throw new InternalServerErrorException( "Failed to update table '$_name'." );
        return array('name' => $table);
    }

    /**
     * {@inheritdoc}
     */
    public function deleteTable($table, $check_empty = false)
    {
        $_name = (is_array($table)) ? ArrayUtils::get($table, 'name') : $table;
        if (empty($_name)) {
            throw new BadRequestException('Table name can not be empty.');
        }

        try {
            $this->selectTable($table)->drop();

            return array('name' => $_name);
        } catch (\Exception $_ex) {
            throw new InternalServerErrorException("Failed to delete table '$_name'.\n{$_ex->getMessage()}");
        }
    }
}