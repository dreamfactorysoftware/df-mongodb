<?php
namespace DreamFactory\Core\MongoDb\Components;

use DreamFactory\Core\Contracts\RequestHandlerInterface;
use DreamFactory\Core\Contracts\SchemaInterface;
use DreamFactory\Core\MongoDb\Services\MongoDb;
use Jenssegers\Mongodb\Connection;

trait MongoDbResource
{
    //*************************************************************************
    //	Members
    //*************************************************************************

    /**
     * @var Connection
     */
    protected $dbConn = null;
    /**
     * @var SchemaInterface
     */
    protected $schema = null;


    //*************************************************************************
    //	Methods
    //*************************************************************************

    /**
     * @param RequestHandlerInterface $parent
     */
    public function setParent(RequestHandlerInterface $parent)
    {
        parent::setParent($parent);

        /** @var MongoDb $parent */
        $this->dbConn = $parent->getConnection();

        /** @var MongoDb $parent */
        $this->schema = $parent->getSchema();
    }
}