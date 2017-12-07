<?php

namespace DreamFactory\Core\MongoDb\Services;

use DreamFactory\Core\File\Services\RemoteFileService;
use DreamFactory\Core\MongoDb\Components\GridFsSystem;


/**
 * GridFs
 *
 * A service to handle MongoDB GridFS file
 * services accessed through the REST API.
 */
class GridFsService extends RemoteFileService
{
    //*************************************************************************
    //	Methods
    //*************************************************************************

    protected function setDriver($config)
    {
        $this->driver = new GridFsSystem($config, $this->name);
    }

}