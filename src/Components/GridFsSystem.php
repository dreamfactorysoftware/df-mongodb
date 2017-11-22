<?php

namespace DreamFactory\Core\MongoDb\Components;

use DreamFactory\Core\Contracts\FileSystemInterface;
use DreamFactory\Core\Exceptions\NotImplementedException;
use DreamFactory\Core\Exceptions\InternalServerErrorException;
use DreamFactory\Core\File\Components\RemoteFileSystem;
use DreamFactory\Core\MongoDb\Database\Schema\Schema;
use Illuminate\Database\DatabaseManager;
use DreamFactory\Core\Exceptions\DfException;
use Symfony\Component\HttpFoundation\StreamedResponse;
use Symfony\Component\HttpFoundation\Response;
use MongoDB\Client as MongoDBClient;
use MongoDB\GridFS;

class GridFsSystem extends RemoteFileSystem
{

    /**
     * Connection string prefix
     */
    const DSN_PREFIX = 'mongodb://';
    /**
     * Connection string prefix length
     */
    const DSN_PREFIX_LENGTH = 10;
    /**
     * @var Connection
     */
    protected $blobConn = null;
    protected $gridFS = null;

    protected $testdb = 'local'; //TODO: need to be able to pass in the db.

    public function __construct($config, $name)
    {
        $config['driver'] = 'mongodb';

        if (!empty($dsn = strval(array_get($config, 'dsn')))) {
            // add prefix if not there
            if (0 != substr_compare($dsn, static::DSN_PREFIX, 0, static::DSN_PREFIX_LENGTH, true)) {
                $dsn = static::DSN_PREFIX . $dsn;
                $config['dsn'] = $dsn;
            }
        }

        // laravel database config requires options to be [], not null
        if (empty($options = array_get($config, 'options', []))) {
            $config['options'] = [];
        }
        if (empty($db = array_get($config, 'database'))) {
            if (!empty($db = array_get($config, 'options.db'))) {
                $config['database'] = $db;
            } elseif (!empty($db = array_get($config, 'options.database'))) {
                $config['database'] = $db;
            } else {
                //  Attempt to find db in connection string
                $db = strstr(substr($dsn, static::DSN_PREFIX_LENGTH), '/');
                if (false !== $pos = strpos($db, '?')) {
                    $db = substr($db, 0, $pos);
                }
                $db = trim($db, '/');
                $config['database'] = $db;
            }
        }

        if (empty($db)) {
            throw new InternalServerErrorException("No MongoDb database selected in configuration.");
        }

        $driverOptions = (array)array_get($config, 'driver_options');
        if (null !== $context = array_get($driverOptions, 'context')) {
            //  Automatically creates a stream from context
            $config['driver_options']['context'] = stream_context_create($context);
        }

        if (empty($prefix = array_get($config, 'dsn'))) {
            $host = array_get($config, 'host');
            $port = array_get($config, 'port');
            $username = array_get($config, 'username');
            $prefix = $host . $port . $username . $db;
        }

        //TODO:Right now hardcoded to localhost, need to pass in dynamic connection params.
        $this->blobConn = new MongoDBClient("mongodb://localhost:27017");
        $this->gridFS = $this->blobConn->local->selectGridFSBucket();
    }

    /**
     * @throws DfException
     */
    protected function checkConnection()
    {
        if (empty($this->blobConn)) {
            throw new DfException('No valid connection to blob file storage.');
        }

        return true;
    }

    protected function gridFind()
    {
        return $this->gridFS->find();
    }

    public function containerExists($container)
    {
        return $this->checkConnection();
    }

    public function listBlobs($container, $prefix = '', $delimiter = '')
    {
        $allFiles = $this->gridFind();
        $return = [];
        foreach ($allFiles as $fileObj) {
            $date = $fileObj->uploadDate->toDateTime();
            $return[] = [
                'oid'            => (string)$fileObj->_id,
                'name'           => $fileObj->filename,
                'content_type'   => $fileObj->contentType,
                'content_length' => $fileObj->length,
                'last_modified'  => $date->format(\DateTime::ATOM),
                'path'           => $fileObj->filename,
            ];
        }

        /** If we have a delimiter (directory), there is no method in GridFS library to get only records under
         * the delimiter as in AWS s3, so we need to manually filter out. */
        $filtered = [];
        foreach ($return as &$filterBlob) {
            if (!is_null($prefix) && $delimiter == '/') {
                if (0 == substr_compare($prefix, $filterBlob['path'], 0, strlen($prefix), true)) {
                    /** Don't include dir file representing the prefix, only subs... But only to a certain level. */
                    if ($filterBlob['path'] !== $prefix) {
                        /** @var Levels deep in path $pathCnt */
                        $pathCnt = count(explode('/', $filterBlob['path']));
                        /** @var Levels deep in prefix $prefixCnt */
                        $prefixCnt = count(explode('/', $prefix));
                        /** Only allow one level deeper than prefix and nothing past a containing directory. */
                        if ($prefixCnt + 1 == $pathCnt && ('/' == substr($filterBlob['path'], -1))) {
                            $filtered[] = $filterBlob;
                        }
                    }
                } else {
                    if ($dirPos = strpos($filterBlob['path'], '/') !== false) {
                        if ($dirPos + 1 == strlen($filterBlob['path'])) {
                            /** same level, add to return */
                            $filtered[] = $filterBlob;
                        }
                    }
                }
            }
        }
        if (!empty($filtered)) {
            $return = $filtered;
        }

        return $return;
    }

    protected
    function selectDb(
        $db
    ){
        $this->blobConn->selectDatabase($db);
    }

    public
    function listContainers(
        $include_properties = false
    ){
        $stop = true;
    }

    public
    function getContainer(
        $container,
        $include_files = true,
        $include_folders = true,
        $full_tree = false
    ){
        // TODO: Implement getContainer() method.
    }

    public
    function getContainerProperties(
        $container
    ){
        // TODO: Implement getContainerProperties() method.
    }

    public
    function createContainer(
        $container,
        $check_exist = false
    ){
        // TODO: Implement createContainer() method.
    }

    public
    function updateContainerProperties(
        $container,
        $properties = []
    ){
        // TODO: Implement updateContainerProperties() method.
    }

    public
    function deleteContainer(
        $container,
        $force = false
    ){
        // TODO: Implement deleteContainer() method.
    }

    public
    function blobExists(
        $container,
        $name
    ){
        try {
            $this->checkConnection();
            $params = ['filename' => $name];
            $cursor = $this->gridFS->findOne($params);
            if (!is_null($cursor)) {
                return true;
            }
        } catch (\Exception $ex) {
            return false;
        }

        return false;
    }

    public
    function putBlobData(
        $container,
        $name,
        $data = null,
        $properties = []
    ){
        try {
            $contentType = (empty($properties) && strpos($name, '/')) ? 'application/x-directory' : $properties;

            /** @var Need to open a stream to GridFS (this creates the empty file meta) $gfsStream */
            $gfsStream = $this->gridFS->openUploadStream($name, ['contentType' => $contentType]);
            /** Writes to the stream */
            fwrite($gfsStream, $data);
        } catch (\Exception $ex) {
            throw new DfException('Failed to create GridFS file "' . $name . '": ' . $ex->getMessage());
        }
    }

    public
    function putBlobFromFile(
        $container,
        $name,
        $localFileName = null,
        $properties = []
    ){

    }

    public
    function copyBlob(
        $container,
        $name,
        $src_container,
        $src_name,
        $properties = []
    ){
        // TODO: Implement copyBlob() method.
    }

    public
    function getBlobAsFile(
        $container,
        $name,
        $localFileName = null
    ){
        $stop = 1;// TODO: Implement getBlobAsFile() method.
    }

    public
    function getBlobData(
        $container,
        $name
    ){
        $stop = 1;// TODO: Implement getBlobAsFile() method.
    }

    public
    function getBlobProperties(
        $container,
        $name
    ){
        $stop = 1;// TODO: Implement getBlobProperties() method.
    }

    public
    function streamBlob(
        $container,
        $name,
        $params = []
    ){
        try {
            $fileObj = $this->gridFS->findOne(['filename' => $name]);

            $stream = $this->gridFS->openDownloadStream($fileObj->_id);
            //$contents = stream_get_contents($stream);
            $date = $fileObj->uploadDate->toDateTime();

            header('Last-Modified: ' . $date->format(\DateTime::ATOM));
            header('Content-Type: ' . $fileObj->contentType);
            header('Content-Length:' . intval($fileObj->length));

            $disposition =
                (isset($params['disposition']) && !empty($params['disposition'])) ? $params['disposition']
                    : 'inline';

            header('Content-Disposition: ' . $disposition . '; filename="' . $name . '";');

            /** TODO: can add offset and maxlength to stream chunks. */
            echo stream_get_contents($stream);
        } catch (\Exception $ex) {
            throw new DfException('Failed to retrieve GridFS file "' . $name . '": ' . $ex->getMessage());
        }
    }

    public
    function deleteBlob(
        $container,
        $name,
        $noCheck = false
    ){
        $stop = 1;
    }
}