<?php

namespace DreamFactory\Core\MongoDb\Components;

use DreamFactory\Core\Exceptions\InternalServerErrorException;
use DreamFactory\Core\Exceptions\NotFoundException;
use DreamFactory\Core\Exceptions\NotImplementedException;
use DreamFactory\Core\File\Components\RemoteFileSystem;
use DreamFactory\Core\Exceptions\DfException;
use DreamFactory\Core\Utility\FileUtilities;

use MongoDB\Client as MongoDBClient;
use Illuminate\Http\Request;
use MongoDB\Driver\Exception\ConnectionTimeoutException;

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
     * MongoDb connection
     */
    protected $blobConn = null;

    /**
     * gridFS bucket reference
     */
    protected $gridFS = null;

    /**
     * reference to the request object
     */
    protected $request = null;

    public function __construct($config, $name)
    {
        $this->request = Request::capture();
        $config['driver'] = 'mongodb';

        if (!empty($dsn = strval(array_get($config, 'dsn')))) {
            // add prefix if not there
            // NOTE: We may want to change this to match the code in MondoDB.php as a regex check
            // if this breaks for calling Atlas / replica sets with the +srv option
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
                // NOTE: We may want to change this to match the code in MondoDB.php as a regex replacement
                // if this breaks for calling Atlas / replica sets with the +srv option
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
            $connectionOptions = [];
            $host = array_get($config, 'host');
            $port = array_get($config, 'port');
            $username = array_get($config, 'username');
            $password = array_get($config, 'password');
            $connectionStr = sprintf("mongodb://%s:%s", $host,
                $port, $db);

            if (!empty($username) && !empty($password)) {
                $connectionOptions = [
                    'username' => $username,
                    'password' => $password,
                ];
            }

            if (!empty($options)) {
                $connectionOptions += $options;
            }
            $this->blobConn = $this->createConnection($connectionStr, $connectionOptions);
        } else {
            $this->blobConn = $this->createConnection($dsn);
        }

        $bucketName = array_get($config, 'bucket_name');

        if (!empty($bucketName)) {
            $this->gridFS = $this->blobConn->$db->selectGridFSBucket(['bucketName' => $bucketName]);
        } else {
            $this->gridFS = $this->blobConn->$db->selectGridFSBucket();
        }
    }

    protected function createConnection($connectionStr, $options = [])
    {
        return new MongoDBClient($connectionStr, $options);
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

    /**
     * @return \MongoDB\Driver\Cursor
     */
    protected function gridFind()
    {
        return $this->gridFS->find();
    }

    /**
     * finds a file based on filename from gridfs
     *
     * @param $name string name of the file to find.
     *
     * @return object file collection
     */
    protected function gridFindOne($name)
    {
        try {
            $params = ['filename' => $name];
            $obj = $this->gridFS->findOne($params);
        } catch (ConnectionTimeoutException $ex) {
            throw new ConnectionTimeoutException($ex);
        } catch (\Exception $ex) {
            throw new NotFoundException('Could not find file "' . $name . '": ' . $ex->getMessage());
        }

        return $obj;
    }

    /**
     * @param string $container
     *
     * @return bool
     */
    public function containerExists($container)
    {
        return $this->checkConnection();
    }

    /**
     * main function to list blobs within a folder, etc.
     *
     * @param string $container
     * @param string $prefix
     * @param string $delimiter - if full_tree is true, delimeter will be empty.
     *
     * @return array
     */
    public function listBlobs($container, $prefix = '', $delimiter = '')
    {
        $allFiles = $this->gridFind();
        $return = [];
        foreach ($allFiles as $fileObj) {
            $return[] = $this->getBlobMeta($fileObj);
        }

        /** If we have a delimiter (directory), there is no method in GridFS library to get only records under
         * the delimiter as in AWS s3, so we need to manually filter out. First reduce down dirs to passed $prefix */
        $reduced = [];
        foreach ($return as &$filterBlob) {
            if (strpos($filterBlob['path'], $prefix) !== false) {
                $reduced[] = $filterBlob;
            }
        }

        if (empty($reduced) && !empty($prefix)) {
            return [];
        }

        $reduced = (empty($reduced)) ? $return : $reduced;

        return $this->filterPaths($prefix, $delimiter, $reduced);
    }

    /**
     * @param $obj object
     *
     * @return array blob metadata
     */
    protected function getBlobMeta($obj)
    {
        $date = $obj->uploadDate->toDateTime();
        $return = [
            'oid'            => (string)$obj->_id,
            'name'           => $obj->filename,
            'content_type'   => (isset($obj->contentType)) ? $obj->contentType : '',
            'content_length' => $obj->length,
            'last_modified'  => $date->format(\DateTime::ATOM),
            'path'           => $obj->filename,
        ];

        return $return;
    }

    /**
     * Takes reduced records and filters out subs if delimeter is present indicating full_path = false
     *
     * @param $prefix       string path to start from
     * @param $delimiter    string directory delimeter
     * @param $reducedBlobs array reduced by starting prefix records
     *
     * @return array filtered records
     */
    protected function filterPaths($prefix, $delimiter, $reducedBlobs)
    {
        $return = [];
        foreach ($reducedBlobs as $reduction) {
            //Skip over self-same directory listings
            if ($reduction['name'] == $prefix) {
                continue;
            } else if ($delimiter == '/') {
                // How many levels in path
                $pathCnt = count(explode('/', $reduction['path']));
                // How many levels in prefix
                $prefixCnt = count(explode('/', $prefix));
                // Directories only listing - To allow one level deeper than prefix.
                if ($prefixCnt + 1 == $pathCnt && ('/' == substr($reduction['path'], -1))) {
                    $return[] = $reduction;
                } elseif ($prefixCnt == $pathCnt) {
                    // Grabs only the files within the path
                    $return[] = $reduction;
                }
            } else {
                $return[] = $reduction;
            }
        }

        return $return;
    }

    /**
     * @param $db
     */
    protected function selectDb($db)
    {
        $this->blobConn->selectDatabase($db);
    }

    /**
     * @param string $container
     * @param string $name
     *
     * @return bool
     */
    public function blobExists($container, $name)
    {
        try {
            $this->checkConnection();
            $cursor = $this->gridFindOne($name);
            if (!is_null($cursor)) {
                return true;
            }
        } catch (\Exception $ex) {
            return false;
        }

        return false;
    }

    /**
     * @param string $container
     * @param string $name
     * @param null   $data
     * @param array  $properties
     *
     * @throws \DreamFactory\Core\Exceptions\DfException
     */
    public function putBlobData($container, $name, $data = null, $properties = [])
    {
        try {
            $existing = null;
            $contentType = (empty($properties) && strpos($name, '/')) ? 'application/x-directory' : $properties;
            // If we are changing blob data, need to do it atomically here.
            // Check for existing
            $existing = $this->gridFindOne($name);
            /** Need to open a stream to GridFS (this creates the empty file meta) $gfsStream */
            $gfsStream = $this->gridFS->openUploadStream($name, ['contentType' => $contentType]);
            /** Writes to the stream */
            if (fwrite($gfsStream, $data) && !is_null($existing)) {
                // if put operation, this will delete the existing file as replaced.
                $this->deleteByObjectId($existing->_id);
            }
        } catch (\Exception $ex) {
            throw new DfException('Failed to create GridFS file "' . $name . '": ' . $ex->getMessage());
        }
    }

    /**
     * @inheritdoc
     */
    public function putBlobFromFile($container, $name, $localFileName = null, $properties = [])
    {
        $fp = fopen($localFileName, 'r');
        $fileData = stream_get_contents($fp);

        return $this->putBlobData($container, $name, $fileData, $properties);
    }

    /**
     * @inheritdoc
     */
    public function fileExists($container, $path)
    {
        if ($this->blobExists($container, $path)) {
            return true;
        }

        return false;
    }

    /**
     * @param string $container
     * @param string $name
     *
     * @return array
     */
    public function getBlobProperties($container, $name)
    {
        $obj = $this->gridFindOne($name);

        return $this->getBlobMeta($obj);
    }

    /**
     *
     * @param string $container
     * @param string $name
     * @param array  $params
     *
     * @throws \DreamFactory\Core\Exceptions\DfException
     */
    public function streamBlob($container, $name, $params = [])
    {
        try {
            $fileObj = $this->gridFindOne($name);
            $range = (isset($params['range'])) ? $params['range'] : null;
            $start = $end = -1;
            $stream = $this->gridFS->openDownloadStream($fileObj->_id);
            $date = $fileObj->uploadDate->toDateTime();
            $size = $fullsize = intval($fileObj->length);

            header('Last-Modified: ' . $date->format(\DateTime::ATOM));

            /** If content type is not set, try to determine it. */
            if (!isset($fileObj->contentType)) {
                $ext = FileUtilities::getFileExtension($name);
                $contentType = FileUtilities::determineContentType($ext);
                header('Content-Type: ' . $contentType);
            } else {
                header('Content-Type: ' . $fileObj->contentType);
            }

            $disposition =
                (isset($params['disposition']) && !empty($params['disposition'])) ? $params['disposition']
                    : 'inline';

            header('Content-Disposition: ' . $disposition . '; filename="' . $name . '";');

            // All this for range header being passed.
            if ($range != null) {
                $eqPos = strpos($range, "=");
                $toPos = strpos($range, "-");
                $unit = substr($range, 0, $eqPos);
                $start = intval(substr($range, $eqPos + 1, $toPos));
                $end = intval(substr($range, $toPos + 1));
                $success = fseek($stream, $start);
                if ($success == 0) {
                    $size = $end - $start;
                    // Don't let the passed size exceed the actual.
                    if ($fullsize <= $size) {
                        $size = $fullsize;
                    }
                    $response_code = 206;
                    header('Accept-Ranges: ' . $unit);
                    header('Content-Range: ' . $unit . " " . $start . "-" . ($fullsize - 1) . "/" . $fullsize, true,
                        $response_code);
                }
            }

            header('Content-Length:' . $size);

            while (!feof($stream)) {
                if ($start = -1 && $end = -1) {
                    /** if entire file is requested... */
                    echo stream_get_contents($stream, \Config::get('df.file_chunk_size'));
                } else {
                    /** if Bit of file, in chunks */
                    echo stream_get_contents($stream, $end, $start);
                }
            }
        } catch (ConnectionTimeoutException $ex) {
            throw new ConnectionTimeoutException($ex->getMessage());
        } catch (\Exception $ex) {
            throw new DfException('Failed to retrieve GridFS file "' . $name . '": ' . $ex->getMessage());
        }
    }

    /**
     * @param string $container
     * @param string $name
     * @param bool   $noCheck
     *
     * @throws \DreamFactory\Core\Exceptions\DfException
     */
    public function deleteBlob($container, $name, $noCheck = false)
    {
        try {
            $cursor = $this->gridFindOne($name);
            $this->deleteByObjectId($cursor->_id);
        } catch (ConnectionTimeoutException $ex) {
            throw new ConnectionTimeoutException($ex->getMessage());
        } catch (\Exception $ex) {
            throw new NotFoundException('Failed to delete GridFS file "' . $name . '": ' . $ex->getMessage());
        }
    }

    /**
     * Deletes a gridfs file and associated data chunks by object id
     *
     * @param $objId object GridFS ID Object
     *
     * @return boolean
     */
    protected function deleteByObjectId($objId)
    {
        return $this->gridFS->delete($objId);
    }

    /**
     * @inheritdoc
     */
    public function listContainers($include_properties = false)
    {
        throw new NotImplementedException('Method ' . __METHOD__ . ' not applicable for current file system.');
    }

    /**
     * @inheritdoc
     */
    public function getContainer($container, $include_files = true, $include_folders = true, $full_tree = false)
    {
        throw new NotImplementedException('Method ' . __METHOD__ . ' not applicable for current file system.');
    }

    /**
     * @inheritdoc
     */
    public function getContainerProperties($container)
    {
        throw new NotImplementedException('Method ' . __METHOD__ . ' not applicable for current file system.');
    }

    /**
     * @inheritdoc
     */
    public function createContainer($container, $check_exist = false)
    {
        throw new NotImplementedException('Method ' . __METHOD__ . ' not applicable for current file system.');
    }

    /**
     * @inheritdoc
     */
    public function updateContainerProperties($container, $properties = [])
    {
        throw new NotImplementedException('Method ' . __METHOD__ . ' not applicable for current file system.');
    }

    /**
     * @inheritdoc
     */
    public function deleteContainer($container, $force = false)
    {
        throw new NotImplementedException('Method ' . __METHOD__ . ' not applicable for current file system.');
    }

    /**
     * @inheritdoc
     */
    public function copyBlob($container, $name, $src_container, $src_name, $properties = [])
    {
        throw new NotImplementedException('Method ' . __METHOD__ . ' not applicable for current file system.');
    }

    /**
     * @inheritdoc
     */
    public function getBlobAsFile($container, $name, $localFileName = null)
    {
        throw new NotImplementedException('Method ' . __METHOD__ . ' not applicable for current file system.');
    }

    /**
     * @inheritdoc
     */
    public function getBlobData($container, $name)
    {
        $blobData = $this->gridFindOne($name);
        $stream = $this->gridFS->openDownloadStream($blobData->_id);
        $data = stream_get_contents($stream);

        return $data;
    }
}