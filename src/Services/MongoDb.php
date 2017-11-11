<?php
namespace DreamFactory\Core\MongoDb\Services;

use DreamFactory\Core\Components\RequireExtensions;
use DreamFactory\Core\Exceptions\InternalServerErrorException;
use DreamFactory\Core\MongoDb\Database\Schema\Schema as DatabaseSchema;
use DreamFactory\Core\MongoDb\Resources\Table;
use DreamFactory\Core\Database\Resources\DbSchemaResource;
use DreamFactory\Core\Database\Services\BaseDbService;
use Illuminate\Database\DatabaseManager;
use Jenssegers\Mongodb\Connection;

/**
 * MongoDb
 *
 * A service to handle MongoDB NoSQL (schema-less) database
 * services accessed through the REST API.
 */
class MongoDb extends BaseDbService
{
    //*************************************************************************
    //	Traits
    //*************************************************************************

    use RequireExtensions;

    //*************************************************************************
    //	Constants
    //*************************************************************************

    /**
     * Connection string prefix
     */
    const DSN_PREFIX = 'mongodb://';
    /**
     * Connection string prefix length
     */
    const DSN_PREFIX_LENGTH = 10;

    //*************************************************************************
    //	Members
    //*************************************************************************

    /**
     * @var Connection
     */
    protected $dbConn = null;
    /**
     * @var array
     */
    protected static $resources = [
        DbSchemaResource::RESOURCE_NAME => [
            'name'       => DbSchemaResource::RESOURCE_NAME,
            'class_name' => DbSchemaResource::class,
            'label'      => 'Schema',
        ],
        Table::RESOURCE_NAME  => [
            'name'       => Table::RESOURCE_NAME,
            'class_name' => Table::class,
            'label'      => 'Table',
        ],
    ];

    //*************************************************************************
    //	Methods
    //*************************************************************************

    /**
     * Create a new MongoDbSvc
     *
     * @param array $settings
     *
     * @throws \InvalidArgumentException
     * @throws \Exception
     */
    public function __construct($settings = [])
    {
        parent::__construct($settings);

        $this->config['driver'] = 'mongodb';
        if (!empty($dsn = strval(array_get($this->config, 'dsn')))) {
            // add prefix if not there
            if (0 != substr_compare($dsn, static::DSN_PREFIX, 0, static::DSN_PREFIX_LENGTH, true)) {
                $dsn = static::DSN_PREFIX . $dsn;
                $this->config['dsn'] = $dsn;
            }
        }

        // laravel database config requires options to be [], not null
        if (empty($options = array_get($this->config, 'options', []))) {
            $this->config['options'] = [];
        }
        if (empty($db = array_get($this->config, 'database'))) {
            if (!empty($db = array_get($this->config, 'options.db'))) {
                $this->config['database'] = $db;
            } elseif (!empty($db = array_get($this->config, 'options.database'))) {
                $this->config['database'] = $db;
            } else {
                //  Attempt to find db in connection string
                $db = strstr(substr($dsn, static::DSN_PREFIX_LENGTH), '/');
                if (false !== $pos = strpos($db, '?')) {
                    $db = substr($db, 0, $pos);
                }
                $db = trim($db, '/');
                $this->config['database'] = $db;
            }
        }

        if (empty($db)) {
            throw new InternalServerErrorException("No MongoDb database selected in configuration.");
        }

        $driverOptions = (array)array_get($this->config, 'driver_options');
        if (null !== $context = array_get($driverOptions, 'context')) {
            //  Automatically creates a stream from context
            $this->config['driver_options']['context'] = stream_context_create($context);
        }

        if (empty($prefix = array_get($this->config, 'dsn'))) {
            $host = array_get($this->config, 'host');
            $port = array_get($this->config, 'port');
            $username = array_get($this->config, 'username');
            $prefix = $host . $port . $username . $db;
        }
        $this->setConfigBasedCachePrefix($prefix . ':');
    }

    protected function initializeConnection()
    {
        // add config to global for reuse, todo check existence and update?
        config(['database.connections.service.' . $this->name => $this->config]);
        /** @type DatabaseManager $db */
        $db = app('db');
        $this->dbConn = $db->connection('service.' . $this->name);
        $this->schema = new DatabaseSchema($this->dbConn);
    }

    /**
     * Object destructor
     */
    public function __destruct()
    {
        try {
            /** @type DatabaseManager $db */
            $db = app('db');
            $db->disconnect('service.' . $this->name);

            parent::__destruct();
        } catch (\ReflectionException $e) {
            if (env('APP_ENV') === 'testing') {
                // When unit test is run. This exception is thrown.
                // Catching it and staying quite helps with unit test
            } else {
                throw $e;
            }
        }
    }
}