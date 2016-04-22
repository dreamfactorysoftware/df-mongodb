<?php
namespace DreamFactory\Core\MongoDb\Services;

use DreamFactory\Core\Components\DbSchemaExtras;
use DreamFactory\Core\Components\RequireExtensions;
use DreamFactory\Core\Contracts\CacheInterface;
use DreamFactory\Core\Contracts\DbExtrasInterface;
use DreamFactory\Core\Contracts\SchemaInterface;
use DreamFactory\Core\Database\TableSchema;
use DreamFactory\Core\Exceptions\InternalServerErrorException;
use DreamFactory\Core\MongoDb\Resources\Schema;
use DreamFactory\Core\MongoDb\Resources\Table;
use DreamFactory\Core\Services\BaseNoSqlDbService;
use DreamFactory\Core\Utility\Session;
use Illuminate\Database\DatabaseManager;
use Jenssegers\Mongodb\Connection;

/**
 * MongoDb
 *
 * A service to handle MongoDB NoSQL (schema-less) database
 * services accessed through the REST API.
 */
class MongoDb extends BaseNoSqlDbService implements CacheInterface, DbExtrasInterface
{
    //*************************************************************************
    //	Traits
    //*************************************************************************

    use DbSchemaExtras, RequireExtensions;

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
     * @var SchemaInterface
     */
    protected $schema = null;
    /**
     * @var array
     */
    protected $tableNames = [];
    /**
     * @var array
     */
    protected $tables = [];
    /**
     * @var array
     */
    protected static $resources = [
        Schema::RESOURCE_NAME => [
            'name'       => Schema::RESOURCE_NAME,
            'class_name' => Schema::class,
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

    public static function adaptConfig(array &$config)
    {
        $dsn = isset($config['dsn']) ? $config['dsn'] : null;
        if (!empty($dsn)) {
            // default PDO DSN pieces
            $dsn = str_replace(' ', '', $dsn);
            if (!isset($config['port']) && (false !== ($pos = strpos($dsn, 'port=')))) {
                $temp = substr($dsn, $pos + 5);
                $config['port'] = (false !== $pos = strpos($temp, ';')) ? substr($temp, 0, $pos) : $temp;
            }
            if (!isset($config['host']) && (false !== ($pos = strpos($dsn, 'host=')))) {
                $temp = substr($dsn, $pos + 5);
                $host = (false !== $pos = stripos($temp, ';')) ? substr($temp, 0, $pos) : $temp;
                if (!isset($config['port']) && (false !== ($pos = stripos($host, ':')))) {
                    $temp = substr($host, $pos + 1);
                    $host = substr($host, 0, $pos);
                    $config['port'] = (false !== $pos = stripos($temp, ';')) ? substr($temp, 0, $pos) : $temp;
                }
                $config['host'] = $host;
            }
            if (!isset($config['database']) && (false !== ($pos = strpos($dsn, 'dbname=')))) {
                $temp = substr($dsn, $pos + 7);
                $config['database'] = (false !== $pos = strpos($temp, ';')) ? substr($temp, 0, $pos) : $temp;
            }
        }

        // must be there
        if (!array_key_exists('database', $config)) {
            $config['database'] = null;
        }

        // must be there
        if (!array_key_exists('prefix', $config)) {
            $config['prefix'] = null;
        }

        // laravel database config requires options to be [], not null
        if (array_key_exists('options', $config) && is_null($config['options'])) {
            $config['options'] = [];
        }
    }

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

        $config = array_get($settings, 'config');
        $config = (empty($config) ? [] : (!is_array($config) ? [$config] : $config));
        Session::replaceLookups($config, true);

        $config['driver'] = 'mongodb';
        $dsn = strval(array_get($config, 'dsn'));
        if (!empty($dsn)) {
            if (0 != substr_compare($dsn, static::DSN_PREFIX, 0, static::DSN_PREFIX_LENGTH, true)) {
                $dsn = static::DSN_PREFIX . $dsn;
            }
        }

        $options = array_get($config, 'options', []);
        if (empty($options)) {
            $options = [];
        }
        $user = array_get($config, 'username');
        $password = array_get($config, 'password');

        // support old configuration options of user, pwd, and db in credentials directly
        if (!isset($options['username']) && isset($user)) {
            $options['username'] = $user;
        }
        if (!isset($options['password']) && isset($password)) {
            $options['password'] = $password;
        }
        if (!isset($options['db']) && (!empty($db = array_get($config, 'db')))) {
            $options['db'] = $db;
        }

        if (!isset($db) && empty($db = array_get($options, 'db'))) {
            //  Attempt to find db in connection string
            $db = strstr(substr($dsn, static::DSN_PREFIX_LENGTH), '/');
            if (false !== $pos = strpos($db, '?')) {
                $db = substr($db, 0, $pos);
            }
            $db = trim($db, '/');
        }

        if (empty($db)) {
            throw new InternalServerErrorException("No MongoDb database selected in configuration.");
        }

        if (!isset($config['database'])) {
            $config['database'] = $db;
        }

        $driverOptions = array_get($config, 'driver_options');
        $driverOptions = (empty($driverOptions) ? [] : (!is_array($driverOptions) ? [$driverOptions] : $driverOptions));
        if (null !== $context = array_get($driverOptions, 'context')) {
            //  Automatically creates a stream from context
            $driverOptions['context'] = stream_context_create($context);
        }

        static::adaptConfig($config);

        // add config to global for reuse, todo check existence and update?
        config(['database.connections.service.' . $this->name => $config]);
        /** @type DatabaseManager $db */
        $db = app('db');
        $this->dbConn = $db->connection('service.' . $this->name);
        $this->schema = new \DreamFactory\Core\MongoDb\Database\Schema\Schema($this->dbConn);

        $this->schema->setCache($this);
        $this->schema->setExtraStore($this);
    }

    /**
     * Object destructor
     */
    public function __destruct()
    {
        try {
            $this->dbConn = null;
        } catch (\Exception $ex) {
            error_log("Failed to disconnect from database.\n{$ex->getMessage()}");
        }
    }

    /**
     * @throws \Exception
     * @return Connection
     */
    public function getConnection()
    {
        if (!isset($this->dbConn)) {
            throw new InternalServerErrorException('Database connection has not been initialized.');
        }

        return $this->dbConn;
    }

    /**
     * @throws \Exception
     * @return SchemaInterface
     */
    public function getSchema()
    {
        if (!isset($this->schema)) {
            throw new InternalServerErrorException('Database schema extension has not been initialized.');
        }

        return $this->schema;
    }

    /**
     * @param null $schema
     * @param bool $refresh
     * @param bool $use_alias
     *
     * @return array|TableSchema[]|mixed
     */
    public function getTableNames($schema = null, $refresh = false, $use_alias = false)
    {
        /** @type TableSchema[] $tables */
        $tables = $this->schema->getTableNames($schema, true, $refresh);
        if ($use_alias) {
            $temp = []; // reassign index to alias
            foreach ($tables as $table) {
                $temp[strtolower($table->getName(true))] = $table;
            }

            return $temp;
        }

        return $tables;
    }

    public function refreshTableCache()
    {
        $this->schema->refresh();
    }
}