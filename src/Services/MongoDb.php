<?php
namespace DreamFactory\Core\MongoDb\Services;

use DreamFactory\Core\Components\DbSchemaExtras;
use DreamFactory\Core\Components\RequireExtensions;
use DreamFactory\Core\Contracts\CacheInterface;
use DreamFactory\Core\Contracts\ConnectionInterface;
use DreamFactory\Core\Contracts\DbExtrasInterface;
use DreamFactory\Core\Database\TableSchema;
use DreamFactory\Core\Exceptions\InternalServerErrorException;
use DreamFactory\Core\MongoDb\Resources\Schema;
use DreamFactory\Core\MongoDb\Resources\Table;
use DreamFactory\Core\Services\BaseNoSqlDbService;
use DreamFactory\Core\Utility\Session;
use Illuminate\Database\DatabaseManager;
use MongoDB\Client;
use MongoDB\Database;

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

    use RequireExtensions;
    use DbSchemaExtras;

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
     * @var Database
     */
    protected $dbConn = null;
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

        // add config to global for reuse, todo check existence and update?
        config(['database.connections.service.' . $this->name => $config]);
        /** @type DatabaseManager $db */
        $db = app('db');
        /** @type ConnectionInterface $client */
        $client = $db->connection('service.' . $this->name);

        $client->setCache($this);
        $client->setExtraStore($this);

        $this->dbConn = $client->getMongoDb();

//        try {
//            $client = new Client($dsn, $options, $driverOptions);
//
//            $this->dbConn = $client->selectDatabase($db);
//        } catch (\Exception $ex) {
//            throw new InternalServerErrorException("Unexpected MongoDb Service Exception:\n{$ex->getMessage()}");
//        }
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
     * @return Database
     */
    public function getConnection()
    {
        if (!isset($this->dbConn)) {
            throw new InternalServerErrorException('Database connection has not been initialized.');
        }

        return $this->dbConn;
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
        if ($refresh ||
            (empty($this->tableNames) &&
                (null === $this->tableNames = $this->getFromCache('table_names')))
        ) {
            /** @type TableSchema[] $tables */
            $names = [];
            $tables = [];
            $collections = $this->dbConn->listCollections();
            foreach ($collections as $collection) {
                $name = $collection->getName();
                $names[] = $name;
                $tables[strtolower($name)] = new TableSchema(['name' => $name]);
            }
            // merge db extras
            if (!empty($extrasEntries = $this->getSchemaExtrasForTables($names, false))) {
                foreach ($extrasEntries as $extras) {
                    if (!empty($extraName = strtolower(strval($extras['table'])))) {
                        if (array_key_exists($extraName, $tables)) {
                            $tables[$extraName]->fill($extras);
                        }
                    }
                }
            }
            $this->tableNames = $tables;
            $this->addToCache('table_names', $this->tableNames, true);
        }

        return $this->tableNames;
    }

    /**
     *
     */
    public function refreshTableCache()
    {
        $this->removeFromCache('table_names');
        $this->tableNames = [];
        $this->tables = [];
    }
}