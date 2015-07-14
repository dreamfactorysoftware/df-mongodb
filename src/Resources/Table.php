<?php
namespace DreamFactory\Core\MongoDb\Resources;

use DreamFactory\Library\Utility\ArrayUtils;
use DreamFactory\Library\Utility\Enums\Verbs;
use DreamFactory\Library\Utility\Inflector;
use DreamFactory\Core\Exceptions\BadRequestException;
use DreamFactory\Core\Exceptions\InternalServerErrorException;
use DreamFactory\Core\Exceptions\NotFoundException;
use DreamFactory\Core\Exceptions\RestException;
use DreamFactory\Core\Resources\BaseDbTableResource;
use DreamFactory\Core\Utility\DbUtilities;
use DreamFactory\Core\MongoDb\Services\MongoDb;

class Table extends BaseDbTableResource
{
    //*************************************************************************
    //	Constants
    //*************************************************************************

    /**
     * Default record identifier field
     */
    const DEFAULT_ID_FIELD = '_id';

    //*************************************************************************
    //	Members
    //*************************************************************************

    /**
     * @var null|MongoDb
     */
    protected $service = null;
    /**
     * @var \MongoCollection
     */
    protected $collection = null;

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
    public function updateRecordsByFilter($table, $record, $filter = null, $params = array(), $extras = array())
    {
        $record = DbUtilities::validateAsArray($record, null, false, 'There are no fields in the record.');
        $_coll = $this->selectTable($table);

        $_fields = ArrayUtils::get($extras, 'fields');
        $_ssFilters = ArrayUtils::get($extras, 'ss_filters');

        $_fieldsInfo = $this->getFieldsInfo($table);
        $_fieldArray = static::buildFieldArray($_fields);

        static::removeIds($record, static::DEFAULT_ID_FIELD);
        $_parsed = $this->parseRecord($record, $_fieldsInfo, $_ssFilters, true);
        if (empty($_parsed)) {
            throw new BadRequestException('No valid fields found in request: ' . print_r($record, true));
        }

        // build criteria from filter parameters
        $_criteria = static::buildCriteriaArray($filter, $params, $_ssFilters);

        try {
            $_result = $_coll->update($_criteria, $_parsed, array('multiple' => true));
            $_rows = static::processResult($_result);
            if ($_rows > 0) {
                /** @var \MongoCursor $_result */
                $_result = $_coll->find($_criteria, $_fieldArray);
                $_out = iterator_to_array($_result);

                return static::cleanRecords($_out);
            }

            return array();
        } catch (\Exception $_ex) {
            throw new InternalServerErrorException("Failed to update records in '$table'.\n{$_ex->getMessage()}");
        }
    }

    /**
     * {@inheritdoc}
     */
    public function patchRecordsByFilter($table, $record, $filter = null, $params = array(), $extras = array())
    {
        $record = DbUtilities::validateAsArray($record, null, false, 'There are no fields in the record.');
        $_coll = $this->selectTable($table);

        $_fields = ArrayUtils::get($extras, 'fields');
        $_ssFilters = ArrayUtils::get($extras, 'ss_filters');

        $_fieldsInfo = $this->getFieldsInfo($table);
        $_fieldArray = static::buildFieldArray($_fields);

        static::removeIds($record, static::DEFAULT_ID_FIELD);
        if (!static::doesRecordContainModifier($record)) {
            $_parsed = $this->parseRecord($record, $_fieldsInfo, $_ssFilters, true);
            if (empty($_parsed)) {
                throw new BadRequestException('No valid fields found in request: ' . print_r($record, true));
            }

            $_parsed = array('$set' => $_parsed);
        } else {
            $_parsed = $record;
            if (empty($_parsed)) {
                throw new BadRequestException('No valid fields found in request: ' . print_r($record, true));
            }
        }

        // build criteria from filter parameters
        $_criteria = static::buildCriteriaArray($filter, $params, $_ssFilters);

        try {
            $_result = $_coll->update($_criteria, $_parsed, array('multiple' => true));
            $_rows = static::processResult($_result);
            if ($_rows > 0) {
                /** @var \MongoCursor $_result */
                $_result = $_coll->find($_criteria, $_fieldArray);
                $_out = iterator_to_array($_result);

                return static::cleanRecords($_out);
            }

            return array();
        } catch (\Exception $_ex) {
            throw new InternalServerErrorException("Failed to update records in '$table'.\n{$_ex->getMessage()}");
        }
    }

    /**
     * {@inheritdoc}
     */
    public function truncateTable($table, $extras = array())
    {
        $_coll = $this->selectTable($table);
        try {
            // build filter string if necessary, add server-side filters if necessary
            $_ssFilters = ArrayUtils::get($extras, 'ss_filters');
            $_criteria = $this->buildCriteriaArray(array(), null, $_ssFilters);
            $_coll->remove($_criteria);

            return array('success' => true);
        } catch (RestException $_ex) {
            throw $_ex;
        } catch (\Exception $_ex) {
            throw new InternalServerErrorException("Failed to delete records from '$table'.\n{$_ex->getMessage()}");
        }
    }

    /**
     * {@inheritdoc}
     */
    public function deleteRecordsByFilter($table, $filter, $params = array(), $extras = array())
    {
        if (empty($filter)) {
            throw new BadRequestException("Filter for delete request can not be empty.");
        }

        $_coll = $this->selectTable($table);

        $_fields = ArrayUtils::get($extras, 'fields');
        $_ssFilters = ArrayUtils::get($extras, 'ss_filters');

        $_fieldArray = static::buildFieldArray($_fields);

        // build criteria from filter parameters
        $_criteria = static::buildCriteriaArray($filter, $params, $_ssFilters);

        try {
            /** @var \MongoCursor $_result */
            $_result = $_coll->find($_criteria, $_fieldArray);
            $_out = iterator_to_array($_result);
            $_coll->remove($_criteria);

            return static::cleanRecords($_out);
        } catch (\Exception $_ex) {
            throw new InternalServerErrorException("Failed to delete records from '$table'.\n{$_ex->getMessage()}");
        }
    }

    /**
     * {@inheritdoc}
     */
    public function retrieveRecordsByFilter($table, $filter = null, $params = array(), $extras = array())
    {
        $_coll = $this->selectTable($table);

        $_fields = ArrayUtils::get($extras, 'fields');
        $_ssFilters = ArrayUtils::get($extras, 'ss_filters');

        $_fieldArray = static::buildFieldArray($_fields);
        $_criteria = static::buildCriteriaArray($filter, $params, $_ssFilters);

        $_limit = intval(ArrayUtils::get($extras, 'limit', 0));
        $_offset = intval(ArrayUtils::get($extras, 'offset', 0));
        $_sort = static::buildSortArray(ArrayUtils::get($extras, 'order'));
        $_addCount = ArrayUtils::getBool($extras, 'include_count', false);

        try {
            /** @var \MongoCursor $_result */
            $_result = $_coll->find($_criteria, $_fieldArray);
            $_count = $_result->count();
            $_maxAllowed = static::getMaxRecordsReturnedLimit();
            $_needMore = (($_count - $_offset) > $_maxAllowed);
            if ($_offset) {
                $_result = $_result->skip($_offset);
            }
            if ($_sort) {
                $_result = $_result->sort($_sort);
            }
            if (($_limit < 1) || ($_limit > $_maxAllowed)) {
                $_limit = $_maxAllowed;
            }
            $_result = $_result->limit($_limit);

            $_out = iterator_to_array($_result);
            $_out = static::cleanRecords($_out);
            if ($_addCount || $_needMore) {
                $_out['meta']['count'] = $_count;
                if ($_needMore) {
                    $_out['meta']['next'] = $_offset + $_limit + 1;
                }
            }

            return $_out;
        } catch (\Exception $_ex) {
            throw new InternalServerErrorException("Failed to filter records from '$table'.\n{$_ex->getMessage()}");
        }
    }

    /**
     * @param      $table
     * @param null $fields_info
     * @param null $requested_fields
     * @param null $requested_types
     *
     * @return array
     */
    protected function getIdsInfo($table, $fields_info = null, &$requested_fields = null, $requested_types = null)
    {
        $requested_fields = static::DEFAULT_ID_FIELD; // can only be this
        $requested_types = ArrayUtils::clean($requested_types);
        $_type = ArrayUtils::get($requested_types, 0, 'string');
        $_type = (empty($_type)) ? 'string' : $_type;

        return array(array('name' => static::DEFAULT_ID_FIELD, 'type' => $_type, 'required' => false));
    }

    /**
     * @param $record
     *
     * @return bool
     */
    protected static function doesRecordContainModifier($record)
    {
        if (is_array($record)) {
            foreach ($record as $_key => $_value) {
                if (!empty($_key) && ('$' == $_key[0])) {
                    return true;
                }
            }
        }

        return false;
    }

    /**
     * @param string|array $include List of keys to include in the output record
     *
     * @return array
     */
    protected static function buildFieldArray($include = '*')
    {
        if ('*' == $include) {
            return array();
        }

        if (empty($include)) {
            $include = static::DEFAULT_ID_FIELD;
        }
        if (!is_array($include)) {
            $include = array_map('trim', explode(',', trim($include, ',')));
        }
        if (false === array_search(static::DEFAULT_ID_FIELD, $include)) {
            $include[] = static::DEFAULT_ID_FIELD;
        }

        $_out = array();
        foreach ($include as $key) {
            $_out[$key] = true;
        }

        return $_out;
    }

    /**
     * @param string|array $filter Filter for querying records by
     * @param array        $params Filter replacement parameters
     *
     * @return array
     */
    protected static function buildFilterArray($filter, $params = null)
    {
        if (empty($filter)) {
            return array();
        }

        if (is_array($filter)) {
            // assume client knows correct usage of Mongo query language
            return static::_toMongoObjects($filter);
        }

        $_search = array(' or ', ' and ', ' nor ');
        $_replace = array(' || ', ' && ', ' NOR ');
        $filter = trim(str_ireplace($_search, $_replace, $filter));

        // handle logical operators first
        $_ops = array_map('trim', explode(' || ', $filter));
        if (count($_ops) > 1) {
            $_parts = array();
            foreach ($_ops as $_op) {
                $_parts[] = static::buildFilterArray($_op, $params);
            }

            return array('$or' => $_parts);
        }

        $_ops = array_map('trim', explode(' NOR ', $filter));
        if (count($_ops) > 1) {
            $_parts = array();
            foreach ($_ops as $_op) {
                $_parts[] = static::buildFilterArray($_op, $params);
            }

            return array('$nor' => $_parts);
        }

        $_ops = array_map('trim', explode(' && ', $filter));
        if (count($_ops) > 1) {
            $_parts = array();
            foreach ($_ops as $_op) {
                $_parts[] = static::buildFilterArray($_op, $params);
            }

            return array('$and' => $_parts);
        }

        // handle negation operator, i.e. starts with NOT?
        if (0 == substr_compare($filter, 'not ', 0, 4, true)) {
            $_parts = trim(substr($filter, 4));

            return array('$not' => $_parts);
        }

        // the rest should be comparison operators
        $_search = array(' eq ', ' ne ', ' gte ', ' lte ', ' gt ', ' lt ', ' in ', ' nin ', ' all ', ' like ', ' <> ');
        $_replace = array('=', '!=', '>=', '<=', '>', '<', ' IN ', ' NIN ', ' ALL ', ' LIKE ', '!=');
        $filter = trim(str_ireplace($_search, $_replace, $filter));

        // Note: order matters, watch '='
        $_sqlOperators = array('!=', '>=', '<=', '=', '>', '<', ' IN ', ' NIN ', ' ALL ', ' LIKE ');
        $_mongoOperators = array('$ne', '$gte', '$lte', '$eq', '$gt', '$lt', '$in', '$nin', '$all', 'MongoRegex');
        foreach ($_sqlOperators as $_key => $_sqlOp) {
            $_ops = array_map('trim', explode($_sqlOp, $filter));
            if (count($_ops) > 1) {
                $_field = $_ops[0];
                $_val = static::_determineValue($_ops[1], $_field, $params);
                $_mongoOp = $_mongoOperators[$_key];
                switch ($_mongoOp) {
                    case '$eq':
                        return array($_field => $_val);

                    case '$in':
                    case '$nin':
                        // todo check for list of mongoIds
                        $_val = array_map('trim', explode(',', trim(trim($_val, '(,)'), ',')));
                        $_valArray = array();
                        foreach ($_val as $_item) {
                            $_valArray[] = static::_determineValue($_item, $_field, $params);
                        }

                        return array($_field => array($_mongoOp => $_valArray));

                    case 'MongoRegex':
//			WHERE name LIKE "%Joe%"	(array("name" => new MongoRegex("/Joe/")));
//			WHERE name LIKE "Joe%"	(array("name" => new MongoRegex("/^Joe/")));
//			WHERE name LIKE "%Joe"	(array("name" => new MongoRegex("/Joe$/")));
                        $_val = static::_determineValue($_ops[1], $_field, $params);
                        if ('%' == $_val[strlen($_val) - 1]) {
                            if ('%' == $_val[0]) {
                                $_val = '/' . trim($_val, '%') . '/ ';
                            } else {
                                $_val = '/^' . rtrim($_val, '%') . '/ ';
                            }
                        } else {
                            if ('%' == $_val[0]) {
                                $_val = '/' . trim($_val, '%') . '$/ ';
                            } else {
                                $_val = '/' . $_val . '/ ';
                            }
                        }

                        return array($_field => new \MongoRegex($_val));

                    default:
                        return array($_field => array($_mongoOp => $_val));
                }
            }
        }

        return $filter;
    }

    /**
     * @param string $value
     * @param string $field
     * @param array  $replacements
     *
     * @return bool|float|int|string|\MongoId
     */
    private static function _determineValue($value, $field = null, $replacements = null)
    {
        // process parameter replacements
        if (is_string($value) && !empty($value) && (':' == $value[0])) {
            if (isset($replacements, $replacements[$value])) {
                $value = $replacements[$value];
            }
        }

        if ($field && (static::DEFAULT_ID_FIELD == $field)) {
            $value = static::idToMongoId($value);
        }

        if (is_string($value)) {
            if (trim($value, "'\"") !== $value) {
                return trim($value, "'\""); // meant to be a string
            }

            if (is_numeric($value)) {
                return ($value == strval(intval($value))) ? intval($value) : floatval($value);
            }

            if (0 == strcasecmp($value, 'true')) {
                return true;
            }

            if (0 == strcasecmp($value, 'false')) {
                return false;
            }
        }

        return $value;
    }

    /**
     * @param      $filter
     * @param null $params
     * @param null $ss_filters
     *
     * @return array|mixed
     * @throws InternalServerErrorException
     */
    protected static function buildCriteriaArray($filter, $params = null, $ss_filters = null)
    {
        // interpret any parameter values as lookups
        $params = static::interpretRecordValues($params);
        // or as Mongo objects
        $params = static::_toMongoObjects($params);

        // build filter array if necessary
        if (!is_array($filter)) {
//            Session::replaceLookups( $filter );
            $_test = json_decode($filter, true);
            if (!is_null($_test)) {
                // original filter was a json string, use it as array
                $filter = $_test;
            }
        }
        $_criteria = static::buildFilterArray($filter, $params);

        // add server-side filters if necessary
        $_serverCriteria = static::buildSSFilterArray($ss_filters);
        if (!empty($_serverCriteria)) {
            $_criteria = (!empty($_criteria)) ? array('$and' => array($_criteria, $_serverCriteria)) : $_serverCriteria;
        }

        return $_criteria;
    }

    /**
     * @param $ss_filters
     *
     * @return array
     * @throws InternalServerErrorException
     */
    protected static function buildSSFilterArray($ss_filters)
    {
        if (empty($ss_filters)) {
            return null;
        }

        // build the server side criteria
        $_filters = ArrayUtils::get($ss_filters, 'filters');
        if (empty($_filters)) {
            return null;
        }

        $_criteria = array();
        $_combiner = ArrayUtils::get($ss_filters, 'filter_op', 'and');
        foreach ($_filters as $_filter) {
            $_name = ArrayUtils::get($_filter, 'name');
            $_op = ArrayUtils::get($_filter, 'operator');
            if (empty($_name) || empty($_op)) {
                // log and bail
                throw new InternalServerErrorException('Invalid server-side filter configuration detected.');
            }

            $_value = ArrayUtils::get($_filter, 'value');
            $_value = static::interpretFilterValue($_value);

            $_criteria[] = static::buildFilterArray("$_name $_op $_value");
        }

        if (1 == count($_criteria)) {
            return $_criteria[0];
        }

        switch (strtoupper($_combiner)) {
            case 'AND':
                $_criteria = array('$and' => $_criteria);
                break;
            case 'OR':
                $_criteria = array('$or' => $_criteria);
                break;
            case 'NOR':
                $_criteria = array('$nor' => $_criteria);
                break;
            default:
                // log and bail
                throw new InternalServerErrorException('Invalid server-side filter configuration detected.');
        }

        return $_criteria;
    }

    /**
     * @param string|array $sort List of fields to sort the output records by
     *
     * @return array
     */
    protected static function buildSortArray($sort)
    {
        if (empty($sort)) {
            return null;
        }

        if (!is_array($sort)) {
            $sort = array_map('trim', explode(',', trim($sort, ',')));
        }
        $_out = array();
        foreach ($sort as $_combo) {
            if (!is_array($_combo)) {
                $_combo = array_map('trim', explode(' ', trim($_combo, ' ')));
            }
            $_dir = 1;
            $_field = '';
            switch (count($_combo)) {
                case 1:
                    $_field = $_combo[0];
                    break;
                case 2:
                    $_field = $_combo[0];
                    switch ($_combo[1]) {
                        case -1:
                        case 'desc':
                        case 'DESC':
                        case 'dsc':
                        case 'DSC':
                            $_dir = -1;
                            break;
                    }
            }
            if (!empty($_field)) {
                $_out[$_field] = $_dir;
            }
        }

        return $_out;
    }

    /**
     * @param array $record
     * @param string|array $include List of keys to include in the output record
     * @param string|array $id_field
     *
     * @return array
     */
    protected static function cleanRecord($record = array(), $include = '*', $id_field = null)
    {
        $_out = parent::cleanRecord($record, $include, $id_field);

        return static::_fromMongoObjects($_out);
    }

    /**
     * @param array $record
     *
     * @return array|string
     */
    protected static function _fromMongoObjects(array $record)
    {
        if (!empty($record)) {
            foreach ($record as &$_data) {
                if (is_object($_data)) {
                    if ($_data instanceof \MongoId) {
                        $_data = $_data->__toString();
                    } elseif ($_data instanceof \MongoDate) {
//                        $_data = $data->__toString();
                        $_data = array('$date' => date(DATE_ISO8601, $_data->sec));
                    } elseif ($_data instanceof \MongoBinData) {
                        $_data = (string)$_data;
                    } elseif ($_data instanceof \MongoDBRef) {
                    }
                }
            }
        }

        return $record;
    }

    /**
     * @param array $record
     *
     * @return array
     */
    protected static function _toMongoObjects($record)
    {
        if (!empty($record)) {
            foreach ($record as $_key => $_data) {
                if (!is_object($_data)) {
                    if (is_string($_data) && (static::DEFAULT_ID_FIELD == $_key)) {
                        $record[$_key] = static::idToMongoId($_data);
                    } elseif (is_array($_data)) {
                        if (1 === count($_data)) {
                            // using typed definition, i.e. {"$date" : "2014-08-02T08:40:12.569Z" }
                            if (array_key_exists('$date', $_data)) {
                                $_temp = $_data['$date'];
                                if (empty($_temp)) {
                                    // empty means create with current time
                                    $record[$_key] = new \MongoDate();
                                } elseif (is_string($_temp)) {
                                    $record[$_key] = new \MongoDate(strtotime($_temp));
                                } elseif (is_int($_temp)) {
                                    $record[$_key] = new \MongoDate($_temp);
                                }
                            } elseif (isset($_data['$id'])) {
                                $record[$_key] = static::idToMongoId($_data['$id']);
                            } else {
                                $record[$_key] = static::_toMongoObjects($_data);
                            }
                        }
                    }
                }
            }
        }

        return $record;
    }

    /**
     * @param mixed $value
     *
     * @return array|string
     */
    protected static function mongoIdToId($value)
    {
        if (is_object($value)) {
            /** $record \MongoId */
            $value = (string)$value;
        }

        return $value;
    }

    /**
     * @param array $records
     *
     * @return mixed
     */
    protected static function mongoIdsToIds($records)
    {
        foreach ($records as $key => $_record) {
            $records[$key] = static::mongoIdToId($_record);
        }

        return $records;
    }

    /**
     * @param mixed $value
     *
     * @return array|bool|float|int|\MongoId|string
     */
    protected static function idToMongoId($value)
    {
        if (is_array($value)) {
            if (array_key_exists('$id', $value)) {
                $value = ArrayUtils::get($value, '$id');
            }
        }

        if (is_string($value)) {
            if ((24 == strlen($value))) {
                try {
                    $_temp = new \MongoId($value);
                    $value = $_temp;
                } catch (\Exception $_ex) {
                    // obviously not a Mongo created Id, let it be
                }
            }
        }

        return $value;
    }

    /**
     * @param string|array $ids
     *
     * @return array
     */
    protected static function idsToMongoIds($ids)
    {
        if (!is_array($ids)) {
            // comma delimited list of ids
            $ids = array_map('trim', explode(',', trim($ids, ',')));
        }

        foreach ($ids as &$_id) {
            $_id = static::idToMongoId($_id);
        }

        return $ids;
    }

    /**
     * @param array $record
     * @param array $fields_info
     * @param array $filter_info
     * @param bool  $for_update
     * @param array $old_record
     *
     * @return array
     * @throws \Exception
     */
    protected function parseRecord($record, $fields_info, $filter_info = null, $for_update = false, $old_record = null)
    {
        $record = $this->interpretRecordValues($record);

        switch ($this->getAction()) {
            case Verbs::MERGE:
            case Verbs::PATCH:
                if (static::doesRecordContainModifier($record)) {
                    return $record;
                }
                break;
        }

        $_parsed = (empty($fields_info)) ? $record : array();
        if (!empty($fields_info)) {
            $_keys = array_keys($record);
            $_values = array_values($record);
            foreach ($fields_info as $_fieldInfo) {
                $_name = ArrayUtils::get($_fieldInfo, 'name', '');
                $_type = ArrayUtils::get($_fieldInfo, 'type');
                $_pos = array_search($_name, $_keys);
                if (false !== $_pos) {
                    $_fieldVal = ArrayUtils::get($_values, $_pos);
                    // due to conversion from XML to array, null or empty xml elements have the array value of an empty array
                    if (is_array($_fieldVal) && empty($_fieldVal)) {
                        $_fieldVal = null;
                    }

                    /** validations **/

                    $_validations = ArrayUtils::get($_fieldInfo, 'validation');

                    if (!static::validateFieldValue($_name, $_fieldVal, $_validations, $for_update, $_fieldInfo)) {
                        unset($_keys[$_pos]);
                        unset($_values[$_pos]);
                        continue;
                    }

                    $_parsed[$_name] = $_fieldVal;
                    unset($_keys[$_pos]);
                    unset($_values[$_pos]);
                }

                // add or override for specific fields
                switch ($_type) {
                    case 'timestamp_on_create':
                        if (!$for_update) {
                            $_parsed[$_name] = new \MongoDate();
                        }
                        break;
                    case 'timestamp_on_update':
                        $_parsed[$_name] = new \MongoDate();
                        break;
                    case 'user_id_on_create':
                        if (!$for_update) {
                            $userId = 1; //Session::getCurrentUserId();
                            if (isset($userId)) {
                                $_parsed[$_name] = $userId;
                            }
                        }
                        break;
                    case 'user_id_on_update':
                        $userId = 1; //Session::getCurrentUserId();
                        if (isset($userId)) {
                            $_parsed[$_name] = $userId;
                        }
                        break;
                }
            }
        }

        if (!empty($filter_info)) {
            $this->validateRecord($_parsed, $filter_info, $for_update, $old_record);
        }

        // convert to native format
        return static::_toMongoObjects($_parsed);
    }

    /**
     * @param $result
     *
     * @return int Number of affected records
     * @throws InternalServerErrorException
     */
    protected static function processResult($result)
    {
        if (!is_array($result) || empty($result)) {
            throw new InternalServerErrorException('MongoDb did not return an array, check configuration.');
        }

        $_errorMsg = ArrayUtils::get($result, 'err');
        if (!empty($_errorMsg)) {
            throw new InternalServerErrorException('MongoDb error:' . $_errorMsg);
        }

        return ArrayUtils::get($result, 'n');
    }

    /**
     * {@inheritdoc}
     */
    protected function initTransaction($handle = null)
    {
        $this->collection = $this->selectTable($handle);

        return parent::initTransaction($handle);
    }

    /**
     * {@inheritdoc}
     */
    protected function addToTransaction(
        $record = null,
        $id = null,
        $extras = null,
        $rollback = false,
        $continue = false,
        $single = false
    ){
        $_ssFilters = ArrayUtils::get($extras, 'ss_filters');
        $_fields = ArrayUtils::get($extras, 'fields');
        $_fieldsInfo = ArrayUtils::get($extras, 'fields_info');
        $_requireMore = ArrayUtils::get($extras, 'require_more');
        $_updates = ArrayUtils::get($extras, 'updates');

        // convert to native format
        $id = static::idToMongoId($id);

        $_fieldArray = ($rollback) ? null : static::buildFieldArray($_fields);

        $_out = array();
        switch ($this->getAction()) {
            case Verbs::POST:
                $_parsed = $this->parseRecord($record, $_fieldsInfo, $_ssFilters);
                if (empty($_parsed)) {
                    throw new BadRequestException('No valid fields were found in record.');
                }

                if (!$continue && !$rollback && !$single) {
                    return parent::addToTransaction($_parsed, $id);
                }

                $_result = $this->collection->insert($_parsed);
                static::processResult($_result);

                $_out = static::cleanRecord($_parsed, $_fields, static::DEFAULT_ID_FIELD);

                if ($rollback) {
                    $this->addToRollback(static::recordAsId($_parsed, static::DEFAULT_ID_FIELD));
                }
                break;

            case Verbs::PUT:
                if (!empty($_updates)) {
                    $_parsed = $this->parseRecord($_updates, $_fieldsInfo, $_ssFilters, true);
                    $_updates = $_parsed;
                } else {
                    $_parsed = $this->parseRecord($record, $_fieldsInfo, $_ssFilters, true);
                }
                if (empty($_parsed)) {
                    throw new BadRequestException('No valid fields were found in record.');
                }

                // only update/patch by ids can use batching
                if (!$continue && !$rollback && !$single && !empty($_updates)) {
                    return parent::addToTransaction(null, $id);
                }

                $_options = array('new' => !$rollback);
                if (empty($_updates)) {
                    $_out = static::cleanRecord($record, $_fields, static::DEFAULT_ID_FIELD);
                    static::removeIds($_parsed, static::DEFAULT_ID_FIELD);
                    $_updates = $_parsed;
                } else {
                    $record = $_updates;
                    $record[static::DEFAULT_ID_FIELD] = $id;
                    $_out = static::cleanRecord($record, $_fields, static::DEFAULT_ID_FIELD);
                }

                // simple update overwrite existing record
                $_filter = array(static::DEFAULT_ID_FIELD => $id);
                $_criteria = $this->buildCriteriaArray($_filter, null, $_ssFilters);
                $_result = $this->collection->findAndModify($_criteria, $_updates, $_fieldArray, $_options);
                if (empty($_result)) {
                    throw new NotFoundException("Record with id '$id' not found.");
                }

                if ($rollback) {
                    $this->addToRollback($_result);
                } else {
                    $_out = static::_fromMongoObjects($_result);
                }
                break;

            case Verbs::MERGE:
            case Verbs::PATCH:
                if (!empty($_updates)) {
                    $_parsed = $this->parseRecord($_updates, $_fieldsInfo, $_ssFilters, true);
                    $_updates = $_parsed;
                } else {
                    $_parsed = $this->parseRecord($record, $_fieldsInfo, $_ssFilters, true);
                }
                if (empty($_parsed)) {
                    throw new BadRequestException('No valid fields were found in record.');
                }

                // only update/patch by ids can use batching
                if (!$continue && !$rollback && !$single && !empty($_updates)) {
                    return parent::addToTransaction(null, $id);
                }

                $_options = array('new' => !$rollback);
                if (empty($_updates)) {
                    static::removeIds($_parsed, static::DEFAULT_ID_FIELD);
                    $_updates = $_parsed;
                }

                $_updates = array('$set' => $_updates);

                // simple merge with existing record
                $_filter = array(static::DEFAULT_ID_FIELD => $id);
                $_criteria = $this->buildCriteriaArray($_filter, null, $_ssFilters);
                $_result = $this->collection->findAndModify($_criteria, $_updates, $_fieldArray, $_options);
                if (empty($_result)) {
                    throw new NotFoundException("Record with id '$id' not found.");
                }

                if ($rollback) {
                    $this->addToRollback($_result);

                    // need to retrieve the full record here
                    if ($_requireMore) {
                        /** @var \MongoCursor $_result */
                        $_result = $this->collection->findOne($_criteria, $_fieldArray);
                    } else {
                        $_result = array(static::DEFAULT_ID_FIELD => $id);
                    }
                }

                $_out = static::_fromMongoObjects($_result);
                break;

            case Verbs::DELETE:
                if (!$continue && !$rollback && !$single) {
                    return parent::addToTransaction(null, $id);
                }

                $_options = array('remove' => true);

                // simple delete existing record
                $_filter = array(static::DEFAULT_ID_FIELD => $id);
                $_criteria = $this->buildCriteriaArray($_filter, null, $_ssFilters);
                $_result = $this->collection->findAndModify($_criteria, null, $_fieldArray, $_options);
                if (empty($_result)) {
                    throw new NotFoundException("Record with id '$id' not found.");
                }

                if ($rollback) {
                    $this->addToRollback($_result);
                    $_out = static::cleanRecord($record, $_fields, static::DEFAULT_ID_FIELD);
                } else {
                    $_out = static::_fromMongoObjects($_result);
                }
                break;

            case Verbs::GET:
                if ($continue && !$single) {
                    return parent::addToTransaction(null, $id);
                }

                $_filter = array(static::DEFAULT_ID_FIELD => $id);
                $_criteria = $this->buildCriteriaArray($_filter, null, $_ssFilters);
                $_result = $this->collection->findOne($_criteria, $_fieldArray);
                if (empty($_result)) {
                    throw new NotFoundException("Record with id '$id' not found.");
                }

                $_out = static::_fromMongoObjects($_result);
                break;
        }

        return $_out;
    }

    /**
     * {@inheritdoc}
     */
    protected function commitTransaction($extras = null)
    {
        if (empty($this->_batchRecords) && empty($this->_batchIds)) {
            return null;
        }

        $_updates = ArrayUtils::get($extras, 'updates');
        $_ssFilters = ArrayUtils::get($extras, 'ss_filters');
        $_fields = ArrayUtils::get($extras, 'fields');
        $_requireMore = ArrayUtils::get($extras, 'require_more');

        $_out = array();
        switch ($this->getAction()) {
            case Verbs::POST:
                $_result = $this->collection->batchInsert($this->_batchRecords, array('continueOnError' => false));
                static::processResult($_result);

                $_out = static::cleanRecords($this->_batchRecords, $_fields);
                break;
            case Verbs::PUT:
                if (empty($_updates)) {
                    throw new BadRequestException('Batch operation not supported for update by records.');
                }

                $_filter = array(static::DEFAULT_ID_FIELD => array('$in' => $this->_batchIds));
                $_criteria = static::buildCriteriaArray($_filter, null, $_ssFilters);

                $_result = $this->collection->update($_criteria, $_updates, null, array('multiple' => true));
                $_rows = static::processResult($_result);
                if (0 === $_rows) {
                    throw new NotFoundException('No requested records were found to update.');
                }

                if (count($this->_batchIds) !== $_rows) {
                    throw new BadRequestException('Batch Error: Not all requested records were found to update.');
                }

                if ($_requireMore) {
                    $_fieldArray = static::buildFieldArray($_fields);
                    /** @var \MongoCursor $_result */
                    $_result = $this->collection->find($_criteria, $_fieldArray);
                    $_out = static::cleanRecords(iterator_to_array($_result));
                } else {
                    $_out = static::idsAsRecords(static::mongoIdsToIds($this->_batchIds), static::DEFAULT_ID_FIELD);
                }
                break;

            case Verbs::MERGE:
            case Verbs::PATCH:
                if (empty($_updates)) {
                    throw new BadRequestException('Batch operation not supported for patch by records.');
                }

                $_updates = array('$set' => $_updates);

                $_filter = array(static::DEFAULT_ID_FIELD => array('$in' => $this->_batchIds));
                $_criteria = static::buildCriteriaArray($_filter, null, $_ssFilters);

                $_result = $this->collection->update($_criteria, $_updates, array('multiple' => true));
                $_rows = static::processResult($_result);
                if (0 === $_rows) {
                    throw new NotFoundException('No requested records were found to patch.');
                }

                if (count($this->_batchIds) !== $_rows) {
                    throw new BadRequestException('Batch Error: Not all requested records were found to patch.');
                }

                if ($_requireMore) {
                    $_fieldArray = static::buildFieldArray($_fields);
                    /** @var \MongoCursor $_result */
                    $_result = $this->collection->find($_criteria, $_fieldArray);
                    $_out = static::cleanRecords(iterator_to_array($_result));
                } else {
                    $_out = static::idsAsRecords(static::mongoIdsToIds($this->_batchIds), static::DEFAULT_ID_FIELD);
                }
                break;

            case Verbs::DELETE:
                $_filter = array(static::DEFAULT_ID_FIELD => array('$in' => $this->_batchIds));
                $_criteria = static::buildCriteriaArray($_filter, null, $_ssFilters);

                if ($_requireMore) {
                    $_fieldArray = static::buildFieldArray($_fields);
                    /** @var \MongoCursor $_result */
                    $_result = $this->collection->find($_criteria, $_fieldArray);
                    $_result = static::cleanRecords(iterator_to_array($_result));
                    if (empty($_result)) {
                        throw new NotFoundException('No records were found using the given identifiers.');
                    }

                    if (count($this->_batchIds) !== count($_result)) {
                        $_errors = array();
                        foreach ($this->_batchIds as $_index => $_id) {
                            $_found = false;
                            foreach ($_result as $_record) {
                                if ($_id == ArrayUtils::get($_record, static::DEFAULT_ID_FIELD)) {
                                    $_out[$_index] = $_record;
                                    $_found = true;
                                    continue;
                                }
                            }
                            if (!$_found) {
                                $_errors[] = $_index;
                                $_out[$_index] = "Record with identifier '" . print_r($_id, true) . "' not found.";
                            }
                        }
                    } else {
                        $_out = $_result;
                    }
                } else {
                    $_out = static::idsAsRecords(static::mongoIdsToIds($this->_batchIds), static::DEFAULT_ID_FIELD);
                }

                $_result = $this->collection->remove($_criteria);
                $_rows = static::processResult($_result);
                if (0 === $_rows) {
                    throw new NotFoundException('No records were found using the given identifiers.');
                }

                if (count($this->_batchIds) !== $_rows) {
                    throw new BadRequestException('Batch Error: Not all requested records were deleted.');
                }
                break;

            case Verbs::GET:
                $_filter = array(static::DEFAULT_ID_FIELD => array('$in' => $this->_batchIds));
                $_criteria = static::buildCriteriaArray($_filter, null, $_ssFilters);
                $_fieldArray = static::buildFieldArray($_fields);

                /** @var \MongoCursor $_result */
                $_result = $this->collection->find($_criteria, $_fieldArray);
                $_result = static::cleanRecords(iterator_to_array($_result));
                if (empty($_result)) {
                    throw new NotFoundException('No records were found using the given identifiers.');
                }

                if (count($this->_batchIds) !== count($_result)) {
                    $_errors = array();
                    foreach ($this->_batchIds as $_index => $_id) {
                        $_found = false;
                        foreach ($_result as $_record) {
                            if ($_id == ArrayUtils::get($_record, static::DEFAULT_ID_FIELD)) {
                                $_out[$_index] = $_record;
                                $_found = true;
                                continue;
                            }
                        }
                        if (!$_found) {
                            $_errors[] = $_index;
                            $_out[$_index] = "Record with identifier '" . print_r($_id, true) . "' not found.";
                        }
                    }

                    if (!empty($_errors)) {
                        $wrapper = \Config::get('df.resources_wrapper', 'resource');
                        $_context = array('error' => $_errors, $wrapper => $_out);
                        throw new NotFoundException('Batch Error: Not all records could be retrieved.', null, null,
                            $_context);
                    }
                }

                $_out = $_result;
                break;

            default:
                break;
        }

        $this->_batchIds = array();
        $this->_batchRecords = array();

        return $_out;
    }

    /**
     * {@inheritdoc}
     */
    protected function rollbackTransaction()
    {
        if (!empty($this->_rollbackRecords)) {
            switch ($this->getAction()) {
                case Verbs::POST:
                    // should be ids here from creation
                    $_filter = array(static::DEFAULT_ID_FIELD => array('$in' => $this->_rollbackRecords));
                    $this->collection->remove($_filter);
                    break;

                case Verbs::PUT:
                case Verbs::PATCH:
                case Verbs::MERGE:
                case Verbs::DELETE:
                    foreach ($this->_rollbackRecords as $_record) {
                        $this->collection->save($_record);
                    }
                    break;

                default:
                    break;
            }

            $this->_rollbackRecords = array();
        }

        return true;
    }
}