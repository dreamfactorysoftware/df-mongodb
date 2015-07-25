<?php
namespace DreamFactory\Core\MongoDb\Resources;

use DreamFactory\Core\Utility\ResourcesWrapper;
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
    protected $parent = null;
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
    public function getResources($only_handlers = false)
    {
        if ($only_handlers) {
            return [];
        }
//        $refresh = $this->request->queryBool('refresh');

        $names = $this->parent->getConnection()->getCollectionNames();

        $extras =
            DbUtilities::getSchemaExtrasForTables($this->parent->getServiceId(), $names, false, 'table,label,plural');

        $tables = [];
        foreach ($names as $name) {
            $label = '';
            $plural = '';
            foreach ($extras as $each) {
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

            $tables[] = ['name' => $name, 'label' => $label, 'plural' => $plural];
        }

        return $tables;
    }

    /**
     * {@inheritdoc}
     */
    public function listAccessComponents($schema = null, $refresh = false)
    {
        $output = [];
        $result = $this->parent->getConnection()->getCollectionNames();
        foreach ($result as $name) {
            $output[] = static::RESOURCE_NAME . '/' . $name;
        }

        return $output;
    }

    /**
     * {@inheritdoc}
     */
    public function updateRecordsByFilter($table, $record, $filter = null, $params = array(), $extras = array())
    {
        $record = DbUtilities::validateAsArray($record, null, false, 'There are no fields in the record.');
        $coll = $this->selectTable($table);

        $fields = ArrayUtils::get($extras, 'fields');
        $ssFilters = ArrayUtils::get($extras, 'ss_filters');

        $fieldsInfo = $this->getFieldsInfo($table);
        $fieldArray = static::buildFieldArray($fields);

        static::removeIds($record, static::DEFAULT_ID_FIELD);
        $parsed = $this->parseRecord($record, $fieldsInfo, $ssFilters, true);
        if (empty($parsed)) {
            throw new BadRequestException('No valid fields found in request: ' . print_r($record, true));
        }

        // build criteria from filter parameters
        $criteria = static::buildCriteriaArray($filter, $params, $ssFilters);

        try {
            $result = $coll->update($criteria, $parsed, array('multiple' => true));
            $rows = static::processResult($result);
            if ($rows > 0) {
                /** @var \MongoCursor $result */
                $result = $coll->find($criteria, $fieldArray);
                $out = iterator_to_array($result);

                return static::cleanRecords($out);
            }

            return array();
        } catch (\Exception $ex) {
            throw new InternalServerErrorException("Failed to update records in '$table'.\n{$ex->getMessage()}");
        }
    }

    /**
     * {@inheritdoc}
     */
    public function patchRecordsByFilter($table, $record, $filter = null, $params = array(), $extras = array())
    {
        $record = DbUtilities::validateAsArray($record, null, false, 'There are no fields in the record.');
        $coll = $this->selectTable($table);

        $fields = ArrayUtils::get($extras, 'fields');
        $ssFilters = ArrayUtils::get($extras, 'ss_filters');

        $fieldsInfo = $this->getFieldsInfo($table);
        $fieldArray = static::buildFieldArray($fields);

        static::removeIds($record, static::DEFAULT_ID_FIELD);
        if (!static::doesRecordContainModifier($record)) {
            $parsed = $this->parseRecord($record, $fieldsInfo, $ssFilters, true);
            if (empty($parsed)) {
                throw new BadRequestException('No valid fields found in request: ' . print_r($record, true));
            }

            $parsed = array('$set' => $parsed);
        } else {
            $parsed = $record;
            if (empty($parsed)) {
                throw new BadRequestException('No valid fields found in request: ' . print_r($record, true));
            }
        }

        // build criteria from filter parameters
        $criteria = static::buildCriteriaArray($filter, $params, $ssFilters);

        try {
            $result = $coll->update($criteria, $parsed, array('multiple' => true));
            $rows = static::processResult($result);
            if ($rows > 0) {
                /** @var \MongoCursor $result */
                $result = $coll->find($criteria, $fieldArray);
                $out = iterator_to_array($result);

                return static::cleanRecords($out);
            }

            return array();
        } catch (\Exception $ex) {
            throw new InternalServerErrorException("Failed to update records in '$table'.\n{$ex->getMessage()}");
        }
    }

    /**
     * {@inheritdoc}
     */
    public function truncateTable($table, $extras = array())
    {
        $coll = $this->selectTable($table);
        try {
            // build filter string if necessary, add server-side filters if necessary
            $ssFilters = ArrayUtils::get($extras, 'ss_filters');
            $criteria = $this->buildCriteriaArray(array(), null, $ssFilters);
            $coll->remove($criteria);

            return array('success' => true);
        } catch (RestException $ex) {
            throw $ex;
        } catch (\Exception $ex) {
            throw new InternalServerErrorException("Failed to delete records from '$table'.\n{$ex->getMessage()}");
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

        $coll = $this->selectTable($table);

        $fields = ArrayUtils::get($extras, 'fields');
        $ssFilters = ArrayUtils::get($extras, 'ss_filters');

        $fieldArray = static::buildFieldArray($fields);

        // build criteria from filter parameters
        $criteria = static::buildCriteriaArray($filter, $params, $ssFilters);

        try {
            /** @var \MongoCursor $result */
            $result = $coll->find($criteria, $fieldArray);
            $out = iterator_to_array($result);
            $coll->remove($criteria);

            return static::cleanRecords($out);
        } catch (\Exception $ex) {
            throw new InternalServerErrorException("Failed to delete records from '$table'.\n{$ex->getMessage()}");
        }
    }

    /**
     * {@inheritdoc}
     */
    public function retrieveRecordsByFilter($table, $filter = null, $params = array(), $extras = array())
    {
        $coll = $this->selectTable($table);

        $fields = ArrayUtils::get($extras, 'fields');
        $ssFilters = ArrayUtils::get($extras, 'ss_filters');

        $fieldArray = static::buildFieldArray($fields);
        $criteria = static::buildCriteriaArray($filter, $params, $ssFilters);

        $limit = intval(ArrayUtils::get($extras, 'limit', 0));
        $offset = intval(ArrayUtils::get($extras, 'offset', 0));
        $sort = static::buildSortArray(ArrayUtils::get($extras, 'order'));
        $addCount = ArrayUtils::getBool($extras, 'include_count', false);

        try {
            /** @var \MongoCursor $result */
            $result = $coll->find($criteria, $fieldArray);
            $count = $result->count();
            $maxAllowed = static::getMaxRecordsReturnedLimit();
            $needMore = (($count - $offset) > $maxAllowed);
            if ($offset) {
                $result = $result->skip($offset);
            }
            if ($sort) {
                $result = $result->sort($sort);
            }
            if (($limit < 1) || ($limit > $maxAllowed)) {
                $limit = $maxAllowed;
            }
            $result = $result->limit($limit);

            $out = iterator_to_array($result);
            $out = static::cleanRecords($out);
            if ($addCount || $needMore) {
                $out['meta']['count'] = $count;
                if ($needMore) {
                    $out['meta']['next'] = $offset + $limit + 1;
                }
            }

            return $out;
        } catch (\Exception $ex) {
            throw new InternalServerErrorException("Failed to filter records from '$table'.\n{$ex->getMessage()}");
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
        $type = ArrayUtils::get($requested_types, 0, 'string');
        $type = (empty($type)) ? 'string' : $type;

        return array(array('name' => static::DEFAULT_ID_FIELD, 'type' => $type, 'required' => false));
    }

    /**
     * @param $record
     *
     * @return bool
     */
    protected static function doesRecordContainModifier($record)
    {
        if (is_array($record)) {
            foreach ($record as $key => $value) {
                if (!empty($key) && ('$' == $key[0])) {
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

        $out = array();
        foreach ($include as $key) {
            $out[$key] = true;
        }

        return $out;
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
            return static::toMongoObjects($filter);
        }

        $search = array(' or ', ' and ', ' nor ');
        $replace = array(' || ', ' && ', ' NOR ');
        $filter = trim(str_ireplace($search, $replace, $filter));

        // handle logical operators first
        $ops = array_map('trim', explode(' || ', $filter));
        if (count($ops) > 1) {
            $parts = array();
            foreach ($ops as $op) {
                $parts[] = static::buildFilterArray($op, $params);
            }

            return array('$or' => $parts);
        }

        $ops = array_map('trim', explode(' NOR ', $filter));
        if (count($ops) > 1) {
            $parts = array();
            foreach ($ops as $op) {
                $parts[] = static::buildFilterArray($op, $params);
            }

            return array('$nor' => $parts);
        }

        $ops = array_map('trim', explode(' && ', $filter));
        if (count($ops) > 1) {
            $parts = array();
            foreach ($ops as $op) {
                $parts[] = static::buildFilterArray($op, $params);
            }

            return array('$and' => $parts);
        }

        // handle negation operator, i.e. starts with NOT?
        if (0 == substr_compare($filter, 'not ', 0, 4, true)) {
            $parts = trim(substr($filter, 4));

            return array('$not' => $parts);
        }

        // the rest should be comparison operators
        $search = array(' eq ', ' ne ', ' gte ', ' lte ', ' gt ', ' lt ', ' in ', ' nin ', ' all ', ' like ', ' <> ');
        $replace = array('=', '!=', '>=', '<=', '>', '<', ' IN ', ' NIN ', ' ALL ', ' LIKE ', '!=');
        $filter = trim(str_ireplace($search, $replace, $filter));

        // Note: order matters, watch '='
        $sqlOperators = array('!=', '>=', '<=', '=', '>', '<', ' IN ', ' NIN ', ' ALL ', ' LIKE ');
        $mongoOperators = array('$ne', '$gte', '$lte', '$eq', '$gt', '$lt', '$in', '$nin', '$all', 'MongoRegex');
        foreach ($sqlOperators as $key => $sqlOp) {
            $ops = array_map('trim', explode($sqlOp, $filter));
            if (count($ops) > 1) {
                $field = $ops[0];
                $val = static::determineValue($ops[1], $field, $params);
                $mongoOp = $mongoOperators[$key];
                switch ($mongoOp) {
                    case '$eq':
                        return array($field => $val);

                    case '$in':
                    case '$nin':
                        // todo check for list of mongoIds
                        $val = array_map('trim', explode(',', trim(trim($val, '(,)'), ',')));
                        $valArray = array();
                        foreach ($val as $item) {
                            $valArray[] = static::determineValue($item, $field, $params);
                        }

                        return array($field => array($mongoOp => $valArray));

                    case 'MongoRegex':
//			WHERE name LIKE "%Joe%"	(array("name" => new MongoRegex("/Joe/")));
//			WHERE name LIKE "Joe%"	(array("name" => new MongoRegex("/^Joe/")));
//			WHERE name LIKE "%Joe"	(array("name" => new MongoRegex("/Joe$/")));
                        $val = static::determineValue($ops[1], $field, $params);
                        if ('%' == $val[strlen($val) - 1]) {
                            if ('%' == $val[0]) {
                                $val = '/' . trim($val, '%') . '/ ';
                            } else {
                                $val = '/^' . rtrim($val, '%') . '/ ';
                            }
                        } else {
                            if ('%' == $val[0]) {
                                $val = '/' . trim($val, '%') . '$/ ';
                            } else {
                                $val = '/' . $val . '/ ';
                            }
                        }

                        return array($field => new \MongoRegex($val));

                    default:
                        return array($field => array($mongoOp => $val));
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
    private static function determineValue($value, $field = null, $replacements = null)
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
        $params = static::toMongoObjects($params);

        // build filter array if necessary
        if (!is_array($filter)) {
//            Session::replaceLookups( $filter );
            $test = json_decode($filter, true);
            if (!is_null($test)) {
                // original filter was a json string, use it as array
                $filter = $test;
            }
        }
        $criteria = static::buildFilterArray($filter, $params);

        // add server-side filters if necessary
        $serverCriteria = static::buildSSFilterArray($ss_filters);
        if (!empty($serverCriteria)) {
            $criteria = (!empty($criteria)) ? array('$and' => array($criteria, $serverCriteria)) : $serverCriteria;
        }

        return $criteria;
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
        $filters = ArrayUtils::get($ss_filters, 'filters');
        if (empty($filters)) {
            return null;
        }

        $criteria = array();
        $combiner = ArrayUtils::get($ss_filters, 'filter_op', 'and');
        foreach ($filters as $filter) {
            $name = ArrayUtils::get($filter, 'name');
            $op = ArrayUtils::get($filter, 'operator');
            if (empty($name) || empty($op)) {
                // log and bail
                throw new InternalServerErrorException('Invalid server-side filter configuration detected.');
            }

            $value = ArrayUtils::get($filter, 'value');
            $value = static::interpretFilterValue($value);

            $criteria[] = static::buildFilterArray("$name $op $value");
        }

        if (1 == count($criteria)) {
            return $criteria[0];
        }

        switch (strtoupper($combiner)) {
            case 'AND':
                $criteria = array('$and' => $criteria);
                break;
            case 'OR':
                $criteria = array('$or' => $criteria);
                break;
            case 'NOR':
                $criteria = array('$nor' => $criteria);
                break;
            default:
                // log and bail
                throw new InternalServerErrorException('Invalid server-side filter configuration detected.');
        }

        return $criteria;
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
        $out = array();
        foreach ($sort as $combo) {
            if (!is_array($combo)) {
                $combo = array_map('trim', explode(' ', trim($combo, ' ')));
            }
            $dir = 1;
            $field = '';
            switch (count($combo)) {
                case 1:
                    $field = $combo[0];
                    break;
                case 2:
                    $field = $combo[0];
                    switch ($combo[1]) {
                        case -1:
                        case 'desc':
                        case 'DESC':
                        case 'dsc':
                        case 'DSC':
                            $dir = -1;
                            break;
                    }
            }
            if (!empty($field)) {
                $out[$field] = $dir;
            }
        }

        return $out;
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
        $out = parent::cleanRecord($record, $include, $id_field);

        return static::fromMongoObjects($out);
    }

    /**
     * @param array $record
     *
     * @return array|string
     */
    protected static function fromMongoObjects(array $record)
    {
        if (!empty($record)) {
            foreach ($record as &$data) {
                if (is_object($data)) {
                    if ($data instanceof \MongoId) {
                        $data = $data->__toString();
                    } elseif ($data instanceof \MongoDate) {
//                        $data = $data->__toString();
                        $data = array('$date' => date(DATE_ISO8601, $data->sec));
                    } elseif ($data instanceof \MongoBinData) {
                        $data = (string)$data;
                    } elseif ($data instanceof \MongoDBRef) {
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
    protected static function toMongoObjects($record)
    {
        if (!empty($record)) {
            foreach ($record as $key => $data) {
                if (!is_object($data)) {
                    if (is_string($data) && (static::DEFAULT_ID_FIELD == $key)) {
                        $record[$key] = static::idToMongoId($data);
                    } elseif (is_array($data)) {
                        if (1 === count($data)) {
                            // using typed definition, i.e. {"$date" : "2014-08-02T08:40:12.569Z" }
                            if (array_key_exists('$date', $data)) {
                                $temp = $data['$date'];
                                if (empty($temp)) {
                                    // empty means create with current time
                                    $record[$key] = new \MongoDate();
                                } elseif (is_string($temp)) {
                                    $record[$key] = new \MongoDate(strtotime($temp));
                                } elseif (is_int($temp)) {
                                    $record[$key] = new \MongoDate($temp);
                                }
                            } elseif (isset($data['$id'])) {
                                $record[$key] = static::idToMongoId($data['$id']);
                            } else {
                                $record[$key] = static::toMongoObjects($data);
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
        foreach ($records as $key => $record) {
            $records[$key] = static::mongoIdToId($record);
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
                    $temp = new \MongoId($value);
                    $value = $temp;
                } catch (\Exception $ex) {
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

        foreach ($ids as &$id) {
            $id = static::idToMongoId($id);
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

        $parsed = (empty($fields_info)) ? $record : array();
        if (!empty($fields_info)) {
            $keys = array_keys($record);
            $values = array_values($record);
            foreach ($fields_info as $fieldInfo) {
                $name = ArrayUtils::get($fieldInfo, 'name', '');
                $type = ArrayUtils::get($fieldInfo, 'type');
                $pos = array_search($name, $keys);
                if (false !== $pos) {
                    $fieldVal = ArrayUtils::get($values, $pos);
                    // due to conversion from XML to array, null or empty xml elements have the array value of an empty array
                    if (is_array($fieldVal) && empty($fieldVal)) {
                        $fieldVal = null;
                    }

                    /** validations **/

                    $validations = ArrayUtils::get($fieldInfo, 'validation');

                    if (!static::validateFieldValue($name, $fieldVal, $validations, $for_update, $fieldInfo)) {
                        unset($keys[$pos]);
                        unset($values[$pos]);
                        continue;
                    }

                    $parsed[$name] = $fieldVal;
                    unset($keys[$pos]);
                    unset($values[$pos]);
                }

                // add or override for specific fields
                switch ($type) {
                    case 'timestamp_on_create':
                        if (!$for_update) {
                            $parsed[$name] = new \MongoDate();
                        }
                        break;
                    case 'timestamp_on_update':
                        $parsed[$name] = new \MongoDate();
                        break;
                    case 'user_id_on_create':
                        if (!$for_update) {
                            $userId = 1; //Session::getCurrentUserId();
                            if (isset($userId)) {
                                $parsed[$name] = $userId;
                            }
                        }
                        break;
                    case 'user_id_on_update':
                        $userId = 1; //Session::getCurrentUserId();
                        if (isset($userId)) {
                            $parsed[$name] = $userId;
                        }
                        break;
                }
            }
        }

        if (!empty($filter_info)) {
            $this->validateRecord($parsed, $filter_info, $for_update, $old_record);
        }

        // convert to native format
        return static::toMongoObjects($parsed);
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

        $errorMsg = ArrayUtils::get($result, 'err');
        if (!empty($errorMsg)) {
            throw new InternalServerErrorException('MongoDb error:' . $errorMsg);
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
        $ssFilters = ArrayUtils::get($extras, 'ss_filters');
        $fields = ArrayUtils::get($extras, 'fields');
        $fieldsInfo = ArrayUtils::get($extras, 'fields_info');
        $requireMore = ArrayUtils::get($extras, 'require_more');
        $updates = ArrayUtils::get($extras, 'updates');

        // convert to native format
        $id = static::idToMongoId($id);

        $fieldArray = ($rollback) ? null : static::buildFieldArray($fields);

        $out = array();
        switch ($this->getAction()) {
            case Verbs::POST:
                $parsed = $this->parseRecord($record, $fieldsInfo, $ssFilters);
                if (empty($parsed)) {
                    throw new BadRequestException('No valid fields were found in record.');
                }

                if (!$continue && !$rollback && !$single) {
                    return parent::addToTransaction($parsed, $id);
                }

                $result = $this->collection->insert($parsed);
                static::processResult($result);

                $out = static::cleanRecord($parsed, $fields, static::DEFAULT_ID_FIELD);

                if ($rollback) {
                    $this->addToRollback(static::recordAsId($parsed, static::DEFAULT_ID_FIELD));
                }
                break;

            case Verbs::PUT:
                if (!empty($updates)) {
                    $parsed = $this->parseRecord($updates, $fieldsInfo, $ssFilters, true);
                    $updates = $parsed;
                } else {
                    $parsed = $this->parseRecord($record, $fieldsInfo, $ssFilters, true);
                }
                if (empty($parsed)) {
                    throw new BadRequestException('No valid fields were found in record.');
                }

                // only update/patch by ids can use batching
                if (!$continue && !$rollback && !$single && !empty($updates)) {
                    return parent::addToTransaction(null, $id);
                }

                $options = array('new' => !$rollback);
                if (empty($updates)) {
                    $out = static::cleanRecord($record, $fields, static::DEFAULT_ID_FIELD);
                    static::removeIds($parsed, static::DEFAULT_ID_FIELD);
                    $updates = $parsed;
                } else {
                    $record = $updates;
                    $record[static::DEFAULT_ID_FIELD] = $id;
                    $out = static::cleanRecord($record, $fields, static::DEFAULT_ID_FIELD);
                }

                // simple update overwrite existing record
                $filter = array(static::DEFAULT_ID_FIELD => $id);
                $criteria = $this->buildCriteriaArray($filter, null, $ssFilters);
                $result = $this->collection->findAndModify($criteria, $updates, $fieldArray, $options);
                if (empty($result)) {
                    throw new NotFoundException("Record with id '$id' not found.");
                }

                if ($rollback) {
                    $this->addToRollback($result);
                } else {
                    $out = static::fromMongoObjects($result);
                }
                break;

            case Verbs::MERGE:
            case Verbs::PATCH:
                if (!empty($updates)) {
                    $parsed = $this->parseRecord($updates, $fieldsInfo, $ssFilters, true);
                    $updates = $parsed;
                } else {
                    $parsed = $this->parseRecord($record, $fieldsInfo, $ssFilters, true);
                }
                if (empty($parsed)) {
                    throw new BadRequestException('No valid fields were found in record.');
                }

                // only update/patch by ids can use batching
                if (!$continue && !$rollback && !$single && !empty($updates)) {
                    return parent::addToTransaction(null, $id);
                }

                $options = array('new' => !$rollback);
                if (empty($updates)) {
                    static::removeIds($parsed, static::DEFAULT_ID_FIELD);
                    $updates = $parsed;
                }

                $updates = array('$set' => $updates);

                // simple merge with existing record
                $filter = array(static::DEFAULT_ID_FIELD => $id);
                $criteria = $this->buildCriteriaArray($filter, null, $ssFilters);
                $result = $this->collection->findAndModify($criteria, $updates, $fieldArray, $options);
                if (empty($result)) {
                    throw new NotFoundException("Record with id '$id' not found.");
                }

                if ($rollback) {
                    $this->addToRollback($result);

                    // need to retrieve the full record here
                    if ($requireMore) {
                        /** @var \MongoCursor $result */
                        $result = $this->collection->findOne($criteria, $fieldArray);
                    } else {
                        $result = array(static::DEFAULT_ID_FIELD => $id);
                    }
                }

                $out = static::fromMongoObjects($result);
                break;

            case Verbs::DELETE:
                if (!$continue && !$rollback && !$single) {
                    return parent::addToTransaction(null, $id);
                }

                $options = array('remove' => true);

                // simple delete existing record
                $filter = array(static::DEFAULT_ID_FIELD => $id);
                $criteria = $this->buildCriteriaArray($filter, null, $ssFilters);
                $result = $this->collection->findAndModify($criteria, null, $fieldArray, $options);
                if (empty($result)) {
                    throw new NotFoundException("Record with id '$id' not found.");
                }

                if ($rollback) {
                    $this->addToRollback($result);
                    $out = static::cleanRecord($record, $fields, static::DEFAULT_ID_FIELD);
                } else {
                    $out = static::fromMongoObjects($result);
                }
                break;

            case Verbs::GET:
                if ($continue && !$single) {
                    return parent::addToTransaction(null, $id);
                }

                $filter = array(static::DEFAULT_ID_FIELD => $id);
                $criteria = $this->buildCriteriaArray($filter, null, $ssFilters);
                $result = $this->collection->findOne($criteria, $fieldArray);
                if (empty($result)) {
                    throw new NotFoundException("Record with id '$id' not found.");
                }

                $out = static::fromMongoObjects($result);
                break;
        }

        return $out;
    }

    /**
     * {@inheritdoc}
     */
    protected function commitTransaction($extras = null)
    {
        if (empty($this->batchRecords) && empty($this->batchIds)) {
            return null;
        }

        $updates = ArrayUtils::get($extras, 'updates');
        $ssFilters = ArrayUtils::get($extras, 'ss_filters');
        $fields = ArrayUtils::get($extras, 'fields');
        $requireMore = ArrayUtils::get($extras, 'require_more');

        $out = array();
        switch ($this->getAction()) {
            case Verbs::POST:
                $result = $this->collection->batchInsert($this->batchRecords, array('continueOnError' => false));
                static::processResult($result);

                $out = static::cleanRecords($this->batchRecords, $fields);
                break;
            case Verbs::PUT:
                if (empty($updates)) {
                    throw new BadRequestException('Batch operation not supported for update by records.');
                }

                $filter = array(static::DEFAULT_ID_FIELD => array('$in' => $this->batchIds));
                $criteria = static::buildCriteriaArray($filter, null, $ssFilters);

                $result = $this->collection->update($criteria, $updates, null, array('multiple' => true));
                $rows = static::processResult($result);
                if (0 === $rows) {
                    throw new NotFoundException('No requested records were found to update.');
                }

                if (count($this->batchIds) !== $rows) {
                    throw new BadRequestException('Batch Error: Not all requested records were found to update.');
                }

                if ($requireMore) {
                    $fieldArray = static::buildFieldArray($fields);
                    /** @var \MongoCursor $result */
                    $result = $this->collection->find($criteria, $fieldArray);
                    $out = static::cleanRecords(iterator_to_array($result));
                } else {
                    $out = static::idsAsRecords(static::mongoIdsToIds($this->batchIds), static::DEFAULT_ID_FIELD);
                }
                break;

            case Verbs::MERGE:
            case Verbs::PATCH:
                if (empty($updates)) {
                    throw new BadRequestException('Batch operation not supported for patch by records.');
                }

                $updates = array('$set' => $updates);

                $filter = array(static::DEFAULT_ID_FIELD => array('$in' => $this->batchIds));
                $criteria = static::buildCriteriaArray($filter, null, $ssFilters);

                $result = $this->collection->update($criteria, $updates, array('multiple' => true));
                $rows = static::processResult($result);
                if (0 === $rows) {
                    throw new NotFoundException('No requested records were found to patch.');
                }

                if (count($this->batchIds) !== $rows) {
                    throw new BadRequestException('Batch Error: Not all requested records were found to patch.');
                }

                if ($requireMore) {
                    $fieldArray = static::buildFieldArray($fields);
                    /** @var \MongoCursor $result */
                    $result = $this->collection->find($criteria, $fieldArray);
                    $out = static::cleanRecords(iterator_to_array($result));
                } else {
                    $out = static::idsAsRecords(static::mongoIdsToIds($this->batchIds), static::DEFAULT_ID_FIELD);
                }
                break;

            case Verbs::DELETE:
                $filter = array(static::DEFAULT_ID_FIELD => array('$in' => $this->batchIds));
                $criteria = static::buildCriteriaArray($filter, null, $ssFilters);

                if ($requireMore) {
                    $fieldArray = static::buildFieldArray($fields);
                    /** @var \MongoCursor $result */
                    $result = $this->collection->find($criteria, $fieldArray);
                    $result = static::cleanRecords(iterator_to_array($result));
                    if (empty($result)) {
                        throw new NotFoundException('No records were found using the given identifiers.');
                    }

                    if (count($this->batchIds) !== count($result)) {
                        $errors = array();
                        foreach ($this->batchIds as $index => $id) {
                            $found = false;
                            foreach ($result as $record) {
                                if ($id == ArrayUtils::get($record, static::DEFAULT_ID_FIELD)) {
                                    $out[$index] = $record;
                                    $found = true;
                                    continue;
                                }
                            }
                            if (!$found) {
                                $errors[] = $index;
                                $out[$index] = "Record with identifier '" . print_r($id, true) . "' not found.";
                            }
                        }
                    } else {
                        $out = $result;
                    }
                } else {
                    $out = static::idsAsRecords(static::mongoIdsToIds($this->batchIds), static::DEFAULT_ID_FIELD);
                }

                $result = $this->collection->remove($criteria);
                $rows = static::processResult($result);
                if (0 === $rows) {
                    throw new NotFoundException('No records were found using the given identifiers.');
                }

                if (count($this->batchIds) !== $rows) {
                    throw new BadRequestException('Batch Error: Not all requested records were deleted.');
                }
                break;

            case Verbs::GET:
                $filter = array(static::DEFAULT_ID_FIELD => array('$in' => $this->batchIds));
                $criteria = static::buildCriteriaArray($filter, null, $ssFilters);
                $fieldArray = static::buildFieldArray($fields);

                /** @var \MongoCursor $result */
                $result = $this->collection->find($criteria, $fieldArray);
                $result = static::cleanRecords(iterator_to_array($result));
                if (empty($result)) {
                    throw new NotFoundException('No records were found using the given identifiers.');
                }

                if (count($this->batchIds) !== count($result)) {
                    $errors = array();
                    foreach ($this->batchIds as $index => $id) {
                        $found = false;
                        foreach ($result as $record) {
                            if ($id == ArrayUtils::get($record, static::DEFAULT_ID_FIELD)) {
                                $out[$index] = $record;
                                $found = true;
                                continue;
                            }
                        }
                        if (!$found) {
                            $errors[] = $index;
                            $out[$index] = "Record with identifier '" . print_r($id, true) . "' not found.";
                        }
                    }

                    if (!empty($errors)) {
                        $wrapper = ResourcesWrapper::getWrapper();
                        $context = array('error' => $errors, $wrapper => $out);
                        throw new NotFoundException('Batch Error: Not all records could be retrieved.', null, null,
                            $context);
                    }
                }

                $out = $result;
                break;

            default:
                break;
        }

        $this->batchIds = array();
        $this->batchRecords = array();

        return $out;
    }

    /**
     * {@inheritdoc}
     */
    protected function rollbackTransaction()
    {
        if (!empty($this->rollbackRecords)) {
            switch ($this->getAction()) {
                case Verbs::POST:
                    // should be ids here from creation
                    $filter = array(static::DEFAULT_ID_FIELD => array('$in' => $this->rollbackRecords));
                    $this->collection->remove($filter);
                    break;

                case Verbs::PUT:
                case Verbs::PATCH:
                case Verbs::MERGE:
                case Verbs::DELETE:
                    foreach ($this->rollbackRecords as $record) {
                        $this->collection->save($record);
                    }
                    break;

                default:
                    break;
            }

            $this->rollbackRecords = array();
        }

        return true;
    }
}