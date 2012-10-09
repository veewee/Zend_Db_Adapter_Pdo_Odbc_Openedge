<?php

/**
 * Zend Framework
 *
 * LICENSE
 *
 * This source file is subject to the new BSD license that is bundled
 * with this package in the file LICENSE.txt.
 * It is also available through the world-wide-web at this URL:
 * http://framework.zend.com/license/new-bsd
 * If you did not receive a copy of the license and are unable to
 * obtain it through the world-wide-web, please send an email
 * to license@zend.com so we can send you a copy immediately.
 *
 * @category   Zend
 * @package    Zend_Db
 * @subpackage Adapter
 * @copyright  Copyright (c) 2005-2012 Zend Technologies USA Inc. (http://www.zend.com)
 * @license    http://framework.zend.com/license/new-bsd     New BSD License
 * @version    unofficial
 */
/**
 * @see Zend_Db_Adapter_Pdo_Odbc
 */
require_once 'Zend/Db/Adapter/Pdo/Odbc.php';

/**
 * Class for connecting to various odbc providers
 *
 * @category   Zend
 * @package    Zend_Db
 * @subpackage Adapter
 * @author Toon Verwerft
 */
class Zend_Db_Adapter_Pdo_Odbc_Openedge extends Zend_Db_Adapter_Pdo_Odbc 
{

    /**
     * Keys are UPPERCASE SQL datatypes or the constants
     * Zend_Db::INT_TYPE, Zend_Db::BIGINT_TYPE, or Zend_Db::FLOAT_TYPE.
     *
     * Values are:
     * 0 = 32-bit integer
     * 1 = 64-bit integer
     * 2 = float or decimal
     *
     * @var array Associative array of datatypes to values 0, 1, or 2.
     */
    protected $_numericDataTypes = array(
        Zend_Db::INT_TYPE => Zend_Db::INT_TYPE,
        Zend_Db::BIGINT_TYPE => Zend_Db::BIGINT_TYPE,
        Zend_Db::FLOAT_TYPE => Zend_Db::FLOAT_TYPE,
        'BIT' => Zend_Db::INT_TYPE,
        'INTEGER' => Zend_Db::INT_TYPE,
        'SMALLINT' => Zend_Db::INT_TYPE,
        'TINYINT' => Zend_Db::INT_TYPE,
        'BIGINT' => Zend_Db::BIGINT_TYPE,
        'DECIMAL' => Zend_Db::FLOAT_TYPE,
        'FLOAT' => Zend_Db::FLOAT_TYPE,
        'MONEY' => Zend_Db::FLOAT_TYPE,
        'NUMERIC' => Zend_Db::FLOAT_TYPE,
        'NUMBER' => Zend_Db::FLOAT_TYPE,
        'REAL' => Zend_Db::FLOAT_TYPE,
        'SMALLMONEY' => Zend_Db::FLOAT_TYPE
    );

    /**
     * Creates a PDO DSN for the adapter from $this->_config settings.
     *
     * @return string
     */
    protected function _dsn() 
    {
        // validate the driver
        if (!isset($this->_config['driver']) || stristr($this->_config['driver'], 'Progress OpenEdge') === false) {
            /** @see Zend_Db_Adapter_Exception */
            require_once 'Zend/Db/Adapter/Exception.php';
            throw new Zend_Db_Adapter_Exception('Invalid driver "' . $this->_config['driver'] . '" specified for an OpenEdge connection');
        }

        // OpenEdge uses db instead of dbname in the connection string
        $this->_config['db'] = $this->_config['dbname'];

        return parent::_dsn();
    }

    /**
     * @return void
     */
    protected function _connect() 
    {
        if ($this->_connection) {
            return;
        }

        parent::_connect();

        // set default schema if set
        if (isset($this->_config['schema'])) {
            $this->query('SET SCHEMA ' . $this->quote($this->_config['schema']));
        }
        
        // set default character set:
        if (isset($this->_config['charset'])) {
            
        }
    }

    /**
     * Adds an adapter-specific LIMIT clause to the SELECT statement.
     * This is the same function as in the MSSQL implementation:
     *
     * @link http://lists.bestpractical.com/pipermail/rt-devel/2005-June/007339.html
     *
     * @param string $sql
     * @param integer $count
     * @param integer $offset OPTIONAL
     * @throws Zend_Db_Adapter_Exception
     * @return string
     */
    public function limit($sql, $count, $offset = 0) 
    {
        $count = intval($count);
        if ($count <= 0) {
            /** @see Zend_Db_Adapter_Exception */
            require_once 'Zend/Db/Adapter/Exception.php';
            throw new Zend_Db_Adapter_Exception("LIMIT argument count=$count is not valid");
        }

        $offset = intval($offset);
        if ($offset < 0) {
            /** @see Zend_Db_Adapter_Exception */
            require_once 'Zend/Db/Adapter/Exception.php';
            throw new Zend_Db_Adapter_Exception("LIMIT argument offset=$offset is not valid");
        }

        $sql = preg_replace(
                '/^SELECT\s+(DISTINCT\s)?/i', 'SELECT $1TOP ' . ($count + $offset) . ' ', $sql
        );

        if ($offset > 0) {
            $orderby = stristr($sql, 'ORDER BY');

            if ($orderby !== false) {
                $orderParts = explode(',', substr($orderby, 8));
                $pregReplaceCount = null;
                $orderbyInverseParts = array();
                foreach ($orderParts as $orderPart) {
                    $orderPart = rtrim($orderPart);
                    $inv = preg_replace('/\s+desc$/i', ' ASC', $orderPart, 1, $pregReplaceCount);
                    if ($pregReplaceCount) {
                        $orderbyInverseParts[] = $inv;
                        continue;
                    }
                    $inv = preg_replace('/\s+asc$/i', ' DESC', $orderPart, 1, $pregReplaceCount);
                    if ($pregReplaceCount) {
                        $orderbyInverseParts[] = $inv;
                        continue;
                    } else {
                        $orderbyInverseParts[] = $orderPart . ' DESC';
                    }
                }

                $orderbyInverse = 'ORDER BY ' . implode(', ', $orderbyInverseParts);
            }

            $sql = 'SELECT * FROM (SELECT TOP ' . $count . ' * FROM (' . $sql . ') AS inner_tbl';
            if ($orderby !== false) {
                $sql .= ' ' . $orderbyInverse . ' ';
            }
            $sql .= ') AS outer_tbl';
            if ($orderby !== false) {
                $sql .= ' ' . $orderby;
            }
        }

        return $sql;
    }

    /**
     * Returns a list of the tables in the database.
     *
     * @return array
     */
    public function listTables() 
    {
        $sql = "SELECT TBL FROM sysprogress.SYSTABLES";
        return $this->fetchCol($sql);
    }

    /**
     * Returns the column descriptions for a table.
     *
     * The return value is an associative array keyed by the column name,
     * as returned by the RDBMS.
     *
     * The value of each array element is an associative array
     * with the following keys:
     *
     * SCHEMA_NAME      => string; name of database or schema
     * TABLE_NAME       => string;
     * COLUMN_NAME      => string; column name
     * COLUMN_POSITION  => number; ordinal position of column in table
     * DATA_TYPE        => string; SQL datatype name of column
     * DEFAULT          => string; default expression of column, null if none
     * NULLABLE         => boolean; true if column can have nulls
     * LENGTH           => number; length of CHAR/VARCHAR
     * SCALE            => number; scale of NUMERIC/DECIMAL
     * PRECISION        => number; precision of NUMERIC/DECIMAL
     * UNSIGNED         => boolean; unsigned property of an integer type
     * PRIMARY          => boolean; true if column is part of the primary key
     * PRIMARY_POSITION => integer; position of column in primary key
     * PRIMARY_AUTO     => integer; position of auto-generated column in primary key
     *
     * @param string $tableName
     * @param string $schemaName OPTIONAL
     * @return array
     */
    public function describeTable($tableName, $schemaName = null) 
    {
        // build query:
        $sql = 'SELECT * FROM sysprogress."SYSCOLUMNS_FULL" WHERE TBL = ' . $this->quote($tableName);
        if ($schemaName != null) {
            $sql .= ' AND OWNER = ' . $this->quote($schemaName);
        }

        // get records:
        $stmt = $this->query($sql);
        $result = $stmt->fetchAll(Zend_Db::FETCH_ASSOC);

        // get primary keys:
        $primaryKeys = $this->_findPrimaryKeys($tableName);

        // run over columns
        $desc = array();
        foreach ($result as $columnInfo) {
            // Check primary key, position is 1-based array:
            $isPrimary = in_array($columnInfo['COL'], $primaryKeys);
            $primaryPos = ($isPrimary) ? (array_search($columnInfo['COL'], $primaryKeys) + 1) : null;
            
            // build data:
            $desc[$this->foldCase($columnInfo['COL'])] = array(
                'SCHEMA_NAME' => $columnInfo['OWNER'],
                'TABLE_NAME' => $this->foldCase($columnInfo['TBL']),
                'COLUMN_NAME' => $this->foldCase($columnInfo['COL']),
                'COLUMN_POSITION' => (int) $columnInfo['ID'],
                'DATA_TYPE' => $columnInfo['COLTYPE'],
                'DEFAULT' => $columnInfo['DFLT_VALUE'],
                'NULLABLE' => (bool) $columnInfo['NULLFLAG'],
                'LENGTH' => $columnInfo['WIDTH'],
                'SCALE' => $columnInfo['SCALE'],
                'PRECISION' => null,
                'UNSIGNED' => null,
                'PRIMARY' => $isPrimary,
                'PRIMARY_POSITION' => $primaryPos,
                'IDENTITY' => null
            );
        }
        
        
        return $desc;
    }

    /**
     * Function that searches all primary key columns in a table
     * @param string $tableName The table we are exploring
     * @return array Column-names which are the primary keys
     */
    protected function _findPrimaryKeys($tableName) 
    {
        $sql = $this->select()
                ->from('pub._index', array('_index-Name'))
                ->joinInner('SYSPROGRESS.SYSTABLES_FULL', $this->quoteIdentifier('SYSPROGRESS.SYSTABLES_FULL.PRIME_INDEX') . ' = ' . $this->quoteIdentifier('pub._index.rowid'), array())
                ->where('SYSPROGRESS.SYSTABLES_FULL.TBLTYPE = ?', 'T')
                ->where('SYSPROGRESS.SYSTABLES_FULL.TBL = ?', $tableName);

        return $this->fetchCol($sql);
    }

    /**
     * Quote a raw string. The PDO::quote function does not work on the OpenEdge driver
     * @param string $value     Raw string
     * @return string           Quoted string
     */
    protected function _quote($value) 
    {
        if (is_int($value) || is_float($value)) {
            return $value;
        }
        return "'" . str_replace("'", "''", $value) . "'";
    }
    
}