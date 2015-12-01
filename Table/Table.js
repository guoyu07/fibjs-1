/**
 * Table class
 * @author dotcoo zhao <dotcoo@163.com>
 * @link http://www.dotcoo.com/table
 */

var db = require("db");
var util = require("util");
var collection = require("collection");

/**
 * 构造函数
 * @param string table 表名
 * @param string pk 主键
 * @param DbConnection pdo 数据库连接
 */
function Table(table, pk, pdo) {
		// 初始化属性
		this._pdo = null;
		this._table = null;
		this._pk = "id";
		this._keywords = [];
		this._where = [];
		this._where_params = [];
		this._count_where = [];
		this._count_where_params = [];
		this._group = null;
		this._having = [];
		this._having_params = [];
		this._order = null;
		this._limit = null;
		this._offset = null;
		this._for_update = "";
		this._lock_in_share_model = "";

		// 参数
		this._table = table;
		this._pk = pk || this._pk;
		this._pdo = pdo || null;
};

// 默认设置
Table.prototype.__pdo = null;					// 默认PDO对象
Table.prototype.__host = "127.0.0.1";			// 默认主机
Table.prototype.__user = "root";				// 默认账户
Table.prototype.__password = "123456";			// 默认密码
Table.prototype.__dbname = "test";				// 默认数据库名称
Table.prototype.__charset = "utf8";				// 默认字符集

// 私有属性
Table.prototype._pdo = null;					// PDO对象
Table.prototype._table = null;					// table
Table.prototype._pk = "id";						// paramry
Table.prototype._keywords = [];					// keywords
Table.prototype._where = [];					// where
Table.prototype._where_params = [];				// where params
Table.prototype._count_where = [];				// count where
Table.prototype._count_where_params = [];		// count where params
Table.prototype._group = null;					// group
Table.prototype._having = [];					// having
Table.prototype._having_params = [];			// having params
Table.prototype._order = null;					// order
Table.prototype._limit = null;					// limit
Table.prototype._offset = null;					// offset
Table.prototype._for_update = "";				// read lock
Table.prototype._lock_in_share_model = "";		// write lock

// 属性
Table.prototype.debug = false;					// 调试模式

/**
 * 获取数据库连接
 * @return DbConnection
 */
Table.prototype.getPDO = function() {
	if (this._pdo) {
		return this._pdo;
	}

	if (this.__pdo) {
		return this.__pdo;
	}

	var dsn = util.format("mysql://%s:%s@%s/%s", this.__user, this.__password, this.__host, this.__dbname);
	var pdo = Table.prototype.__pdo = db.open(dsn);
	pdo.execute("set names " + this.__charset);
	return pdo;
};

/**
 * 获取主键列名
 * @return string
 */
Table.prototype.getPK = function() {
	return this._pk;
}

/**
 * 执行语句
 * @param string sql
 * @return DBResult
 */
Table.prototype.query = function(sql) {
	var params = Array.apply(null, arguments);
	params.shift();
	return this.vquery(sql, params);
}

/**
 * 执行语句
 * @param string sql
 * @return DBResult
 */
Table.prototype.vquery = function(sql, params) {
	var sqls = sql.split("?");
	var sql_new = sqls.shift();
	var params_new = [];
	for (var i in sqls) {
		var sql_item = sqls[i];
		if (Array.isArray(params[i])) {
			sql_new += "?,".repeat(params[i].length-1)+"?"+sql_item;
			params_new = params_new.concat(params[i]);
		} else {
			sql_new += "?"+sql_item;
			params_new.push(params[i]);
		}
	}
	if (this.debug) {
		console.log(sql_new, "|", params_new.join(", "));
	}
	var conn = this.getPDO();
	var stmt = conn.execute.apply(conn, [sql_new].concat(params_new));
	this.reset();
	return stmt;
}

/**
 * 查询数据
 * @param string field
 * @return DBResult
 */
Table.prototype.select = function(columns) {
	columns = columns || "*";
	var params = this._where_params.concat(this._having_params);
	var keywords = this._keywords.length == 0 ? "" : " "+this._keywords.join(" ");
	var sql = util.format("SELECT%s %s FROM `%s`", keywords, columns, this._table);
	sql += this._where.length == 0 ? "" : " WHERE " + this._where.join(" AND ");
	sql += this._group == null ? "" : " GROUP BY " + this._group;
	sql += this._having.length == 0 ? "" : " HAVING " + this._having.join(" AND ");
	sql += this._order == null ? "" : " ORDER BY " + this._order;
	if (this._limit) {
		sql += " LIMIT ?";
		params.push(this._limit);
		if (this._offset) {
			sql += " OFFSET ?";
			params.push(this._offset);
		}
	}
	sql += this._for_update;
	sql += this._lock_in_share_model;
	this._count_where = this._where;
	this._count_where_params = this._where_params;
	return this.vquery(sql, params);
}


/**
 * 添加数据
 * @param array data
 * @return DBResult
 */
Table.prototype.insert = function(data) {
	var sql = util.format("INSERT `%s` SET", this._table);
	var params = [];
	for (var col in data) {
		sql += util.format(" `%s` = ?,", col);
		params.push(data[col]);
	}
	sql = sql.substr(0, sql.length - 1);
	return this.vquery(sql, params);
}

/**
 * 批量插入数据
 * @param array names
 * @param array rows
 * @param int batch
 * @return Table
 */
Table.prototype.batchInsert = function(fields, rows, batch) {
	batch = batch || 1000;
	var addslashes = function(v){ return db.escape(v, true); };
	var i = 0;
	var sql = util.format("INSERT `%s` (`%s`) VALUES ", this._table, fields.join("`, `"));
	for (var n in rows) {
		i++;
		sql += util.format("('%s'),", rows[n].map(addslashes).join("','"));
		if (i >= batch) {
			sql = sql.substr(0, sql.length - 1);
			this.query(sql);
			i = 0;
			sql = util.format("INSERT `%s` (`%s`) VALUES ", this._table, fields.join("`"));
		}
	}
	if (i > 0) {
		sql = sql.substr(0, sql.length - 1);
		this.query(sql);
	}
	return this;
}

/**
 * 更新数据
 * @param array data
 * @return DBResult
 */
Table.prototype.update = function(data) {
	var sql = util.format("UPDATE `%s` SET", this._table);
	var params = [];
	for (var col in data) {
		var val = data[col];
		sql += util.format(" `%s` = ?,", col);
		params.push(val);
	}
	sql = sql.substr(0, sql.length - 1);
	sql += this._where.length == 0 ? "" : " WHERE " + this._where.join(" AND ");
	return this.vquery(sql, params.concat(this._where_params));
}

/**
 * 替换数据
 * @param array data
 * @return DBResult
 */
Table.prototype.replace = function(data) {
	var sql = util.format("REPLACE `%s` SET", this._table);
	var params = [];
	for (var col in data) {
		var val = data[col];
		sql += util.format(" `%s` = ?,", col);
		params.push(val);
	}
	sql = sql.substr(0, sql.length - 1);
	sql += this._where.length == 0 ? "" : "WHERE " + this._where.join(" AND ");
	return this.vquery(sql, params.concat(this._where_params));
}

/**
 * 删除数据
 * @return DBResult
 */
Table.prototype.delete = function() {
	var sql = util.format("DELETE FROM `%s`", this._table);
	sql += this._where == 0 ? "" : " WHERE " + this._where.join(" AND ");
	return this.vquery(sql, this._where_params);
}

/**
 * 重置所有
 * @return Table
 */
Table.prototype.reset = function() {
	this._keywords = [];
	this._where = [];
	this._where_params = [];
	this._group = null;
	this._having = [];
	this._having_params = [];
	this._order = null;
	this._limit = null;
	this._offset = null;
	this._for_update = "";
	this._lock_in_share_model = "";
	return this;
}

/**
 * 设置MySQL关键字
 * @param string keyword
 * @return Table
 */
Table.prototype.keyword = function(keyword) {
	this._keywords.push(keyword);
	return this;
}

/**
 * 设置 SQL_CALC_FOUND_ROWS
 * @return Table
 */
Table.prototype.calcFoundRows = function() {
	return this.keyword("SQL_CALC_FOUND_ROWS");
}

/**
 * where查询条件
 * @param string format
 * @return Table
 */
Table.prototype.where = function(format) {
	var args = Array.apply(null, arguments);
	args.shift();
	this._where.push(format);
	this._where_params = this._where_params.concat(args);
	return this;
}

/**
 * group分组
 * @param string columns
 * @return Table
 */
Table.prototype.group = function(columns) {
	this._group = columns;
	return this;
}

/**
 * having过滤条件
 * @param string format
 * @return Table
 */
Table.prototype.having = function(format) {
	var args = Array.apply(null, arguments);
	args.shift();
	this._having.push(format);
	this._having_params = this._having_params.concat(args);
	return this;
}

/**
 * order排序
 * @param string columns
 * @return Table
 */
Table.prototype.order = function(order) {
	this._order = order;
	return this;
}

/**
 * limit数据偏移
 * @param int offset
 * @param int limit
 * @return Table
 */
Table.prototype.limitOffset = function(limit, offset) {
	this._limit = limit;
	this._offset = offset || null;
	return this;
}

/**
 * 独占锁，不可读不可写
 * @return Table
 */
Table.prototype.forUpdate = function() {
	this.forUpdate = " FOR UPDATE";
	return this;
}

/**
 * 共享锁，可读不可写
 * @return Table
 */
Table.prototype.lockInShareMode = function() {
	this._lock_in_share_model = " LOCK IN SHARE MODE";
	return this;
}

/**
 * 事务开始
 * @return bool
 */
Table.prototype.begin = function() {
	return this.getPDO().begin();
}

/**
 * 事务提交
 * @return bool
 */
Table.prototype.commit = function() {
	return this.getPDO().commit();
}

/**
 * 事务回滚
 * @return bool
 */
Table.prototype.rollBack = function() {
	return this.getPDO().rollback();
}

/**
 * page分页
 * @param int page
 * @param int pagesize
 * @return Table
 */
Table.prototype.page = function(page, pagesize) {
	pagesize = pagesize || 15;
	this._limit = pagesize;
	this._offset = (page - 1) * pagesize;
	return this;
}

/**
 * 获取自增ID
 * @return int
 */
Table.prototype.lastInsertId = function() {
	return "lastInsertId not support!";
}

/**
 * 获取符合条件的行数
 * @return int
 */
Table.prototype.count = function() {
	return this.vquery("SELECT FOUND_ROWS() as count").toArray()[0].count;
	// var sql = util.format("SELECT count(*) as count FROM `%s`", this._table);
	// sql += this._count_where == 0 ? "" : " WHERE " + this._count_where.join(" AND ");
	// return this.vquery(sql, this._count_where_params).toArray()[0].count;
}

/**
 * 将选中行的指定字段加一
 * @param string col
 * @param int val
 * @return Table
 */
Table.prototype.plus = function(col, val) {
	val = val || 1;
	var sets = [util.format("`%s` = `%s` + ?", col, col)];
	var vals = [val];
	var args = Array.apply(null, arguments).slice(2);
	while (args.length > 1) {
		col = args.shift();
		val = args.shift();
		sets.push(util.format("`%s` = `%s` + ?", col, col));
		vals.push(val);
	}
	var sql = util.format("UPDATE `%s` SET %s", this._table, sets.join(", "));
	sql += this._where.length == 0 ? "" : " WHERE " + this._where.join(" AND ");
	var params = vals.concat(this._where_params);
	this.vquery(sql, params);
	return this;
}

/**
 * 将选中行的指定字段加一
 * @param string col
 * @param int val
 * @return int
 */
Table.prototype.incr = function(col, val) {
	val = val || 1;
	var sql = util.format("UPDATE `%s` SET `%s` =  last_insert_id(`%s` + ?)", this._table, col, col);
	sql += this._where.length == 0 ? "" : " WHERE " + this._where.join(" AND ");
	var params = [val].concat(this._where_params);
	var stmt = this.vquery(sql, params);
	return stmt.insertId;
}

/**
 * 根据主键查找行
 * @param int id
 * @return array
 */
Table.prototype.find = function(id) {
	return this.where(util.format("`%s` = ?", this._pk), id).select().toArray().shift();
}

/**
 * 保存数据,自动判断是新增还是更新
 * @param array data
 * @return DBResult
 */
Table.prototype.save = function(data) {
	if (data.hasOwnProperty(this._pk)) {
		var pk_val = data[this._pk];
		delete data[this._pk];
		return this.where(util.format("`%s` = ?", this._pk), pk_val).update(data);
	} else {
		return this.insert(data);
	}
}

/**
 * 获取外键数据
 * @param array rows
 * @param string foreign_key
 * @param string field
 * @return DBResult
 */
Table.prototype.foreignKey = function(rows, foreign_key, field) {
	field = field || "*";
	var ids = {};
	for (var i in rows) {
		ids[rows[i][foreign_key]] = true;
	}
	ids = Object.keys(ids);
	if (ids.length == 0) {
		// return new db.DBResult();
		return new collection.List();
	}
	return this.where(util.format("`%s` in (?)", this._pk), ids).select(field);
}

exports.Table = Table;
