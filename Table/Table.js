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
 * @param string tab 表别名
 * @param string pk 主键
 * @param DbConnection pdo 数据库连接
 */
function Table(table, tab, pk, pdo) {
	// 属性
	this.pdo = null;
	this.prefix = "";
	this.table = "table";
	this.tab = "t";
	this.pk = "id";
	this.debug = false;

	// 私有属性
	this._keywords = [];
	this._columns = [];
	this._table = "";
	this._joins = [];
	this._wheres = [];
	this._wheres_params = [];
	// this._count_wheres = [];
	// this._count_wheres_params = [];
	this._groups = [];
	this._havings = [];
	this._havings_params = [];
	this._orders = [];
	this._limit = null;
	this._offset = null;
	this._for_update = "";
	this._lock_in_share_mode = "";

	// 参数
	this.table = table || this.table;
	this.tab = tab || this.tab;
	this.pk = pk || this.pk;
	this.pdo = pdo || this.pdo;
	this._table = this.prefix+this.table;
};

// 默认设置
Table.prototype.__pdo = null;					// 默认PDO对象
Table.prototype.__host = "127.0.0.1";			// 默认主机
Table.prototype.__user = "root";				// 默认账户
Table.prototype.__password = "123456";			// 默认密码
Table.prototype.__dbname = "test";				// 默认数据库名称
Table.prototype.__charset = "utf8";				// 默认字符集

// 属性
Table.prototype.pdo = null;						// PDO对象
Table.prototype.prefix = "";					// 表前缀
Table.prototype.table = "table";				// 表名
Table.prototype.tab = "t";						// 表别名
Table.prototype.pk = "id";						// 主键
Table.prototype.debug = false;					// 调试模式

// 私有属性
Table.prototype._keywords = [];					// keywords
Table.prototype._columns = [];					// columns
Table.prototype._table = "";					// table
Table.prototype._joins = [];					// join
Table.prototype._wheres = [];					// where
Table.prototype._wheres_params = [];			// where params
// Table.prototype._count_wheres = [];				// count where
// Table.prototype._count_wheres_params = [];		// count where params
Table.prototype._groups = [];					// group
Table.prototype._havings = [];					// having
Table.prototype._havings_params = [];			// having params
Table.prototype._orders = [];					// order
Table.prototype._limit = null;					// limit
Table.prototype._offset = null;					// offset
Table.prototype._for_update = "";				// read lock
Table.prototype._lock_in_share_mode = "";		// write lock

/**
 * 获取数据库连接
 * @return DbConnection
 */
Table.prototype.getPDO = function() {
	if (this.pdo) {
		return this.pdo;
	}

	if (this.__pdo) {
		return this.__pdo;
	}

	var dsn = util.format("mysql://%s:%s@%s/%s", this.__user, this.__password, this.__host, this.__dbname);
	var pdo = db.open(dsn);
	pdo.execute("set names " + this.__charset);
	return Table.prototype.__pdo = pdo;
};

/**
 * 获取主键列名
 * @return string
 */
Table.prototype.getPK = function() {
	return this.pk;
}

/**
 * 设置表前缀
 * @param string prefix
 * @return Table
 */
Table.prototype.setPrefix = function(prefix) {
	this.prefix = prefix;
	this._table = this.prefix+this.table;
	return this;
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
 * @param array params
 * @return DBResult
 */
Table.prototype.vquery = function(sql, params) {
	params = params || [];
	if (this.debug) {
		console.log(sql, "|", params.join(", "));
	}
	var conn = this.getPDO();
	var stmt = conn.execute.apply(conn, [sql].concat(params));
	this.reset();
	return stmt;
}

/**
 * 查询数据
 * @param string columns
 * @return DBResult
 */
Table.prototype.select = function(columns) {
	columns = columns || null;
	if (columns != null) {
		this._columns.push(columns);
	}

	var keywords = this._keywords.length == 0 ? "" : " "+this._keywords.join(" ");
	var columns = this._columns.length == 0 ? "*" : this._columns.join(", ");
	var table = this._table + (this._joins.length == 0 ? "" : "` AS `"+this.tab);
	var joins = this._joins.length == 0 ? "" : " LEFT JOIN "+this._joins.join(" LEFT JOIN ");
	var wheres = this._wheres.length == 0 ? "" : " WHERE "+this._wheres.join(" AND ");
	var groups = this._groups.length == 0 ? "" : " GROUP BY "+this._groups.join(", ");
	var havings = this._havings.length == 0 ? "" : " HAVING "+this._havings.join(" AND ");
	var orders = this._orders.length == 0 ? "" : " ORDER BY "+this._orders.join(", ");
	var limit = this._limit == null ? "" : " LIMIT ?";
	var offset = this._offset == null ? "" : " OFFSET ?";
	var forUpdate = this._for_update;
	var lockInShareMode = this._lock_in_share_mode;
	var sql = util.format("SELECT%s %s FROM `%s`%s%s%s%s%s%s%s%s%s", keywords, columns, table, joins, wheres, groups, havings, orders, limit, offset, forUpdate, lockInShareMode);

	var params = this._wheres_params.concat(this._havings_params);
	if (this._limit) {
		params.push(this._limit);
	}
	if (this._offset) {
		params.push(this._offset);
	}

	// this._count_wheres = this._wheres;
	// this._count_wheres_params = this._wheres_params;

	return this.vquery(sql, params);
}


/**
 * 添加数据
 * @param array data
 * @return DBResult
 */
Table.prototype.insert = function(data) {
	var sets = [];
	var params = [];
	for (var col in data) {
		sets.push(util.format("`%s` = ?", col));
		params.push(data[col]);
	}
	console.dir(this._table);
	var sql = util.format("INSERT `%s` SET %s", this._table, sets.join(", "));
	return this.vquery(sql, params);
}

/**
 * 批量插入数据
 * @param array columns
 * @param array rows
 * @param int batch
 * @return Table
 */
Table.prototype.batchInsert = function(columns, rows, batch) {
	batch = batch || 1000;
	var value = "(?"+",?".repeat(columns.length-1)+")";
	var columns = columns.join("`,`");
	var values = [];
	var params = [];
	var i = 0;
	var sql = "";
	for (var n in rows) {
		values.push(value);
		params = params.concat(rows[n]);
		i++;
		if (i >= batch) {
			sql = util.format("INSERT `%s` (`%s`) VALUES %s", this._table, columns, values.join(","));
			this.vquery(sql, params);
			i = 0;
			values = [];
			params = [];
		}
	}
	if (i > 0) {
		sql = util.format("INSERT `%s` (`%s`) VALUES %s", this._table, columns, values.join(","));
		this.vquery(sql, params);
	}
	return this;
}

/**
 * 更新数据
 * @param array data
 * @return DBResult
 */
Table.prototype.update = function(data) {
	var sets = [];
	var params = [];
	for (var col in data) {
		sets.push(util.format("`%s` = ?", col));
		params.push(data[col]);
	}
	var wheres = this._wheres.length == 0 ? " WHERE 0" : " WHERE "+this._wheres.join(" AND ");
	var sql = util.format("UPDATE `%s` SET %s%s", this._table, sets.join(", "), wheres);
	params = params.concat(this.wheres_params);
	return this.vquery(sql, params);
}

/**
 * 替换数据
 * @param array data
 * @return DBResult
 */
Table.prototype.replace = function(data) {
	var sets = [];
	var params = [];
	for (var col in data) {
		sets.push(util.format("`%s` = ?", col));
		params.push(data[col]);
	}
	var sql = util.format("REPLACE `%s` SET %s", this._table, sets.join(", "));
	return this.vquery(sql, params);
}

/**
 * 删除数据
 * @return DBResult
 */
Table.prototype.delete = function() {
	var wheres = this._wheres.length == 0 ? " WHERE 0" : " WHERE "+this._wheres.join(" AND ");
	var sql = util.format("DELETE FROM `%s`%s", this._table, wheres);
	return this.vquery(sql, this._wheres_params);
}

/**
 * 重置所有
 * @return Table
 */
Table.prototype.reset = function() {
	this._keywords = [];
	this._columns = [];
	this._joins = [];
	this._wheres = [];
	this._wheres_params = [];
	// this._count_wheres = [];
	// this._count_wheres_params = [];
	this._groups = [];
	this._havings = [];
	this._havings_params = [];
	this._orders = [];
	this._limit = null;
	this._offset = null;
	this._for_update = "";
	this._lock_in_share_mode = "";
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
 * 设置SQL_CALC_FOUND_ROWS关键字
 * @return Table
 */
Table.prototype.calcFoundRows = function() {
	return this.keyword("SQL_CALC_FOUND_ROWS");
}

/**
 * column返回的列
 * @param string column
 * @return Table
 */
Table.prototype.column = function(column) {
	this._columns.push(column);
	return this;
}

/**
 * join连表查询
 * @param string join
 * @param string cond
 * @return Table
 */
Table.prototype.join = function(join, cond) {
	this._joins.push(util.format("%s ON %s", join, cond));
	return this;
}

/**
 * where查询条件
 * @param string where
 * @return Table
 */
Table.prototype.where = function(where) {
	var args = Array.apply(null, arguments);
	args.shift();

	var ws = where.split("?");
	var where = ws.shift();
	var params = [];
	for (var i in ws) {
		var w = ws[i];
		if (Array.isArray(args[i])) {
			where += "?"+",?".repeat(args[i].length-1)+w;
			params = params.concat(args[i]);
		} else {
			where += "?"+w;
			params.push(args[i]);
		}
	}

	this._wheres.push(where);
	this._wheres_params = this._wheres_params.concat(params);
	return this;
}

/**
 * group分组
 * @param string group
 * @return Table
 */
Table.prototype.group = function(group) {
	this._groups.push(group);
	return this;
}

/**
 * having过滤条件
 * @param string having
 * @return Table
 */
Table.prototype.having = function(having) {
	var args = Array.apply(null, arguments);
	args.shift();

	var ws = having.split("?");
	var having = ws.shift();
	var params = [];
	for (var i in ws) {
		var w = ws[i];
		if (Array.isArray(args[i])) {
			having += "?"+",?"+repeat(args[i].length-1)+w;
			params = params.concat(args[i]);
		} else {
			having += "?"+w;
			params.push(args[i]);
		}
	}

	this._havings.push(having);
	this._havings_params = this._havings_params.concat(params);
	return this;
}

/**
 * order排序
 * @param string order
 * @return Table
 */
Table.prototype.order = function(order) {
	this._orders.push(order);
	return this;
}

/**
 * limit数据
 * @param int limit
 * @return Table
 */
Table.prototype.limit = function(limit) {
	this._limit = limit;
	return this;
}

/**
 * offset偏移
 * @param int offset
 * @return Table
 */
Table.prototype.offset = function(offset) {
	this._offset = offset;
	return this;
}

/**
 * 独占锁，不可读不可写
 * @return Table
 */
Table.prototype.forUpdate = function() {
	this._for_update = " FOR UPDATE";
	return this;
}

/**
 * 共享锁，可读不可写
 * @return Table
 */
Table.prototype.lockInShareMode = function() {
	this._lock_in_share_mode = " LOCK IN SHARE MODE";
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
	// var wheres = this._count_wheres.length == 0 ? "" : " WHERE "+this._count_wheres.join(" AND ");
	// var sql = util.format("SELECT count(*) as count FROM `%s`%s", this._table, wheres);
	// return this.vquery(sql, this._count_wheres_params).toArray()[0].count;
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
	var wheres = this._wheres.length == 0 ? " WHERE 0" : " WHERE "+this._wheres.join(" AND ");
	var sql = util.format("UPDATE `%s` SET %s%s", this._table, sets.join(", "), wheres);
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
	var wheres = this._wheres.length == 0 ? " WHERE 0" : " WHERE "+this._wheres.join(" AND ");
	var sql = util.format("UPDATE `%s` SET `%s` = last_insert_id(`%s` + ?)%s", this._table, col, col, wheres);
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
	return this.where(util.format("`%s` = ?", this.pk), id).select().toArray().shift();
}

/**
 * 保存数据,自动判断是新增还是更新
 * @param array data
 * @return DBResult
 */
Table.prototype.save = function(data) {
	if (data.hasOwnProperty(this.pk)) {
		var pk_val = data[this.pk];
		delete data[this.pk];
		return this.where(util.format("`%s` = ?", this.pk), pk_val).update(data);
	} else {
		return this.insert(data);
	}
}

/**
 * 获取外键数据
 * @param array rows
 * @param string foreign_key
 * @param string columns
 * @return DBResult
 */
Table.prototype.foreignKey = function(rows, foreign_key, columns) {
	columns = columns || "*";
	var ids = {};
	for (var i in rows) {
		ids[rows[i][foreign_key]] = true;
	}
	ids = Object.keys(ids);
	if (ids.length == 0) {
		// return new db.DBResult();
		return new collection.List();
	}
	return this.where(util.format("`%s` in (?)", this.pk), ids).select(columns);
}

exports.Table = Table;
