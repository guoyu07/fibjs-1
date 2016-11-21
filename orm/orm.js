/**
 * Fibjs ORM
 * @author dotcoo zhao <dotcoo@163.com>
 * @link http://www.dotcoo.com/table
 */

var db = require("db")
var collection = require("collection")

// const
const AND = " AND "
const OR = " OR "
const SELECT = 0
const INSERT = 1
const UPDATE = 2
const DELETE = 3
const REPLACE = 4

exports.SELECT = SELECT
exports.INSERT = INSERT
exports.UPDATE = UPDATE
exports.DELETE = DELETE
exports.REPLACE = REPLACE

// SQL
function SQL(table, alias = "", pk = "id", mode = SELECT) {
	// private
	this._mode = mode // sql mode
	this._table = table // table
	this._alias = alias // table alias
	this._pk = pk // pk
	this._keywords = [] // keywords
	this._columns = [] // columns
	this._from = "" // from
	this._from_as = "" // from alias
	this._cols = [] // cols
	this._sets = [] // sets
	this._setsArgs = [] // sets args
	this._joins = [] // joins
	this._wheres = [] // where
	this._wheresArgs = [] // where args
	this._groups = [] // group
	this._havings = [] // having
	this._havingsArgs = [] // having args
	this._orders = [] // order
	this._limit = -1 // limit
	this._offset = -1 // offset
	this._forUpdate = "" // read lock
	this._lockInShareMode = "" // write lock
	this._orm = null // ORM
	// init
	this.table(this._table, this._alias);
}

SQL.prototype.reset = function() {
	this._keywords = []
	this._columns = []
	this._cols = []
	this._sets = []
	this._setsArgs = []
	this._joins = []
	this._wheres = []
	this._wheresArgs = []
	this._groups = []
	this._havings = []
	this._havingsArgs = []
	this._orders = []
	this._limit = -1
	this._offset = -1
	this._forUpdate = ""
	this._lockInShareMode = ""
}

// sql syntax

SQL.prototype.keywords = function(...keywords) {
	this._keywords.push(...keywords)
	return this
}

SQL.prototype.calcFoundRows = function() {
	return this.keywords("SQL_CALC_FOUND_ROWS")
}

SQL.prototype.columns = function(...columns) {
	this._columns.push(...columns)
	return this
}

SQL.prototype.table = function(table, alias = "") {
	this._table = table
	this._alias = alias
	this._from = `\`${this._table}\``
	if (alias == "") {
		this._from_as = `\`${this._table}\``
	} else {
		this._from_as = `\`${this._table}\` AS \`${this._alias}\``
	}
	return this
}

SQL.prototype.set = function(col, val) {
	this._cols.push(`\`${col}\``)
	this._sets.push(`\`${col}\` = ?`)
	this._setsArgs.push(val)
	return this
}

SQL.prototype.join = function(table, alias, cond) {
	if (alias == "") {
		this._joins.push(`\`${table}\` ON \`${cond}\``)
	} else {
		this._joins.push(`\`${table}\` AS \`${alias}\` ON ${cond}`)
	}
	return this
}

SQL.prototype._where = function(wheres, wheresArgs, and, where, ...args) {
	if (where == "(") {
		wheres.push(and)
		wheres.push(where)
	} else if (where == ")") {
		wheres.push(where)
	} else if (and == AND || and == OR) {
		if (wheres.length == 0 || wheres[wheres.length-1] == "(") {
			wheres.push(where)
		} else {
			wheres.push(and)
			wheres.push(where)
		}
		wheresArgs.push(...args)
	} else {
		throw "not reached"
	}
	return this
}

SQL.prototype.where = function(where, ...args) {
	return this._where(this._wheres, this._wheresArgs, AND, where, ...args)
}

SQL.prototype.whereOr = function(where, ...args) {
	return this._where(this._wheres, this._wheresArgs, OR, where, ...args)
}

SQL.prototype._whereIn = function(and, where, ...args) {
	if (args.length == 0) {
		throw "args is null!"
	}
	where = where.replace("?", ", ?".repeat(args.length).substr(2))
	return this._where(this._wheres, this._wheresArgs, and, where, ...args)
}

SQL.prototype.whereIn = function(where, ...args) {
	return this._whereIn(AND, where, ...args)
}

SQL.prototype.whereOrIn = function(where, ...args) {
	return this._whereIn(OR, where, ...args)
}

SQL.prototype.group = function(...groups) {
	this._groups.push(...groups)
	return this
}

SQL.prototype.having = function(having, ...args) {
	return this._where(this._havings, this._havingsArgs, AND, having, ...args)
}

SQL.prototype.havingOr = function(having, ...args) {
	return this._where(this._havings, this._havingsArgs, OR, having, ...args)
}

SQL.prototype.order = function(...orders) {
	this._orders.push(...orders)
	return this
}

SQL.prototype.limit = function(limit) {
	this._limit = limit
	return this
}

SQL.prototype.offset = function(offset) {
	this._offset = offset
	return this
}

SQL.prototype.forUpdate = function() {
	this._forUpdate = " FOR UPDATE"
	return this
}

SQL.prototype.lockInShareMode = function() {
	this._lockInShareMode = " LOCK IN SHARE MODE"
	return this
}

// sql tool

SQL.prototype.page = function(page, pagesize) {
	this._limit = pagesize
	this._offset = (page - 1) * pagesize
	return this
}

SQL.prototype.plus = function(col, val = 1) {
	this._sets.push(`\`${col}\` = \`${col}\` + ?`)
	this._setsArgs.push(val)
	return this
}

SQL.prototype.incr = function(col, val = 1) {
	this._sets.push(`\`${col}\` = last_insert_id(\`${col}\` + ?)`)
	this._setsArgs.push(val)
	return this
}

// generate sql

SQL.prototype.toSelect = function(...columns) {
	this._columns.push(...columns)

	var keyword = ""
	if (this._keywords.length > 0) {
		keyword = " " + this._keywords.join(" ")
	}
	var column = "*"
	if (this._columns.length > 0) {
		column = this._columns.join(", ")
	}
	var table = this._from
	var join = ""
	if (this._joins.length > 0) {
		table = this._from_as
		join = " LEFT JOIN " + this._joins.join(" LEFT JOIN ")
	}
	var where = ""
	if (this._wheres.length > 0) {
		where = " WHERE " + this._wheres.join("")
	}
	var group = ""
	if (this._groups.length > 0) {
		group = " GROUP BY " + this._groups.join(", ")
	}
	var having = ""
	if (this._havings.length > 0) {
		having = " HAVING " + this._havings.join("")
	}
	var order = ""
	if (this._orders.length > 0) {
		order = " ORDER BY " + this._orders.join(", ")
	}
	var limit = ""
	if (this._limit > -1) {
		limit = ` LIMIT ${this._limit}`
	}
	var offset = ""
	if (this._offset > -1) {
		offset = ` OFFSET ${this._offset}`
	}
	var forUpdate = this._forUpdate
	var lockInShareMode = this._lockInShareMode

	return [`SELECT${keyword} ${column} FROM ${table}${join}${where}${group}${having}${order}${limit}${offset}${forUpdate}${lockInShareMode}`, this._wheresArgs.concat(this._havingsArgs)]
}

SQL.prototype.toInsert = function() {
	if (this._sets.length == 0) {
		throw "Insert sets is empty!"
	}

	var cols = this._cols.join(", ")
	var vals = ", ?".repeat(this._cols.length).substr(2)
	return [`INSERT INTO ${this._from} (${cols}) VALUES (${vals})`, this._setsArgs]
}

SQL.prototype.toReplace = function() {
	if (this._sets.length == 0) {
		throw "Replace sets is empty!"
	}

	var cols = this._cols.join(", ")
	var vals = ", ?".repeat(this._cols.length).substr(2)
	return [`REPLACE INTO ${this._from} (${cols}) VALUES (${vals})`, this._setsArgs]
}

SQL.prototype.toUpdate = function() {
	if (this._sets.length == 0) {
		throw "Update sets is empty!"
	}
	if (this._wheres.length == 0) {
		throw "Update where is empty!"
	}

	var set = this._sets.join(", ")
	var where = this._wheres.join("")
	var order = ""
	if (this._orders.length > 0) {
		order = " ORDER BY " + this._orders.join(", ")
	}
	var limit = ""
	if (this._limit > -1) {
		limit = ` LIMIT ${this._limit}`
	}

	return [`UPDATE ${this._from} SET ${set} WHERE ${where}${order}${limit}`, this._setsArgs.concat(this._wheresArgs)]
}

SQL.prototype.toDelete = function() {
	if (this._wheres.length == 0) {
		throw "Delete wheres is empty!"
	}

	var where = this._wheres.join("")
	var order = ""
	if (this._orders.length > 0) {
		order = " ORDER BY " + this._orders.join(", ")
	}
	var limit = ""
	if (this._limit > -1) {
		limit = ` LIMIT ${this._limit}`
	}

	return [`DELETE FROM ${this._from} WHERE ${where}${order}${limit}`, this._wheresArgs]
}

SQL.prototype.SQL = function() {
	switch (this._mode) {
	case SELECT:
		return this.toSelect()
	case INSERT:
		return this.toInsert()
	case REPLACE:
		return this.toReplace()
	case UPDATE:
		return this.toUpdate()
	case DELETE:
		return this.toDelete()
	default:
		throw "not reached"
	}
}

SQL.prototype.toString = function() {
	var [sql, args] = this.SQL()
	return `${sql}, ${args.join(',')}`
}

// sql count
SQL.prototype.newCount = function() {
	var sc = new SQL(this._table, this._pk, this._alias, SELECT)
	sc._columns = ["count(*) AS count"]
	sc._joins = this._joins.concat()
	sc._wheres = this._wheres.concat()
	sc._wheresArgs = this._wheresArgs.concat()
	sc._groups = this._groups.concat()
	sc._havings = this._havings.concat()
	sc._havingsArgs = this._havingsArgs.concat()
	sc._orm = this._orm
	return sc
}

// ORM快捷方法

SQL.prototype.setMode = function(mode) {
	this._mode = mode
	return this
}

SQL.prototype.setORM = function(orm) {
	this._orm = orm
	return this
}

SQL.prototype.select = function() {
	return this._orm.executeSQL(this).toArray()
}

SQL.prototype.insert = SQL.prototype.replace = SQL.prototype.update = SQL.prototype.delete = function() {
	return this._orm.executeSQL(this)
}

exports.SQL = SQL

// ORMConnection
function ORMConnection(connString) {
	// public
	this.connString = connString // 连接字符串
	this.conn = db.open(connString) // 数据库连接
	this.name = "" // 连接名称
	// private
	this._orm = null // orm对象
}

// 格式化sql语句
ORMConnection.prototype.format = function(sql, ...args) {
	return this.conn.format(sql, ...args)
}

// 执行sql语句
ORMConnection.prototype.execute = function(sql, ...args) {
	return this.conn.execute(sql, ...args)
}

// 开启事务
ORMConnection.prototype.begin = function() {
	this.conn.begin()
}

// 提交事务
ORMConnection.prototype.commit = function() {
	this.conn.commit()
}

// 回滚事务
ORMConnection.prototype.rollback = function() {
	this.conn.rollback()
}

// 关闭连接
ORMConnection.prototype.close = function() {
	if (this._orm) {
		this._orm.putConnection(this)
	}
}

exports.ORMConnection = ORMConnection

// ORM
function ORM(connString, max_open_conns = 1024, max_idle_conns = 100) {
	// pubilc
	this.connString = connString
	this.max_open_conns = max_open_conns
	this.max_idle_conns = max_idle_conns
	// private
	this._no = 0
	this._all_conns = new collection.Map()
	this._idle_conns = new collection.Queue(this.max_idle_conns)
	this._closed = false
	this._prefix = ""
	this._tables = {}
}

// 设置最大连接数
ORM.prototype.setMaxOpenConns = function(max_open_conns) {
	this.max_open_conns = max_open_conns
}

// 设置最大空闲连接
ORM.prototype.setMaxIdleConns = function(max_idle_conns) {
	var idle_conns = new collection.Queue(max_idle_conns)
	var conn = null
	while (conn = this._idle_conns.poll()) {
		idle_conns.offer(conn)
	}
	this.max_idle_conns = max_idle_conns
	this._idle_conns = idle_conns
}

// 创建一个连接
ORM.prototype.newConnection = function() {
	var conn = new ORMConnection(this.connString)
	conn.name = `conn_${this._no++}`
	conn._orm = this
	return conn
}

// 关闭一个连接
ORM.prototype.closeConnection = function(conn) {
	this._all_conns.remove(conn.name)
	conn.conn.close()
}

// 获取一个连接
ORM.prototype.getConnection = function() {
	if (this._beginConnection) {
		return this._beginConnection
	}
	if ((this._idle_conns.length > 0)) {
		return this._idle_conns.poll()
	}
	if ((this._all_conns.size >= this.max_open_conns)) {
		throw "max connections"
	}
	var conn = this.newConnection()
	this._all_conns.put(conn.name, conn)
	return conn
}

// 回收一个连接
ORM.prototype.putConnection = function(conn) {
	if ((this._closed || this._idle_conns.length >= this.max_idle_conns)) {
		this.closeConnection(conn)
	} else {
		this._idle_conns.offer(conn)
	}
}

// 执行sql语句
ORM.prototype.execute = function(sql, ...args) {
	var conn = this.getConnection()
	var result = conn.execute(sql, ...args)
	conn.close()
	return result
}

// 关闭连接
ORM.prototype.close = function() {
	this._closed = true
	var conn = null
	while (conn = this._idle_conns.poll()) {
		this.closeConnection(conn)
	}
	this._idle_conns.clear()
}

// begin
ORM.prototype.begin = function() {
	var conn = this.getConnection()
	conn.begin()
	return new ORMTransaction(this, conn)
}

// commit
ORM.prototype.commit = function() {
	throw "ORM not support commit!"
}

// rollback
ORM.prototype.rollback = function() {
	throw "ORM not support rollback!"
}

// 前缀
ORM.prototype.prefix = function(prefix) {
	this._prefix = prefix
}

// 注册表信息
ORM.prototype.register = function(table, alias, pk) {
	this._tables[table] = {table: `${this._prefix}${table}`, alias: alias, pk: pk}
}

// 获取表信息
ORM.prototype.tableinfo = function(table) {
	return table in this._tables ? this._tables[table] : {table: `${this._prefix}${table}`, alias: "", pk: "id"}
}

// new sql

ORM.prototype.sql = function(table, mode) {
	var ti = this.tableinfo(table);
	var sql = new SQL(ti.table, ti.alias, ti.pk, mode)
	sql.setORM(this)
	return sql
}

ORM.prototype.select = function(table) {
	return this.sql(table, SELECT)
}

ORM.prototype.insert = function(table) {
	return this.sql(table, INSERT)
}

ORM.prototype.replace = function(table) {
	return this.sql(table, REPLACE)
}

ORM.prototype.update = function(table) {
	return this.sql(table, UPDATE)
}

ORM.prototype.delete = function(table) {
	return this.sql(table, DELETE)
}

// orm method

ORM.prototype.executeSQL = function(sq) {
	var [sql, args] = sq.SQL()
	return this.execute(sql, ...args)
}

ORM.prototype.get = function(table, id, ...columns) {
	var sq = this.select(table)
	sq.columns(...columns)
	sq.where(`\`${sq._pk}\` = ?`, id)
	return this.executeSQL(sq).toArray().shift()
}

ORM.prototype.getBy = function(table, data, ...cols_null_columns) {
	var idx = cols_null_columns.indexOf("")
	var cols = idx == -1 ? cols_null_columns : cols_null_columns.concat().splice(0, idx)
	var columns = idx == -1 ? [] : cols_null_columns.concat().splice(idx + 1)
	var sq = this.select(table)
	sq.columns(...columns)
	for (var col of cols) {
		sq.where(`\`${col}\` = ?`, data[col])
	}
	return this.executeSQL(sq).toArray().shift()
}

ORM.prototype.add = function(table, data, ...columns) {
	columns = columns.length > 0 ? columns : Object.keys(data)
	var sq = this.insert(table)
	for (var col of columns) {
		sq.set(col, data[col])
	}
	var result = this.executeSQL(sq)
	data[sq._pk] = result.insertId
	return result
}

ORM.prototype.up = function(table, data, ...columns) {
	columns = columns.length > 0 ? columns : Object.keys(data)
	var sq = this.update(table)
	for (var col of columns) {
		if (col == sq._pk) {
			continue
		}
		sq.set(col, data[col])
	}
	sq.where(`\`${sq._pk}\` = ?`, data[sq._pk])
	return this.executeSQL(sq)
}

ORM.prototype.del = function(table, id) {
	var sq = this.delete(table)
	sq.where(`\`${sq._pk}\` = ?`, id)
	return this.executeSQL(sq)
}

ORM.prototype.save = function(table, data) {
	var ti = this.table(table)
	if (ti.pk in data) {
		return this.up(table, data)
	} else {
		return this.add(table, data)
	}
}

exports.ORM = ORM

// ORMTransaction
function ORMTransaction(orm, conn) {
	this._orm = orm
	this._conn = conn
}

// 回收一个连接
ORMTransaction.prototype.putConnection = function(conn) {
	if (this._conn != conn) {
		throw "Unknown connection!"
	}
	this._orm.putConnection(conn)
	this._conn = null;
}

// 执行sql语句
ORMTransaction.prototype.execute = function(sql, ...args) {
	return this._conn.execute(sql, ...args)
}

// begin
ORMTransaction.prototype.begin = function() {
	throw "ORMTransaction not support begin!"
}

// commit
ORMTransaction.prototype.commit = function() {
	this._conn.commit()
	this._conn.close()
}

// rollback
ORMTransaction.prototype.rollback = function() {
	this._conn.rollback()
	this._conn.close()
}

// 获取表信息
ORMTransaction.prototype.tableinfo = function(table) {
	return this._orm.tableinfo(table)
}

// new sql
ORMTransaction.prototype.sql = ORM.prototype.sql
ORMTransaction.prototype.select = ORM.prototype.select
ORMTransaction.prototype.insert = ORM.prototype.insert
ORMTransaction.prototype.replace = ORM.prototype.replace
ORMTransaction.prototype.update = ORM.prototype.update
ORMTransaction.prototype.delete = ORM.prototype.delete
// orm method
ORMTransaction.prototype.executeSQL = ORM.prototype.executeSQL
ORMTransaction.prototype.get = ORM.prototype.get
ORMTransaction.prototype.add = ORM.prototype.add
ORMTransaction.prototype.up = ORM.prototype.up
ORMTransaction.prototype.del = ORM.prototype.del
ORMTransaction.prototype.save = ORM.prototype.save

// new sql

// ORMTransaction.prototype.sql = function(table, mode) {
// 	var ti = this.tableinfo(table);
// 	var sql = new SQL(ti.table, ti.pk, ti.alias, mode)
// 	sql.setORM(this)
// 	return sql
// }

// ORMTransaction.prototype.select = function(table) {
// 	return this.sql(table, SELECT)
// }

// ORMTransaction.prototype.insert = function(table) {
// 	return this.sql(table, INSERT)
// }

// ORMTransaction.prototype.replace = function(table) {
// 	return this.sql(table, REPLACE)
// }

// ORMTransaction.prototype.update = function(table) {
// 	return this.sql(table, UPDATE)
// }

// ORMTransaction.prototype.delete = function(table) {
// 	return this.sql(table, DELETE)
// }

// orm method

// ORMTransaction.prototype.get = function(table, id) {
// 	return this.select(table).get(id).select()
// }

// ORMTransaction.prototype.add = function(table, data) {
// 	return this.insert(table).add(data).insert()
// }

// ORMTransaction.prototype.up = function(table, data) {
// 	return this.update(this).up(data).update()
// }

// ORMTransaction.prototype.del = function(table, id) {
// 	return this.delete(table).del(id).delete()
// }

// ORMTransaction.prototype.save = function(table, data) {
// 	var ti = this.table(table)
// 	if (ti.pk in data) {
// 		return this.up(table, data)
// 	} else {
// 		return this.add(table, data)
// 	}
// }

exports.ORMTransaction = ORMTransaction

// quick method

// var orm = new ORM("mysql://root:123456@/test")

// exports.open = function(connString, max_open_conns = 1024, max_idle_conns = 100) {
// 	orm = new ORM(connString, max_open_conns, max_idle_conns)
// }

// exports.setMaxOpenConns = function(max_open_conns) {
// 	orm.setMaxOpenConns(max_open_conns)
// }

// exports.setMaxIdleConns = function(max_idle_conns) {
// 	orm.setMaxIdleConns(max_idle_conns)
// }

// exports.get = function() {
// 	return orm.get()
// }

// exports.execute = function(sql, ...args) {
// 	return orm.execute(sql, ...args)
// }

// exports.close = function() {
// 	return orm.close()
// }
