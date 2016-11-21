var db = require("db");
var orm = require("orm");
var coroutine = require("coroutine");

var conn_str = "mysql://root:123456@/mingo?charset=utf8"

// 初始化数据库
var init_sqls = [
	`DROP TABLE IF EXISTS test_user;`,
	`CREATE TABLE test_user (
	  id int(11) NOT NULL AUTO_INCREMENT COMMENT '用户ID',
	  username varchar(16) CHARACTER SET ascii NOT NULL COMMENT '用户名',
	  password varchar(32) CHARACTER SET ascii NOT NULL COMMENT '密码',
	  reg_time int(11) NOT NULL COMMENT '注册时间',
	  reg_ip int(11) NOT NULL COMMENT '注册IP',
	  update_time int(11) NOT NULL COMMENT '更新时间',
	  update_ip int(11) NOT NULL COMMENT '更新IP',
	  PRIMARY KEY (id)
	) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='用户表';`,

	`DROP TABLE IF EXISTS test_category;`,
	`CREATE TABLE test_category (
	  id int(11) NOT NULL AUTO_INCREMENT COMMENT '分类编号',
	  name varchar(45) NOT NULL COMMENT '分类名称',
	  PRIMARY KEY (id)
	) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='分类表';`,

	`DROP TABLE IF EXISTS test_blog;`,
	`CREATE TABLE test_blog (
	  id int(11) NOT NULL AUTO_INCREMENT COMMENT '域名ID',
	  category_id int(11) NOT NULL COMMENT '分类ID',
	  title varchar(45) NOT NULL COMMENT '标题',
	  content text NOT NULL COMMENT '内容',
	  add_time int(11) NOT NULL COMMENT '添加时间',
	  update_time int(11) NOT NULL COMMENT '更新时间',
	  PRIMARY KEY (id)
	) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='博客表';`,
]

function init() {
	var conn = db.open(conn_str)
	for (var sql of init_sqls) {
		conn.execute(sql)
	}
}

init()

// function
function now() {
	return Date.now() / 1000 | 0
}

function row_clone(data) {
	var obj = {}
	for (var k of Object.keys(data)) {
		obj[k] = data[k]
	}
	return obj
}

// sql
if (argv.length == 2 || argv[2] == "sql") {
	// === SQL ===
	// select all method
	var sql = new orm.SQL("user", "", "id", orm.SELECT);
	sql.keywords("SQL_NO_CACHE").
		calcFoundRows().
		columns("username, password", "email", "count(*) as count").
		table("user").
		where("username = ?", "dotcoo").
		where("age BETWEEN ? AND ?", 18, 25).
		whereIn("no IN (?)", 1, 2, 3, 4, 5).
		group("age").
		having("count > ?", 3).
		having("count < ?", 10).
		order("id DESC, username", "password DESC").
		limit(10).
		offset(20).
		forUpdate().
		lockInShareMode()
	var [sq, args] = sql.SQL()
	var sq_select = "SELECT SQL_NO_CACHE SQL_CALC_FOUND_ROWS username, password, email, count(*) as count FROM `user` WHERE username = ? AND age BETWEEN ? AND ? AND no IN (?, ?, ?, ?, ?) GROUP BY age HAVING count > ? AND count < ? ORDER BY id DESC, username, password DESC LIMIT 10 OFFSET 20 FOR UPDATE LOCK IN SHARE MODE"
	var args_select = ["dotcoo", 18, 25, 1, 2, 3, 4, 5, 3, 10]
	if (sq != sq_select || args.join("|") != args_select.join("|")) {
		throw `sq_select error: ${sq}, ${args.join(',')}`
	}

	// count
	[sq, args] = sql.newCount().SQL()
	var sq_count = "SELECT count(*) AS count FROM `user` WHERE username = ? AND age BETWEEN ? AND ? AND no IN (?, ?, ?, ?, ?) GROUP BY age HAVING count > ? AND count < ?"
	var args_count = ["dotcoo", 18, 25, 1, 2, 3, 4, 5, 3, 10]
	if (sq != sq_count || args.join("|") != args_count.join("|")) {
		throw `sq_count error: ${sq}, ${args.join(',')}`
	}

	// where or
	[sq, args] = new orm.SQL("user", "", "id", orm.SELECT).table("user").where("username = ?", "dotcoo").where("(").whereOr("uid = ?", 1).whereOr("uid = ?", 2).whereOr("uid BETWEEN ? AND ?", 5, 9).whereOrIn("uid IN (?)", 11, 12, 13, 14, 15).whereOr("uid >= ?", 20).where(")").SQL()
	var sq_where = "SELECT * FROM `user` WHERE username = ? AND (uid = ? OR uid = ? OR uid BETWEEN ? AND ? OR uid IN (?, ?, ?, ?, ?) OR uid >= ?)"
	var args_where = ["dotcoo", 1, 2, 5, 9, 11, 12, 13, 14, 15, 20]
	if (sq != sq_where || args.join("|") != args_where.join("|")) {
		throw `sq_where error: ${sq}, ${args.join(',')}`
	}

	// join
	[sq, args] = new orm.SQL("user", "", "id", orm.SELECT).table("blog", "b").join("user", "u", "b.user_id = u.id").where("b.start > ?", 200).page(3, 10).SQL()
	var sq_join = "SELECT * FROM `blog` AS `b` LEFT JOIN `user` AS `u` ON b.user_id = u.id WHERE b.start > ? LIMIT 10 OFFSET 20"
	var args_join = [200]
	// console.log(sq.length, sq_join.length)
	// console.log(sq+"x", sq_join)
	if (sq != sq_join || args.join("|") != args_join.join("|")) {
		throw `sq_join error: ${sq}, ${args.join(',')}`
	}

	// insert
	[sq, args] = new orm.SQL("user", "", "id", orm.INSERT).table("user").set("username", "dotcoo").set("password", "dotcoopwd").set("age", 1).SQL()
	var sq_insert = "INSERT INTO `user` (`username`, `password`, `age`) VALUES (?, ?, ?)"
	var args_insert = ["dotcoo", "dotcoopwd", 1]
	if (sq != sq_insert || args.join("|") != args_insert.join("|")) {
		throw `sq_insert error: ${sq}, ${args.join(',')}`
	}

	// replace
	[sq, args] = new orm.SQL("user", "", "id", orm.REPLACE).table("user").set("username", "dotcoo").set("password", "dotcoopwd").set("age", 1).SQL()
	var sq_replace = "REPLACE INTO `user` (`username`, `password`, `age`) VALUES (?, ?, ?)"
	var args_replace = ["dotcoo", "dotcoopwd", 1]
	if (sq != sq_replace || args.join("|") != args_replace.join("|")) {
		throw `sq_replace error: ${sq}, ${args.join(',')}`
	}

	// update
	[sq, args] = new orm.SQL("user", "", "id", orm.UPDATE).table("user").set("username", "dotcoo").set("password", "dotcoopwd").set("age", 1).where("id = ?", 1).SQL()
	var sq_update = "UPDATE `user` SET `username` = ?, `password` = ?, `age` = ? WHERE id = ?"
	var args_update = ["dotcoo", "dotcoopwd", 1, 1]
	if (sq != sq_update || args.join("|") != args_update.join("|")) {
		throw `sq_update error: ${sq}, ${args.join(',')}`
	}

	// delete
	[sq, args] = new orm.SQL("user", "", "id", orm.DELETE).where("id = ?", 1).SQL()
	var sq_delete = "DELETE FROM `user` WHERE id = ?"
	var args_delete = [1]
	if (sq != sq_delete || args.join("|") != args_delete.join("|")) {
		throw `sq_delete error: ${sq}, ${args.join(',')}`
	}

	// page
	[sq, args] = new orm.SQL("user", "", "id", orm.SELECT).page(3, 10).SQL()
	var sq_page = "SELECT * FROM `user` LIMIT 10 OFFSET 20"
	var args_page = []
	if (sq != sq_page || args.join("|") != args_page.join("|")) {
		throw `sq_page error: ${sq}, ${args.join(',')}`
	}

	// plus
	[sq, args] = new orm.SQL("user", "", "id", orm.UPDATE).plus("age", 1).where("id = ?", 1).SQL()
	var sq_plus = "UPDATE `user` SET `age` = `age` + ? WHERE id = ?"
	var args_plus = [1, 1]
	if (sq != sq_plus || args.join("|") != args_plus.join("|")) {
		throw `sq_plus error: ${sq}, ${args.join(',')}`
	}

	// incr
	[sq, args] = new orm.SQL("user", "", "id", orm.UPDATE).incr("age", 1).where("id = ?", 1).SQL()
	var sq_incr = "UPDATE `user` SET `age` = last_insert_id(`age` + ?) WHERE id = ?"
	var args_incr = [1, 1]
	if (sq != sq_incr || args.join("|") != args_incr.join("|")) {
		throw `sq_incr error: ${sq}, ${args.join(',')}`
	}
}

// conn pool
if (argv.length == 2 || argv[2] == "pool") {
	var o = new orm.ORM(conn_str);

	o.execute("insert into test_user (username, password) values ('dotcootest', 'dotcoopwd')")

	var result = []

	function db_select() {
		result.push(o.execute("select *, connection_id() from test_user").toArray()[0]["connection_id()"]);
	}

	db_select();
	db_select();

	function db_connection_id() {
		result.push(o.execute("select connection_id()").toArray()[0]["connection_id()"]);
	}

	db_connection_id();
	db_connection_id();

	coroutine.parallel([db_connection_id, db_connection_id]);

	function db_connection_id_sleep(n) {
		return function() {
			result.push(o.execute("select sleep(?), connection_id()", n).toArray()[0]["connection_id()"]);
		}
	}

	coroutine.parallel([db_connection_id_sleep(0.2), db_connection_id_sleep(0.4), db_connection_id_sleep(0.6)]);

	var expect_result = [1, 1, 1, 1, 1, 2, 1, 2, 3]
	var real_result = result.map(v => v - result[0] + 1)

	// console.log(result.join(','), real_result.join(','), expect_result.join(','))
	if (real_result.join(',') != expect_result.join(',')) {
		throw "real_result.join(',') != expect_result.join(',')"
	}

	o.close();
}

// orm
if (argv.length == 2 || argv[2] == "orm") {
	var o = new orm.ORM(conn_str);

	o.prefix("test_")

	o.register("user", "u", "id")
	o.register("blog", "b", "id")

	var u1 = {}
	u1.username = "dotcoo"
	u1.password = "dotcoopwd"
	var result = o.add("user", u1)
	if (u1.id != result.insertId) {
		throw "u1.id != result.insertId"
	}

	var u2  = o.get("user", u1.id)
	if (u2.id != u1.id) {
		throw "u2.id != u1.id"
	}

	var u3  = o.getBy("user", u1, "username", "", "id", "username", "password")
	if (u3.id != u1.id) {
		throw "u3.id != u1.id"
	}

	u3 = row_clone(u3)
	u3.password = "123456"
	o.up("user", u3, "password")

	var u4  = o.get("user", u1.id)
	if (u4.id != u1.id || u4.password != u3.password) {
		throw "u4.id != u1.id || u4.password != u3.password"
	}

	o.del("user", u4.id)

	var u5 = o.get("user", u4.id)
	if (u5) {
		throw "u5 exist!"
	}

	o.close();
}

// transaction
if (argv.length == 2 || argv[2] == "trans") {
	var o = new orm.ORM(conn_str);

	o.prefix("test_")

	o.register("user", "u", "id")
	o.register("blog", "b", "id")

	var u1 = {}
	u1.username = "dotcoo"
	u1.password = "dotcoopwd"
	o.add("user", u1)

	var otx = o.begin()
	var u2 = otx.select("user").where("id = ?", u1.id).forUpdate().select().shift()

	otx.update("user").where("id = ?", u1.id).plus("reg_time").update()

	var u3 = otx.select("user").where("id = ?", u1.id).forUpdate().select().shift()

	if (u2.reg_time + 1 != u3.reg_time) {
		otx.rollback()
		throw "u2.reg_time + 1 != u3.reg_time"
	}

	otx.commit()

	o.close();
}

console.log("OK");
