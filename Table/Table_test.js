// 数据库表结构
/*
CREATE TABLE `table_user` (
`id` int(11) NOT NULL AUTO_INCREMENT,
`username` varchar(45) NOT NULL,
`password` varchar(45) NOT NULL,
`nickname` varchar(45) NOT NULL,
`r` tinyint(4) NOT NULL,
PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE `table_blog` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `user_id` int(11) NOT NULL,
  `title` varchar(45) NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
*/

// 初始化
var tab = require("Table");
var Table = tab.Table;

// 初始化
Table.prototype.__host = "127.0.0.1";
Table.prototype.__user = "root";
Table.prototype.__password = "";
Table.prototype.__dbname = "test";
Table.prototype.__charset = "utf8";

// 创建实体对象
var userTable = new Table("table_user", "u");
var blogTable = new Table("table_blog", "b");

// 开启调试模式
userTable.debug = true;
blogTable.debug = true;

// sql查询
var sql = "select * from table_user where id > ? and id < ?";
console.dir(userTable.query(sql, 10, 20));
console.dir(userTable.vquery(sql, [10, 20]));

// 插入数据
for (i=1; i<=10; i++) {
	var user = {
			"username": "admini",
			"password": "admini",
			"nickname": "管理员i",
			"r": Math.random() * 4 | 0,
	};
	var result = userTable.insert(user);
	console.log(result.affected, result.insertId);
}

// 查询数据
console.dir(userTable.select().toArray()); // 获取所有数据
console.dir(userTable.select().toArray().shift()); // 获取一行数据
console.dir(userTable.select().toArray()[0][0]); // 获取第一行第一列数据
console.dir(userTable.select().toArray()[0][1]); // 获取第一行第二列数据

// 批量插入数据
var fields = ["username","password","nickname","r"];
var rows = [];
for (var i=11; i<=100; i++) {
	rows.push(["admin"+i,"admin"+i,"管理员"+i,Math.random() * 4 | 0]);
}
userTable.batchInsert(fields, rows);
// 获取数据
console.dir(userTable.select().toArray());

// 修改数据
var user = {
		"username": "admin4-1",
		"password": "admin4-1",
		"nickname": "管理员4-1",
		"r": Math.random() * 4 | 0,
};
console.log(userTable.where("id = ?", 4).update(user).affected);
// 根据主键查询数据
console.dir(userTable.find(4));

// replace数据
var user = {
		"id": 4,
		"username": "admin4",
		"password": "admin4",
		"nickname": "管理员4",
		"r": Math.random() * 4 | 0,
};
console.log(userTable.replace(user).affected);
// 根据主键查询数据
console.dir(userTable.find(4));

// 删除数据
console.log(userTable.where("id = ?", 4).delete().affected);
// 根据主键查询数据
console.dir(userTable.find(4));

// 多where条件
console.dir(userTable.where("id > ?", 4).where("id in (?)", [5,7,9]).select().toArray());

// 分组 过滤
console.dir(userTable.group("r").having("c between ? and ?", 1, 40).having("c > ?", 1).select("*, r, count(*) as c").toArray());

// 排序
console.dir(userTable.order("username, id desc").select().toArray());

// 限制行数
console.dir(userTable.limit(3).offset(3).select().toArray());

// 分页
console.dir(userTable.page(3, 3).select().toArray());

// 条件 分页 总行数
console.dir(userTable.calcFoundRows().where("r=?", 3).order("id desc").page(2, 3).select().toArray());
console.log(userTable.count());

// 复杂查询
console.dir(userTable.where("id > ?", 0).where("id < ?", 100)
	.group("r").having("c between ? and ?", 1, 100).having("c > ?", 1)
	.order("c desc").page(2, 3).select("*, count(*) as c").toArray());

// 联合查询
console.dir(blogTable.join("table_user AS u", "b.user_id = u.id").where("b.id < ?", 20).select("b.*, u.username").toArray());

// 列加减
var id = 2;
console.dir(userTable.find(id));
// 加一
console.dir(userTable.where("id = ?", id).plus("r").find(id));
// 减一
console.dir(userTable.where("id = ?", id).plus("r", -1).find(id));
// 多列
console.dir(userTable.where("id = ?", id).plus("r", 1, "r", -1).find(id));

// 列加减 并获得修改后的值
var id = 2;
console.dir(userTable.find(id));
// 加一
console.log(userTable.where("id = ?", id).incr("r"));
console.dir(userTable.find(id));
// 减一
console.log(userTable.where("id = ?", id).incr("r", -1));
console.dir(userTable.find(id));

// 保存 修改
var id = 3;
var user = {
	"id": id,
	"nickname": "管理员3-33",
};
console.log(userTable.save(user).affected);
console.dir(userTable.find(id));

// 保存 添加
user = {
		"username": "admin11",
		"password": "admin11",
		"nickname": "管理员11",
		"r": Math.random() * 4 | 0,
};
var result = userTable.save(user);
console.log(result.affected);
id = result.insertId;
console.log(id);
console.dir(userTable.find(id));

// 生成外键测试数据
var users = userTable.select("id").toArray();
var id = 0;
for (var i in users) {
	var user = users[i];
	for (var j=0; j<10; j++) {
		id++;
		var blog = {
				"user_id": user.id,
				"title": "blog" + id,
		};
		blogTable.insert(blog);
	}
}

// 外键 测试
var blogs = blogTable.where("id in (?)", [1,12,23,34,56,67,78,89,90,101]).select().toArray();
console.dir(blogs);
console.dir(userTable.foreignKey(blogs, "user_id", "*,id").toArray()); // 获取数据
