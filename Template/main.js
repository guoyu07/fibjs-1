var fs = require('fs');
var template = require('template');

var data = {
	header: {
		title: 'page title'
	},
	footer: {
		copyright: 'page end'
	},
	books: [
		{id:1, name:"java", price:45, cover:"java.png"},
		{id:2, name:".net", price:40, cover:"net.png"},
		{id:3, name:"php", price:60, cover:"php.png"}
	]
};

var tpl = new template.Template("./templates", "./templates");
tpl.builds();

console.log(tpl.render("view", data));
